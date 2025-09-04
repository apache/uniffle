/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.tez.common.RssTezConfig;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.counters.GenericCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.ValuesIterator;
import org.apache.tez.runtime.library.common.combine.Combiner;
import org.apache.tez.runtime.library.common.comparator.TezBytesComparator;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.collections.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.response.CompressedShuffleBlock;
import org.apache.uniffle.common.ShuffleReadTimes;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.compression.Lz4Codec;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class RssTezShuffleDataFetcherTest {
  private static final Logger LOG = LoggerFactory.getLogger(RssTezShuffleDataFetcherTest.class);

  enum TestWithComparator {
    LONG,
    INT,
    BYTES,
    TEZ_BYTES,
    TEXT
  }

  Configuration conf;
  FileSystem fs;

  final Class keyClass;
  final Class valClass;
  final RawComparator comparator;
  final boolean expectedTestResult;

  int mergeFactor;
  // For storing original data
  final ListMultimap<Writable, Writable> originalData;

  TezRawKeyValueIterator rawKeyValueIterator;

  Path baseDir;
  Path tmpDir;
  static List<byte[]> bytesData = new ArrayList<>();
  static Codec codec = new Lz4Codec();

  public RssTezShuffleDataFetcherTest() throws IOException, ClassNotFoundException {
    this.keyClass = Class.forName("org.apache.hadoop.io.Text");
    this.valClass = Class.forName("org.apache.hadoop.io.Text");
    this.comparator = getComparator(TestWithComparator.TEXT);
    this.expectedTestResult = true;
    originalData = LinkedListMultimap.create();
    setupConf();
  }

  private void setupConf() throws IOException, ClassNotFoundException {
    mergeFactor = 2;
    conf = new Configuration();
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR, mergeFactor);
    conf.setClass(
        TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS,
        Class.forName("org.apache.tez.runtime.library.common.comparator.TezBytesComparator"),
        Class.forName("org.apache.hadoop.io.WritableComparator"));
    baseDir = new Path(".", this.getClass().getName());
    String localDirs = baseDir.toString();
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, localDirs);
    fs = FileSystem.getLocal(conf);
  }

  @Test
  public void testIteratorWithInMemoryReader() throws Throwable {
    fs.mkdirs(baseDir);
    tmpDir = new Path(baseDir, "tmp");

    ValuesIterator iterator = createIterator();
    verifyIteratorData(iterator);

    fs.delete(baseDir, true);
    originalData.clear();
  }

  private void getNextFromFinishedIterator(ValuesIterator iterator) {
    try {
      boolean hasNext = iterator.moveToNext();
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Please check if you are invoking moveToNext()"));
    }
  }

  /**
   * Tests whether data in valuesIterator matches with sorted input data set.
   *
   * <p>Returns a list of value counts for each key.
   *
   * @param valuesIterator
   * @return List
   * @throws IOException
   */
  private void verifyIteratorData(ValuesIterator valuesIterator) throws IOException {
    boolean result = true;

    // sort original data based on comparator
    ListMultimap<Writable, Writable> sortedMap =
        new ImmutableListMultimap.Builder<Writable, Writable>()
            .orderKeysBy(this.comparator)
            .putAll(originalData)
            .build();

    Set<Map.Entry<Writable, Writable>> oriKeySet = Sets.newSet();
    oriKeySet.addAll(sortedMap.entries());

    // Iterate through sorted data and valuesIterator for verification
    for (Map.Entry<Writable, Writable> entry : oriKeySet) {
      assertTrue(valuesIterator.moveToNext());
      Writable oriKey = entry.getKey();
      // Verify if the key and the original key are same
      if (!oriKey.equals((Writable) valuesIterator.getKey())) {
        result = false;
        break;
      }

      int valueCount = 0;
      // Verify values
      Iterator<Writable> vItr = valuesIterator.getValues().iterator();
      for (Writable val : sortedMap.get(oriKey)) {
        assertTrue(vItr.hasNext());
        // Verify if the values are same
        if (!val.equals((Writable) vItr.next())) {
          result = false;
          break;
        }
        valueCount++;
      }
      assertTrue(valueCount > 0);
    }
    assertTrue(result);
    assertFalse(valuesIterator.moveToNext());
    getNextFromFinishedIterator(valuesIterator);
  }

  /**
   * Create sample data (in memory / disk based), merge them and return ValuesIterator
   *
   * @return ValuesIterator
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  private ValuesIterator createIterator() throws Throwable {
    createInMemStreams();

    ShuffleReadClient shuffleReadClient = new MockedShuffleReadClient(bytesData);

    FileSystem localFS = FileSystem.getLocal(this.conf);
    LocalDirAllocator localDirAllocator =
        new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);

    InputContext inputContext = createTezInputContext();

    Combiner combiner = TezRuntimeUtils.instantiateCombiner(conf, inputContext);

    MergeManager mergeManager =
        new MergeManager(
            this.conf,
            localFS,
            localDirAllocator,
            inputContext,
            combiner,
            null,
            null,
            null,
            null,
            1024 * 1024 * 256,
            null,
            false,
            0);

    RssTezShuffleDataFetcher fetcher =
        new RssTezShuffleDataFetcher(
            new InputAttemptIdentifier(1, 0),
            9,
            mergeManager,
            new TezCounters(),
            shuffleReadClient,
            3,
            RssTezConfig.toRssConf(conf),
            null);

    fetcher.fetchAllRssBlocks();

    rawKeyValueIterator = mergeManager.close(true);

    return new ValuesIterator(
        rawKeyValueIterator,
        comparator,
        keyClass,
        valClass,
        conf,
        (TezCounter) new GenericCounter("inputKeyCounter", "y3"),
        (TezCounter) new GenericCounter("inputValueCounter", "y4"));
  }

  private RawComparator getComparator(TestWithComparator comparator) {
    switch (comparator) {
      case LONG:
        return new LongWritable.Comparator();
      case INT:
        return new IntWritable.Comparator();
      case BYTES:
        return new BytesWritable.Comparator();
      case TEZ_BYTES:
        return new TezBytesComparator();
      case TEXT:
        return new Text.Comparator();
      default:
        return null;
    }
  }

  /**
   * create byte array test data
   *
   * @return
   * @throws IOException
   */
  public void createInMemStreams() throws IOException {
    int numberOfStreams = 5;
    LOG.info("No of streams : " + numberOfStreams);

    SerializationFactory serializationFactory = new SerializationFactory(conf);
    Serializer keySerializer = serializationFactory.getSerializer(keyClass);
    Serializer valueSerializer = serializationFactory.getSerializer(valClass);

    LocalDirAllocator localDirAllocator =
        new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);
    InputContext context = createTezInputContext();
    MergeManager mergeManager =
        new MergeManager(
            conf,
            fs,
            localDirAllocator,
            context,
            null,
            null,
            null,
            null,
            null,
            1024 * 1024 * 10,
            null,
            false,
            -1);

    DataOutputBuffer keyBuf = new DataOutputBuffer();
    DataOutputBuffer valBuf = new DataOutputBuffer();
    DataInputBuffer keyIn = new DataInputBuffer();
    DataInputBuffer valIn = new DataInputBuffer();
    keySerializer.open(keyBuf);
    valueSerializer.open(valBuf);

    for (int i = 0; i < numberOfStreams; i++) {
      BoundedByteArrayOutputStream bout = new BoundedByteArrayOutputStream(1024 * 1024 * 10);
      InMemoryWriter writer = new InMemoryWriter(bout);
      Map<Writable, Writable> data = createData();
      // write data
      for (Map.Entry<Writable, Writable> entry : data.entrySet()) {
        keySerializer.serialize(entry.getKey());
        valueSerializer.serialize(entry.getValue());
        keyIn.reset(keyBuf.getData(), 0, keyBuf.getLength());
        valIn.reset(valBuf.getData(), 0, valBuf.getLength());
        writer.append(keyIn, valIn);

        originalData.put(entry.getKey(), entry.getValue());
        keyBuf.reset();
        valBuf.reset();
        keyIn.reset();
        valIn.reset();
      }
      data.clear();
      writer.close();
      bytesData.add(bout.getBuffer());
    }
  }

  private InputContext createTezInputContext() {
    TezCounters counters = new TezCounters();
    InputContext inputContext = mock(InputContext.class);
    doReturn(1024 * 1024 * 100L).when(inputContext).getTotalMemoryAvailableToTask();
    doReturn(counters).when(inputContext).getCounters();
    doReturn(1).when(inputContext).getInputIndex();
    doReturn("srcVertex").when(inputContext).getSourceVertexName();
    doReturn(1).when(inputContext).getTaskVertexIndex();
    doReturn(UserPayload.create(ByteBuffer.wrap(new byte[1024])))
        .when(inputContext)
        .getUserPayload();
    doReturn("test_input").when(inputContext).getUniqueIdentifier();
    return inputContext;
  }

  private Map<Writable, Writable> createData() {
    Map<Writable, Writable> map = new TreeMap<Writable, Writable>(comparator);
    for (int j = 0; j < 10; j++) {
      Writable key = new Text(String.valueOf(j));
      Writable value = new Text(String.valueOf(j));
      map.put(key, value);
    }
    return map;
  }

  static class MockedShuffleReadClient implements ShuffleReadClient {
    List<CompressedShuffleBlock> blocks;
    int index = 0;

    MockedShuffleReadClient(List<byte[]> data) {
      this.blocks = new LinkedList<>();
      data.forEach(
          bytes -> {
            byte[] compressed = codec.compress(bytes);
            blocks.add(new CompressedShuffleBlock(ByteBuffer.wrap(compressed), bytes.length));
          });
    }

    @Override
    public CompressedShuffleBlock readShuffleBlockData() {
      if (index < blocks.size()) {
        return blocks.get(index++);
      } else {
        return null;
      }
    }

    @Override
    public void checkProcessedBlockIds() {}

    @Override
    public void close() {}

    @Override
    public void logStatics() {}

    @Override
    public ShuffleReadTimes getShuffleReadTimes() {
      return new ShuffleReadTimes();
    }
  }
}
