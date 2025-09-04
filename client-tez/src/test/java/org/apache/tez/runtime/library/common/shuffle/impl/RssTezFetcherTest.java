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

package org.apache.tez.runtime.library.common.shuffle.impl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.readers.UnorderedKVReader;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetcherCallback;
import org.apache.tez.runtime.library.common.shuffle.MemoryFetchedInput;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.InMemoryReader;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.InMemoryWriter;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.utils.BufferUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.response.CompressedShuffleBlock;
import org.apache.uniffle.common.ShuffleReadTimes;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.compression.Lz4Codec;
import org.apache.uniffle.common.config.RssConf;

import static org.apache.tez.common.RssTezConfig.RSS_REDUCE_REMOTE_SPILL_ENABLED;
import static org.apache.tez.common.RssTezConfig.RSS_REMOTE_SPILL_STORAGE_PATH;
import static org.apache.tez.common.TezRuntimeFrameworkConfigs.LOCAL_DIRS;
import static org.apache.tez.runtime.library.api.TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS;
import static org.apache.tez.runtime.library.api.TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT;
import static org.apache.tez.runtime.library.api.TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class RssTezFetcherTest {
  private static final Logger LOG = LoggerFactory.getLogger(RssTezFetcherTest.class);

  static Configuration conf = new Configuration();

  static FileSystem fs;
  static List<byte[]> data;
  static List<KVPair> textData;
  static Codec codec = new Lz4Codec();

  private static FileSystem remoteFS;
  private static MiniDFSCluster cluster;

  @BeforeAll
  public static void setUpHdfs(@TempDir File tempDir) throws Exception {
    Configuration conf = new Configuration();
    File baseDir = tempDir;
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    cluster = (new MiniDFSCluster.Builder(conf)).build();
    String hdfsUri = cluster.getURI().toString() + "/";
    remoteFS = (new Path(hdfsUri)).getFileSystem(conf);
  }

  @AfterAll
  public static void tearDownHdfs() throws Exception {
    remoteFS.close();
    cluster.shutdown();
  }

  @Test
  public void writeAndReadDataTestWithoutRss() throws Throwable {
    fs = FileSystem.getLocal(conf);
    initRssData();
    ShuffleReadClient shuffleReadClient = new MockedShuffleReadClient(data);

    SimpleFetchedInputAllocator inputManager =
        new SimpleFetchedInputAllocator(
            TezUtilsInternal.cleanVertexName("Map 1"),
            "1",
            2,
            conf,
            200 * 1024 * 1024L,
            300 * 1024 * 1024L);

    List<byte[]> result = new ArrayList<byte[]>();
    FetcherCallback fetcherCallback =
        new FetcherCallback() {
          @Override
          public void fetchSucceeded(
              String host,
              InputAttemptIdentifier srcAttemptIdentifier,
              FetchedInput fetchedInput,
              long fetchedBytes,
              long decompressedLength,
              long copyDuration)
              throws IOException {
            LOG.info("Fetch success");
            fetchedInput.commit();
            result.add(((MemoryFetchedInput) fetchedInput).getBytes());
          }

          @Override
          public void fetchFailed(
              String host, InputAttemptIdentifier srcAttemptIdentifier, boolean connectFailed) {
            fail();
          }
        };

    RssTezFetcher rssFetcher =
        new RssTezFetcher(fetcherCallback, inputManager, shuffleReadClient, null, 2, new RssConf());
    rssFetcher.fetchAllRssBlocks();

    for (int i = 0; i < data.size(); i++) {
      Text readKey = new Text();
      IntWritable readValue = new IntWritable();
      Deserializer<Text> keyDeserializer;
      Deserializer<IntWritable> valDeserializer;
      SerializationFactory serializationFactory = new SerializationFactory(conf);
      keyDeserializer = serializationFactory.getDeserializer(Text.class);
      valDeserializer = serializationFactory.getDeserializer(IntWritable.class);
      DataInputBuffer keyIn = new DataInputBuffer();
      DataInputBuffer valIn = new DataInputBuffer();
      keyDeserializer.open(keyIn);
      valDeserializer.open(valIn);

      InMemoryReader reader =
          new InMemoryReader(
              null, new InputAttemptIdentifier(0, 0), result.get(i), 0, result.get(i).length);
      int numRecordsRead = 0;
      while (reader.nextRawKey(keyIn)) {
        reader.nextRawValue(valIn);
        readKey = keyDeserializer.deserialize(readKey);
        readValue = valDeserializer.deserialize(readValue);

        KVPair expected = textData.get(numRecordsRead);
        assertEquals(expected.getKey(), readKey);
        assertEquals(expected.getvalue(), readValue);

        numRecordsRead++;
      }
      assertEquals(textData.size(), numRecordsRead);
      LOG.info("Found: " + numRecordsRead + " records");
    }
  }

  @Test
  public void testReadWithDiskFetchedInput(@TempDir File tmpDir) throws Throwable {
    initRssData();
    conf.setFloat(TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, 0.25F);
    conf.set(LOCAL_DIRS, tmpDir + "/local");
    conf.set(TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TEZ_RUNTIME_VALUE_CLASS, IntWritable.class.getName());
    RssSimpleFetchedInputAllocator inputManager =
        new RssSimpleFetchedInputAllocator(
            "test", "uniqueId1", 0, conf, 2 * 1024 * 1024, 2 * 1024 * 1024, "appattemptid1");
    List<FetchedInput> inputs = new ArrayList<>();
    FetcherCallback fetcherCallback =
        new FetcherCallback() {
          @Override
          public void fetchSucceeded(
              String host,
              InputAttemptIdentifier srcAttemptIdentifier,
              FetchedInput fetchedInput,
              long fetchedBytes,
              long decompressedLength,
              long copyDuration)
              throws IOException {
            LOG.info("Fetch success");
            fetchedInput.commit();
            inputs.add(fetchedInput);
          }

          @Override
          public void fetchFailed(
              String host, InputAttemptIdentifier srcAttemptIdentifier, boolean connectFailed) {
            fail();
          }
        };

    ShuffleReadClient shuffleReadClient = new MockedShuffleReadClient(data);
    RssTezFetcher rssFetcher =
        new RssTezFetcher(fetcherCallback, inputManager, shuffleReadClient, null, 2, new RssConf());
    rssFetcher.fetchAllRssBlocks();
    for (int i = 0; i < data.size(); i++) {
      Text readKey = new Text();
      IntWritable readValue = new IntWritable();
      Deserializer<Text> keyDeserializer;
      Deserializer<IntWritable> valDeserializer;
      SerializationFactory serializationFactory = new SerializationFactory(conf);
      keyDeserializer = serializationFactory.getDeserializer(Text.class);
      valDeserializer = serializationFactory.getDeserializer(IntWritable.class);
      DataInputBuffer keyIn = new DataInputBuffer();
      DataInputBuffer valIn = new DataInputBuffer();
      keyDeserializer.open(keyIn);
      valDeserializer.open(valIn);

      UnorderedKVReader unorderedKVReader =
          new UnorderedKVReader(null, conf, null, false, 0, 0, null, null);
      IFile.Reader reader = unorderedKVReader.openIFileReader(inputs.get(i));
      int numRecordsRead = 0;
      while (reader.nextRawKey(keyIn)) {
        reader.nextRawValue(valIn);
        readKey = keyDeserializer.deserialize(readKey);
        readValue = valDeserializer.deserialize(readValue);

        KVPair expected = textData.get(numRecordsRead);
        assertEquals(expected.getKey(), readKey);
        assertEquals(expected.getvalue(), readValue);

        numRecordsRead++;
      }
      assertEquals(textData.size(), numRecordsRead);
    }
  }

  @Test
  public void testReadWithRemoteFetchedInput(@TempDir File tmpDir) throws Throwable {
    initRssData();
    conf.setFloat(TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, 0.25F);
    conf.set(LOCAL_DIRS, tmpDir + "/local");
    conf.set(TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TEZ_RUNTIME_VALUE_CLASS, IntWritable.class.getName());
    conf.setBoolean(RSS_REDUCE_REMOTE_SPILL_ENABLED, true);
    conf.set(RSS_REMOTE_SPILL_STORAGE_PATH, "/tmp/spill");
    RssSimpleFetchedInputAllocator inputManager =
        new RssSimpleFetchedInputAllocator(
            "test", "uniqueId1", 0, conf, 2 * 1024 * 1024, 2 * 1024 * 1024, "appattemptid1");
    inputManager.setRemoteFS(remoteFS);
    List<FetchedInput> inputs = new ArrayList<>();
    FetcherCallback fetcherCallback =
        new FetcherCallback() {
          @Override
          public void fetchSucceeded(
              String host,
              InputAttemptIdentifier srcAttemptIdentifier,
              FetchedInput fetchedInput,
              long fetchedBytes,
              long decompressedLength,
              long copyDuration)
              throws IOException {
            LOG.info("Fetch success");
            fetchedInput.commit();
            inputs.add(fetchedInput);
          }

          @Override
          public void fetchFailed(
              String host, InputAttemptIdentifier srcAttemptIdentifier, boolean connectFailed) {
            fail();
          }
        };

    ShuffleReadClient shuffleReadClient = new MockedShuffleReadClient(data);
    RssTezFetcher rssFetcher =
        new RssTezFetcher(fetcherCallback, inputManager, shuffleReadClient, null, 2, new RssConf());
    rssFetcher.fetchAllRssBlocks();
    for (int i = 0; i < data.size(); i++) {
      Text readKey = new Text();
      IntWritable readValue = new IntWritable();
      Deserializer<Text> keyDeserializer;
      Deserializer<IntWritable> valDeserializer;
      SerializationFactory serializationFactory = new SerializationFactory(conf);
      keyDeserializer = serializationFactory.getDeserializer(Text.class);
      valDeserializer = serializationFactory.getDeserializer(IntWritable.class);
      DataInputBuffer keyIn = new DataInputBuffer();
      DataInputBuffer valIn = new DataInputBuffer();
      keyDeserializer.open(keyIn);
      valDeserializer.open(valIn);

      UnorderedKVReader unorderedKVReader =
          new UnorderedKVReader(null, conf, null, false, 0, 0, null, null);
      IFile.Reader reader = unorderedKVReader.openIFileReader(inputs.get(i));
      int numRecordsRead = 0;
      while (reader.nextRawKey(keyIn)) {
        reader.nextRawValue(valIn);
        readKey = keyDeserializer.deserialize(readKey);
        readValue = valDeserializer.deserialize(readValue);

        KVPair expected = textData.get(numRecordsRead);
        assertEquals(expected.getKey(), readKey);
        assertEquals(expected.getvalue(), readValue);

        numRecordsRead++;
      }
      assertEquals(textData.size(), numRecordsRead);
    }
  }

  private static void initRssData() throws Exception {
    InMemoryWriter writer = null;
    BoundedByteArrayOutputStream bout = new BoundedByteArrayOutputStream(1024 * 1024);
    List<KVPair> pairData = generateTestData(true, 10);
    // No RLE, No RepeatKeys, no compression
    writer = new InMemoryWriter(bout);
    writeTestFileUsingDataBuffer(writer, false, pairData);
    data = new ArrayList<>();
    data.add(bout.getBuffer());
    textData = new ArrayList<>();
    textData.addAll(pairData);
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

  /**
   * Generate key value pair
   *
   * @param sorted whether data should be sorted by key
   * @param repeatCount number of keys to be repeated
   * @return
   */
  public static List<KVPair> generateTestData(boolean sorted, int repeatCount) {
    List<KVPair> data = new LinkedList<KVPair>();
    Random rnd = new Random();
    KVPair kvp = null;
    for (int i = 0; i < 5; i++) {
      String keyStr = (sorted) ? ("key" + i) : (rnd.nextLong() + "key" + i);
      Text key = new Text(keyStr);
      IntWritable value = new IntWritable(i + repeatCount);
      kvp = new KVPair(key, value);
      data.add(kvp);
      if ((repeatCount > 0) && (i % 2 == 0)) { // Repeat this key for random number of times
        int count = rnd.nextInt(5);
        for (int j = 0; j < count; j++) {
          repeatCount++;
          value.set(i + rnd.nextInt());
          kvp = new KVPair(key, value);
          data.add(kvp);
        }
      }
    }
    // If we need to generated repeated keys, try to add some repeated keys to the end of file also.
    if (repeatCount > 0 && kvp != null) {
      data.add(kvp);
      data.add(kvp);
    }
    return data;
  }

  private static IFile.Writer writeTestFileUsingDataBuffer(
      IFile.Writer writer, boolean repeatKeys, List<KVPair> data) throws IOException {
    DataInputBuffer previousKey = new DataInputBuffer();
    DataInputBuffer key = new DataInputBuffer();
    DataInputBuffer value = new DataInputBuffer();
    for (KVPair kvp : data) {
      populateData(kvp, key, value);

      if (repeatKeys && (previousKey != null && BufferUtils.compare(key, previousKey) == 0)) {
        writer.append(org.apache.tez.runtime.library.common.sort.impl.IFile.REPEAT_KEY, value);
      } else {
        writer.append(key, value);
      }
      previousKey.reset(key.getData(), 0, key.getLength());
    }
    writer.close();
    LOG.info("Uncompressed: " + writer.getRawLength());
    LOG.info("CompressedSize: " + writer.getCompressedLength());
    return writer;
  }

  private static void populateData(KVPair kvp, DataInputBuffer key, DataInputBuffer value)
      throws IOException {
    DataOutputBuffer k = new DataOutputBuffer();
    DataOutputBuffer v = new DataOutputBuffer();
    kvp.getKey().write(k);
    kvp.getvalue().write(v);
    key.reset(k.getData(), 0, k.getLength());
    value.reset(v.getData(), 0, v.getLength());
  }

  public static class KVPair {
    private Text key;
    private IntWritable value;

    public KVPair(Text key, IntWritable value) {
      this.key = key;
      this.value = value;
    }

    public Text getKey() {
      return this.key;
    }

    public IntWritable getvalue() {
      return this.value;
    }
  }
}
