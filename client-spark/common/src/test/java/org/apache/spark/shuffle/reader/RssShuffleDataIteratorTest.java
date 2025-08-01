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

package org.apache.spark.shuffle.reader;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.RssSparkConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.impl.ShuffleReadClientImpl;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.ChecksumUtils;
import org.apache.uniffle.storage.handler.impl.HadoopShuffleWriteHandler;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RssShuffleDataIteratorTest extends AbstractRssReaderTest {

  private static final Serializer KRYO_SERIALIZER = new KryoSerializer(new SparkConf(false));
  private static final String EXPECTED_EXCEPTION_MESSAGE = "Exception should be thrown";

  private ShuffleServerInfo ssi1 = new ShuffleServerInfo("host1-0", "host1", 0);
  private ShuffleServerInfo ssi2 = new ShuffleServerInfo("host2-0", "host2", 0);

  public static Stream<Arguments> testBlockIdLayouts() {
    return Stream.of(
        Arguments.of(BlockIdLayout.DEFAULT), Arguments.of(BlockIdLayout.from(20, 21, 22)));
  }

  @ParameterizedTest
  @MethodSource("testBlockIdLayouts")
  public void readTest1(BlockIdLayout layout) throws Exception {
    String basePath = HDFS_URI + "readTest1";
    HadoopShuffleWriteHandler writeHandler =
        new HadoopShuffleWriteHandler("appId", 0, 0, 1, basePath, ssi1.getId(), conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(
        writeHandler, 2, 5, layout, expectedData, blockIdBitmap, "key", KRYO_SERIALIZER, 0);

    RssShuffleDataIterator rssShuffleDataIterator =
        getDataIterator(basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(ssi1));

    validateResult(rssShuffleDataIterator, expectedData, 10);

    blockIdBitmap.add(layout.getBlockId(layout.maxSequenceNo, 0, 0));
    rssShuffleDataIterator =
        getDataIterator(basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(ssi1));
    int recNum = 0;
    try {
      // can't find all expected block id, data loss
      while (rssShuffleDataIterator.hasNext()) {
        rssShuffleDataIterator.next();
        recNum++;
      }
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Blocks read inconsistent:"));
    }
    assertEquals(10, recNum);
  }

  private RssShuffleDataIterator getDataIterator(
      String basePath,
      Roaring64NavigableMap blockIdBitmap,
      Roaring64NavigableMap taskIdBitmap,
      List<ShuffleServerInfo> serverInfos) {
    return getDataIterator(basePath, blockIdBitmap, taskIdBitmap, serverInfos, true);
  }

  private RssShuffleDataIterator getDataIterator(
      String basePath,
      Roaring64NavigableMap blockIdBitmap,
      Roaring64NavigableMap taskIdBitmap,
      List<ShuffleServerInfo> serverInfos,
      boolean compress) {
    ShuffleReadClientImpl readClient =
        ShuffleClientFactory.newReadBuilder()
            .clientType(ClientType.GRPC)
            .storageType(StorageType.HDFS.name())
            .appId("appId")
            .shuffleId(0)
            .partitionId(1)
            .indexReadLimit(100)
            .partitionNumPerRange(2)
            .partitionNum(10)
            .readBufferSize(10000)
            .basePath(basePath)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(Lists.newArrayList(serverInfos))
            .build();
    RssConf rc;
    if (!compress) {
      SparkConf sc = new SparkConf();
      sc.set(RssSparkConfig.SPARK_SHUFFLE_COMPRESS_KEY, String.valueOf(false));
      rc = RssSparkConfig.toRssConf(sc);
    } else {
      rc = new RssConf();
    }
    return new RssShuffleDataIterator(KRYO_SERIALIZER, readClient, new ShuffleReadMetrics(), rc);
  }

  @Test
  public void readTest2() throws Exception {
    readTestCompressOrNot("readTest2", true);
  }

  @Test
  public void readTest3() throws Exception {
    String basePath = HDFS_URI + "readTest3";
    HadoopShuffleWriteHandler writeHandler1 =
        new HadoopShuffleWriteHandler("appId", 0, 0, 1, basePath, ssi1.getId(), conf);
    HadoopShuffleWriteHandler writeHandler2 =
        new HadoopShuffleWriteHandler("appId", 0, 0, 1, basePath, ssi2.getId(), conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    final Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler1, 2, 5, expectedData, blockIdBitmap, "key1", KRYO_SERIALIZER, 0);
    writeTestData(writeHandler2, 2, 5, expectedData, blockIdBitmap, "key2", KRYO_SERIALIZER, 0);

    // duplicate file created, it should be used in product environment
    String shuffleFolder = basePath + "/appId/0/0-1";
    String ssi1Prefix = shuffleFolder + "/" + ssi1.getId();
    String ssi2Prefix = shuffleFolder + "/" + ssi2.getId();
    FileUtil.copy(
        fs, new Path(ssi1Prefix + "_0.data"), fs, new Path(ssi1Prefix + "_0.cp.data"), false, conf);
    FileUtil.copy(
        fs,
        new Path(ssi1Prefix + "_0.index"),
        fs,
        new Path(ssi1Prefix + "_0.cp.index"),
        false,
        conf);
    FileUtil.copy(
        fs, new Path(ssi2Prefix + "_0.data"), fs, new Path(ssi2Prefix + "_0.cp.data"), false, conf);
    FileUtil.copy(
        fs,
        new Path(ssi2Prefix + "_0.index"),
        fs,
        new Path(ssi2Prefix + "_0.cp.index"),
        false,
        conf);

    RssShuffleDataIterator rssShuffleDataIterator =
        getDataIterator(basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(ssi1, ssi2));

    validateResult(rssShuffleDataIterator, expectedData, 20);
  }

  @Test
  public void readTest4() throws Exception {
    String basePath = HDFS_URI + "readTest4";
    HadoopShuffleWriteHandler writeHandler =
        new HadoopShuffleWriteHandler("appId", 0, 0, 1, basePath, ssi1.getId(), conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 2, 5, expectedData, blockIdBitmap, "key", KRYO_SERIALIZER, 0);

    RssShuffleDataIterator rssShuffleDataIterator =
        getDataIterator(basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(ssi1));
    // data file is deleted after iterator initialization
    Path dataFile = new Path(basePath + "/appId/0/0-1/" + ssi1.getId() + "_0.data");
    fs.delete(dataFile, true);
    try {
      fs.listStatus(dataFile);
      fail("Index file should be deleted");
    } catch (Exception e) {
      // ignore
    }

    try {
      while (rssShuffleDataIterator.hasNext()) {
        rssShuffleDataIterator.next();
      }
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      // the underlying hdfs files have been deleted, so that the reader will initialize failed
      assertTrue(e.getMessage().contains("don't exist or is not a file"));
    }
  }

  @Test
  public void readTest5() throws Exception {
    String basePath = HDFS_URI + "readTest5";
    HadoopShuffleWriteHandler writeHandler =
        new HadoopShuffleWriteHandler("appId", 0, 0, 1, basePath, ssi1.getId(), conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 2, 5, expectedData, blockIdBitmap, "key", KRYO_SERIALIZER, 0);

    final RssShuffleDataIterator rssShuffleDataIterator =
        getDataIterator(basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(ssi1));
    // index file is deleted after iterator initialization, it should be ok, all index infos are
    // read already
    Path indexFile = new Path(basePath + "/appId/0/0-1/" + ssi1.getId() + ".index");
    fs.delete(indexFile, true);
    try {
      fs.listStatus(indexFile);
      fail("Index file should be deleted");
    } catch (Exception e) {
      // ignore
    }
    validateResult(rssShuffleDataIterator, expectedData, 10);
  }

  @Test
  public void readTest7() throws Exception {
    String basePath = HDFS_URI + "readTest7";
    HadoopShuffleWriteHandler writeHandler =
        new HadoopShuffleWriteHandler("appId", 0, 0, 1, basePath, ssi1.getId(), conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 2, 5, expectedData, blockIdBitmap, "key", KRYO_SERIALIZER, 0);

    RssShuffleDataIterator rssShuffleDataIterator =
        getDataIterator(basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(ssi1));
    RssShuffleDataIterator rssShuffleDataIterator2 =
        getDataIterator(basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(ssi1, ssi2));
    // crc32 is incorrect
    try (MockedStatic<ChecksumUtils> checksumUtilsMock = Mockito.mockStatic(ChecksumUtils.class)) {
      checksumUtilsMock.when(() -> ChecksumUtils.getCrc32((ByteBuffer) any())).thenReturn(-1L);

      try {
        while (rssShuffleDataIterator.hasNext()) {
          rssShuffleDataIterator.next();
        }
        fail(EXPECTED_EXCEPTION_MESSAGE);
      } catch (Exception e) {
        assertTrue(
            e.getMessage()
                .startsWith("Unexpected crc value for blockId[0 (seq: 0, part: 0, task: 0)]"));
      }

      try {
        while (rssShuffleDataIterator2.hasNext()) {
          rssShuffleDataIterator2.next();
        }
        fail(EXPECTED_EXCEPTION_MESSAGE);
      } catch (Exception e) {
        assertTrue(e.getMessage().startsWith("Blocks read inconsistent"));
      }
    }
  }

  @Test
  public void readTestUncompressedShuffle() throws Exception {
    readTestCompressOrNot("readTestUncompressedShuffle", false);
  }

  private void readTestCompressOrNot(String path, boolean compress) throws Exception {
    String basePath = HDFS_URI + path;
    HadoopShuffleWriteHandler writeHandler1 =
        new HadoopShuffleWriteHandler("appId", 0, 0, 1, basePath, ssi1.getId(), conf);
    HadoopShuffleWriteHandler writeHandler2 =
        new HadoopShuffleWriteHandler("appId", 0, 0, 1, basePath, ssi2.getId(), conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(
        writeHandler1, 2, 5, expectedData, blockIdBitmap, "key1", KRYO_SERIALIZER, 0, compress);
    writeTestData(
        writeHandler2, 2, 5, expectedData, blockIdBitmap, "key2", KRYO_SERIALIZER, 0, compress);

    RssShuffleDataIterator rssShuffleDataIterator =
        getDataIterator(
            basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(ssi1, ssi2), compress);
    Optional<Codec> codec =
        (Optional<Codec>) FieldUtils.readField(rssShuffleDataIterator, "codec", true);
    if (compress) {
      Assertions.assertTrue(codec.isPresent());
    } else {
      Assertions.assertFalse(codec.isPresent());
    }

    validateResult(rssShuffleDataIterator, expectedData, 20);
    assertEquals(20, rssShuffleDataIterator.getShuffleReadMetrics().recordsRead());
    assertTrue(rssShuffleDataIterator.getShuffleReadMetrics().fetchWaitTime() > 0);
  }

  @Test
  public void cleanup() throws Exception {
    ShuffleReadClient mockClient = mock(ShuffleReadClient.class);
    doNothing().when(mockClient).close();
    RssShuffleDataIterator dataIterator =
        new RssShuffleDataIterator(
            KRYO_SERIALIZER, mockClient, new ShuffleReadMetrics(), new RssConf());
    dataIterator.cleanup();
    verify(mockClient, times(1)).close();
  }
}
