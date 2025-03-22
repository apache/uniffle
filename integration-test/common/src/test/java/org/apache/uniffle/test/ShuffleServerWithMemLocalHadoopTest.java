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

package org.apache.uniffle.test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcNettyClient;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.ByteBufUtils;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.buffer.ShuffleBuffer;
import org.apache.uniffle.storage.handler.api.ClientReadHandler;
import org.apache.uniffle.storage.handler.impl.ComposedClientReadHandler;
import org.apache.uniffle.storage.handler.impl.HadoopClientReadHandler;
import org.apache.uniffle.storage.handler.impl.LocalFileClientReadHandler;
import org.apache.uniffle.storage.handler.impl.MemoryClientReadHandler;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ShuffleServerWithMemLocalHadoopTest extends ShuffleReadWriteBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(ShuffleServerWithMemLocalHadoopTest.class);
  private ShuffleServerGrpcClient grpcShuffleServerClient;
  private ShuffleServerGrpcNettyClient nettyShuffleServerClient;
  private static String REMOTE_STORAGE = HDFS_URI + "rss/test_%s";

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    storeCoordinatorConf(coordinatorConf);

    storeShuffleServerConf(buildShuffleServerConf(0, tmpDir, ServerType.GRPC));
    storeShuffleServerConf(buildShuffleServerConf(1, tmpDir, ServerType.GRPC_NETTY));

    startServersWithRandomPorts();
  }

  @BeforeEach
  public void createClient() throws Exception {
    grpcShuffleServerClient =
        new ShuffleServerGrpcClient(LOCALHOST, grpcShuffleServers.get(0).getGrpcPort());
    nettyShuffleServerClient =
        new ShuffleServerGrpcNettyClient(
            LOCALHOST,
            nettyShuffleServers.get(0).getGrpcPort(),
            nettyShuffleServers.get(0).getNettyPort());
  }

  private static ShuffleServerConf buildShuffleServerConf(
      int subDirIndex, File tempDir, ServerType serverType) {
    ShuffleServerConf shuffleServerConf =
        shuffleServerConfWithoutPort(subDirIndex, tempDir, serverType);
    shuffleServerConf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    shuffleServerConf.setLong(ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, 450L);
    shuffleServerConf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 5000L);
    shuffleServerConf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 5.0);
    shuffleServerConf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 15.0);
    shuffleServerConf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 2000L);
    shuffleServerConf.set(ShuffleServerConf.SERVER_TRIGGER_FLUSH_CHECK_INTERVAL, 500L);
    return shuffleServerConf;
  }

  @AfterEach
  public void closeClient() throws Exception {
    grpcShuffleServerClient.close();
    nettyShuffleServerClient.close();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    shutdownServers();
  }

  private static Stream<Arguments> memoryLocalFileHadoopReadWithFilterProvider() {
    return Stream.of(
        Arguments.of(true, false),
        Arguments.of(false, false),
        Arguments.of(true, true),
        Arguments.of(false, true));
  }

  @ParameterizedTest
  @MethodSource("memoryLocalFileHadoopReadWithFilterProvider")
  public void memoryLocalFileHadoopReadWithFilterTest(
      boolean checkSkippedMetrics, boolean isNettyMode) throws Exception {
    runTest(checkSkippedMetrics, isNettyMode);
  }

  private void runTest(boolean checkSkippedMetrics, boolean isNettyMode) throws Exception {
    LOG.info("checkSkippedMetrics={}, isNettyMode={}", checkSkippedMetrics, isNettyMode);
    ShuffleServerGrpcClient shuffleServerClient =
        isNettyMode ? nettyShuffleServerClient : grpcShuffleServerClient;
    String testAppId = "memoryLocalFileHDFSReadWithFilterTest_ship_" + checkSkippedMetrics;
    int shuffleId = 0;
    int partitionId = 0;
    RssRegisterShuffleRequest rrsr =
        new RssRegisterShuffleRequest(
            testAppId,
            0,
            Lists.newArrayList(new PartitionRange(0, 0)),
            String.format(REMOTE_STORAGE, isNettyMode));
    shuffleServerClient.registerShuffle(rrsr);
    Roaring64NavigableMap expectBlockIds = Roaring64NavigableMap.bitmapOf();
    Map<Long, byte[]> dataMap = Maps.newHashMap();
    Roaring64NavigableMap[] bitmaps = new Roaring64NavigableMap[1];
    bitmaps[0] = Roaring64NavigableMap.bitmapOf();
    List<ShuffleBlockInfo> blocks =
        createShuffleBlockList(shuffleId, partitionId, 0, 3, 25, expectBlockIds, dataMap, mockSSI);
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(partitionId, blocks);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(shuffleId, partitionToBlocks);

    // send data to shuffle server's memory
    RssSendShuffleDataRequest rssdr =
        new RssSendShuffleDataRequest(testAppId, 3, 1000, shuffleToBlocks);

    RssSendShuffleDataResponse response = shuffleServerClient.sendShuffleData(rssdr);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());

    Roaring64NavigableMap processBlockIds = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap exceptTaskIds = Roaring64NavigableMap.bitmapOf(0);
    // read the 1-th segment from memory
    MemoryClientReadHandler memoryClientReadHandler =
        new MemoryClientReadHandler(
            testAppId, shuffleId, partitionId, 150, shuffleServerClient, exceptTaskIds);
    LocalFileClientReadHandler localFileClientReadHandler =
        new LocalFileClientReadHandler(
            testAppId,
            shuffleId,
            partitionId,
            0,
            1,
            3,
            75,
            expectBlockIds,
            processBlockIds,
            shuffleServerClient);
    HadoopClientReadHandler hadoopClientReadHandler =
        new HadoopClientReadHandler(
            testAppId,
            shuffleId,
            partitionId,
            0,
            1,
            3,
            500,
            expectBlockIds,
            processBlockIds,
            String.format(REMOTE_STORAGE, isNettyMode),
            conf);
    ClientReadHandler[] handlers = new ClientReadHandler[3];
    handlers[0] = memoryClientReadHandler;
    handlers[1] = localFileClientReadHandler;
    handlers[2] = hadoopClientReadHandler;
    ShuffleServerInfo ssi =
        isNettyMode
            ? new ShuffleServerInfo(
                LOCALHOST,
                nettyShuffleServers.get(0).getGrpcPort(),
                nettyShuffleServers.get(0).getNettyPort())
            : new ShuffleServerInfo(LOCALHOST, grpcShuffleServers.get(0).getGrpcPort());
    ComposedClientReadHandler composedClientReadHandler =
        new ComposedClientReadHandler(ssi, handlers);
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    expectedData.clear();
    expectedData.put(blocks.get(0).getBlockId(), ByteBufUtils.readBytes(blocks.get(0).getData()));
    expectedData.put(blocks.get(1).getBlockId(), ByteBufUtils.readBytes(blocks.get(1).getData()));
    expectedData.put(blocks.get(2).getBlockId(), ByteBufUtils.readBytes(blocks.get(2).getData()));
    ShuffleDataResult sdr = composedClientReadHandler.readShuffleData();
    validateResult(expectedData, sdr);
    processBlockIds.addLong(blocks.get(0).getBlockId());
    processBlockIds.addLong(blocks.get(1).getBlockId());
    processBlockIds.addLong(blocks.get(2).getBlockId());
    sdr.getBufferSegments()
        .forEach(bs -> composedClientReadHandler.updateConsumedBlockInfo(bs, checkSkippedMetrics));

    // send data to shuffle server, and wait until flush to LocalFile
    List<ShuffleBlockInfo> blocks2 =
        createShuffleBlockList(shuffleId, partitionId, 0, 3, 50, expectBlockIds, dataMap, mockSSI);
    partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(partitionId, blocks2);
    shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(shuffleId, partitionToBlocks);
    rssdr = new RssSendShuffleDataRequest(testAppId, 3, 1000, shuffleToBlocks);
    response = shuffleServerClient.sendShuffleData(rssdr);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());
    waitFlush(testAppId, shuffleId, isNettyMode);

    // read the 2-th segment from localFile
    // notice: the 1-th segment is skipped, because it is processed
    sdr = composedClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks2.get(0).getBlockId(), ByteBufUtils.readBytes(blocks2.get(0).getData()));
    expectedData.put(blocks2.get(1).getBlockId(), ByteBufUtils.readBytes(blocks2.get(1).getData()));
    validateResult(expectedData, sdr);
    processBlockIds.addLong(blocks2.get(0).getBlockId());
    processBlockIds.addLong(blocks2.get(1).getBlockId());
    sdr.getBufferSegments()
        .forEach(bs -> composedClientReadHandler.updateConsumedBlockInfo(bs, checkSkippedMetrics));

    // read the 3-th segment from localFile
    sdr = composedClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks2.get(2).getBlockId(), ByteBufUtils.readBytes(blocks2.get(2).getData()));
    validateResult(expectedData, sdr);
    processBlockIds.addLong(blocks2.get(2).getBlockId());
    sdr.getBufferSegments()
        .forEach(bs -> composedClientReadHandler.updateConsumedBlockInfo(bs, checkSkippedMetrics));

    // send data to shuffle server, and wait until flush to HDFS
    List<ShuffleBlockInfo> blocks3 =
        createShuffleBlockList(shuffleId, partitionId, 0, 2, 200, expectBlockIds, dataMap, mockSSI);
    partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(partitionId, blocks3);
    shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(shuffleId, partitionToBlocks);
    rssdr = new RssSendShuffleDataRequest(testAppId, 3, 1000, shuffleToBlocks);
    response = shuffleServerClient.sendShuffleData(rssdr);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());
    waitFlush(testAppId, shuffleId, isNettyMode);

    // read the 4-th segment from HDFS
    sdr = composedClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks3.get(0).getBlockId(), ByteBufUtils.readBytes(blocks3.get(0).getData()));
    expectedData.put(blocks3.get(1).getBlockId(), ByteBufUtils.readBytes(blocks3.get(1).getData()));
    validateResult(expectedData, sdr);
    processBlockIds.addLong(blocks3.get(0).getBlockId());
    processBlockIds.addLong(blocks3.get(1).getBlockId());
    sdr.getBufferSegments()
        .forEach(bs -> composedClientReadHandler.updateConsumedBlockInfo(bs, checkSkippedMetrics));

    // all segments are processed
    sdr = composedClientReadHandler.readShuffleData();
    assertNull(sdr);

    if (checkSkippedMetrics) {
      String readBlockNumInfo = composedClientReadHandler.getReadBlockNumInfo();
      assertTrue(readBlockNumInfo.contains("Client read 0 blocks from [" + ssi + "]"));
      assertTrue(readBlockNumInfo.contains("Skipped[ hot:3 warm:3 cold:2 frozen:0 ]"));
      assertTrue(readBlockNumInfo.contains("Consumed[ hot:0 warm:0 cold:0 frozen:0 ]"));
      String readLengthInfo = composedClientReadHandler.getReadLengthInfo();
      assertTrue(readLengthInfo.contains("Client read 0 bytes from [" + ssi + "]"));
      assertTrue(readLengthInfo.contains("Skipped[ hot:75 warm:150 cold:400 frozen:0 ]"));
      assertTrue(readBlockNumInfo.contains("Consumed[ hot:0 warm:0 cold:0 frozen:0 ]"));
      String readUncompressLengthInfo = composedClientReadHandler.getReadUncompressLengthInfo();
      assertTrue(
          readUncompressLengthInfo.contains("Client read 0 uncompressed bytes from [" + ssi + "]"));
      assertTrue(readUncompressLengthInfo.contains("Skipped[ hot:75 warm:150 cold:400 frozen:0 ]"));
      assertTrue(readBlockNumInfo.contains("Consumed[ hot:0 warm:0 cold:0 frozen:0 ]"));
    } else {
      String readBlockNumInfo = composedClientReadHandler.getReadBlockNumInfo();
      assertTrue(readBlockNumInfo.contains("Client read 8 blocks from [" + ssi + "]"));
      assertTrue(readBlockNumInfo.contains("Consumed[ hot:3 warm:3 cold:2 frozen:0 ]"));
      assertTrue(readBlockNumInfo.contains("Skipped[ hot:0 warm:0 cold:0 frozen:0 ]"));
      String readLengthInfo = composedClientReadHandler.getReadLengthInfo();
      assertTrue(readLengthInfo.contains("Client read 625 bytes from [" + ssi + "]"));
      assertTrue(readLengthInfo.contains("Consumed[ hot:75 warm:150 cold:400 frozen:0 ]"));
      assertTrue(readBlockNumInfo.contains("Skipped[ hot:0 warm:0 cold:0 frozen:0 ]"));
      String readUncompressLengthInfo = composedClientReadHandler.getReadUncompressLengthInfo();
      assertTrue(
          readUncompressLengthInfo.contains(
              "Client read 625 uncompressed bytes from [" + ssi + "]"));
      assertTrue(
          readUncompressLengthInfo.contains("Consumed[ hot:75 warm:150 cold:400 frozen:0 ]"));
      assertTrue(readBlockNumInfo.contains("Skipped[ hot:0 warm:0 cold:0 frozen:0 ]"));
    }
  }

  protected void waitFlush(String appId, int shuffleId, boolean isNettyMode)
      throws InterruptedException {
    List<ShuffleServer> shuffleServers = isNettyMode ? nettyShuffleServers : grpcShuffleServers;
    int retry = 0;
    while (true) {
      if (retry > 5) {
        fail(String.format("Timeout for flush data, isNettyMode=%s", isNettyMode));
      }
      ShuffleBuffer shuffleBuffer =
          shuffleServers.get(0).getShuffleBufferManager().getShuffleBuffer(appId, shuffleId, 0);
      if (shuffleBuffer.getBlocks().size() == 0 && shuffleBuffer.getInFlushBlockMap().size() == 0) {
        break;
      }
      Thread.sleep(1000);
      retry++;
    }
  }

  protected void validateResult(Map<Long, byte[]> expectedData, ShuffleDataResult sdr) {
    byte[] buffer = sdr.getData();
    List<BufferSegment> bufferSegments = sdr.getBufferSegments();
    assertEquals(expectedData.size(), bufferSegments.size());
    for (Map.Entry<Long, byte[]> entry : expectedData.entrySet()) {
      BufferSegment bs = findBufferSegment(entry.getKey(), bufferSegments);
      assertNotNull(bs);
      byte[] data = new byte[bs.getLength()];
      System.arraycopy(buffer, bs.getOffset(), data, 0, bs.getLength());
    }
  }

  private BufferSegment findBufferSegment(long blockId, List<BufferSegment> bufferSegments) {
    for (BufferSegment bs : bufferSegments) {
      if (bs.getBlockId() == blockId) {
        return bs;
      }
    }
    return null;
  }
}
