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
import org.apache.uniffle.storage.handler.impl.LocalFileClientReadHandler;
import org.apache.uniffle.storage.handler.impl.MemoryClientReadHandler;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

public class ShuffleServerWithMemoryTest extends ShuffleReadWriteBase {

  private ShuffleServerGrpcClient grpcShuffleServerClient;
  private ShuffleServerGrpcNettyClient nettyShuffleServerClient;

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    storeCoordinatorConf(coordinatorConf);

    ShuffleServerConf grpcShuffleServerConf = buildShuffleServerConf(0, tmpDir, ServerType.GRPC);
    storeShuffleServerConf(grpcShuffleServerConf);

    ShuffleServerConf nettyShuffleServerConf =
        buildShuffleServerConf(0, tmpDir, ServerType.GRPC_NETTY);
    storeShuffleServerConf(nettyShuffleServerConf);

    startServersWithRandomPorts();
  }

  private static ShuffleServerConf buildShuffleServerConf(
      int subDirIndex, File tempDir, ServerType serverType) {
    ShuffleServerConf shuffleServerConf =
        shuffleServerConfWithoutPort(subDirIndex, tempDir, serverType);
    shuffleServerConf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE.name());
    shuffleServerConf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 5000L);
    shuffleServerConf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 5.0);
    shuffleServerConf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 15.0);
    shuffleServerConf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 2000L);
    shuffleServerConf.set(ShuffleServerConf.SERVER_TRIGGER_FLUSH_CHECK_INTERVAL, 500L);
    return shuffleServerConf;
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

  @AfterEach
  public void closeClient() {
    grpcShuffleServerClient.close();
    nettyShuffleServerClient.close();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    shutdownServers();
  }

  private static Stream<Arguments> memoryWriteReadTestProvider() {
    return Stream.of(Arguments.of(true), Arguments.of(false));
  }

  @ParameterizedTest
  @MethodSource("memoryWriteReadTestProvider")
  private void memoryWriteReadTestProvider(boolean isNettyMode) throws Exception {
    ShuffleServerGrpcClient shuffleServerClient =
        isNettyMode ? nettyShuffleServerClient : grpcShuffleServerClient;
    String testAppId = "memoryWriteReadTest";
    int shuffleId = 0;
    int partitionId = 0;
    RssRegisterShuffleRequest rrsr =
        new RssRegisterShuffleRequest(
            testAppId, 0, Lists.newArrayList(new PartitionRange(0, 0)), "");
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

    // send data to shuffle server
    RssSendShuffleDataRequest rssdr =
        new RssSendShuffleDataRequest(testAppId, 3, 1000, shuffleToBlocks);
    RssSendShuffleDataResponse response = shuffleServerClient.sendShuffleData(rssdr);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());

    // data is cached
    List<ShuffleServer> shuffleServers = isNettyMode ? nettyShuffleServers : grpcShuffleServers;
    assertEquals(
        3,
        shuffleServers
            .get(0)
            .getShuffleBufferManager()
            .getShuffleBuffer(testAppId, shuffleId, 0)
            .getBlocks()
            .size());

    Roaring64NavigableMap exceptTaskIds = Roaring64NavigableMap.bitmapOf(0);
    // create memory handler to read data,
    MemoryClientReadHandler memoryClientReadHandler =
        new MemoryClientReadHandler(
            testAppId, shuffleId, partitionId, 20, shuffleServerClient, exceptTaskIds);
    // start to read data, one block data for every call
    ShuffleDataResult sdr = memoryClientReadHandler.readShuffleData();
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    expectedData.put(blocks.get(0).getBlockId(), ByteBufUtils.readBytes(blocks.get(0).getData()));
    validateResult(expectedData, sdr);

    sdr = memoryClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks.get(1).getBlockId(), ByteBufUtils.readBytes(blocks.get(1).getData()));
    validateResult(expectedData, sdr);

    sdr = memoryClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks.get(2).getBlockId(), ByteBufUtils.readBytes(blocks.get(2).getData()));
    validateResult(expectedData, sdr);

    // no data in cache, empty return
    sdr = memoryClientReadHandler.readShuffleData();
    assertEquals(0, sdr.getBufferSegments().size());

    // case: read with ComposedClientReadHandler
    memoryClientReadHandler =
        new MemoryClientReadHandler(
            testAppId, shuffleId, partitionId, 50, shuffleServerClient, exceptTaskIds);

    Roaring64NavigableMap processBlockIds = Roaring64NavigableMap.bitmapOf();
    LocalFileClientReadHandler localFileQuorumClientReadHandler =
        new LocalFileClientReadHandler(
            testAppId,
            shuffleId,
            partitionId,
            0,
            1,
            3,
            50,
            expectBlockIds,
            processBlockIds,
            shuffleServerClient);
    ClientReadHandler[] handlers = new ClientReadHandler[2];
    handlers[0] = memoryClientReadHandler;
    handlers[1] = localFileQuorumClientReadHandler;
    ShuffleServerInfo ssi =
        isNettyMode
            ? new ShuffleServerInfo(
                LOCALHOST,
                nettyShuffleServers.get(0).getGrpcPort(),
                nettyShuffleServers.get(0).getNettyPort())
            : new ShuffleServerInfo(LOCALHOST, grpcShuffleServers.get(0).getGrpcPort());
    ComposedClientReadHandler composedClientReadHandler =
        new ComposedClientReadHandler(ssi, handlers);
    // read from memory with ComposedClientReadHandler
    sdr = composedClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks.get(0).getBlockId(), ByteBufUtils.readBytes(blocks.get(0).getData()));
    expectedData.put(blocks.get(1).getBlockId(), ByteBufUtils.readBytes(blocks.get(1).getData()));
    validateResult(expectedData, sdr);

    // send data to shuffle server, flush should happen
    processBlockIds.addLong(blocks.get(0).getBlockId());
    processBlockIds.addLong(blocks.get(1).getBlockId());

    List<ShuffleBlockInfo> blocks2 =
        createShuffleBlockList(shuffleId, partitionId, 0, 3, 50, expectBlockIds, dataMap, mockSSI);
    partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(partitionId, blocks2);
    shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(shuffleId, partitionToBlocks);

    rssdr = new RssSendShuffleDataRequest(testAppId, 3, 1000, shuffleToBlocks);
    response = shuffleServerClient.sendShuffleData(rssdr);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());
    // wait until flush finished
    int retry = 0;
    while (true) {
      if (retry > 5) {
        fail(String.format("Timeout for flush data, isNettyMode=%s", isNettyMode));
      }
      ShuffleBuffer shuffleBuffer =
          shuffleServers.get(0).getShuffleBufferManager().getShuffleBuffer(testAppId, shuffleId, 0);
      if (shuffleBuffer.getBlocks().size() == 0 && shuffleBuffer.getInFlushBlockMap().size() == 0) {
        break;
      }
      Thread.sleep(1000);
      retry++;
    }

    // when segment filter is introduced, there is no need to read duplicated data
    sdr = composedClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks.get(2).getBlockId(), ByteBufUtils.readBytes(blocks.get(2).getData()));
    expectedData.put(blocks2.get(0).getBlockId(), ByteBufUtils.readBytes(blocks2.get(0).getData()));
    validateResult(expectedData, sdr);
    processBlockIds.addLong(blocks.get(2).getBlockId());
    processBlockIds.addLong(blocks2.get(0).getBlockId());

    sdr = composedClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks2.get(1).getBlockId(), ByteBufUtils.readBytes(blocks2.get(1).getData()));
    validateResult(expectedData, sdr);
    processBlockIds.addLong(blocks2.get(1).getBlockId());

    sdr = composedClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks2.get(2).getBlockId(), ByteBufUtils.readBytes(blocks2.get(2).getData()));
    validateResult(expectedData, sdr);
    processBlockIds.addLong(blocks2.get(2).getBlockId());

    sdr = composedClientReadHandler.readShuffleData();
    assertNull(sdr);
  }

  private static Stream<Arguments> memoryWriteReadWithMultiReplicaTestProvider() {
    return Stream.of(Arguments.of(true), Arguments.of(false));
  }

  @ParameterizedTest
  @MethodSource("memoryWriteReadWithMultiReplicaTestProvider")
  private void memoryWriteReadWithMultiReplicaTest(boolean isNettyMode) throws Exception {
    ShuffleServerGrpcClient shuffleServerClient =
        isNettyMode ? nettyShuffleServerClient : grpcShuffleServerClient;
    String testAppId = "memoryWriteReadWithMultiReplicaTest";
    int shuffleId = 0;
    int partitionId = 0;
    RssRegisterShuffleRequest rrsr =
        new RssRegisterShuffleRequest(
            testAppId, 0, Lists.newArrayList(new PartitionRange(0, 0)), "");
    shuffleServerClient.registerShuffle(rrsr);
    Roaring64NavigableMap expectBlockIds = Roaring64NavigableMap.bitmapOf();
    Map<Long, byte[]> dataMap = Maps.newHashMap();
    Roaring64NavigableMap[] bitmaps = new Roaring64NavigableMap[1];
    bitmaps[0] = Roaring64NavigableMap.bitmapOf();
    // create blocks which belong to different tasks
    List<ShuffleBlockInfo> blocks = Lists.newArrayList();
    for (int i = 0; i < 3; i++) {
      blocks.addAll(
          createShuffleBlockList(
              shuffleId, partitionId, i, 1, 25, expectBlockIds, dataMap, mockSSI));
    }
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(partitionId, blocks);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(shuffleId, partitionToBlocks);

    // send data to shuffle server
    RssSendShuffleDataRequest rssdr =
        new RssSendShuffleDataRequest(testAppId, 3, 1000, shuffleToBlocks);
    RssSendShuffleDataResponse response = shuffleServerClient.sendShuffleData(rssdr);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());

    // data is cached
    List<ShuffleServer> shuffleServers = isNettyMode ? nettyShuffleServers : grpcShuffleServers;
    assertEquals(
        3,
        shuffleServers
            .get(0)
            .getShuffleBufferManager()
            .getShuffleBuffer(testAppId, shuffleId, 0)
            .getBlocks()
            .size());

    Roaring64NavigableMap processBlockIds = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap exceptTaskIds = Roaring64NavigableMap.bitmapOf(0, 1, 2);
    // create memory handler to read data,
    MemoryClientReadHandler memoryClientReadHandler =
        new MemoryClientReadHandler(
            testAppId, shuffleId, partitionId, 20, shuffleServerClient, exceptTaskIds);
    // start to read data, one block data for every call
    ShuffleDataResult sdr = memoryClientReadHandler.readShuffleData();
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    expectedData.put(blocks.get(0).getBlockId(), ByteBufUtils.readBytes(blocks.get(0).getData()));
    validateResult(expectedData, sdr);
    // read by different reader, the first block should be skipped.
    exceptTaskIds.removeLong(blocks.get(0).getTaskAttemptId());
    MemoryClientReadHandler memoryClientReadHandler2 =
        new MemoryClientReadHandler(
            testAppId, shuffleId, partitionId, 20, shuffleServerClient, exceptTaskIds);
    sdr = memoryClientReadHandler2.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks.get(1).getBlockId(), ByteBufUtils.readBytes(blocks.get(1).getData()));
    validateResult(expectedData, sdr);

    sdr = memoryClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks.get(1).getBlockId(), ByteBufUtils.readBytes(blocks.get(1).getData()));
    validateResult(expectedData, sdr);

    sdr = memoryClientReadHandler2.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks.get(2).getBlockId(), ByteBufUtils.readBytes(blocks.get(2).getData()));
    validateResult(expectedData, sdr);
    // no data in cache, empty return
    sdr = memoryClientReadHandler2.readShuffleData();
    assertEquals(0, sdr.getBufferSegments().size());
  }

  private static Stream<Arguments> memoryAndLocalFileReadWithFilterTestProvider() {
    return Stream.of(Arguments.of(true), Arguments.of(false));
  }

  @ParameterizedTest
  @MethodSource("memoryAndLocalFileReadWithFilterTestProvider")
  private void memoryAndLocalFileReadWithFilterTest(boolean isNettyMode) throws Exception {
    ShuffleServerGrpcClient shuffleServerClient =
        isNettyMode ? nettyShuffleServerClient : grpcShuffleServerClient;
    List<ShuffleServer> shuffleServers = isNettyMode ? nettyShuffleServers : grpcShuffleServers;
    String testAppId = "memoryAndLocalFileReadWithFilterTest";
    int shuffleId = 0;
    int partitionId = 0;
    RssRegisterShuffleRequest rrsr =
        new RssRegisterShuffleRequest(
            testAppId, 0, Lists.newArrayList(new PartitionRange(0, 0)), "");
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

    // read the 1-th segment from memory
    Roaring64NavigableMap processBlockIds = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap exceptTaskIds = Roaring64NavigableMap.bitmapOf(0);
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
    ClientReadHandler[] handlers = new ClientReadHandler[2];
    handlers[0] = memoryClientReadHandler;
    handlers[1] = localFileClientReadHandler;
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

    // send data to shuffle server, and wait until flush finish
    List<ShuffleBlockInfo> blocks2 =
        createShuffleBlockList(shuffleId, partitionId, 0, 3, 50, expectBlockIds, dataMap, mockSSI);
    partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(partitionId, blocks2);
    shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(shuffleId, partitionToBlocks);
    rssdr = new RssSendShuffleDataRequest(testAppId, 3, 1000, shuffleToBlocks);
    response = shuffleServerClient.sendShuffleData(rssdr);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());

    int retry = 0;
    while (true) {
      if (retry > 5) {
        fail(String.format("Timeout for flush data, isNettyMode=%s", isNettyMode));
      }
      ShuffleBuffer shuffleBuffer =
          shuffleServers.get(0).getShuffleBufferManager().getShuffleBuffer(testAppId, shuffleId, 0);
      if (shuffleBuffer.getBlocks().size() == 0 && shuffleBuffer.getInFlushBlockMap().size() == 0) {
        break;
      }
      Thread.sleep(1000);
      retry++;
    }

    // read the 2-th segment from localFile
    // notice: the 1-th segment is skipped, because it is processed
    sdr = composedClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks2.get(0).getBlockId(), ByteBufUtils.readBytes(blocks2.get(0).getData()));
    expectedData.put(blocks2.get(1).getBlockId(), ByteBufUtils.readBytes(blocks2.get(1).getData()));
    validateResult(expectedData, sdr);
    processBlockIds.addLong(blocks2.get(0).getBlockId());
    processBlockIds.addLong(blocks2.get(1).getBlockId());

    // read the 3-th segment from localFile
    sdr = composedClientReadHandler.readShuffleData();
    expectedData.clear();
    expectedData.put(blocks2.get(2).getBlockId(), ByteBufUtils.readBytes(blocks2.get(2).getData()));
    validateResult(expectedData, sdr);
    processBlockIds.addLong(blocks2.get(2).getBlockId());

    // all segments are processed
    sdr = composedClientReadHandler.readShuffleData();
    assertNull(sdr);
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
