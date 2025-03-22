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
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.impl.ShuffleReadClientImpl;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcNettyClient;
import org.apache.uniffle.client.request.RssFinishShuffleRequest;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssSendCommitRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.util.BlockId;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SparkClientWithLocalTest extends ShuffleReadWriteBase {

  private static final String EXPECTED_EXCEPTION_MESSAGE = "Exception should be thrown";
  private static File GRPC_DATA_DIR1;
  private static File GRPC_DATA_DIR2;
  private static File NETTY_DATA_DIR1;
  private static File NETTY_DATA_DIR2;
  private ShuffleServerGrpcClient grpcShuffleServerClient;
  private ShuffleServerGrpcNettyClient nettyShuffleServerClient;

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    storeCoordinatorConf(coordinatorConfWithoutPort());

    GRPC_DATA_DIR1 = new File(tmpDir, "data1");
    GRPC_DATA_DIR2 = new File(tmpDir, "data2");
    String grpcBasePath = GRPC_DATA_DIR1.getAbsolutePath() + "," + GRPC_DATA_DIR2.getAbsolutePath();
    ShuffleServerConf grpcShuffleServerConf = buildShuffleServerConf(grpcBasePath, ServerType.GRPC);
    storeShuffleServerConf(grpcShuffleServerConf);

    NETTY_DATA_DIR1 = new File(tmpDir, "netty_data1");
    NETTY_DATA_DIR2 = new File(tmpDir, "netty_data2");
    String nettyBasePath =
        NETTY_DATA_DIR1.getAbsolutePath() + "," + NETTY_DATA_DIR2.getAbsolutePath();
    ShuffleServerConf nettyShuffleServerConf =
        buildShuffleServerConf(nettyBasePath, ServerType.GRPC_NETTY);
    storeShuffleServerConf(nettyShuffleServerConf);

    startServersWithRandomPorts();
  }

  private static ShuffleServerConf buildShuffleServerConf(String basePath, ServerType serverType) {
    ShuffleServerConf shuffleServerConf = shuffleServerConfWithoutPort(0, null, serverType);
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    return shuffleServerConf;
  }

  @BeforeEach
  public void createClient() {
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

  private ShuffleClientFactory.ReadClientBuilder baseReadBuilder(boolean isNettyMode) {
    List<ShuffleServerInfo> shuffleServerInfo =
        isNettyMode
            ? Lists.newArrayList(
                new ShuffleServerInfo(
                    LOCALHOST,
                    nettyShuffleServers.get(0).getGrpcPort(),
                    nettyShuffleServers.get(0).getNettyPort()))
            : Lists.newArrayList(
                new ShuffleServerInfo(LOCALHOST, grpcShuffleServers.get(0).getGrpcPort()));
    return ShuffleClientFactory.newReadBuilder()
        .clientType(isNettyMode ? ClientType.GRPC_NETTY : ClientType.GRPC)
        .storageType(StorageType.LOCALFILE.name())
        .shuffleId(0)
        .partitionId(0)
        .indexReadLimit(100)
        .partitionNumPerRange(1)
        .partitionNum(10)
        .readBufferSize(1000)
        .shuffleServerInfoList(shuffleServerInfo);
  }

  private static Stream<Arguments> isNettyModeProvider() {
    return Stream.of(Arguments.of(true), Arguments.of(false));
  }

  @ParameterizedTest
  @MethodSource("isNettyModeProvider")
  public void readTest1(boolean isNettyMode) {
    String testAppId = "localReadTest1";
    BlockIdLayout layout = BlockIdLayout.DEFAULT;
    registerApp(testAppId, Lists.newArrayList(new PartitionRange(0, 0)), isNettyMode);
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    createTestData(testAppId, expectedData, blockIdBitmap, taskIdBitmap, isNettyMode);
    blockIdBitmap.addLong(layout.getBlockId(0, 1, 0));
    ShuffleReadClientImpl readClient;
    readClient =
        baseReadBuilder(isNettyMode)
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    validateResult(readClient, expectedData);
    try {
      // can't find all expected block id, data loss
      readClient.checkProcessedBlockIds();
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Blocks read inconsistent:"));
    } finally {
      readClient.close();
    }
  }

  @ParameterizedTest
  @MethodSource("isNettyModeProvider")
  public void readTest2(boolean isNettyMode) {
    String testAppId = "localReadTest2";
    registerApp(testAppId, Lists.newArrayList(new PartitionRange(0, 0)), isNettyMode);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    final Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    List<ShuffleBlockInfo> blocks =
        createShuffleBlockList(0, 0, 0, 2, 30, blockIdBitmap, expectedData, mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);
    blocks = createShuffleBlockList(0, 0, 0, 2, 30, blockIdBitmap, expectedData, mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);

    ShuffleReadClientImpl readClient =
        baseReadBuilder(isNettyMode)
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @ParameterizedTest
  @MethodSource("isNettyModeProvider")
  public void readTest3(boolean isNettyMode) throws Exception {
    String testAppId = "localReadTest3";
    registerApp(testAppId, Lists.newArrayList(new PartitionRange(0, 0)), isNettyMode);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    List<ShuffleBlockInfo> blocks =
        createShuffleBlockList(0, 0, 0, 2, 30, blockIdBitmap, expectedData, mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);

    if (isNettyMode) {
      FileUtils.deleteDirectory(
          new File(NETTY_DATA_DIR1.getAbsolutePath() + "/" + testAppId + "/0/0-0"));
      FileUtils.deleteDirectory(
          new File(NETTY_DATA_DIR2.getAbsolutePath() + "/" + testAppId + "/0/0-0"));
    } else {
      FileUtils.deleteDirectory(
          new File(GRPC_DATA_DIR1.getAbsolutePath() + "/" + testAppId + "/0/0-0"));
      FileUtils.deleteDirectory(
          new File(GRPC_DATA_DIR2.getAbsolutePath() + "/" + testAppId + "/0/0-0"));
    }
    // sleep to wait delete operation
    Thread.sleep(2000);

    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    ShuffleReadClientImpl readClient =
        baseReadBuilder(isNettyMode)
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    assertNull(readClient.readShuffleBlockData());
    readClient.close();
  }

  @ParameterizedTest
  @MethodSource("isNettyModeProvider")
  public void readTest4(boolean isNettyMode) {
    String testAppId = "localReadTest4";
    registerApp(testAppId, Lists.newArrayList(new PartitionRange(0, 1)), isNettyMode);

    Map<Long, byte[]> expectedData1 = Maps.newHashMap();
    Map<Long, byte[]> expectedData2 = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();
    final Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    List<ShuffleBlockInfo> blocks =
        createShuffleBlockList(0, 0, 0, 10, 30, blockIdBitmap1, expectedData1, mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);

    Roaring64NavigableMap blockIdBitmap2 = Roaring64NavigableMap.bitmapOf();
    blocks = createShuffleBlockList(0, 1, 0, 10, 30, blockIdBitmap2, expectedData2, mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);

    blocks = createShuffleBlockList(0, 0, 0, 10, 30, blockIdBitmap1, expectedData1, mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);

    ShuffleReadClientImpl readClient1 =
        baseReadBuilder(isNettyMode)
            .appId(testAppId)
            .partitionNumPerRange(2)
            .blockIdBitmap(blockIdBitmap1)
            .taskIdBitmap(taskIdBitmap)
            .build();
    final ShuffleReadClientImpl readClient2 =
        baseReadBuilder(isNettyMode)
            .appId(testAppId)
            .partitionId(1)
            .partitionNumPerRange(2)
            .blockIdBitmap(blockIdBitmap2)
            .taskIdBitmap(taskIdBitmap)
            .build();
    validateResult(readClient1, expectedData1);
    readClient1.checkProcessedBlockIds();
    readClient1.close();

    validateResult(readClient2, expectedData2);
    readClient2.checkProcessedBlockIds();
    readClient2.close();
  }

  @ParameterizedTest
  @MethodSource("isNettyModeProvider")
  public void readTest5(boolean isNettyMode) {
    String testAppId = "localReadTest5";
    ShuffleReadClientImpl readClient =
        baseReadBuilder(isNettyMode)
            .appId(testAppId)
            .partitionId(1)
            .partitionNumPerRange(2)
            .blockIdBitmap(Roaring64NavigableMap.bitmapOf())
            .taskIdBitmap(Roaring64NavigableMap.bitmapOf())
            .build();
    assertNull(readClient.readShuffleBlockData());
    readClient.checkProcessedBlockIds();
  }

  @ParameterizedTest
  @MethodSource("isNettyModeProvider")
  public void readTest6(boolean isNettyMode) {
    String testAppId = "localReadTest6";
    BlockIdLayout layout = BlockIdLayout.DEFAULT;
    registerApp(testAppId, Lists.newArrayList(new PartitionRange(0, 0)), isNettyMode);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    List<ShuffleBlockInfo> blocks =
        createShuffleBlockList(0, 0, 0, 5, 30, blockIdBitmap, expectedData, mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);

    Roaring64NavigableMap wrongBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    LongIterator iter = blockIdBitmap.getLongIterator();
    while (iter.hasNext()) {
      BlockId blockId = layout.asBlockId(iter.next());
      wrongBlockIdBitmap.addLong(
          layout.getBlockId(blockId.sequenceNo, blockId.partitionId + 1, blockId.taskAttemptId));
    }

    ShuffleReadClientImpl readClient =
        baseReadBuilder(isNettyMode)
            .appId(testAppId)
            .blockIdBitmap(wrongBlockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    assertNull(readClient.readShuffleBlockData());
    try {
      readClient.checkProcessedBlockIds();
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Blocks read inconsistent:"));
    }
  }

  @ParameterizedTest
  @MethodSource("isNettyModeProvider")
  public void readTest7(boolean isNettyMode) {
    String testAppId = "localReadTest7";
    registerApp(testAppId, Lists.newArrayList(new PartitionRange(0, 0)), isNettyMode);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    final Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0, 1);

    List<ShuffleBlockInfo> blocks =
        createShuffleBlockList(0, 0, 0, 5, 30, blockIdBitmap, expectedData, mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);

    blocks = createShuffleBlockList(0, 0, 1, 5, 30, blockIdBitmap, expectedData, mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);

    blocks = createShuffleBlockList(0, 0, 2, 5, 30, blockIdBitmap, Maps.newHashMap(), mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);

    // unexpected taskAttemptId should be filtered
    ShuffleReadClientImpl readClient =
        baseReadBuilder(isNettyMode)
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @ParameterizedTest
  @MethodSource("isNettyModeProvider")
  public void readTest8(boolean isNettyMode) {
    String testAppId = "localReadTest8";
    registerApp(testAppId, Lists.newArrayList(new PartitionRange(0, 0)), isNettyMode);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    final Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0, 3);
    List<ShuffleBlockInfo> blocks =
        createShuffleBlockList(0, 0, 0, 5, 30, blockIdBitmap, expectedData, mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);

    // test case: data generated by speculation task without report result
    blocks =
        createShuffleBlockList(
            0, 0, 1, 5, 30, Roaring64NavigableMap.bitmapOf(), Maps.newHashMap(), mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);
    // test case: data generated by speculation task with report result
    blocks = createShuffleBlockList(0, 0, 2, 5, 30, blockIdBitmap, Maps.newHashMap(), mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);

    blocks =
        createShuffleBlockList(
            0, 0, 3, 5, 30, Roaring64NavigableMap.bitmapOf(), Maps.newHashMap(), mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);

    // unexpected taskAttemptId should be filtered
    ShuffleReadClientImpl readClient =
        baseReadBuilder(isNettyMode)
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @ParameterizedTest
  @MethodSource("isNettyModeProvider")
  public void readTest9(boolean isNettyMode) throws Exception {
    String testAppId = "localReadTest9";
    registerApp(testAppId, Lists.newArrayList(new PartitionRange(0, 0)), isNettyMode);
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);

    List<ShuffleBlockInfo> blocks;

    createTestData(testAppId, expectedData, blockIdBitmap, taskIdBitmap, isNettyMode);
    Roaring64NavigableMap beforeAdded = RssUtils.cloneBitMap(blockIdBitmap);
    // write data by another task, read data again, the cache for index file should be updated
    blocks = createShuffleBlockList(0, 0, 1, 3, 25, blockIdBitmap, Maps.newHashMap(), mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);
    // test with un-changed expected blockId
    ShuffleReadClientImpl readClient;
    baseReadBuilder(isNettyMode)
        .appId(testAppId)
        .blockIdBitmap(beforeAdded)
        .taskIdBitmap(taskIdBitmap)
        .build();
    readClient =
        baseReadBuilder(isNettyMode)
            .appId(testAppId)
            .blockIdBitmap(beforeAdded)
            .taskIdBitmap(taskIdBitmap)
            .build();
    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();

    // test with changed expected blockId
    readClient =
        baseReadBuilder(isNettyMode)
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @ParameterizedTest
  @MethodSource("isNettyModeProvider")
  public void readTest10(boolean isNettyMode) throws Exception {
    String testAppId = "localReadTest10";
    registerApp(testAppId, Lists.newArrayList(new PartitionRange(0, 0)), isNettyMode);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap expectedBlockIds = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap unexpectedBlockIds = Roaring64NavigableMap.bitmapOf();
    final Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0, 1);
    // send some expected data
    List<ShuffleBlockInfo> blocks =
        createShuffleBlockList(0, 0, 0, 2, 30, expectedBlockIds, expectedData, mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);
    // send some unexpected data
    blocks = createShuffleBlockList(0, 0, 0, 2, 30, unexpectedBlockIds, Maps.newHashMap(), mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);
    // send some expected data
    blocks = createShuffleBlockList(0, 0, 1, 2, 30, expectedBlockIds, expectedData, mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);
    baseReadBuilder(isNettyMode)
        .appId(testAppId)
        .blockIdBitmap(expectedBlockIds)
        .taskIdBitmap(taskIdBitmap)
        .build();
    ShuffleReadClientImpl readClient =
        baseReadBuilder(isNettyMode)
            .appId(testAppId)
            .blockIdBitmap(expectedBlockIds)
            .taskIdBitmap(taskIdBitmap)
            .build();

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  protected void registerApp(
      String testAppId, List<PartitionRange> partitionRanges, boolean isNettyMode) {
    ShuffleServerGrpcClient shuffleServerClient =
        isNettyMode ? nettyShuffleServerClient : grpcShuffleServerClient;
    RssRegisterShuffleRequest rrsr =
        new RssRegisterShuffleRequest(testAppId, 0, partitionRanges, "");
    shuffleServerClient.registerShuffle(rrsr);
  }

  protected void sendTestData(
      String testAppId, List<ShuffleBlockInfo> blocks, boolean isNettyMode) {
    ShuffleServerGrpcClient shuffleServerClient =
        isNettyMode ? nettyShuffleServerClient : grpcShuffleServerClient;
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blocks);

    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);

    RssSendShuffleDataRequest rssdr =
        new RssSendShuffleDataRequest(testAppId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rssdr);
    RssSendCommitRequest rscr = new RssSendCommitRequest(testAppId, 0);
    shuffleServerClient.sendCommit(rscr);
    RssFinishShuffleRequest rfsr = new RssFinishShuffleRequest(testAppId, 0);
    shuffleServerClient.finishShuffle(rfsr);
  }

  private void createTestData(
      String testAppId,
      Map<Long, byte[]> expectedData,
      Roaring64NavigableMap blockIdBitmap,
      Roaring64NavigableMap taskIdBitmap,
      boolean isNettyMode) {
    List<ShuffleBlockInfo> blocks =
        createShuffleBlockList(0, 0, 0, 3, 25, blockIdBitmap, expectedData, mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);

    ShuffleReadClientImpl readClient =
        baseReadBuilder(isNettyMode)
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .build();
    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }
}
