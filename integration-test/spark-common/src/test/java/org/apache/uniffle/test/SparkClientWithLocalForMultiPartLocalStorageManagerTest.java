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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleDataReadEvent;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.storage.MultiPartLocalStorageManager;
import org.apache.uniffle.storage.common.LocalStorage;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class SparkClientWithLocalForMultiPartLocalStorageManagerTest extends ShuffleReadWriteBase {
  private ShuffleServerGrpcClient grpcShuffleServerClient;
  private ShuffleServerGrpcNettyClient nettyShuffleServerClient;

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    storeCoordinatorConf(coordinatorConf);

    storeShuffleServerConf(buildShuffleServerConf(0, tmpDir, ServerType.GRPC));
    storeShuffleServerConf(buildShuffleServerConf(1, tmpDir, ServerType.GRPC_NETTY));

    startServersWithRandomPorts();
  }

  private static ShuffleServerConf buildShuffleServerConf(
      int subDirIndex, File tmpDir, ServerType serverType) {
    ShuffleServerConf shuffleServerConf =
        shuffleServerConfWithoutPort(subDirIndex, tmpDir, serverType);
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    shuffleServerConf.set(
        ShuffleServerConf.SERVER_LOCAL_STORAGE_MANAGER_CLASS,
        MultiPartLocalStorageManager.class.getName());
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

  private ShuffleClientFactory.ReadClientBuilder baseReadBuilder(boolean isNettyMode) {
    List<ShuffleServerInfo> shuffleServerInfo =
        isNettyMode
            ? Lists.newArrayList(
                new ShuffleServerInfo(
                    LOCALHOST,
                    nettyShuffleServers.get(0).getGrpcPort(),
                    nettyShuffleServers.get(0).getNettyPort()))
            : Lists.newArrayList(
                new ShuffleServerInfo(
                    LOCALHOST, LOCALHOST, grpcShuffleServers.get(0).getGrpcPort()));
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
  public void testClientRemoteReadFromMultipleDisk(boolean isNettyMode) {
    String testAppId = "testClientRemoteReadFromMultipleDisk_appId";
    registerApp(testAppId, Lists.newArrayList(new PartitionRange(0, 0)), isNettyMode);

    // Send shuffle data
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();

    List<ShuffleBlockInfo> blocks =
        createShuffleBlockList(0, 0, 0, 5, 30, blockIdBitmap, expectedData, mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);

    List<ShuffleServer> shuffleServers = isNettyMode ? nettyShuffleServers : grpcShuffleServers;
    // Mark one storage reaching high watermark, it should switch another storage for next writing
    ShuffleServer shuffleServer = shuffleServers.get(0);
    ShuffleDataReadEvent readEvent = new ShuffleDataReadEvent(testAppId, 0, 0, 0, 0);
    LocalStorage storage1 =
        (LocalStorage) shuffleServer.getStorageManager().selectStorage(readEvent);
    storage1.getMetaData().setSize(20 * 1024 * 1024);

    blocks = createShuffleBlockList(0, 0, 0, 3, 25, blockIdBitmap, expectedData, mockSSI);
    sendTestData(testAppId, blocks, isNettyMode);

    readEvent = new ShuffleDataReadEvent(testAppId, 0, 0, 0, 1);
    LocalStorage storage2 =
        (LocalStorage) shuffleServer.getStorageManager().selectStorage(readEvent);
    assertNotEquals(storage1, storage2);

    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
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
}
