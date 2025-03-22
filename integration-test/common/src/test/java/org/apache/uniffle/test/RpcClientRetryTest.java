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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.impl.ShuffleReadClientImpl;
import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.MockedGrpcServer;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class RpcClientRetryTest extends ShuffleReadWriteBase {

  private static ShuffleServerInfo shuffleServerInfo0;
  private static ShuffleServerInfo shuffleServerInfo1;
  private static ShuffleServerInfo shuffleServerInfo2;
  private static MockedShuffleWriteClientImpl shuffleWriteClientImpl;

  private ShuffleClientFactory.ReadClientBuilder baseReadBuilder(StorageType storageType) {
    return ShuffleClientFactory.newReadBuilder()
        .clientType(ClientType.GRPC)
        .storageType(storageType.name())
        .shuffleId(0)
        .partitionId(0)
        .indexReadLimit(100)
        .partitionNumPerRange(1)
        .partitionNum(10)
        .readBufferSize(1000);
  }

  public static ShuffleServerConf buildMockShuffleConf(int id, File tmpDir) {
    ShuffleServerConf shuffleServerConf = shuffleServerConfWithoutPort(id, tmpDir, ServerType.GRPC);
    shuffleServerConf.setString("rss.storage.type", StorageType.MEMORY_LOCALFILE.name());
    shuffleServerConf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 5.0);
    shuffleServerConf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 15.0);
    shuffleServerConf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 600L);
    shuffleServerConf.set(ShuffleServerConf.SINGLE_BUFFER_FLUSH_BLOCKS_NUM_THRESHOLD, 1);
    return shuffleServerConf;
  }

  @BeforeAll
  public static void initCluster(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    storeCoordinatorConf(coordinatorConf);

    storeMockShuffleServerConf(buildMockShuffleConf(0, tmpDir));
    storeMockShuffleServerConf(buildMockShuffleConf(1, tmpDir));
    storeMockShuffleServerConf(buildMockShuffleConf(2, tmpDir));

    startServersWithRandomPorts();

    shuffleServerInfo0 = new ShuffleServerInfo(LOCALHOST, grpcShuffleServers.get(0).getGrpcPort());
    shuffleServerInfo1 = new ShuffleServerInfo(LOCALHOST, grpcShuffleServers.get(1).getGrpcPort());
    shuffleServerInfo2 = new ShuffleServerInfo(LOCALHOST, grpcShuffleServers.get(2).getGrpcPort());
  }

  @AfterAll
  public static void cleanEnv() throws Exception {
    if (shuffleWriteClientImpl != null) {
      shuffleWriteClientImpl.close();
    }
    shutdownServers();
  }

  private static Stream<Arguments> testRpcRetryLogicProvider() {
    return Stream.of(
        Arguments.of(StorageType.MEMORY_LOCALFILE),
        // According to SERVER_BUFFER_CAPACITY & SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE,
        // data will be flushed to disk, so read from disk only
        Arguments.of(StorageType.LOCALFILE));
  }

  @ParameterizedTest
  @MethodSource("testRpcRetryLogicProvider")
  public void testRpcRetryLogic(StorageType storageType) {
    String testAppId = "testRpcRetryLogic";
    registerShuffleServer(testAppId, 3, 2, 2, true);
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();

    List<ShuffleBlockInfo> blocks =
        createShuffleBlockList(
            0,
            0,
            0,
            3,
            25,
            blockIdBitmap,
            expectedData,
            Lists.newArrayList(shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2));

    SendShuffleDataResult result = shuffleWriteClientImpl.sendShuffleData(testAppId, blocks);
    Roaring64NavigableMap failedBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap successfulBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    for (Long blockId : result.getSuccessBlockIds()) {
      successfulBlockIdBitmap.addLong(blockId);
    }
    for (Long blockId : result.getFailedBlockIds()) {
      failedBlockIdBitmap.addLong(blockId);
    }
    assertEquals(0, failedBlockIdBitmap.getLongCardinality());
    assertEquals(blockIdBitmap, successfulBlockIdBitmap);

    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);

    ShuffleReadClientImpl readClient1 =
        baseReadBuilder(storageType)
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(
                Lists.newArrayList(shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2))
            .retryMax(3)
            .retryIntervalMax(1)
            .build();

    // The data cannot be read because the maximum number of retries is 3
    enableFirstNReadRequestsToFail(4);
    try {
      validateResult(readClient1, expectedData);
      fail();
    } catch (Exception e) {
      // do nothing
    }
    disableFirstNReadRequestsToFail();

    ShuffleReadClientImpl readClient2 =
        baseReadBuilder(storageType)
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(
                Lists.newArrayList(shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2))
            .retryMax(3)
            .retryIntervalMax(1)
            .build();

    // The data can be read because the reader will retry
    enableFirstNReadRequestsToFail(1);
    validateResult(readClient2, expectedData);
    disableFirstNReadRequestsToFail();
    readClient1.close();
    readClient2.close();
  }

  private static void enableFirstNReadRequestsToFail(int failedCount) {
    for (ShuffleServer server : grpcShuffleServers) {
      ((MockedGrpcServer) server.getServer())
          .getService()
          .enableFirstNReadRequestToFail(failedCount);
    }
  }

  private static void disableFirstNReadRequestsToFail() {
    for (ShuffleServer server : grpcShuffleServers) {
      ((MockedGrpcServer) server.getServer()).getService().resetFirstNReadRequestToFail();
    }
  }

  static class MockedShuffleWriteClientImpl extends ShuffleWriteClientImpl {
    MockedShuffleWriteClientImpl(ShuffleClientFactory.WriteClientBuilder builder) {
      super(builder);
    }

    public SendShuffleDataResult sendShuffleData(
        String appId, List<ShuffleBlockInfo> shuffleBlockInfoList) {
      return super.sendShuffleData(appId, shuffleBlockInfoList, () -> false);
    }
  }

  private void registerShuffleServer(
      String testAppId, int replica, int replicaWrite, int replicaRead, boolean replicaSkip) {

    shuffleWriteClientImpl =
        new MockedShuffleWriteClientImpl(
            ShuffleClientFactory.newWriteBuilder()
                .clientType(ClientType.GRPC.name())
                .retryMax(3)
                .retryIntervalMax(1000)
                .heartBeatThreadNum(1)
                .replica(replica)
                .replicaWrite(replicaWrite)
                .replicaRead(replicaRead)
                .replicaSkipEnabled(replicaSkip)
                .dataTransferPoolSize(1)
                .dataCommitPoolSize(1)
                .unregisterThreadPoolSize(10)
                .unregisterTimeSec(10)
                .unregisterRequestTimeSec(10));

    List<ShuffleServerInfo> allServers =
        Lists.newArrayList(shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2);

    for (int i = 0; i < replica; i++) {
      shuffleWriteClientImpl.registerShuffle(
          allServers.get(i),
          testAppId,
          0,
          Lists.newArrayList(new PartitionRange(0, 0)),
          new RemoteStorageInfo(""),
          ShuffleDataDistributionType.NORMAL,
          1);
    }
  }
}
