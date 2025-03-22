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
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.factory.ShuffleServerClientFactory;
import org.apache.uniffle.client.impl.ShuffleReadClientImpl;
import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.response.CompressedShuffleBlock;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.MockedGrpcServer;
import org.apache.uniffle.server.MockedShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class QuorumTest extends ShuffleReadWriteBase {

  private static final String EXPECTED_EXCEPTION_MESSAGE = "Exception should be thrown";
  private static ShuffleServerInfo shuffleServerInfo0;
  private static ShuffleServerInfo shuffleServerInfo1;
  private static ShuffleServerInfo shuffleServerInfo2;
  private static ShuffleServerInfo shuffleServerInfo3;
  private static ShuffleServerInfo shuffleServerInfo4;
  private static ShuffleServerInfo fakedShuffleServerInfo0;
  private static ShuffleServerInfo fakedShuffleServerInfo1;
  private static ShuffleServerInfo fakedShuffleServerInfo2;
  private static ShuffleServerInfo fakedShuffleServerInfo3;
  private static ShuffleServerInfo fakedShuffleServerInfo4;
  private MockedShuffleWriteClientImpl shuffleWriteClientImpl;

  private ShuffleClientFactory.ReadClientBuilder baseReadBuilder() {
    return ShuffleClientFactory.newReadBuilder()
        .clientType(ClientType.GRPC)
        .storageType(StorageType.MEMORY_LOCALFILE.name())
        .shuffleId(0)
        .partitionId(0)
        .indexReadLimit(100)
        .partitionNumPerRange(1)
        .partitionNum(10)
        .readBufferSize(1000);
  }

  public static ShuffleServerConf buildServerConf(int id, File tmpDir) {
    ShuffleServerConf shuffleServerConf = shuffleServerConfWithoutPort(id, tmpDir, ServerType.GRPC);
    // app expires time should be greater than the client send data time which is 180s by default.
    // (rpcTimeout*retryTimes)
    // this can avoid get NO_REGISTER when second data to secondaryServer
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 240 * 1000L);
    shuffleServerConf.setLong("rss.server.heartbeat.interval", 5000L);
    shuffleServerConf.setString("rss.storage.type", StorageType.MEMORY_LOCALFILE.name());
    return shuffleServerConf;
  }

  @BeforeEach
  public void initCluster(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    storeCoordinatorConf(coordinatorConf);

    for (int i = 0; i < 5; i++) {
      storeMockShuffleServerConf(buildServerConf(i, tmpDir));
    }
    startServersWithRandomPorts();

    shuffleServerInfo0 = new ShuffleServerInfo(LOCALHOST, grpcShuffleServers.get(0).getGrpcPort());
    shuffleServerInfo1 = new ShuffleServerInfo(LOCALHOST, grpcShuffleServers.get(1).getGrpcPort());
    shuffleServerInfo2 = new ShuffleServerInfo(LOCALHOST, grpcShuffleServers.get(2).getGrpcPort());
    shuffleServerInfo3 = new ShuffleServerInfo(LOCALHOST, grpcShuffleServers.get(3).getGrpcPort());
    shuffleServerInfo4 = new ShuffleServerInfo(LOCALHOST, grpcShuffleServers.get(4).getGrpcPort());

    // simulator of failed servers
    List<Integer> fakePortList = generateNonExistingPorts(5);
    fakedShuffleServerInfo0 =
        new ShuffleServerInfo(
            "127.0.0.1-20001", grpcShuffleServers.get(0).getIp(), fakePortList.get(0));
    fakedShuffleServerInfo1 =
        new ShuffleServerInfo(
            "127.0.0.1-20002", grpcShuffleServers.get(1).getIp(), fakePortList.get(1));
    fakedShuffleServerInfo2 =
        new ShuffleServerInfo(
            "127.0.0.1-20003", grpcShuffleServers.get(2).getIp(), fakePortList.get(2));
    fakedShuffleServerInfo3 =
        new ShuffleServerInfo(
            "127.0.0.1-20004", grpcShuffleServers.get(2).getIp(), fakePortList.get(3));
    fakedShuffleServerInfo4 =
        new ShuffleServerInfo(
            "127.0.0.1-20005", grpcShuffleServers.get(2).getIp(), fakePortList.get(4));

    // spark.rss.data.replica=3
    // spark.rss.data.replica.write=2
    // spark.rss.data.replica.read=2
    ((ShuffleServerGrpcClient)
            ShuffleServerClientFactory.getInstance()
                .getShuffleServerClient("GRPC", shuffleServerInfo0))
        .adjustTimeout(300);
    ((ShuffleServerGrpcClient)
            ShuffleServerClientFactory.getInstance()
                .getShuffleServerClient("GRPC", shuffleServerInfo1))
        .adjustTimeout(300);
    ((ShuffleServerGrpcClient)
            ShuffleServerClientFactory.getInstance()
                .getShuffleServerClient("GRPC", shuffleServerInfo2))
        .adjustTimeout(300);

    Thread.sleep(2000);
  }

  @AfterEach
  public void cleanEnv() throws Exception {
    if (shuffleWriteClientImpl != null) {
      shuffleWriteClientImpl.close();
    }
    shutdownServers();
  }

  @Test
  public void quorumConfigTest() throws Exception {
    try {
      RssUtils.checkQuorumSetting(3, 1, 1);
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Replica config is unsafe"));
    }
    try {
      RssUtils.checkQuorumSetting(3, 4, 1);
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Replica config is invalid"));
    }
    try {
      RssUtils.checkQuorumSetting(0, 0, 0);
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Replica config is invalid"));
    }
  }

  @Test
  public void rpcFailedTest() throws Exception {
    String testAppId = "rpcFailedTest";
    registerShuffleServer(testAppId, 3, 2, 2, true);
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();

    // case1: When only 1 server is failed, the block sending should success
    List<ShuffleBlockInfo> blocks =
        createShuffleBlockList(
            0,
            0,
            0,
            3,
            25,
            blockIdBitmap,
            expectedData,
            Lists.newArrayList(shuffleServerInfo0, shuffleServerInfo1, fakedShuffleServerInfo2));

    SendShuffleDataResult result = shuffleWriteClientImpl.sendShuffleData(testAppId, blocks);
    Roaring64NavigableMap failedBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap succBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    for (Long blockId : result.getSuccessBlockIds()) {
      succBlockIdBitmap.addLong(blockId);
    }
    for (Long blockId : result.getFailedBlockIds()) {
      failedBlockIdBitmap.addLong(blockId);
    }
    assertEquals(0, failedBlockIdBitmap.getLongCardinality());
    assertEquals(blockIdBitmap, succBlockIdBitmap);

    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);

    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(
                Lists.newArrayList(shuffleServerInfo0, shuffleServerInfo1, fakedShuffleServerInfo2))
            .build();
    // The data should be read
    validateResult(readClient, expectedData);

    // case2: When 2 servers are failed, the block sending should fail
    blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    blocks =
        createShuffleBlockList(
            0,
            0,
            0,
            3,
            25,
            blockIdBitmap,
            expectedData,
            Lists.newArrayList(
                shuffleServerInfo0, fakedShuffleServerInfo1, fakedShuffleServerInfo2));
    result = shuffleWriteClientImpl.sendShuffleData(testAppId, blocks);
    failedBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    succBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    for (Long blockId : result.getSuccessBlockIds()) {
      succBlockIdBitmap.addLong(blockId);
    }
    for (Long blockId : result.getFailedBlockIds()) {
      failedBlockIdBitmap.addLong(blockId);
    }
    assertEquals(blockIdBitmap, failedBlockIdBitmap);
    assertEquals(0, succBlockIdBitmap.getLongCardinality());

    // The client should not read any data, because write is failed
    assertEquals(readClient.readShuffleBlockData(), null);
  }

  private void enableTimeout(MockedShuffleServer server, long timeout) {
    ((MockedGrpcServer) server.getServer()).getService().enableMockedTimeout(timeout);
  }

  private void disableTimeout(MockedShuffleServer server) {
    ((MockedGrpcServer) server.getServer()).getService().disableMockedTimeout();
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
                .retryMax(2)
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
        Lists.newArrayList(
            shuffleServerInfo0,
            shuffleServerInfo1,
            shuffleServerInfo2,
            shuffleServerInfo3,
            shuffleServerInfo4);

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

  @Test
  public void case1() throws Exception {
    String testAppId = "case1";
    registerShuffleServer(testAppId, 3, 2, 2, true);
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();

    // only 1 server is timeout, the block sending should success
    enableTimeout((MockedShuffleServer) grpcShuffleServers.get(2), 500);

    // report result should success
    Map<Integer, Set<Long>> partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(0, Sets.newHashSet(blockIdBitmap.stream().iterator()));
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds = Maps.newHashMap();
    serverToPartitionToBlockIds.put(shuffleServerInfo0, partitionToBlockIds);
    serverToPartitionToBlockIds.put(shuffleServerInfo1, partitionToBlockIds);
    serverToPartitionToBlockIds.put(shuffleServerInfo2, partitionToBlockIds);
    shuffleWriteClientImpl.reportShuffleResult(serverToPartitionToBlockIds, testAppId, 0, 0L, 1);
    Roaring64NavigableMap report =
        shuffleWriteClientImpl.getShuffleResult(
            "GRPC",
            Sets.newHashSet(shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2),
            testAppId,
            0,
            0);
    assertEquals(report, blockIdBitmap);

    // data read should success
    Map<Long, byte[]> expectedData = Maps.newHashMap();
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
    Roaring64NavigableMap succBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    for (Long blockId : result.getSuccessBlockIds()) {
      succBlockIdBitmap.addLong(blockId);
    }
    assertEquals(0, result.getFailedBlockIds().size());
    assertEquals(blockIdBitmap, succBlockIdBitmap);

    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(
                Lists.newArrayList(shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2))
            .build();
    validateResult(readClient, expectedData);
  }

  @Test
  public void case2() throws Exception {
    String testAppId = "case2";
    registerShuffleServer(testAppId, 3, 2, 2, true);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    // When 2 servers are timeout, the block sending should fail
    enableTimeout((MockedShuffleServer) grpcShuffleServers.get(1), 500);
    enableTimeout((MockedShuffleServer) grpcShuffleServers.get(2), 500);
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
    for (Long blockId : result.getFailedBlockIds()) {
      failedBlockIdBitmap.addLong(blockId);
    }
    assertEquals(blockIdBitmap, failedBlockIdBitmap);
    assertEquals(0, result.getSuccessBlockIds().size());

    // report result should fail
    Map<Integer, Set<Long>> partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(0, Sets.newHashSet(blockIdBitmap.stream().iterator()));
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds = Maps.newHashMap();
    serverToPartitionToBlockIds.put(shuffleServerInfo0, partitionToBlockIds);
    serverToPartitionToBlockIds.put(shuffleServerInfo1, partitionToBlockIds);
    serverToPartitionToBlockIds.put(shuffleServerInfo2, partitionToBlockIds);
    try {
      shuffleWriteClientImpl.reportShuffleResult(serverToPartitionToBlockIds, testAppId, 0, 0L, 1);
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Quorum check of report shuffle result is failed"));
    }
    //  get result should also fail
    try {
      shuffleWriteClientImpl.getShuffleResult(
          "GRPC",
          Sets.newHashSet(shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2),
          testAppId,
          0,
          0);
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Get shuffle result is failed"));
    }
  }

  @Test
  public void case3() throws Exception {
    String testAppId = "case3";
    registerShuffleServer(testAppId, 3, 2, 2, true);
    disableTimeout((MockedShuffleServer) grpcShuffleServers.get(0));
    disableTimeout((MockedShuffleServer) grpcShuffleServers.get(1));
    disableTimeout((MockedShuffleServer) grpcShuffleServers.get(2));

    // When 1 server is timeout and 1 server is failed after sending, the block sending should fail
    enableTimeout((MockedShuffleServer) grpcShuffleServers.get(2), 500);

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
    Roaring64NavigableMap succBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    for (Long blockId : result.getSuccessBlockIds()) {
      succBlockIdBitmap.addLong(blockId);
    }
    for (Long blockId : result.getFailedBlockIds()) {
      failedBlockIdBitmap.addLong(blockId);
    }
    assertEquals(blockIdBitmap, succBlockIdBitmap);
    assertEquals(0, failedBlockIdBitmap.getLongCardinality());

    Map<Integer, Set<Long>> partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(0, Sets.newHashSet(blockIdBitmap.stream().iterator()));
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds = Maps.newHashMap();
    serverToPartitionToBlockIds.put(shuffleServerInfo0, partitionToBlockIds);
    serverToPartitionToBlockIds.put(shuffleServerInfo1, partitionToBlockIds);
    serverToPartitionToBlockIds.put(shuffleServerInfo2, partitionToBlockIds);

    shuffleWriteClientImpl.reportShuffleResult(serverToPartitionToBlockIds, testAppId, 0, 0L, 1);

    Roaring64NavigableMap report =
        shuffleWriteClientImpl.getShuffleResult(
            "GRPC",
            Sets.newHashSet(shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2),
            testAppId,
            0,
            0);
    assertEquals(report, blockIdBitmap);

    // let this server be failed, the reading will be also be failed
    grpcShuffleServers.get(1).stopServer();
    try {
      report =
          shuffleWriteClientImpl.getShuffleResult(
              "GRPC",
              Sets.newHashSet(shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2),
              testAppId,
              0,
              0);
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Get shuffle result is failed"));
    }

    // When the timeout of one server is recovered, the block sending should success
    disableTimeout((MockedShuffleServer) grpcShuffleServers.get(2));
    report =
        shuffleWriteClientImpl.getShuffleResult(
            "GRPC",
            Sets.newHashSet(shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2),
            testAppId,
            0,
            0);
    assertEquals(report, blockIdBitmap);
  }

  @Test
  public void case4() throws Exception {
    String testAppId = "case4";
    registerShuffleServer(testAppId, 3, 2, 2, true);
    // when 1 server is timeout, the sending multiple blocks should success
    enableTimeout((MockedShuffleServer) grpcShuffleServers.get(2), 500);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    for (int i = 0; i < 5; i++) {
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
      assertTrue(result.getSuccessBlockIds().size() == 3);
      assertTrue(result.getFailedBlockIds().size() == 0);
    }

    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(
                Lists.newArrayList(shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2))
            .build();
    validateResult(readClient, expectedData);
  }

  @Test
  public void case5(@TempDir File tmpDir) throws Exception {
    // this case is to simulate server restarting.
    String testAppId = "case5";
    registerShuffleServer(testAppId, 3, 2, 2, true);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    final List<ShuffleBlockInfo> blocks =
        createShuffleBlockList(
            0,
            0,
            0,
            3,
            25,
            blockIdBitmap,
            expectedData,
            Lists.newArrayList(shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2));

    // report result should success
    Map<Integer, Set<Long>> partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(0, Sets.newHashSet(blockIdBitmap.stream().iterator()));
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds = Maps.newHashMap();
    serverToPartitionToBlockIds.put(shuffleServerInfo0, partitionToBlockIds);
    serverToPartitionToBlockIds.put(shuffleServerInfo1, partitionToBlockIds);
    serverToPartitionToBlockIds.put(shuffleServerInfo2, partitionToBlockIds);
    shuffleWriteClientImpl.reportShuffleResult(serverToPartitionToBlockIds, testAppId, 0, 0L, 1);
    Roaring64NavigableMap report =
        shuffleWriteClientImpl.getShuffleResult(
            "GRPC",
            Sets.newHashSet(shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2),
            testAppId,
            0,
            0);
    assertEquals(report, blockIdBitmap);

    // data read should success
    SendShuffleDataResult result = shuffleWriteClientImpl.sendShuffleData(testAppId, blocks);
    Roaring64NavigableMap succBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    for (Long blockId : result.getSuccessBlockIds()) {
      succBlockIdBitmap.addLong(blockId);
    }
    assertEquals(0, result.getFailedBlockIds().size());
    assertEquals(blockIdBitmap, succBlockIdBitmap);

    // when one server is restarted, getShuffleResult should success
    grpcShuffleServers.get(1).stopServer();
    ShuffleServerConf shuffleServerConf1 = buildServerConf(5, tmpDir);
    shuffleServerConf1.setString("rss.coordinator.quorum", getQuorum());
    shuffleServerConf1.setInteger(RssBaseConf.RPC_SERVER_PORT, 0);
    shuffleServerConf1.setInteger(RssBaseConf.JETTY_HTTP_PORT, 0);
    grpcShuffleServers.set(1, new MockedShuffleServer(shuffleServerConf1));
    grpcShuffleServers.get(1).start();
    report =
        shuffleWriteClientImpl.getShuffleResult(
            "GRPC",
            Sets.newHashSet(shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2),
            testAppId,
            0,
            0);
    assertEquals(report, blockIdBitmap);

    // when two servers are restarted, getShuffleResult should fail
    grpcShuffleServers.get(2).stopServer();
    ShuffleServerConf shuffleServerConf2 = buildServerConf(5, tmpDir);
    shuffleServerConf2.setString("rss.coordinator.quorum", getQuorum());
    shuffleServerConf2.setInteger(RssBaseConf.RPC_SERVER_PORT, 0);
    shuffleServerConf2.setInteger(RssBaseConf.JETTY_HTTP_PORT, 0);
    grpcShuffleServers.set(2, new MockedShuffleServer(shuffleServerConf2));
    grpcShuffleServers.get(2).start();
    try {
      report =
          shuffleWriteClientImpl.getShuffleResult(
              "GRPC",
              Sets.newHashSet(shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2),
              testAppId,
              0,
              0);
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Get shuffle result is failed"));
    }
  }

  @Test
  public void case6() throws Exception {
    String testAppId = "case6";
    registerShuffleServer(testAppId, 3, 2, 2, true);
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap0 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap2 = Roaring64NavigableMap.bitmapOf();

    List<ShuffleBlockInfo> partition0 =
        createShuffleBlockList(
            0,
            0,
            0,
            3,
            25,
            blockIdBitmap0,
            expectedData,
            Lists.newArrayList(shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2));
    List<ShuffleBlockInfo> partition1 =
        createShuffleBlockList(
            0,
            0,
            0,
            3,
            25,
            blockIdBitmap1,
            expectedData,
            Lists.newArrayList(shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2));
    List<ShuffleBlockInfo> partition2 =
        createShuffleBlockList(
            0,
            0,
            0,
            3,
            25,
            blockIdBitmap2,
            expectedData,
            Lists.newArrayList(shuffleServerInfo2, shuffleServerInfo3, shuffleServerInfo4));

    // server 0,1,2 are ok, server 3,4 are timeout
    enableTimeout((MockedShuffleServer) grpcShuffleServers.get(3), 2000);
    enableTimeout((MockedShuffleServer) grpcShuffleServers.get(4), 2000);

    Map<Integer, Set<Long>> partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(0, Sets.newHashSet(blockIdBitmap0.stream().iterator()));
    partitionToBlockIds.put(1, Sets.newHashSet(blockIdBitmap1.stream().iterator()));
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds = Maps.newHashMap();
    serverToPartitionToBlockIds.put(shuffleServerInfo0, partitionToBlockIds);
    serverToPartitionToBlockIds.put(shuffleServerInfo1, partitionToBlockIds);
    serverToPartitionToBlockIds.put(shuffleServerInfo2, partitionToBlockIds);

    Map<Integer, Set<Long>> partitionToBlockIds2 = Maps.newHashMap();
    partitionToBlockIds2.put(2, Sets.newHashSet(blockIdBitmap2.stream().iterator()));
    serverToPartitionToBlockIds.put(shuffleServerInfo3, partitionToBlockIds2);
    serverToPartitionToBlockIds.put(shuffleServerInfo4, partitionToBlockIds2);
    // report result should fail because partition2 is failed to report server 3,4
    try {
      shuffleWriteClientImpl.reportShuffleResult(serverToPartitionToBlockIds, testAppId, 0, 0L, 1);
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Quorum check of report shuffle result is failed"));
    }
  }

  @Test
  public void case7() throws Exception {
    String testAppId = "case7";
    registerShuffleServer(testAppId, 3, 2, 2, true);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);

    // attempt to send data to "all servers", but only the server 0,1 receive data actually
    for (int i = 0; i < 5; i++) {
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
      assertTrue(result.getSuccessBlockIds().size() == 3);
      assertTrue(result.getFailedBlockIds().size() == 0);
    }

    // we cannot read any blocks from server 2
    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(Lists.newArrayList(shuffleServerInfo2))
            .build();
    assertTrue(readClient.readShuffleBlockData() == null);

    // we can read blocks from server 0,1
    readClient =
        baseReadBuilder()
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(Lists.newArrayList(shuffleServerInfo0, shuffleServerInfo1))
            .build();
    validateResult(readClient, expectedData);

    // we can also read blocks from server 0,1,2
    readClient =
        baseReadBuilder()
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(
                Lists.newArrayList(shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2))
            .build();
    validateResult(readClient, expectedData);
  }

  @Test
  public void case8() throws Exception {
    String testAppId = "case8";
    registerShuffleServer(testAppId, 3, 2, 2, true);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);

    // attempt to send data to "all servers", but only the server 0,2 receive data actually
    // primary round: server 0/1
    // secondary round: server 2
    for (int i = 0; i < 5; i++) {
      List<ShuffleBlockInfo> blocks =
          createShuffleBlockList(
              0,
              0,
              0,
              3,
              25,
              blockIdBitmap,
              expectedData,
              Lists.newArrayList(shuffleServerInfo0, fakedShuffleServerInfo1, shuffleServerInfo2));
      SendShuffleDataResult result = shuffleWriteClientImpl.sendShuffleData(testAppId, blocks);
      assertTrue(result.getSuccessBlockIds().size() == 3);
      assertTrue(result.getFailedBlockIds().size() == 0);
    }

    // we cannot read any blocks from server 1
    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(Lists.newArrayList(shuffleServerInfo1))
            .build();
    assertTrue(readClient.readShuffleBlockData() == null);

    // we can read blocks from server 2, which is sent in to secondary round
    readClient =
        baseReadBuilder()
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(Lists.newArrayList(shuffleServerInfo2))
            .build();
    validateResult(readClient, expectedData);

    // we can read blocks from server 0,1,2
    readClient =
        baseReadBuilder()
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(
                Lists.newArrayList(shuffleServerInfo0, fakedShuffleServerInfo1, shuffleServerInfo2))
            .build();
    validateResult(readClient, expectedData);
  }

  @Test
  public void case9() throws Exception {
    String testAppId = "case9";
    // test different quorum configurations:[5,3,3]
    registerShuffleServer(testAppId, 5, 3, 3, true);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);

    // attempt to send data to "all servers", but only the server 0,1,2 receive data actually
    for (int i = 0; i < 5; i++) {
      List<ShuffleBlockInfo> blocks =
          createShuffleBlockList(
              0,
              0,
              0,
              3,
              25,
              blockIdBitmap,
              expectedData,
              Lists.newArrayList(
                  shuffleServerInfo0,
                  shuffleServerInfo1,
                  shuffleServerInfo2,
                  shuffleServerInfo3,
                  shuffleServerInfo4));
      SendShuffleDataResult result = shuffleWriteClientImpl.sendShuffleData(testAppId, blocks);
      assertTrue(result.getSuccessBlockIds().size() == 3);
      assertTrue(result.getFailedBlockIds().size() == 0);
    }

    // we cannot read any blocks from server 3, 4
    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(Lists.newArrayList(shuffleServerInfo3, shuffleServerInfo4))
            .build();
    assertTrue(readClient.readShuffleBlockData() == null);

    // we can also read blocks from server 0,1,2
    readClient =
        baseReadBuilder()
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(
                Lists.newArrayList(shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2))
            .build();
    validateResult(readClient, expectedData);
  }

  @Test
  public void case10() throws Exception {
    String testAppId = "case10";
    // test different quorum configurations:[5,3,3]
    registerShuffleServer(testAppId, 5, 3, 3, true);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);

    // attempt to send data to "all servers", but the secondary round is activated due to failures
    // in primary round.
    for (int i = 0; i < 5; i++) {
      List<ShuffleBlockInfo> blocks =
          createShuffleBlockList(
              0,
              0,
              0,
              3,
              25,
              blockIdBitmap,
              expectedData,
              Lists.newArrayList(
                  shuffleServerInfo0,
                  fakedShuffleServerInfo1,
                  shuffleServerInfo2,
                  shuffleServerInfo3,
                  shuffleServerInfo4));
      SendShuffleDataResult result = shuffleWriteClientImpl.sendShuffleData(testAppId, blocks);
      assertEquals(3, result.getSuccessBlockIds().size());
      assertEquals(0, result.getFailedBlockIds().size());
    }

    // we cannot read any blocks from server 1 due to failures
    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(Lists.newArrayList(shuffleServerInfo1))
            .build();
    assertTrue(readClient.readShuffleBlockData() == null);

    // we can also read blocks from server 3,4
    readClient =
        baseReadBuilder()
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(Lists.newArrayList(shuffleServerInfo3, shuffleServerInfo4))
            .build();
    validateResult(readClient, expectedData);
  }

  @Test
  public void case11() throws Exception {
    String testAppId = "case11";
    // test different quorum configurations:[5,4,2]
    registerShuffleServer(testAppId, 5, 4, 2, true);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);

    // attempt to send data to "all servers", but only the server 0,1,2 receive data actually
    for (int i = 0; i < 5; i++) {
      List<ShuffleBlockInfo> blocks =
          createShuffleBlockList(
              0,
              0,
              0,
              3,
              25,
              blockIdBitmap,
              expectedData,
              Lists.newArrayList(
                  shuffleServerInfo0,
                  shuffleServerInfo1,
                  shuffleServerInfo2,
                  shuffleServerInfo3,
                  shuffleServerInfo4));
      SendShuffleDataResult result = shuffleWriteClientImpl.sendShuffleData(testAppId, blocks);
      assertTrue(result.getSuccessBlockIds().size() == 3);
      assertTrue(result.getFailedBlockIds().size() == 0);
    }

    // we cannot read any blocks from server 4 because the secondary round is skipped
    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(Lists.newArrayList(shuffleServerInfo4))
            .build();
    assertTrue(readClient.readShuffleBlockData() == null);

    // we can read blocks from server 0,1,2,3
    readClient =
        baseReadBuilder()
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(
                Lists.newArrayList(
                    shuffleServerInfo0, shuffleServerInfo1, shuffleServerInfo2, shuffleServerInfo3))
            .build();
    validateResult(readClient, expectedData);
  }

  @Test
  public void case12() throws Exception {
    String testAppId = "case12";
    // test when replica skipping is disabled.
    registerShuffleServer(testAppId, 3, 2, 2, false);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);

    for (int i = 0; i < 5; i++) {
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
      assertTrue(result.getSuccessBlockIds().size() == 3);
      assertTrue(result.getFailedBlockIds().size() == 0);
    }

    // we can read blocks from server 0
    ShuffleReadClientImpl readClient =
        baseReadBuilder()
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(Lists.newArrayList(shuffleServerInfo0))
            .build();
    validateResult(readClient, expectedData);

    // we can also read blocks from server 1
    readClient =
        baseReadBuilder()
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(Lists.newArrayList(shuffleServerInfo1))
            .build();
    validateResult(readClient, expectedData);

    // we can also read blocks from server 2
    readClient =
        baseReadBuilder()
            .appId(testAppId)
            .blockIdBitmap(blockIdBitmap)
            .taskIdBitmap(taskIdBitmap)
            .shuffleServerInfoList(Lists.newArrayList(shuffleServerInfo2))
            .build();
    validateResult(readClient, expectedData);
  }

  protected void validateResult(
      ShuffleReadClientImpl readClient,
      Map<Long, byte[]> expectedData,
      Roaring64NavigableMap blockIdBitmap) {
    CompressedShuffleBlock csb = readClient.readShuffleBlockData();
    Roaring64NavigableMap matched = Roaring64NavigableMap.bitmapOf();
    while (csb != null && csb.getByteBuffer() != null) {
      for (Map.Entry<Long, byte[]> entry : expectedData.entrySet()) {
        if (compareByte(entry.getValue(), csb.getByteBuffer())) {
          matched.addLong(entry.getKey());
          break;
        }
      }
      csb = readClient.readShuffleBlockData();
    }
    assertTrue(blockIdBitmap.equals(matched));
  }
}
