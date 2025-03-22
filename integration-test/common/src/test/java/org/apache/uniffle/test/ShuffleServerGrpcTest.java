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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.UnsafeByteOperations;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcNettyClient;
import org.apache.uniffle.client.request.RssAppHeartBeatRequest;
import org.apache.uniffle.client.request.RssFinishShuffleRequest;
import org.apache.uniffle.client.request.RssGetShuffleDataRequest;
import org.apache.uniffle.client.request.RssGetShuffleIndexRequest;
import org.apache.uniffle.client.request.RssGetShuffleResultRequest;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssReportShuffleResultRequest;
import org.apache.uniffle.client.request.RssSendCommitRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.client.response.RssGetShuffleResultResponse;
import org.apache.uniffle.client.response.RssRegisterShuffleResponse;
import org.apache.uniffle.client.response.RssReportShuffleResultResponse;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.metrics.TestUtils;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerGrpcMetrics;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.server.storage.HybridStorageManager;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ShuffleServerGrpcTest extends IntegrationTestBase {

  private ShuffleServerGrpcClient grpcShuffleServerClient;
  private ShuffleServerGrpcNettyClient nettyShuffleServerClient;
  private final AtomicInteger atomicInteger = new AtomicInteger(0);
  private static final BlockIdLayout layout = BlockIdLayout.DEFAULT;
  private static final Long EVENT_THRESHOLD_SIZE = 2048L;
  private static final int GB = 1024 * 1024 * 1024;
  protected static final long FAILED_REQUIRE_ID = -1;

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    coordinatorConf.setLong(CoordinatorConf.COORDINATOR_APP_EXPIRED, 2000);
    storeCoordinatorConf(coordinatorConf);

    storeShuffleServerConf(buildShuffleServerConf(0, tmpDir, ServerType.GRPC));
    storeShuffleServerConf(buildShuffleServerConf(1, tmpDir, ServerType.GRPC_NETTY));

    startServersWithRandomPorts();
  }

  private static ShuffleServerConf buildShuffleServerConf(
      int subDirIndex, File tmpDir, ServerType serverType) {
    ShuffleServerConf shuffleServerConf =
        shuffleServerConfWithoutPort(subDirIndex, tmpDir, serverType);
    shuffleServerConf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    shuffleServerConf.set(
        ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, EVENT_THRESHOLD_SIZE);
    shuffleServerConf.set(RssBaseConf.RPC_METRICS_ENABLED, true);
    shuffleServerConf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 2000L);
    shuffleServerConf.set(ShuffleServerConf.SERVER_PRE_ALLOCATION_EXPIRED, 5000L);
    shuffleServerConf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 1024 * 1024 * 50L);
    shuffleServerConf.set(ShuffleServerConf.HUGE_PARTITION_SIZE_THRESHOLD, 1024 * 1024 * 10L);
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

  @Test
  public void clearResourceTest() throws Exception {
    final ShuffleWriteClient shuffleWriteClient =
        ShuffleClientFactory.getInstance()
            .createShuffleWriteClient(
                ShuffleClientFactory.newWriteBuilder()
                    .clientType("GRPC")
                    .retryMax(2)
                    .retryIntervalMax(10000L)
                    .heartBeatThreadNum(4)
                    .replica(1)
                    .replicaWrite(1)
                    .replicaRead(1)
                    .replicaSkipEnabled(true)
                    .dataTransferPoolSize(1)
                    .dataCommitPoolSize(1)
                    .unregisterThreadPoolSize(10)
                    .unregisterTimeSec(10)
                    .unregisterRequestTimeSec(10));
    shuffleWriteClient.registerCoordinators(getQuorum());
    shuffleWriteClient.registerShuffle(
        new ShuffleServerInfo(LOCALHOST, grpcShuffleServers.get(0).getGrpcPort()),
        "application_clearResourceTest1",
        0,
        Lists.newArrayList(new PartitionRange(0, 1)),
        new RemoteStorageInfo(""),
        ShuffleDataDistributionType.NORMAL,
        -1);
    shuffleWriteClient.registerApplicationInfo("application_clearResourceTest1", 500L, "user");
    shuffleWriteClient.sendAppHeartbeat("application_clearResourceTest1", 500L);
    shuffleWriteClient.registerApplicationInfo("application_clearResourceTest2", 500L, "user");
    shuffleWriteClient.sendAppHeartbeat("application_clearResourceTest2", 500L);

    RssRegisterShuffleRequest rrsr =
        new RssRegisterShuffleRequest(
            "application_clearResourceTest1", 0, Lists.newArrayList(new PartitionRange(0, 1)), "");
    grpcShuffleServerClient.registerShuffle(rrsr);
    rrsr =
        new RssRegisterShuffleRequest(
            "application_clearResourceTest2", 0, Lists.newArrayList(new PartitionRange(0, 1)), "");
    grpcShuffleServerClient.registerShuffle(rrsr);
    assertEquals(
        Sets.newHashSet("application_clearResourceTest1", "application_clearResourceTest2"),
        grpcShuffleServers.get(0).getShuffleTaskManager().getAppIds());

    // Thread will keep refresh clearResourceTest1 in coordinator
    Thread t =
        new Thread(
            () -> {
              int i = 0;
              while (i < 20) {
                shuffleWriteClient.sendAppHeartbeat("application_clearResourceTest1", 500L);
                i++;
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                  return;
                }
              }
            });
    t.start();

    // Heartbeat is sent to coordinator too]
    Thread.sleep(3000);
    grpcShuffleServerClient.registerShuffle(
        new RssRegisterShuffleRequest(
            "application_clearResourceTest1", 0, Lists.newArrayList(new PartitionRange(0, 1)), ""));
    assertEquals(
        Sets.newHashSet("application_clearResourceTest1"),
        coordinators.get(0).getApplicationManager().getAppIds());
    // clearResourceTest2 will be removed because of rss.server.app.expired.withoutHeartbeat
    Thread.sleep(2000);
    assertEquals(
        Sets.newHashSet("application_clearResourceTest1"),
        grpcShuffleServers.get(0).getShuffleTaskManager().getAppIds());

    // clearResourceTest1 will be removed because of rss.server.app.expired.withoutHeartbeat
    t.interrupt();
    Awaitility.await()
        .timeout(20, TimeUnit.SECONDS)
        .until(() -> grpcShuffleServers.get(0).getShuffleTaskManager().getAppIds().size() == 0);
    assertEquals(0, grpcShuffleServers.get(0).getShuffleTaskManager().getAppIds().size());
  }

  @Test
  public void shuffleResultTest() throws Exception {
    Map<Integer, List<Long>> partitionToBlockIds = Maps.newHashMap();
    List<Long> blockIds1 = getBlockIdList(1, 3);
    List<Long> blockIds2 = getBlockIdList(2, 2);
    List<Long> blockIds3 = getBlockIdList(3, 1);
    partitionToBlockIds.put(1, blockIds1);
    partitionToBlockIds.put(2, blockIds2);
    partitionToBlockIds.put(3, blockIds3);

    RssReportShuffleResultRequest request =
        new RssReportShuffleResultRequest("shuffleResultTest", 0, 0L, partitionToBlockIds, 1);
    try {
      grpcShuffleServerClient.reportShuffleResult(request);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("NO_REGISTER"));
    }

    RssGetShuffleResultRequest req =
        new RssGetShuffleResultRequest("shuffleResultTest", 1, 1, layout);
    try {
      grpcShuffleServerClient.getShuffleResult(req);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("NO_REGISTER"));
    }

    RssRegisterShuffleRequest rrsr =
        new RssRegisterShuffleRequest(
            "shuffleResultTest", 100, Lists.newArrayList(new PartitionRange(0, 1)), "");
    grpcShuffleServerClient.registerShuffle(rrsr);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 1, layout);
    RssGetShuffleResultResponse result = grpcShuffleServerClient.getShuffleResult(req);
    Roaring64NavigableMap blockIdBitmap = result.getBlockIdBitmap();
    assertEquals(Roaring64NavigableMap.bitmapOf(), blockIdBitmap);

    request = new RssReportShuffleResultRequest("shuffleResultTest", 0, 0L, partitionToBlockIds, 1);
    RssReportShuffleResultResponse response = grpcShuffleServerClient.reportShuffleResult(request);
    assertEquals(StatusCode.SUCCESS, response.getStatusCode());
    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 1, layout);
    result = grpcShuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    Roaring64NavigableMap expectedP1 = Roaring64NavigableMap.bitmapOf();
    addExpectedBlockIds(expectedP1, blockIds1);
    assertEquals(expectedP1, blockIdBitmap);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 2, layout);
    result = grpcShuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    Roaring64NavigableMap expectedP2 = Roaring64NavigableMap.bitmapOf();
    addExpectedBlockIds(expectedP2, blockIds2);
    assertEquals(expectedP2, blockIdBitmap);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 3, layout);
    result = grpcShuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    Roaring64NavigableMap expectedP3 = Roaring64NavigableMap.bitmapOf();
    addExpectedBlockIds(expectedP3, blockIds3);
    assertEquals(expectedP3, blockIdBitmap);

    partitionToBlockIds = Maps.newHashMap();
    blockIds1 = getBlockIdList(1, 3);
    blockIds2 = getBlockIdList(2, 2);
    blockIds3 = getBlockIdList(3, 1);
    partitionToBlockIds.put(1, blockIds1);
    partitionToBlockIds.put(2, blockIds2);
    partitionToBlockIds.put(3, blockIds3);

    request = new RssReportShuffleResultRequest("shuffleResultTest", 0, 1L, partitionToBlockIds, 1);
    grpcShuffleServerClient.reportShuffleResult(request);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 1, layout);
    result = grpcShuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    addExpectedBlockIds(expectedP1, blockIds1);
    assertEquals(expectedP1, blockIdBitmap);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 2, layout);
    result = grpcShuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    addExpectedBlockIds(expectedP2, blockIds2);
    assertEquals(expectedP2, blockIdBitmap);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 3, layout);
    result = grpcShuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    addExpectedBlockIds(expectedP3, blockIds3);
    assertEquals(expectedP3, blockIdBitmap);

    request = new RssReportShuffleResultRequest("shuffleResultTest", 1, 1L, Maps.newHashMap(), 1);
    grpcShuffleServerClient.reportShuffleResult(request);
    req = new RssGetShuffleResultRequest("shuffleResultTest", 1, 1, layout);
    result = grpcShuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    assertEquals(Roaring64NavigableMap.bitmapOf(), blockIdBitmap);

    // test with bitmapNum > 1
    partitionToBlockIds = Maps.newHashMap();
    blockIds1 = getBlockIdList(1, 3);
    blockIds2 = getBlockIdList(2, 2);
    blockIds3 = getBlockIdList(3, 1);
    partitionToBlockIds.put(1, blockIds1);
    partitionToBlockIds.put(2, blockIds2);
    partitionToBlockIds.put(3, blockIds3);
    request = new RssReportShuffleResultRequest("shuffleResultTest", 2, 1L, partitionToBlockIds, 3);
    grpcShuffleServerClient.reportShuffleResult(request);
    // validate bitmap in shuffleTaskManager
    long bitmapNum =
        grpcShuffleServers
            .get(0)
            .getShuffleTaskManager()
            .getShuffleBlockIdManager()
            .getBitmapNum("shuffleResultTest", 2);
    assertEquals(3, bitmapNum);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 2, 1, layout);
    result = grpcShuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    expectedP1 = Roaring64NavigableMap.bitmapOf();
    addExpectedBlockIds(expectedP1, blockIds1);
    assertEquals(expectedP1, blockIdBitmap);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 2, 2, layout);
    result = grpcShuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    expectedP2 = Roaring64NavigableMap.bitmapOf();
    addExpectedBlockIds(expectedP2, blockIds2);
    assertEquals(expectedP2, blockIdBitmap);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 2, 3, layout);
    result = grpcShuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    expectedP3 = Roaring64NavigableMap.bitmapOf();
    addExpectedBlockIds(expectedP3, blockIds3);
    assertEquals(expectedP3, blockIdBitmap);

    partitionToBlockIds = Maps.newHashMap();
    blockIds1 = getBlockIdList(layout.maxPartitionId, 3);
    blockIds2 = getBlockIdList(2, 2);
    blockIds3 = getBlockIdList(3, 1);
    partitionToBlockIds.put(layout.maxPartitionId, blockIds1);
    partitionToBlockIds.put(2, blockIds2);
    partitionToBlockIds.put(3, blockIds3);
    // bimapNum = 2
    request = new RssReportShuffleResultRequest("shuffleResultTest", 4, 1L, partitionToBlockIds, 2);
    grpcShuffleServerClient.reportShuffleResult(request);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 4, layout.maxPartitionId, layout);
    result = grpcShuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    expectedP1 = Roaring64NavigableMap.bitmapOf();
    addExpectedBlockIds(expectedP1, blockIds1);
    assertEquals(expectedP1, blockIdBitmap);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 4, 2, layout);
    result = grpcShuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    expectedP2 = Roaring64NavigableMap.bitmapOf();
    addExpectedBlockIds(expectedP2, blockIds2);
    assertEquals(expectedP2, blockIdBitmap);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 4, 3, layout);
    result = grpcShuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    expectedP3 = Roaring64NavigableMap.bitmapOf();
    addExpectedBlockIds(expectedP3, blockIds3);
    assertEquals(expectedP3, blockIdBitmap);

    // wait resources are deleted
    Thread.sleep(12000);
    req = new RssGetShuffleResultRequest("shuffleResultTest", 1, 1, layout);
    try {
      grpcShuffleServerClient.getShuffleResult(req);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Can't get shuffle result"));
    }
  }

  @Test
  public void registerTest() {
    grpcShuffleServerClient.registerShuffle(
        new RssRegisterShuffleRequest(
            "registerTest", 0, Lists.newArrayList(new PartitionRange(0, 1)), ""));
    RssGetShuffleResultRequest req = new RssGetShuffleResultRequest("registerTest", 0, 0, layout);
    // no exception with getShuffleResult means register successfully
    grpcShuffleServerClient.getShuffleResult(req);
    req = new RssGetShuffleResultRequest("registerTest", 0, 1, layout);
    grpcShuffleServerClient.getShuffleResult(req);
    grpcShuffleServerClient.registerShuffle(
        new RssRegisterShuffleRequest(
            "registerTest",
            1,
            Lists.newArrayList(
                new PartitionRange(0, 0), new PartitionRange(1, 1), new PartitionRange(2, 2)),
            ""));
    req = new RssGetShuffleResultRequest("registerTest", 1, 0, layout);
    grpcShuffleServerClient.getShuffleResult(req);
    req = new RssGetShuffleResultRequest("registerTest", 1, 1, layout);
    grpcShuffleServerClient.getShuffleResult(req);
    req = new RssGetShuffleResultRequest("registerTest", 1, 2, layout);
    grpcShuffleServerClient.getShuffleResult(req);
    // registerShuffle with remote storage
    String appId1 = "remote_storage_register_app1";
    String appId2 = "remote_storage_register_app2";
    String remoteStorage = "hdfs://cluster1";
    grpcShuffleServerClient.registerShuffle(
        new RssRegisterShuffleRequest(
            appId1, 0, Lists.newArrayList(new PartitionRange(0, 1)), remoteStorage));
    ShuffleDataFlushEvent event1 =
        new ShuffleDataFlushEvent(1, appId1, 1, 1, 1, EVENT_THRESHOLD_SIZE + 1, null, null, null);
    assertEquals(
        remoteStorage,
        grpcShuffleServers.get(0).getStorageManager().selectStorage(event1).getStoragePath());
    ShuffleDataFlushEvent event2 =
        new ShuffleDataFlushEvent(1, appId2, 1, 1, 1, EVENT_THRESHOLD_SIZE + 1, null, null, null);
    try {
      // can't find storage info with appId2
      ((HybridStorageManager) grpcShuffleServers.get(0).getStorageManager())
          .getColdStorageManager()
          .selectStorage(event2)
          .getStoragePath();
      fail("Exception should be thrown with un-register appId");
    } catch (Exception e) {
      // expected exception, ignore
    }
    // appId -> remote storage won't change if register again with the different remote storage
    grpcShuffleServerClient.registerShuffle(
        new RssRegisterShuffleRequest(
            appId1, 0, Lists.newArrayList(new PartitionRange(0, 1)), remoteStorage + "another"));
    assertEquals(
        remoteStorage,
        grpcShuffleServers.get(0).getStorageManager().selectStorage(event1).getStoragePath());
  }

  private static Stream<Arguments> sendDataAndRequireBufferTestProvider() {
    return Stream.of(Arguments.of(true), Arguments.of(false));
  }

  @ParameterizedTest
  @MethodSource("sendDataAndRequireBufferTestProvider")
  private void sendDataAndRequireBufferTest(boolean isNettyMode) throws IOException {
    String appId = "sendDataAndRequireBufferTest";
    ShuffleServerGrpcClient shuffleServerClient =
        isNettyMode ? nettyShuffleServerClient : grpcShuffleServerClient;
    int shuffleId = 0;
    int partitionId = 0;
    // bigger than the config above: HUGE_PARTITION_SIZE_THRESHOLD : 1024 * 1024 * 10L
    int hugePartitionDataLength = 1024 * 1024 * 11;
    List<PartitionRange> partitionIds = Lists.newArrayList(new PartitionRange(0, 3));

    RssRegisterShuffleRequest registerShuffleRequest =
        new RssRegisterShuffleRequest(appId, shuffleId, partitionIds, "");
    RssRegisterShuffleResponse registerResponse =
        shuffleServerClient.registerShuffle(registerShuffleRequest);
    assertSame(StatusCode.SUCCESS, registerResponse.getStatusCode());

    List<ShuffleBlockInfo> blockInfos =
        Lists.newArrayList(
            new ShuffleBlockInfo(
                shuffleId,
                partitionId,
                0,
                hugePartitionDataLength,
                0,
                new byte[hugePartitionDataLength],
                Lists.newArrayList(),
                0,
                100,
                0));

    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(partitionId, blockInfos);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(shuffleId, partitionToBlocks);

    RssSendShuffleDataRequest sendShuffleDataRequest =
        new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    RssSendShuffleDataResponse response =
        shuffleServerClient.sendShuffleData(sendShuffleDataRequest);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());

    // trigger NoBufferForHugePartitionException and get FAILED_REQUIRE_ID
    long requireId =
        shuffleServerClient.requirePreAllocation(
            appId,
            shuffleId,
            Lists.newArrayList(partitionId),
            Lists.newArrayList(hugePartitionDataLength),
            hugePartitionDataLength,
            3,
            100);
    assertEquals(FAILED_REQUIRE_ID, requireId);

    // Add NoBufferForHugePartitionException check
    // and ShuffleServerMetrics.TOTAL_REQUIRE_BUFFER_FAILED_FOR_HUGE_PARTITION metric should be 1
    int jettyPort =
        isNettyMode
            ? nettyShuffleServers.get(0).getJettyPort()
            : grpcShuffleServers.get(0).getJettyPort();
    String content =
        TestUtils.httpGet(String.format("http://127.0.0.1:%s/metrics/server", jettyPort));
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(content);
    JsonNode metricsNode = actualObj.get("metrics");
    boolean checkSuccess = false;
    for (int i = 0; i < metricsNode.size(); i++) {
      JsonNode metricsName = metricsNode.get(i).get("name");
      if (ShuffleServerMetrics.TOTAL_REQUIRE_BUFFER_FAILED_FOR_HUGE_PARTITION.equals(
          metricsName.textValue())) {
        double labelValues = mapper.convertValue(metricsNode.get(i).get("value"), Double.class);
        assertEquals(isNettyMode ? 4 : 8, labelValues); // There is retry in ShuffleServerGrpcClient
        checkSuccess = true;
        break;
      }
    }
    assertTrue(checkSuccess);

    partitionId = 3;
    List<ShuffleBlockInfo> blockInfos2 =
        Lists.newArrayList(
            new ShuffleBlockInfo(
                shuffleId,
                partitionId,
                0,
                hugePartitionDataLength,
                0,
                new byte[hugePartitionDataLength],
                Lists.newArrayList(),
                0,
                100,
                0));

    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks3 = Maps.newHashMap();
    partitionToBlocks3.put(partitionId, blockInfos2);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks2 = Maps.newHashMap();
    shuffleToBlocks2.put(shuffleId, partitionToBlocks3);

    sendShuffleDataRequest = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks2);
    response = shuffleServerClient.sendShuffleData(sendShuffleDataRequest);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());
  }

  private static Stream<Arguments> sendDataWithoutRegisterTestProvider() {
    return Stream.of(
        Arguments.of("sendDataWithoutRegisterTest_netty", true),
        Arguments.of("sendDataWithoutRegisterTest_grpc", false));
  }

  @ParameterizedTest
  @MethodSource("sendDataWithoutRegisterTestProvider")
  private void sendDataWithoutRegisterTest(String appId, boolean isNettyMode) {
    ShuffleServerGrpcClient shuffleServerClient =
        isNettyMode ? nettyShuffleServerClient : grpcShuffleServerClient;
    List<ShuffleBlockInfo> blockInfos =
        Lists.newArrayList(
            new ShuffleBlockInfo(0, 0, 0, 100, 0, new byte[100], Lists.newArrayList(), 0, 100, 0));
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blockInfos);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);

    RssSendShuffleDataRequest rssdr =
        new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    RssSendShuffleDataResponse response = shuffleServerClient.sendShuffleData(rssdr);
    // NO_REGISTER
    List<ShuffleServer> shuffleServers = isNettyMode ? nettyShuffleServers : grpcShuffleServers;
    assertSame(StatusCode.INTERNAL_ERROR, response.getStatusCode());
    assertEquals(0, shuffleServers.get(0).getPreAllocatedMemory());
  }

  @Test
  public void sendDataWithoutRequirePreAllocation() {
    String appId = "sendDataWithoutRequirePreAllocation";
    RssRegisterShuffleRequest registerShuffleRequest =
        new RssRegisterShuffleRequest(appId, 0, Lists.newArrayList(new PartitionRange(0, 0)), "");
    RssRegisterShuffleResponse registerResponse =
        grpcShuffleServerClient.registerShuffle(registerShuffleRequest);
    assertSame(StatusCode.SUCCESS, registerResponse.getStatusCode());

    List<ShuffleBlockInfo> blockInfos =
        Lists.newArrayList(
            new ShuffleBlockInfo(0, 0, 0, 100, 0, new byte[100], Lists.newArrayList(), 0, 100, 0));
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blockInfos);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);
    for (Map.Entry<Integer, Map<Integer, List<ShuffleBlockInfo>>> stb :
        shuffleToBlocks.entrySet()) {
      List<RssProtos.ShuffleData> shuffleData = Lists.newArrayList();
      for (Map.Entry<Integer, List<ShuffleBlockInfo>> ptb : stb.getValue().entrySet()) {
        List<RssProtos.ShuffleBlock> shuffleBlocks = Lists.newArrayList();
        for (ShuffleBlockInfo sbi : ptb.getValue()) {
          shuffleBlocks.add(
              RssProtos.ShuffleBlock.newBuilder()
                  .setBlockId(sbi.getBlockId())
                  .setCrc(sbi.getCrc())
                  .setLength(sbi.getLength())
                  .setTaskAttemptId(sbi.getTaskAttemptId())
                  .setUncompressLength(sbi.getUncompressLength())
                  .setData(UnsafeByteOperations.unsafeWrap(sbi.getData().nioBuffer()))
                  .build());
        }
        shuffleData.add(
            RssProtos.ShuffleData.newBuilder()
                .setPartitionId(ptb.getKey())
                .addAllBlock(shuffleBlocks)
                .build());
      }

      RssProtos.SendShuffleDataRequest rpcRequest =
          RssProtos.SendShuffleDataRequest.newBuilder()
              .setAppId(appId)
              .setShuffleId(0)
              .setRequireBufferId(10000)
              .addAllShuffleData(shuffleData)
              .build();
      RssProtos.SendShuffleDataResponse response =
          grpcShuffleServerClient.getBlockingStub().sendShuffleData(rpcRequest);
      assertEquals(RssProtos.StatusCode.INTERNAL_ERROR, response.getStatus());
      assertTrue(response.getRetMsg().contains("Can't find requireBufferId[10000]"));
    }
  }

  public static Stream<Arguments> testBlockIdLayouts() {
    return Stream.of(
        Arguments.of(BlockIdLayout.DEFAULT), Arguments.of(BlockIdLayout.from(20, 21, 22)));
  }

  @ParameterizedTest
  @MethodSource("testBlockIdLayouts")
  public void multipleShuffleResultTest(BlockIdLayout layout) throws Exception {
    String appId = "multipleShuffleResultTest_" + layout.sequenceNoBits;
    Set<Long> expectedBlockIds = Sets.newConcurrentHashSet();
    RssRegisterShuffleRequest rrsr =
        new RssRegisterShuffleRequest(appId, 100, Lists.newArrayList(new PartitionRange(0, 1)), "");
    grpcShuffleServerClient.registerShuffle(rrsr);

    Runnable r1 =
        () -> {
          for (int i = 0; i < 100; i++) {
            Map<Integer, List<Long>> ptbs = Maps.newHashMap();
            List<Long> blockIds = Lists.newArrayList();
            Long blockId = layout.getBlockId(i, 1, 0);
            expectedBlockIds.add(blockId);
            blockIds.add(blockId);
            ptbs.put(1, blockIds);
            RssReportShuffleResultRequest req1 =
                new RssReportShuffleResultRequest(appId, 1, 0, ptbs, 1);
            grpcShuffleServerClient.reportShuffleResult(req1);
          }
        };
    Runnable r2 =
        () -> {
          for (int i = 100; i < 200; i++) {
            Map<Integer, List<Long>> ptbs = Maps.newHashMap();
            List<Long> blockIds = Lists.newArrayList();
            Long blockId = layout.getBlockId(i, 1, 1);
            expectedBlockIds.add(blockId);
            blockIds.add(blockId);
            ptbs.put(1, blockIds);
            RssReportShuffleResultRequest req1 =
                new RssReportShuffleResultRequest(appId, 1, 1, ptbs, 1);
            grpcShuffleServerClient.reportShuffleResult(req1);
          }
        };
    Runnable r3 =
        () -> {
          for (int i = 200; i < 300; i++) {
            Map<Integer, List<Long>> ptbs = Maps.newHashMap();
            List<Long> blockIds = Lists.newArrayList();
            Long blockId = layout.getBlockId(i, 1, 2);
            expectedBlockIds.add(blockId);
            blockIds.add(blockId);
            ptbs.put(1, blockIds);
            RssReportShuffleResultRequest req1 =
                new RssReportShuffleResultRequest(appId, 1, 2, ptbs, 1);
            grpcShuffleServerClient.reportShuffleResult(req1);
          }
        };
    Thread t1 = new Thread(r1);
    Thread t2 = new Thread(r2);
    Thread t3 = new Thread(r3);
    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    for (Long blockId : expectedBlockIds) {
      blockIdBitmap.addLong(blockId);
    }

    RssGetShuffleResultRequest req = new RssGetShuffleResultRequest(appId, 1, 1, layout);
    RssGetShuffleResultResponse result = grpcShuffleServerClient.getShuffleResult(req);
    Roaring64NavigableMap actualBlockIdBitmap = result.getBlockIdBitmap();
    assertEquals(blockIdBitmap, actualBlockIdBitmap);
  }

  @Disabled("flaky test")
  @Test
  public void rpcMetricsTest() throws Exception {
    String appId = "rpcMetricsTest";
    int shuffleId = 0;
    final double oldGrpcTotal =
        grpcShuffleServers.get(0).getGrpcMetrics().getCounterGrpcTotal().get();
    double oldValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.REGISTER_SHUFFLE_METHOD)
            .get();
    grpcShuffleServerClient.registerShuffle(
        new RssRegisterShuffleRequest(
            appId, shuffleId, Lists.newArrayList(new PartitionRange(0, 1)), ""));
    double newValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.REGISTER_SHUFFLE_METHOD)
            .get();
    assertEquals(oldValue + 1, newValue, 0.5);
    assertEquals(
        0,
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getGaugeMap()
            .get(ShuffleServerGrpcMetrics.REGISTER_SHUFFLE_METHOD)
            .get(),
        0.5);

    oldValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.APP_HEARTBEAT_METHOD)
            .get();
    grpcShuffleServerClient.sendHeartBeat(new RssAppHeartBeatRequest(appId, 10000));
    newValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.APP_HEARTBEAT_METHOD)
            .get();
    assertEquals(oldValue + 1, newValue, 0.5);
    assertEquals(
        0,
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getGaugeMap()
            .get(ShuffleServerGrpcMetrics.APP_HEARTBEAT_METHOD)
            .get(),
        0.5);

    oldValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.REQUIRE_BUFFER_METHOD)
            .get();
    grpcShuffleServerClient.requirePreAllocation(appId, 100, 10, 1000);
    newValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.REQUIRE_BUFFER_METHOD)
            .get();
    assertEquals(oldValue + 1, newValue, 0.5);
    assertEquals(
        0,
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getGaugeMap()
            .get(ShuffleServerGrpcMetrics.REQUIRE_BUFFER_METHOD)
            .get(),
        0.5);

    oldValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.SEND_SHUFFLE_DATA_METHOD)
            .get();
    List<ShuffleBlockInfo> blockInfos =
        Lists.newArrayList(
            new ShuffleBlockInfo(
                shuffleId, 0, 0, 100, 0, new byte[100], Lists.newArrayList(), 0, 100, 0));
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blockInfos);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);
    RssSendShuffleDataRequest rssdr =
        new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    grpcShuffleServerClient.sendShuffleData(rssdr);
    newValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.SEND_SHUFFLE_DATA_METHOD)
            .get();
    assertEquals(oldValue + 1, newValue, 0.5);
    assertEquals(
        0,
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getGaugeMap()
            .get(ShuffleServerGrpcMetrics.SEND_SHUFFLE_DATA_METHOD)
            .get(),
        0.5);

    oldValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.COMMIT_SHUFFLE_TASK_METHOD)
            .get();
    grpcShuffleServerClient.sendCommit(new RssSendCommitRequest(appId, shuffleId));
    newValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.COMMIT_SHUFFLE_TASK_METHOD)
            .get();
    assertEquals(oldValue + 1, newValue, 0.5);
    assertEquals(
        0,
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getGaugeMap()
            .get(ShuffleServerGrpcMetrics.COMMIT_SHUFFLE_TASK_METHOD)
            .get(),
        0.5);

    oldValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.FINISH_SHUFFLE_METHOD)
            .get();
    grpcShuffleServerClient.finishShuffle(new RssFinishShuffleRequest(appId, shuffleId));
    newValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.FINISH_SHUFFLE_METHOD)
            .get();
    assertEquals(oldValue + 1, newValue, 0.5);
    assertEquals(
        0,
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getGaugeMap()
            .get(ShuffleServerGrpcMetrics.FINISH_SHUFFLE_METHOD)
            .get(),
        0.5);

    oldValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.REPORT_SHUFFLE_RESULT_METHOD)
            .get();
    Map<Integer, List<Long>> partitionToBlockIds = Maps.newHashMap();
    List<Long> blockIds1 = getBlockIdList(1, 3);
    List<Long> blockIds2 = getBlockIdList(2, 2);
    List<Long> blockIds3 = getBlockIdList(3, 1);
    partitionToBlockIds.put(1, blockIds1);
    partitionToBlockIds.put(2, blockIds2);
    partitionToBlockIds.put(3, blockIds3);
    RssReportShuffleResultRequest request =
        new RssReportShuffleResultRequest(appId, shuffleId, 0L, partitionToBlockIds, 1);
    grpcShuffleServerClient.reportShuffleResult(request);
    newValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.REPORT_SHUFFLE_RESULT_METHOD)
            .get();
    assertEquals(oldValue + 1, newValue, 0.5);
    assertEquals(
        0,
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getGaugeMap()
            .get(ShuffleServerGrpcMetrics.REPORT_SHUFFLE_RESULT_METHOD)
            .get(),
        0.5);

    oldValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.GET_SHUFFLE_RESULT_METHOD)
            .get();
    grpcShuffleServerClient.getShuffleResult(
        new RssGetShuffleResultRequest(appId, shuffleId, 1, layout));
    newValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.GET_SHUFFLE_RESULT_METHOD)
            .get();
    assertEquals(oldValue + 1, newValue, 0.5);
    assertEquals(
        0,
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getGaugeMap()
            .get(ShuffleServerGrpcMetrics.GET_SHUFFLE_RESULT_METHOD)
            .get(),
        0.5);

    oldValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.GET_SHUFFLE_INDEX_METHOD)
            .get();
    try {
      grpcShuffleServerClient.getShuffleIndex(
          new RssGetShuffleIndexRequest(appId, shuffleId, 1, 1, 3));
    } catch (Exception e) {
      // ignore the exception, just test metrics value
    }
    newValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.GET_SHUFFLE_INDEX_METHOD)
            .get();
    assertEquals(oldValue + 1, newValue, 0.5);
    assertEquals(
        0,
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getGaugeMap()
            .get(ShuffleServerGrpcMetrics.GET_SHUFFLE_INDEX_METHOD)
            .get(),
        0.5);

    oldValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.GET_SHUFFLE_DATA_METHOD)
            .get();
    try {
      grpcShuffleServerClient.getShuffleData(
          new RssGetShuffleDataRequest(appId, shuffleId, 0, 1, 3, 0, 100));
    } catch (Exception e) {
      // ignore the exception, just test metrics value
    }
    newValue =
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(ShuffleServerGrpcMetrics.GET_SHUFFLE_DATA_METHOD)
            .get();
    assertEquals(oldValue + 1, newValue, 0.5);
    assertEquals(
        0,
        grpcShuffleServers
            .get(0)
            .getGrpcMetrics()
            .getGaugeMap()
            .get(ShuffleServerGrpcMetrics.GET_SHUFFLE_DATA_METHOD)
            .get(),
        0.5);

    double newGrpcTotal = grpcShuffleServers.get(0).getGrpcMetrics().getCounterGrpcTotal().get();
    // require buffer will be called one more time when send data
    assertEquals(oldGrpcTotal + 11, newGrpcTotal, 0.5);
    assertEquals(0, grpcShuffleServers.get(0).getGrpcMetrics().getGaugeGrpcOpen().get(), 0.5);

    oldValue = ShuffleServerMetrics.counterTotalRequireBufferFailed.get();
    // the next two allocations will fail
    assertEquals(grpcShuffleServerClient.requirePreAllocation(appId, GB, 0, 10), -1);
    assertEquals(grpcShuffleServerClient.requirePreAllocation(appId, GB, 0, 10), -1);
    // the next two allocations will success
    assertNotEquals(grpcShuffleServerClient.requirePreAllocation(appId, 10, 0, 10), -1);
    assertNotEquals(grpcShuffleServerClient.requirePreAllocation(appId, 10, 0, 10), -1);
    newValue = ShuffleServerMetrics.counterTotalRequireBufferFailed.get();
    assertEquals((int) newValue, (int) oldValue + 2);
  }

  private List<Long> getBlockIdList(int partitionId, int blockNum) {
    List<Long> blockIds = Lists.newArrayList();
    for (int i = 0; i < blockNum; i++) {
      blockIds.add(layout.getBlockId(atomicInteger.getAndIncrement(), partitionId, 0));
    }
    return blockIds;
  }

  private void addExpectedBlockIds(Roaring64NavigableMap bitmap, List<Long> blockIds) {
    for (int i = 0; i < blockIds.size(); i++) {
      bitmap.addLong(blockIds.get(i));
    }
  }
}
