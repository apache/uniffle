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

package org.apache.uniffle.server;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcNettyClient;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.client.response.RssRegisterShuffleResponse;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.metrics.TestUtils;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

public class TopNShuffleDataSizeOfAppCalcTaskTest {

  protected static List<ShuffleServer> shuffleServers = Lists.newArrayList();
  private static ShuffleServerConf grpcShuffleServerConfig;
  private static ShuffleServerConf nettyShuffleServerConfig;
  private static final Long EVENT_THRESHOLD_SIZE = 2048L;
  protected static final int SHUFFLE_SERVER_PORT = 20001;

  private static AtomicInteger serverRpcPortCounter = new AtomicInteger();
  private static AtomicInteger nettyPortCounter = new AtomicInteger();
  private static AtomicInteger jettyPortCounter = new AtomicInteger();

  static @TempDir File tempDir;

  protected static final String LOCALHOST;

  static {
    try {
      LOCALHOST = RssUtils.getHostIp();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected static final int COORDINATOR_PORT_1 = 19999;
  protected static final int NETTY_PORT = 21000;
  protected static final String COORDINATOR_QUORUM = LOCALHOST + ":" + COORDINATOR_PORT_1;

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    File dataDir1 = new File(tmpDir, "data1");
    String grpcBasePath = dataDir1.getAbsolutePath();
    ShuffleServerConf grpcShuffleServerConf = buildShuffleServerConf(ServerType.GRPC, grpcBasePath);
    createShuffleServer(grpcShuffleServerConf);

    File dataDir2 = new File(tmpDir, "data2");
    String nettyBasePath = dataDir2.getAbsolutePath();
    ShuffleServerConf nettyShuffleServerConf =
        buildShuffleServerConf(ServerType.GRPC_NETTY, nettyBasePath);
    createShuffleServer(nettyShuffleServerConf);

    startServers();
    grpcShuffleServerConfig = grpcShuffleServerConf;
    nettyShuffleServerConfig = nettyShuffleServerConf;
  }

  private static ShuffleServerConf buildShuffleServerConf(ServerType serverType, String basePath)
      throws Exception {
    ShuffleServerConf shuffleServerConf = buildShuffleServerConf(serverType);
    shuffleServerConf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    shuffleServerConf.set(
        ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, EVENT_THRESHOLD_SIZE);
    shuffleServerConf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(basePath));
    shuffleServerConf.set(ShuffleServerConf.RPC_METRICS_ENABLED, true);
    shuffleServerConf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 2000L);
    shuffleServerConf.set(ShuffleServerConf.SERVER_PRE_ALLOCATION_EXPIRED, 5000L);
    shuffleServerConf.set(ShuffleServerConf.TOP_N_APP_SHUFFLE_DATA_REFRESH_INTERVAL, 700);
    shuffleServerConf.set(ShuffleServerConf.TOP_N_APP_SHUFFLE_DATA_SIZE_NUMBER, 5);
    return shuffleServerConf;
  }

  protected static ShuffleServerConf buildShuffleServerConf(ServerType serverType)
      throws Exception {
    ShuffleServerConf serverConf = new ShuffleServerConf();
    serverConf.setInteger(
        "rss.rpc.server.port", SHUFFLE_SERVER_PORT + serverRpcPortCounter.getAndIncrement());
    serverConf.setString("rss.storage.type", StorageType.MEMORY_LOCALFILE_HDFS.name());
    serverConf.setString("rss.storage.basePath", tempDir.getAbsolutePath());
    serverConf.setString("rss.server.buffer.capacity", "671088640");
    serverConf.setString("rss.server.memory.shuffle.highWaterMark", "50.0");
    serverConf.setString("rss.server.memory.shuffle.lowWaterMark", "0.0");
    serverConf.setString("rss.server.read.buffer.capacity", "335544320");
    serverConf.setString("rss.coordinator.quorum", COORDINATOR_QUORUM);
    serverConf.setString("rss.server.heartbeat.delay", "1000");
    serverConf.setString("rss.server.heartbeat.interval", "1000");
    serverConf.setInteger("rss.jetty.http.port", 18080 + jettyPortCounter.getAndIncrement());
    serverConf.setInteger("rss.jetty.corePool.size", 64);
    serverConf.setInteger("rss.rpc.executor.size", 10);
    serverConf.setString("rss.server.hadoop.dfs.replication", "2");
    serverConf.setLong("rss.server.disk.capacity", 10L * 1024L * 1024L * 1024L);
    serverConf.setBoolean("rss.server.health.check.enable", false);
    serverConf.setBoolean(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    serverConf.set(ShuffleServerConf.SERVER_TRIGGER_FLUSH_CHECK_INTERVAL, 500L);
    serverConf.set(ShuffleServerConf.RPC_SERVER_TYPE, serverType);
    if (serverType == ServerType.GRPC_NETTY) {
      serverConf.setInteger(
          ShuffleServerConf.NETTY_SERVER_PORT, NETTY_PORT + nettyPortCounter.getAndIncrement());
    }
    return serverConf;
  }

  protected static void createShuffleServer(ShuffleServerConf serverConf) throws Exception {
    shuffleServers.add(new ShuffleServer(serverConf));
  }

  public static void startServers() throws Exception {
    for (ShuffleServer shuffleServer : shuffleServers) {
      shuffleServer.start();
    }
  }

  private void registerAndRequireBuffer(String appId, int length, boolean isNettyMode)
      throws Exception {
    ShuffleServerGrpcClient shuffleServerClient =
        isNettyMode
            ? new ShuffleServerGrpcNettyClient(
                LOCALHOST,
                nettyShuffleServerConfig.getInteger(ShuffleServerConf.RPC_SERVER_PORT),
                nettyShuffleServerConfig.getInteger(ShuffleServerConf.NETTY_SERVER_PORT))
            : new ShuffleServerGrpcClient(
                LOCALHOST, grpcShuffleServerConfig.getInteger(ShuffleServerConf.RPC_SERVER_PORT));
    int shuffleId = 0;
    int partitionId = 0;
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
                length,
                0,
                new byte[length],
                Lists.newArrayList(),
                0,
                100,
                0));

    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(partitionId, blockInfos);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(shuffleId, partitionToBlocks);

    RssSendShuffleDataRequest sendShuffleDataRequest =
        new RssSendShuffleDataRequest(appId, 1, 1000, shuffleToBlocks);
    RssSendShuffleDataResponse response =
        shuffleServerClient.sendShuffleData(sendShuffleDataRequest);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());
    shuffleServerClient.close();
  }

  private static Stream<Arguments> testTopNShuffleDataSizeOfAppCalcTaskProvider() {
    return Stream.of(Arguments.of(true), Arguments.of(false));
  }

  @ParameterizedTest
  @MethodSource("testTopNShuffleDataSizeOfAppCalcTaskProvider")
  public void testTopNShuffleDataSizeOfAppCalcTask(boolean isNettyMode) throws Exception {
    // Here is 6 app, but config max top n number is 5
    registerAndRequireBuffer("application_id_1", 1000, isNettyMode);
    registerAndRequireBuffer("application_id_2", 2000, isNettyMode);
    registerAndRequireBuffer("application_id_3", 3000, isNettyMode);
    registerAndRequireBuffer("application_id_4", 4000, isNettyMode);
    registerAndRequireBuffer("application_id_5", 5000, isNettyMode);
    registerAndRequireBuffer("application_id_6", 6000, isNettyMode);

    int jettyPort =
        isNettyMode
            ? nettyShuffleServerConfig.getInteger(ShuffleServerConf.JETTY_HTTP_PORT)
            : grpcShuffleServerConfig.getInteger(ShuffleServerConf.JETTY_HTTP_PORT);
    Set<String> expectedTopNApps =
        Sets.newHashSet(
            "[\"application_id_6\"]",
            "[\"application_id_5\"]",
            "[\"application_id_4\"]",
            "[\"application_id_3\"]",
            "[\"application_id_2\"]");
    // Both shuffle servers in this JVM share the static TopN gauges, and each server's TopN task
    // clears them before writing its own result, so poll until this server's result is visible.
    Awaitility.await()
        .timeout(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              String content =
                  TestUtils.httpGet(String.format("http://127.0.0.1:%s/metrics/server", jettyPort));
              JsonNode metricsNode = new ObjectMapper().readTree(content).get("metrics");
              assertEquals(
                  expectedTopNApps,
                  labelValuesOf(metricsNode, ShuffleServerMetrics.TOPN_OF_TOTAL_DATA_SIZE_FOR_APP));
              assertEquals(
                  expectedTopNApps,
                  labelValuesOf(
                      metricsNode, ShuffleServerMetrics.TOPN_OF_IN_MEMORY_DATA_SIZE_FOR_APP));
            });
  }

  private static Set<String> labelValuesOf(JsonNode metricsNode, String metricName) {
    Set<String> labelValues = new HashSet<>();
    for (int i = 0; i < metricsNode.size(); i++) {
      if (metricName.equals(metricsNode.get(i).get("name").textValue())) {
        Iterator<Map.Entry<String, JsonNode>> it = metricsNode.get(i).fields();
        while (it.hasNext()) {
          Map.Entry<String, JsonNode> entry = it.next();
          if ("labelValues".equalsIgnoreCase(entry.getKey())) {
            labelValues.add(entry.getValue().toString());
          }
        }
      }
    }
    return labelValues;
  }

  @AfterEach
  public void cleanMetrics() throws Exception {
    ShuffleServerMetrics.clear();
  }

  @AfterAll
  public static void shutdownServers() throws Exception {
    for (ShuffleServer shuffleServer : shuffleServers) {
      shuffleServer.stopServer();
    }
    shuffleServers = Lists.newArrayList();
    ShuffleServerMetrics.clear();
  }
}
