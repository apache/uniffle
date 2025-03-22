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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.TestUtils;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.impl.ShuffleReadClientImpl;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcNettyClient;
import org.apache.uniffle.client.request.RssFinishShuffleRequest;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssSendCommitRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.client.response.CompressedShuffleBlock;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.KerberizedHadoopBase;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.CoordinatorServer;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.test.ShuffleReadWriteBase.mockSSI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShuffleServerWithKerberizedHadoopTest extends KerberizedHadoopBase {

  protected static final String LOCALHOST;

  static {
    try {
      LOCALHOST = RssUtils.getHostIp();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private ShuffleServerGrpcClient grpcShuffleServerClient;
  private ShuffleServerGrpcNettyClient nettyShuffleServerClient;
  private static CoordinatorServer coordinatorServer;
  private static ShuffleServer grpcShuffleServer;
  private static ShuffleServer nettyShuffleServer;

  private static ShuffleServerConf getShuffleServerConf(
      int id, File tmpDir, int coordinatorRpcPort, ServerType serverType) throws Exception {
    ShuffleServerConf serverConf = new ShuffleServerConf();
    File dataDir1 = new File(tmpDir, id + "_1");
    File dataDir2 = new File(tmpDir, id + "_2");
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    serverConf.setInteger("rss.rpc.server.port", 0);
    serverConf.setString("rss.storage.basePath", basePath);
    serverConf.setString("rss.server.buffer.capacity", "671088640");
    serverConf.setString("rss.server.memory.shuffle.highWaterMark", "50.0");
    serverConf.setString("rss.server.memory.shuffle.lowWaterMark", "0.0");
    serverConf.setString("rss.server.read.buffer.capacity", "335544320");
    serverConf.setString("rss.coordinator.quorum", LOCALHOST + ":" + coordinatorRpcPort);
    serverConf.setString("rss.server.heartbeat.delay", "1000");
    serverConf.setString("rss.server.heartbeat.interval", "1000");
    serverConf.setInteger("rss.jetty.http.port", 0);
    serverConf.setInteger("rss.jetty.corePool.size", 64);
    serverConf.setInteger("rss.rpc.executor.size", 10);
    serverConf.setString("rss.server.hadoop.dfs.replication", "2");
    serverConf.setLong("rss.server.disk.capacity", 10L * 1024L * 1024L * 1024L);
    serverConf.setBoolean("rss.server.health.check.enable", false);
    serverConf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.HDFS.name());
    serverConf.setBoolean(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    serverConf.set(ShuffleServerConf.RPC_SERVER_TYPE, serverType);
    if (serverType == ServerType.GRPC_NETTY) {
      serverConf.setInteger(ShuffleServerConf.NETTY_SERVER_PORT, 0);
    }
    return serverConf;
  }

  @BeforeAll
  public static void setup(@TempDir File tempDir) throws Exception {
    testRunner = ShuffleServerWithKerberizedHadoopTest.class;
    KerberizedHadoopBase.init();

    CoordinatorConf coordinatorConf = new CoordinatorConf();
    coordinatorConf.setInteger(CoordinatorConf.RPC_SERVER_PORT, 0);
    coordinatorConf.setInteger(CoordinatorConf.JETTY_HTTP_PORT, 0);
    coordinatorConf.setInteger(CoordinatorConf.RPC_EXECUTOR_SIZE, 10);
    coordinatorServer = new CoordinatorServer(coordinatorConf);
    coordinatorServer.start();

    ShuffleServerConf grpcShuffleServerConf =
        getShuffleServerConf(0, tempDir, coordinatorServer.getRpcListenPort(), ServerType.GRPC);
    grpcShuffleServer = new ShuffleServer(grpcShuffleServerConf);
    grpcShuffleServer.start();

    ShuffleServerConf nettyShuffleServerConf =
        getShuffleServerConf(
            1, tempDir, coordinatorServer.getRpcListenPort(), ServerType.GRPC_NETTY);
    nettyShuffleServer = new ShuffleServer(nettyShuffleServerConf);
    nettyShuffleServer.start();
  }

  @AfterAll
  public static void afterAll() throws Exception {
    if (coordinatorServer != null) {
      coordinatorServer.stopServer();
    }
    if (grpcShuffleServer != null) {
      grpcShuffleServer.stopServer();
    }
    if (nettyShuffleServer != null) {
      nettyShuffleServer.stopServer();
    }
  }

  @BeforeEach
  public void beforeEach() throws Exception {
    initHadoopSecurityContext();
    grpcShuffleServerClient =
        new ShuffleServerGrpcClient(LOCALHOST, grpcShuffleServer.getGrpcPort());
    nettyShuffleServerClient =
        new ShuffleServerGrpcNettyClient(
            LOCALHOST, nettyShuffleServer.getGrpcPort(), nettyShuffleServer.getNettyPort());
  }

  @AfterEach
  public void clearEach() throws Exception {
    removeHadoopSecurityContext();
    grpcShuffleServerClient.close();
    nettyShuffleServerClient.close();
  }

  private Map<String, String> conf2Map(Configuration conf) {
    Map<String, String> confMap = new HashMap<>();
    for (Map.Entry<String, String> entry : conf) {
      confMap.put(entry.getKey(), entry.getValue());
    }
    return confMap;
  }

  private Map<Integer, List<ShuffleBlockInfo>> createTestData(
      Roaring64NavigableMap[] bitmaps, Map<Long, byte[]> expectedData) {
    for (int i = 0; i < 4; i++) {
      bitmaps[i] = Roaring64NavigableMap.bitmapOf();
    }
    List<ShuffleBlockInfo> blocks1 =
        ShuffleReadWriteBase.createShuffleBlockList(
            0, 0, 0, 3, 25, bitmaps[0], expectedData, mockSSI);
    List<ShuffleBlockInfo> blocks2 =
        ShuffleReadWriteBase.createShuffleBlockList(
            0, 1, 1, 5, 25, bitmaps[1], expectedData, mockSSI);
    List<ShuffleBlockInfo> blocks3 =
        ShuffleReadWriteBase.createShuffleBlockList(
            0, 2, 2, 4, 25, bitmaps[2], expectedData, mockSSI);
    List<ShuffleBlockInfo> blocks4 =
        ShuffleReadWriteBase.createShuffleBlockList(
            0, 3, 3, 1, 25, bitmaps[3], expectedData, mockSSI);
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blocks1);
    partitionToBlocks.put(1, blocks2);
    partitionToBlocks.put(2, blocks3);
    partitionToBlocks.put(3, blocks4);
    return partitionToBlocks;
  }

  private ShuffleClientFactory.ReadClientBuilder baseReadBuilder(boolean isNettyMode) {
    return ShuffleClientFactory.newReadBuilder()
        .clientType(isNettyMode ? ClientType.GRPC_NETTY : ClientType.GRPC)
        .storageType(StorageType.HDFS.name())
        .shuffleId(0)
        .partitionId(0)
        .indexReadLimit(100)
        .partitionNumPerRange(2)
        .partitionNum(10)
        .readBufferSize(1000);
  }

  private static Stream<Arguments> hadoopWriteReadTestProvider() {
    return Stream.of(Arguments.of(true), Arguments.of(false));
  }

  @ParameterizedTest
  @MethodSource("hadoopWriteReadTestProvider")
  private void hadoopWriteReadTest(boolean isNettyMode) throws Exception {
    ShuffleServerGrpcClient shuffleServerClient =
        isNettyMode ? nettyShuffleServerClient : grpcShuffleServerClient;
    String alexDir = kerberizedHadoop.getSchemeAndAuthorityPrefix() + "/alex/";

    String user = "alex";
    String appId = "app_hdfs_read_write";
    String dataBasePath = alexDir + "rss/test";

    RemoteStorageInfo remoteStorageInfo =
        new RemoteStorageInfo(dataBasePath, conf2Map(kerberizedHadoop.getConf()));

    RssRegisterShuffleRequest rrsr =
        new RssRegisterShuffleRequest(
            appId,
            0,
            Lists.newArrayList(new PartitionRange(0, 1)),
            remoteStorageInfo,
            user,
            ShuffleDataDistributionType.NORMAL);
    shuffleServerClient.registerShuffle(rrsr);

    rrsr =
        new RssRegisterShuffleRequest(
            appId,
            0,
            Lists.newArrayList(new PartitionRange(2, 3)),
            remoteStorageInfo,
            user,
            ShuffleDataDistributionType.NORMAL);
    shuffleServerClient.registerShuffle(rrsr);

    Roaring64NavigableMap[] bitmaps = new Roaring64NavigableMap[4];
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Map<Integer, List<ShuffleBlockInfo>> dataBlocks = createTestData(bitmaps, expectedData);
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, dataBlocks.get(0));
    partitionToBlocks.put(1, dataBlocks.get(1));

    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);

    RssSendShuffleDataRequest rssdr =
        new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    RssSendShuffleDataResponse response = shuffleServerClient.sendShuffleData(rssdr);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());

    ShuffleServer shuffleServer = isNettyMode ? grpcShuffleServer : nettyShuffleServer;
    assertEquals(456, shuffleServer.getShuffleBufferManager().getUsedMemory());
    assertEquals(0, shuffleServer.getShuffleBufferManager().getPreAllocatedSize());
    RssSendCommitRequest rscr = new RssSendCommitRequest(appId, 0);
    shuffleServerClient.sendCommit(rscr);
    RssFinishShuffleRequest rfsr = new RssFinishShuffleRequest(appId, 0);

    ShuffleServerInfo ssi =
        isNettyMode
            ? new ShuffleServerInfo(
                LOCALHOST, nettyShuffleServer.getGrpcPort(), nettyShuffleServer.getNettyPort())
            : new ShuffleServerInfo(LOCALHOST, grpcShuffleServer.getGrpcPort());
    ShuffleReadClientImpl readClient =
        baseReadBuilder(isNettyMode)
            .appId(appId)
            .basePath(dataBasePath)
            .blockIdBitmap(bitmaps[0])
            .taskIdBitmap(Roaring64NavigableMap.bitmapOf(0))
            .shuffleServerInfoList(Lists.newArrayList(ssi))
            .build();
    assertNull(readClient.readShuffleBlockData());
    shuffleServerClient.finishShuffle(rfsr);

    partitionToBlocks.clear();
    partitionToBlocks.put(2, dataBlocks.get(2));
    shuffleToBlocks.clear();
    shuffleToBlocks.put(0, partitionToBlocks);
    rssdr = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    response = shuffleServerClient.sendShuffleData(rssdr);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());
    assertEquals(0, shuffleServer.getShuffleBufferManager().getPreAllocatedSize());
    rscr = new RssSendCommitRequest(appId, 0);
    shuffleServerClient.sendCommit(rscr);
    rfsr = new RssFinishShuffleRequest(appId, 0);
    shuffleServerClient.finishShuffle(rfsr);

    partitionToBlocks.clear();
    partitionToBlocks.put(3, dataBlocks.get(3));
    shuffleToBlocks.clear();
    shuffleToBlocks.put(0, partitionToBlocks);
    rssdr = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    response = shuffleServerClient.sendShuffleData(rssdr);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());
    rscr = new RssSendCommitRequest(appId, 0);
    shuffleServerClient.sendCommit(rscr);
    rfsr = new RssFinishShuffleRequest(appId, 0);
    shuffleServerClient.finishShuffle(rfsr);

    readClient =
        baseReadBuilder(isNettyMode)
            .appId(appId)
            .basePath(dataBasePath)
            .blockIdBitmap(bitmaps[0])
            .taskIdBitmap(Roaring64NavigableMap.bitmapOf(0))
            .shuffleServerInfoList(Lists.newArrayList(ssi))
            .build();
    validateResult(readClient, expectedData, bitmaps[0]);

    readClient =
        baseReadBuilder(isNettyMode)
            .appId(appId)
            .partitionId(1)
            .basePath(dataBasePath)
            .blockIdBitmap(bitmaps[1])
            .taskIdBitmap(Roaring64NavigableMap.bitmapOf(1))
            .shuffleServerInfoList(Lists.newArrayList(ssi))
            .build();
    validateResult(readClient, expectedData, bitmaps[1]);

    readClient =
        baseReadBuilder(isNettyMode)
            .appId(appId)
            .partitionId(2)
            .basePath(dataBasePath)
            .blockIdBitmap(bitmaps[2])
            .taskIdBitmap(Roaring64NavigableMap.bitmapOf(2))
            .shuffleServerInfoList(Lists.newArrayList(ssi))
            .build();
    validateResult(readClient, expectedData, bitmaps[2]);

    readClient =
        baseReadBuilder(isNettyMode)
            .appId(appId)
            .partitionId(3)
            .basePath(dataBasePath)
            .blockIdBitmap(bitmaps[3])
            .taskIdBitmap(Roaring64NavigableMap.bitmapOf(3))
            .shuffleServerInfoList(Lists.newArrayList(ssi))
            .build();
    validateResult(readClient, expectedData, bitmaps[3]);
  }

  protected void validateResult(
      ShuffleReadClientImpl readClient,
      Map<Long, byte[]> expectedData,
      Roaring64NavigableMap blockIdBitmap) {
    CompressedShuffleBlock csb = readClient.readShuffleBlockData();
    Roaring64NavigableMap matched = Roaring64NavigableMap.bitmapOf();
    while (csb != null && csb.getByteBuffer() != null) {
      for (Map.Entry<Long, byte[]> entry : expectedData.entrySet()) {
        if (TestUtils.compareByte(entry.getValue(), csb.getByteBuffer())) {
          matched.addLong(entry.getKey());
          break;
        }
      }
      csb = readClient.readShuffleBlockData();
    }
    assertTrue(blockIdBitmap.equals(matched));
  }
}
