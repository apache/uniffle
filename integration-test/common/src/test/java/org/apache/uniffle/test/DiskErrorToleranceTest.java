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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
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
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DiskErrorToleranceTest extends ShuffleReadWriteBase {
  private ShuffleServerGrpcClient grpcShuffleServerClient;
  private ShuffleServerGrpcNettyClient nettyShuffleServerClient;

  private static File data1;
  private static File data2;
  private List<ShuffleServerInfo> grpcShuffleServerInfoList;
  private List<ShuffleServerInfo> nettyShuffleServerInfoList;

  @BeforeAll
  public static void setupServers(@TempDir File serverTmpDir) throws Exception {
    data1 = new File(serverTmpDir, "data1");
    data2 = new File(serverTmpDir, "data2");
    storeCoordinatorConf(coordinatorConfWithoutPort());

    storeShuffleServerConf(buildShuffleServerConf(ServerType.GRPC));
    storeShuffleServerConf(buildShuffleServerConf(ServerType.GRPC_NETTY));

    startServersWithRandomPorts();
  }

  @BeforeEach
  public void createClient(@TempDir File serverTmpDir) throws Exception {
    grpcShuffleServerClient =
        new ShuffleServerGrpcClient(LOCALHOST, grpcShuffleServers.get(0).getGrpcPort());
    nettyShuffleServerClient =
        new ShuffleServerGrpcNettyClient(
            LOCALHOST,
            nettyShuffleServers.get(0).getGrpcPort(),
            nettyShuffleServers.get(0).getNettyPort());

    grpcShuffleServerInfoList =
        Lists.newArrayList(
            new ShuffleServerInfo(LOCALHOST, grpcShuffleServers.get(0).getGrpcPort()));
    nettyShuffleServerInfoList =
        Lists.newArrayList(
            new ShuffleServerInfo(
                LOCALHOST,
                nettyShuffleServers.get(0).getGrpcPort(),
                nettyShuffleServers.get(0).getNettyPort()));
  }

  private static ShuffleServerConf buildShuffleServerConf(ServerType serverType) throws Exception {
    ShuffleServerConf shuffleServerConf = shuffleServerConfWithoutPort(0, null, serverType);
    shuffleServerConf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE.name());
    shuffleServerConf.set(
        ShuffleServerConf.RSS_STORAGE_BASE_PATH,
        Arrays.asList(data1.getAbsolutePath(), data2.getAbsolutePath()));
    shuffleServerConf.setBoolean(ShuffleServerConf.HEALTH_CHECK_ENABLE, true);
    return shuffleServerConf;
  }

  @AfterEach
  public void closeClient() throws Exception {
    grpcShuffleServerClient.close();
    nettyShuffleServerClient.close();
    shutdownServers();
  }

  private static Stream<Arguments> diskErrorTestProvider() {
    return Stream.of(Arguments.of(true), Arguments.of(false));
  }

  @ParameterizedTest
  @MethodSource("diskErrorTestProvider")
  private void diskErrorTest(boolean isNettyMode) throws Exception {
    ShuffleServerGrpcClient shuffleServerClient =
        isNettyMode ? nettyShuffleServerClient : grpcShuffleServerClient;
    String appId = "ap_disk_error_data";
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Set<Long> expectedBlock1 = Sets.newHashSet();
    Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();
    List<ShuffleBlockInfo> blocks1 =
        createShuffleBlockList(0, 0, 1, 3, 25, blockIdBitmap1, expectedData);
    RssRegisterShuffleRequest rr1 =
        new RssRegisterShuffleRequest(appId, 0, Lists.newArrayList(new PartitionRange(0, 0)), "");
    shuffleServerClient.registerShuffle(rr1);
    blocks1.forEach(b -> expectedBlock1.add(b.getBlockId()));
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blocks1);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);
    RssSendShuffleDataRequest rs1 = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    RssSendCommitRequest rc1 = new RssSendCommitRequest(appId, 0);
    RssFinishShuffleRequest rf1 = new RssFinishShuffleRequest(appId, 0);
    RssSendShuffleDataResponse response = shuffleServerClient.sendShuffleData(rs1);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());
    shuffleServerClient.sendCommit(rc1);
    shuffleServerClient.finishShuffle(rf1);
    List<ShuffleServerInfo> shuffleServerInfoList =
        isNettyMode ? nettyShuffleServerInfoList : grpcShuffleServerInfoList;
    ShuffleReadClientImpl readClient =
        ShuffleClientFactory.newReadBuilder()
            .clientType(isNettyMode ? ClientType.GRPC_NETTY : ClientType.GRPC)
            .storageType(StorageType.LOCALFILE.name())
            .appId(appId)
            .shuffleId(0)
            .partitionId(0)
            .indexReadLimit(100)
            .partitionNumPerRange(1)
            .partitionNum(10)
            .readBufferSize(1000)
            .basePath(null)
            .blockIdBitmap(blockIdBitmap1)
            .taskIdBitmap(Roaring64NavigableMap.bitmapOf(1))
            .shuffleServerInfoList(shuffleServerInfoList)
            .hadoopConf(conf)
            .build();
    validateResult(readClient, expectedData);

    File shuffleData = new File(data2, appId);
    assertTrue(shuffleData.exists());
    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);

    expectedData.clear();
    partitionToBlocks.clear();
    shuffleToBlocks.clear();
    Roaring64NavigableMap blockIdBitmap2 = Roaring64NavigableMap.bitmapOf();
    Set<Long> expectedBlock2 = Sets.newHashSet();
    List<ShuffleBlockInfo> blocks2 =
        createShuffleBlockList(0, 0, 2, 5, 30, blockIdBitmap2, expectedData);
    blocks2.forEach(b -> expectedBlock2.add(b.getBlockId()));
    partitionToBlocks.put(0, blocks2);
    shuffleToBlocks.put(0, partitionToBlocks);
    rs1 = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    response = shuffleServerClient.sendShuffleData(rs1);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());

    shuffleServerClient.sendCommit(rc1);
    shuffleServerClient.finishShuffle(rf1);

    readClient =
        ShuffleClientFactory.newReadBuilder()
            .storageType(StorageType.LOCALFILE.name())
            .appId(appId)
            .shuffleId(0)
            .partitionId(0)
            .indexReadLimit(100)
            .partitionNumPerRange(1)
            .partitionNum(10)
            .readBufferSize(1000)
            .basePath(null)
            .blockIdBitmap(blockIdBitmap2)
            .taskIdBitmap(Roaring64NavigableMap.bitmapOf(2))
            .shuffleServerInfoList(shuffleServerInfoList)
            .hadoopConf(conf)
            .build();
    validateResult(readClient, expectedData);
    shuffleData = new File(data1, appId);
    assertTrue(shuffleData.exists());
  }
}
