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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.factory.ShuffleServerClientFactory;
import org.apache.uniffle.client.impl.ShuffleReadClientImpl;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcNettyClient;
import org.apache.uniffle.client.request.RssFinishShuffleRequest;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssReportShuffleResultRequest;
import org.apache.uniffle.client.request.RssSendCommitRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.client.response.CompressedShuffleBlock;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class HybridStorageFaultToleranceBase extends ShuffleReadWriteBase {
  protected ShuffleServerGrpcClient grpcShuffleServerClient;
  protected ShuffleServerGrpcNettyClient nettyShuffleServerClient;
  private static String REMOTE_STORAGE = HDFS_URI + "rss/multi_storage_fault_%s";

  @BeforeEach
  public void createClient() throws Exception {
    ShuffleServerClientFactory.getInstance().cleanupCache();
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

  abstract void makeChaos();

  private static Stream<Arguments> fallbackTestProvider() {
    return Stream.of(Arguments.of(true), Arguments.of(false));
  }

  @ParameterizedTest
  @MethodSource("fallbackTestProvider")
  private void fallbackTest(boolean isNettyMode) throws Exception {
    String appId = "fallback_test_" + this.getClass().getSimpleName();
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Map<Integer, List<Integer>> map = Maps.newHashMap();
    map.put(0, Lists.newArrayList(0));
    registerShuffle(appId, map, isNettyMode);
    Roaring64NavigableMap blockBitmap = Roaring64NavigableMap.bitmapOf();
    final List<ShuffleBlockInfo> blocks =
        createShuffleBlockList(0, 0, 0, 40, 2 * 1024 * 1024, blockBitmap, expectedData);
    makeChaos();
    sendSinglePartitionToShuffleServer(appId, 0, 0, 0, blocks, isNettyMode);
    validateResult(
        appId, 0, 0, blockBitmap, Roaring64NavigableMap.bitmapOf(0), expectedData, isNettyMode);
  }

  private void registerShuffle(
      String appId, Map<Integer, List<Integer>> registerMap, boolean isNettyMode) {
    ShuffleServerClient shuffleServerClient =
        isNettyMode ? nettyShuffleServerClient : grpcShuffleServerClient;
    for (Map.Entry<Integer, List<Integer>> entry : registerMap.entrySet()) {
      for (int partition : entry.getValue()) {
        RssRegisterShuffleRequest rr =
            new RssRegisterShuffleRequest(
                appId,
                entry.getKey(),
                Lists.newArrayList(new PartitionRange(partition, partition)),
                String.format(REMOTE_STORAGE, isNettyMode));
        shuffleServerClient.registerShuffle(rr);
      }
    }
  }

  private void sendSinglePartitionToShuffleServer(
      String appId,
      int shuffle,
      int partition,
      long taskAttemptId,
      List<ShuffleBlockInfo> blocks,
      boolean isNettyMode) {
    ShuffleServerGrpcClient shuffleServerClient =
        isNettyMode ? nettyShuffleServerClient : grpcShuffleServerClient;
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    partitionToBlocks.put(partition, blocks);
    shuffleToBlocks.put(shuffle, partitionToBlocks);
    RssSendShuffleDataRequest rs = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    RssSendCommitRequest rc = new RssSendCommitRequest(appId, shuffle);
    RssFinishShuffleRequest rf = new RssFinishShuffleRequest(appId, shuffle);
    RssSendShuffleDataResponse response = shuffleServerClient.sendShuffleData(rs);
    assertSame(StatusCode.SUCCESS, response.getStatusCode());
    shuffleServerClient.sendCommit(rc);
    shuffleServerClient.finishShuffle(rf);

    Map<Integer, List<Long>> partitionToBlockIds = Maps.newHashMap();
    Set<Long> expectBlockIds = getExpectBlockIds(blocks);
    partitionToBlockIds.put(shuffle, new ArrayList<>(expectBlockIds));
    RssReportShuffleResultRequest rrp =
        new RssReportShuffleResultRequest(appId, shuffle, taskAttemptId, partitionToBlockIds, 1);
    shuffleServerClient.reportShuffleResult(rrp);
  }

  protected void validateResult(
      String appId,
      int shuffleId,
      int partitionId,
      Roaring64NavigableMap blockBitmap,
      Roaring64NavigableMap taskBitmap,
      Map<Long, byte[]> expectedData,
      boolean isNettyMode) {
    ShuffleServerInfo ssi =
        isNettyMode
            ? new ShuffleServerInfo(
                LOCALHOST,
                nettyShuffleServers.get(0).getGrpcPort(),
                nettyShuffleServers.get(0).getNettyPort())
            : new ShuffleServerInfo(LOCALHOST, grpcShuffleServers.get(0).getGrpcPort());
    ShuffleReadClientImpl readClient =
        ShuffleClientFactory.newReadBuilder()
            .clientType(isNettyMode ? ClientType.GRPC_NETTY : ClientType.GRPC)
            .storageType(StorageType.LOCALFILE_HDFS.name())
            .appId(appId)
            .shuffleId(shuffleId)
            .partitionId(partitionId)
            .indexReadLimit(100)
            .partitionNumPerRange(1)
            .partitionNum(10)
            .readBufferSize(1000)
            .basePath(String.format(REMOTE_STORAGE, isNettyMode))
            .blockIdBitmap(blockBitmap)
            .taskIdBitmap(taskBitmap)
            .shuffleServerInfoList(Lists.newArrayList(ssi))
            .hadoopConf(conf)
            .build();
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
    assertTrue(blockBitmap.equals(matched));
  }

  private Set<Long> getExpectBlockIds(List<ShuffleBlockInfo> blocks) {
    List<Long> expectBlockIds = Lists.newArrayList();
    blocks.forEach(b -> expectBlockIds.add(b.getBlockId()));
    return Sets.newHashSet(expectBlockIds);
  }
}
