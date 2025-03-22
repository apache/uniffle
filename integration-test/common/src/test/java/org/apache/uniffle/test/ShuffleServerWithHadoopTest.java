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
import java.util.Map.Entry;
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
import org.apache.uniffle.client.response.CompressedShuffleBlock;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShuffleServerWithHadoopTest extends ShuffleReadWriteBase {

  protected ShuffleServerGrpcClient grpcShuffleServerClient;
  protected ShuffleServerGrpcNettyClient nettyShuffleServerClient;

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    storeCoordinatorConf(coordinatorConf);

    storeShuffleServerConf(buildShuffleServerConf(0, tmpDir, ServerType.GRPC));
    storeShuffleServerConf(buildShuffleServerConf(1, tmpDir, ServerType.GRPC_NETTY));

    startServersWithRandomPorts();
  }

  private static ShuffleServerConf buildShuffleServerConf(
      int subDirIndex, File tempDir, ServerType serverType) {
    ShuffleServerConf shuffleServerConf =
        shuffleServerConfWithoutPort(subDirIndex, tempDir, serverType);
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
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
  private void hadoopWriteReadTest(boolean isNettyMode) {
    ShuffleServerGrpcClient shuffleServerClient =
        isNettyMode ? nettyShuffleServerClient : grpcShuffleServerClient;

    String appId = "app_hdfs_read_write";
    String dataBasePath = HDFS_URI + "rss/test";
    RssRegisterShuffleRequest rrsr =
        new RssRegisterShuffleRequest(
            appId, 0, Lists.newArrayList(new PartitionRange(0, 1)), dataBasePath);
    shuffleServerClient.registerShuffle(rrsr);
    rrsr =
        new RssRegisterShuffleRequest(
            appId, 0, Lists.newArrayList(new PartitionRange(2, 3)), dataBasePath);
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
    List<ShuffleServer> shuffleServers = isNettyMode ? nettyShuffleServers : grpcShuffleServers;
    assertSame(StatusCode.SUCCESS, response.getStatusCode());
    assertEquals(456, shuffleServers.get(0).getShuffleBufferManager().getUsedMemory());
    assertEquals(0, shuffleServers.get(0).getShuffleBufferManager().getPreAllocatedSize());
    RssSendCommitRequest rscr = new RssSendCommitRequest(appId, 0);
    shuffleServerClient.sendCommit(rscr);
    RssFinishShuffleRequest rfsr = new RssFinishShuffleRequest(appId, 0);

    ShuffleServerInfo ssi =
        isNettyMode
            ? new ShuffleServerInfo(
                LOCALHOST,
                nettyShuffleServers.get(0).getGrpcPort(),
                nettyShuffleServers.get(0).getNettyPort())
            : new ShuffleServerInfo(LOCALHOST, grpcShuffleServers.get(0).getGrpcPort());

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
    assertEquals(0, shuffleServers.get(0).getShuffleBufferManager().getPreAllocatedSize());
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
      for (Entry<Long, byte[]> entry : expectedData.entrySet()) {
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
