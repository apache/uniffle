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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Option;

import com.google.common.collect.Maps;
import org.apache.spark.MapOutputTrackerMaster;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.shuffle.RssSparkShuffleUtils;
import org.apache.spark.shuffle.handle.ShuffleHandleInfo;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.util.BlockId;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.shuffle.manager.RssShuffleManagerBase;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RssShuffleManagerTest extends SparkIntegrationTestBase {

  @BeforeAll
  public static void beforeAll() throws Exception {
    shutdownServers();
  }

  @AfterEach
  public void after() throws Exception {
    shutdownServers();
  }

  public static Map<String, String> startServers(BlockIdLayout dynamicConfLayout) throws Exception {
    Map<String, String> dynamicConf = Maps.newHashMap();
    dynamicConf.put(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_PATH.key(), HDFS_URI + "rss/test");
    dynamicConf.put(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE.name());
    // configure block id layout (if set)
    if (dynamicConfLayout != null) {
      dynamicConf.put(
          RssClientConf.BLOCKID_SEQUENCE_NO_BITS.key(),
          String.valueOf(dynamicConfLayout.sequenceNoBits));
      dynamicConf.put(
          RssClientConf.BLOCKID_PARTITION_ID_BITS.key(),
          String.valueOf(dynamicConfLayout.partitionIdBits));
      dynamicConf.put(
          RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS.key(),
          String.valueOf(dynamicConfLayout.taskAttemptIdBits));
    }
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    addDynamicConf(coordinatorConf, dynamicConf);
    storeCoordinatorConf(coordinatorConf);
    storeShuffleServerConf(shuffleServerConfWithoutPort(0, tempDir, ServerType.GRPC));
    startServersWithRandomPorts();
    return dynamicConf;
  }

  @Override
  Map runTest(SparkSession spark, String fileName) throws Exception {
    // we don't need to run any spark application here, just return an empty map.
    return new HashMap();
  }

  private static final BlockIdLayout DEFAULT = BlockIdLayout.from(21, 20, 22);
  private static final BlockIdLayout CUSTOM1 = BlockIdLayout.from(20, 21, 22);
  private static final BlockIdLayout CUSTOM2 = BlockIdLayout.from(22, 18, 23);

  public static Stream<Arguments> testBlockIdLayouts() {
    return Stream.of(Arguments.of(DEFAULT), Arguments.of(CUSTOM1), Arguments.of(CUSTOM2));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testRssShuffleManager(boolean enableDynamicClientConf) throws Exception {
    doTestRssShuffleManager(null, null, DEFAULT, enableDynamicClientConf);
  }

  @ParameterizedTest
  @MethodSource("testBlockIdLayouts")
  public void testRssShuffleManagerClientConf(BlockIdLayout layout) throws Exception {
    doTestRssShuffleManager(layout, null, layout, true);
  }

  @ParameterizedTest
  @MethodSource("testBlockIdLayouts")
  public void testRssShuffleManagerDynamicClientConf(BlockIdLayout layout) throws Exception {
    doTestRssShuffleManager(null, layout, layout, true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testRssShuffleManagerClientConfOverride(boolean enableDynamicClientConf)
      throws Exception {
    doTestRssShuffleManager(CUSTOM1, CUSTOM2, CUSTOM1, enableDynamicClientConf);
  }

  private void doTestRssShuffleManager(
      BlockIdLayout clientConfLayout,
      BlockIdLayout dynamicConfLayout,
      BlockIdLayout expectedLayout,
      boolean enableDynamicClientConf)
      throws Exception {
    Map<String, String> dynamicConf = startServers(dynamicConfLayout);

    SparkConf conf = createSparkConf();
    updateSparkConfWithRssGrpc(conf);
    // enable stage recompute
    conf.set("spark." + RssClientConfig.RSS_RESUBMIT_STAGE, "true");
    // enable dynamic client conf
    conf.set(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED, enableDynamicClientConf);
    // configure storage type
    conf.set("spark." + RssClientConfig.RSS_STORAGE_TYPE, StorageType.MEMORY_LOCALFILE.name());
    // restarting the coordinator may cause RssException: There isn't enough shuffle servers
    // retry quickly (default is 65s interval)
    conf.set("spark." + RssClientConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL, "1000");
    conf.set("spark." + RssClientConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES, "10");

    // configure client conf block id layout (if set)
    if (clientConfLayout != null) {
      conf.set(
          "spark." + RssClientConf.BLOCKID_SEQUENCE_NO_BITS.key(),
          String.valueOf(clientConfLayout.sequenceNoBits));
      conf.set(
          "spark." + RssClientConf.BLOCKID_PARTITION_ID_BITS.key(),
          String.valueOf(clientConfLayout.partitionIdBits));
      conf.set(
          "spark." + RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS.key(),
          String.valueOf(clientConfLayout.taskAttemptIdBits));
    }

    JavaSparkContext sc = null;
    try {
      Option<SparkSession> spark = SparkSession.getActiveSession();
      if (spark.nonEmpty()) {
        spark.get().stop();
      }
      sc = new JavaSparkContext(SparkSession.builder().config(conf).getOrCreate().sparkContext());
      // create a rdd that triggers shuffle registration
      long count = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2).groupBy(x -> x).count();
      assertEquals(5, count);

      // configure expected block id layout
      conf.set(
          "spark." + RssClientConf.BLOCKID_SEQUENCE_NO_BITS.key(),
          String.valueOf(expectedLayout.sequenceNoBits));
      conf.set(
          "spark." + RssClientConf.BLOCKID_PARTITION_ID_BITS.key(),
          String.valueOf(expectedLayout.partitionIdBits));
      conf.set(
          "spark." + RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS.key(),
          String.valueOf(expectedLayout.taskAttemptIdBits));

      // get written block ids (we know there is one shuffle where two task attempts wrote two
      // partitions)
      RssConf rssConf = RssSparkConfig.toRssConf(conf);
      if (clientConfLayout == null && dynamicConfLayout != null) {
        RssSparkShuffleUtils.applyDynamicClientConf(conf, dynamicConf);
      }
      RssShuffleManagerBase shuffleManager =
          (RssShuffleManagerBase) SparkEnv.get().shuffleManager();
      shuffleManager.configureBlockIdLayout(conf, rssConf);
      ShuffleWriteClient shuffleWriteClient =
          ShuffleClientFactory.newWriteBuilder()
              .clientType(ClientType.GRPC.name())
              .retryMax(3)
              .retryIntervalMax(2000)
              .heartBeatThreadNum(4)
              .replica(1)
              .replicaWrite(1)
              .replicaRead(1)
              .replicaSkipEnabled(true)
              .dataTransferPoolSize(1)
              .dataCommitPoolSize(1)
              .unregisterThreadPoolSize(10)
              .unregisterTimeSec(10)
              .unregisterRequestTimeSec(10)
              .rssConf(rssConf)
              .build();
      ShuffleHandleInfo handle = shuffleManager.getShuffleHandleInfoByShuffleId(0);
      Set<ShuffleServerInfo> servers =
          handle.getAvailablePartitionServersForWriter().values().stream()
              .flatMap(Collection::stream)
              .collect(Collectors.toSet());

      for (int partitionId : new int[] {0, 1}) {
        Roaring64NavigableMap blockIdLongs =
            shuffleWriteClient.getShuffleResult(
                ClientType.GRPC.name(), servers, shuffleManager.getAppId(), 0, partitionId);
        List<BlockId> blockIds =
            blockIdLongs.stream()
                .sorted()
                .mapToObj(expectedLayout::asBlockId)
                .collect(Collectors.toList());
        assertEquals(2, blockIds.size());
        long taskAttemptId0 = shuffleManager.getTaskAttemptIdForBlockId(0, 0);
        long taskAttemptId1 = shuffleManager.getTaskAttemptIdForBlockId(1, 0);
        assertEquals(expectedLayout.asBlockId(0, partitionId, taskAttemptId0), blockIds.get(0));
        assertEquals(expectedLayout.asBlockId(0, partitionId, taskAttemptId1), blockIds.get(1));
      }

      shuffleManager.unregisterAllMapOutput(0);
      MapOutputTrackerMaster master = (MapOutputTrackerMaster) SparkEnv.get().mapOutputTracker();
      assertTrue(master.containsShuffle(0));
    } finally {
      if (sc != null) {
        sc.stop();
      }
    }
  }
}
