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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.RssShuffleManager;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.shuffle.ShuffleReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.apache.spark.sql.execution.joins.SortMergeJoinExec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.MockedGrpcServer;
import org.apache.uniffle.server.MockedShuffleServerGrpcService;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_BLOCK_ID_SELF_MANAGEMENT_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GetShuffleReportForMultiPartTest extends SparkIntegrationTestBase {

  private static final int replicateWrite = 3;
  private static final int replicateRead = 2;

  @BeforeAll
  public static void setupServers(@TempDir File tmpdir) throws Exception {
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    Map<String, String> dynamicConf = Maps.newHashMap();
    dynamicConf.put(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_PATH.key(), HDFS_URI + "rss/test");
    dynamicConf.put(
        RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    addDynamicConf(coordinatorConf, dynamicConf);
    storeCoordinatorConf(coordinatorConf);
    // Create multi shuffle servers
    createShuffleServers(tmpdir);
    startServersWithRandomPorts();

    enableRecordGetShuffleResult();
  }

  private static void createShuffleServers(File tmpdir) throws Exception {
    int index = 0;
    for (int i = 0; i < 4; i++) {
      storeMockShuffleServerConf(shuffleServerConfWithoutPort(index++, tmpdir, ServerType.GRPC));
      storeMockShuffleServerConf(
          shuffleServerConfWithoutPort(index++, tmpdir, ServerType.GRPC_NETTY));
    }
  }

  private static void enableRecordGetShuffleResult() {
    for (ShuffleServer shuffleServer : grpcShuffleServers) {
      ((MockedGrpcServer) shuffleServer.getServer()).getService().enableRecordGetShuffleResult();
    }
  }

  @Override
  public void updateCommonSparkConf(SparkConf sparkConf) {
    sparkConf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "true");
    sparkConf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD().key(), "-1");
    sparkConf.set(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM().key(), "1");
    sparkConf.set(SQLConf.SHUFFLE_PARTITIONS().key(), "100");
    sparkConf.set(SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD().key(), "800");
    sparkConf.set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES().key(), "800");
  }

  @Override
  public void updateSparkConfCustomer(SparkConf sparkConf) {
    sparkConf.set(RssSparkConfig.RSS_STORAGE_TYPE.key(), "HDFS");
    sparkConf.set(RssSparkConfig.RSS_REMOTE_STORAGE_PATH.key(), HDFS_URI + "rss/test");
  }

  @Override
  public void updateSparkConfWithRssGrpc(SparkConf sparkConf) {
    super.updateSparkConfWithRssGrpc(sparkConf);
    // Add multi replica conf
    sparkConf.set(RssSparkConfig.RSS_DATA_REPLICA.key(), String.valueOf(replicateWrite));
    sparkConf.set(RssSparkConfig.RSS_DATA_REPLICA_WRITE.key(), String.valueOf(replicateWrite));
    sparkConf.set(RssSparkConfig.RSS_DATA_REPLICA_READ.key(), String.valueOf(replicateRead));

    sparkConf.set(
        "spark.shuffle.manager",
        "org.apache.uniffle.test.GetShuffleReportForMultiPartTest$RssShuffleManagerWrapper");
  }

  @Test
  public void resultCompareTest() throws Exception {
    run();
  }

  @Override
  Map<Integer, String> runTest(SparkSession spark, String fileName) throws Exception {
    Thread.sleep(4000);
    Map<Integer, String> map = Maps.newHashMap();
    Dataset<Row> df2 =
        spark
            .range(0, 1000, 1, 10)
            .select(
                functions
                    .when(functions.col("id").$less(250), 249)
                    .otherwise(functions.col("id"))
                    .as("key2"),
                functions.col("id").as("value2"));
    Dataset<Row> df1 =
        spark
            .range(0, 1000, 1, 10)
            .select(
                functions
                    .when(functions.col("id").$less(250), 249)
                    .when(functions.col("id").$greater(750), 1000)
                    .otherwise(functions.col("id"))
                    .as("key1"),
                functions.col("id").as("value2"));
    Dataset<Row> df3 = df1.join(df2, df1.col("key1").equalTo(df2.col("key2")));

    List<String> result = Lists.newArrayList();
    assertTrue(
        df3.queryExecution()
            .executedPlan()
            .toString()
            .startsWith("AdaptiveSparkPlan isFinalPlan=false"));
    df3.collectAsList()
        .forEach(
            row -> {
              result.add(row.json());
            });
    assertTrue(
        df3.queryExecution()
            .executedPlan()
            .toString()
            .startsWith("AdaptiveSparkPlan isFinalPlan=true"));
    AdaptiveSparkPlanExec plan = (AdaptiveSparkPlanExec) df3.queryExecution().executedPlan();
    SortMergeJoinExec joinExec =
        (SortMergeJoinExec) plan.executedPlan().children().iterator().next();
    assertTrue(joinExec.isSkewJoin());
    result.sort(
        new Comparator<String>() {
          @Override
          public int compare(String o1, String o2) {
            return o1.compareTo(o2);
          }
        });
    int i = 0;
    for (String str : result) {
      map.put(i, str);
      i++;
    }
    SparkConf conf = spark.sparkContext().conf();
    if (conf.get("spark.shuffle.manager", "")
        .equals(
            "org.apache.uniffle.test.GetShuffleReportForMultiPartTest$RssShuffleManagerWrapper")) {
      RssShuffleManagerWrapper mockRssShuffleManager =
          (RssShuffleManagerWrapper) spark.sparkContext().env().shuffleManager();
      int expectRequestNum =
          mockRssShuffleManager.getShuffleIdToPartitionNum().values().stream()
              .mapToInt(x -> x.get())
              .sum();
      // Validate getShuffleResultForMultiPart is correct before return result
      ClientType clientType =
          ClientType.valueOf(spark.sparkContext().getConf().get(RssSparkConfig.RSS_CLIENT_TYPE));
      boolean blockIdSelfManagedEnabled =
          RssSparkConfig.toRssConf(spark.sparkContext().getConf())
              .get(RSS_BLOCK_ID_SELF_MANAGEMENT_ENABLED);
      if (ClientType.GRPC == clientType && !blockIdSelfManagedEnabled) {
        // TODO skip validating for GRPC_NETTY, needs to mock ShuffleServerNettyHandler
        // skip validating when blockId is managed in spark driver side.
        validateRequestCount(
            spark.sparkContext().applicationId(), expectRequestNum * replicateRead);
      }
    }
    return map;
  }

  public void validateRequestCount(String appId, int expectRequestNum) {
    for (ShuffleServer shuffleServer : grpcShuffleServers) {
      MockedShuffleServerGrpcService service =
          ((MockedGrpcServer) shuffleServer.getServer()).getService();
      Map<String, Map<Integer, AtomicInteger>> serviceRequestCount =
          service.getShuffleIdToPartitionRequest();
      int requestNum =
          serviceRequestCount.entrySet().stream()
              .filter(x -> x.getKey().startsWith(appId))
              .flatMap(x -> x.getValue().values().stream())
              .mapToInt(AtomicInteger::get)
              .sum();
      expectRequestNum -= requestNum;
    }
    assertEquals(0, expectRequestNum);
  }

  public static class RssShuffleManagerWrapper extends RssShuffleManager {

    // shuffleId -> partShouldRequestNum
    Map<Integer, AtomicInteger> shuffleToPartShouldRequestNum = JavaUtils.newConcurrentMap();

    public RssShuffleManagerWrapper(SparkConf conf, boolean isDriver) {
      super(conf, isDriver);
    }

    @Override
    public <K, C> ShuffleReader<K, C> getReaderImpl(
        ShuffleHandle handle,
        int startMapIndex,
        int endMapIndex,
        int startPartition,
        int endPartition,
        TaskContext context,
        ShuffleReadMetricsReporter metrics,
        Roaring64NavigableMap taskIdBitmap) {
      int shuffleId = handle.shuffleId();
      RssShuffleHandle<?, ?, ?> rssShuffleHandle = (RssShuffleHandle<?, ?, ?>) handle;
      Map<Integer, List<ShuffleServerInfo>> allPartitionToServers =
          rssShuffleHandle.getPartitionToServers();
      int partitionNum =
          (int)
              allPartitionToServers.entrySet().stream()
                  .filter(x -> x.getKey() >= startPartition && x.getKey() < endPartition)
                  .count();
      AtomicInteger partShouldRequestNum =
          shuffleToPartShouldRequestNum.computeIfAbsent(shuffleId, x -> new AtomicInteger(0));
      partShouldRequestNum.addAndGet(partitionNum);
      return super.getReaderImpl(
          handle,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition,
          context,
          metrics,
          taskIdBitmap);
    }

    public Map<Integer, AtomicInteger> getShuffleIdToPartitionNum() {
      return shuffleToPartShouldRequestNum;
    }
  }
}
