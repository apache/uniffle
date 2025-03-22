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

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;

import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.StorageType;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.test.listener.WriteAndReadMetricsSparkListener;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MapSideCombineTest extends SparkIntegrationTestBase {

  @BeforeAll
  public static void setupServers() throws Exception {
    storeCoordinatorConf(coordinatorConfWithoutPort());

    storeShuffleServerConf(shuffleServerConfWithoutPort(0, null, ServerType.GRPC));
    storeShuffleServerConf(shuffleServerConfWithoutPort(1, null, ServerType.GRPC_NETTY));

    startServersWithRandomPorts();
  }

  @Override
  public void updateSparkConfCustomer(SparkConf sparkConf) {
    sparkConf.set(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    sparkConf.set(RssSparkConfig.RSS_REMOTE_STORAGE_PATH.key(), HDFS_URI + "rss/test");
    sparkConf.set("spark." + RssSparkConfig.RSS_CLIENT_MAP_SIDE_COMBINE_ENABLED.key(), "true");
  }

  @Test
  public void resultCompareTest() throws Exception {
    run();
  }

  @Override
  Map runTest(SparkSession spark, String fileName) throws Exception {
    Thread.sleep(4000);

    WriteAndReadMetricsSparkListener listener = new WriteAndReadMetricsSparkListener();
    spark.sparkContext().addSparkListener(listener);

    Map<String, Object> result = Maps.newHashMap();
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    List<Integer> data = Stream.iterate(1, n -> n + 1).limit(10000).collect(Collectors.toList());
    JavaPairRDD<Integer, Integer> dataSourceRdd =
        sc.parallelize(data, 10).mapToPair(x -> new Tuple2<>(x % 10, 1));
    int jobId = -1;

    // reduceByKey
    checkMapSideCombine(
        spark,
        listener,
        "reduceByKey",
        dataSourceRdd.reduceByKey(Integer::sum).collectAsMap(),
        result,
        ++jobId);

    // combineByKey
    checkMapSideCombine(
        spark,
        listener,
        "combineByKey",
        dataSourceRdd.combineByKey(x -> 1, Integer::sum, Integer::sum).collectAsMap(),
        result,
        ++jobId);

    // aggregateByKey
    checkMapSideCombine(
        spark,
        listener,
        "aggregateByKey",
        dataSourceRdd.aggregateByKey(10, Integer::sum, Integer::sum).collectAsMap(),
        result,
        ++jobId);

    // foldByKey
    checkMapSideCombine(
        spark,
        listener,
        "foldByKey",
        dataSourceRdd.foldByKey(10, Integer::sum).collectAsMap(),
        result,
        ++jobId);

    // countByKey
    checkMapSideCombine(spark, listener, "countByKey", dataSourceRdd.countByKey(), result, ++jobId);

    return result;
  }

  private <K, V> void checkMapSideCombine(
      SparkSession spark,
      WriteAndReadMetricsSparkListener listener,
      String method,
      Map<K, V> rddResult,
      Map<String, Object> result,
      int jobId)
      throws TimeoutException {
    rddResult.forEach((key, value) -> result.put(method + "-result-value-" + key, value));

    spark.sparkContext().listenerBus().waitUntilEmpty();

    for (int stageId : spark.sparkContext().statusTracker().getJobInfo(jobId).get().stageIds()) {
      long writeRecords = listener.getWriteRecords(stageId);
      long readRecords = listener.getReadRecords(stageId);
      result.put(stageId + "-write-records", writeRecords);
      result.put(stageId + "-read-records", readRecords);
    }

    // each job has two stages, so each job start stageId = jobId * 2
    int shuffleStageId = jobId * 2;
    // check map side combine
    assertEquals(100L, result.get(shuffleStageId + "-write-records"));
  }
}
