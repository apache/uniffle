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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.Tuple2;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.shuffle.RssShuffleManager;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.shuffle.Spark3VersionUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.StorageType;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.test.listener.WriteAndReadMetricsSparkListener;

import static org.apache.spark.shuffle.RssSparkConfig.toSparkConfKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class IntegrityValidationTest extends SparkIntegrationTestBase {
  private boolean serverManagementEnabled;

  @BeforeAll
  public static void setupServers() throws Exception {
    assumeTrue(Spark3VersionUtils.isSparkVersionAtLeast("3.5.0"));
    storeCoordinatorConf(coordinatorConfWithoutPort());
    storeShuffleServerConf(shuffleServerConfWithoutPort(0, null, ServerType.GRPC));
    storeShuffleServerConf(shuffleServerConfWithoutPort(1, null, ServerType.GRPC_NETTY));
    startServersWithRandomPorts();
  }

  @Override
  public void updateSparkConfCustomer(SparkConf sparkConf) {
    sparkConf.set(
        toSparkConfKey(RssBaseConf.RSS_STORAGE_TYPE), StorageType.MEMORY_LOCALFILE_HDFS.name());
    sparkConf.set(toSparkConfKey(RssClientConf.RSS_REMOTE_STORAGE_PATH), HDFS_URI + "rss/test");
    sparkConf.set(toSparkConfKey(RssSparkConfig.RSS_CLIENT_INTEGRITY_VALIDATION_ENABLED), "true");
    sparkConf.set(
        toSparkConfKey(RssSparkConfig.RSS_DATA_INTEGRITY_VALIDATION_SERVER_MANAGEMENT_ENABLED),
        Boolean.toString(serverManagementEnabled));
  }

  @Test
  public void driverManagementTest() throws Exception {
    serverManagementEnabled = false;
    run();
  }

  @Test
  public void serverManagementTest() throws Exception {
    serverManagementEnabled = true;
    run();
  }

  @Override
  Map<Integer, Integer> runTest(SparkSession spark, String fileName) throws Exception {
    Thread.sleep(4000);
    if (spark.sparkContext().env().shuffleManager() instanceof RssShuffleManager) {
      RssConf rssConf = RssSparkConfig.toRssConf(spark.sparkContext().getConf());
      assertTrue(RssShuffleManager.isIntegrityValidationEnabled(rssConf));
      assertEquals(
          serverManagementEnabled,
          RssShuffleManager.isIntegrityValidationServerManagementEnabled(rssConf));
      assertEquals(
          !serverManagementEnabled,
          RssShuffleManager.isIntegrityValidationClientManagementEnabled(rssConf));
    }

    WriteAndReadMetricsSparkListener listener = new WriteAndReadMetricsSparkListener();
    spark.sparkContext().addSparkListener(listener);
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    List<Integer> data = IntStream.rangeClosed(1, 1000).boxed().collect(Collectors.toList());
    Map<Integer, Integer> result =
        sc.parallelize(data, 4)
            .mapToPair(value -> new Tuple2<>(value, value))
            .partitionBy(new HashPartitioner(4))
            .collectAsMap();
    assertEquals(1000, result.size());
    spark.sparkContext().listenerBus().waitUntilEmpty();
    assertEquals(1000L, listener.getWriteRecords(0));
    assertEquals(1000L, listener.getReadRecords(1));
    return result;
  }
}
