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

import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.MockedGrpcServer;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.storage.util.StorageType;

public class RSSStageResubmitTest extends SparkTaskFailureIntegrationTestBase {

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    final CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    Map<String, String> dynamicConf = Maps.newHashMap();
    dynamicConf.put(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_PATH.key(), HDFS_URI + "rss/test");
    dynamicConf.put(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE.name());
    dynamicConf.put(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConfig.RSS_RESUBMIT_STAGE, "true");
    dynamicConf.put(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE.name());
    addDynamicConf(coordinatorConf, dynamicConf);
    storeCoordinatorConf(coordinatorConf);

    storeMockShuffleServerConf(shuffleServerConfWithoutPort(0, tmpDir, ServerType.GRPC));
    storeMockShuffleServerConf(shuffleServerConfWithoutPort(1, tmpDir, ServerType.GRPC_NETTY));

    startServersWithRandomPorts();
    enableFirstReadRequest(2 * maxTaskFailures);
  }

  private static void enableFirstReadRequest(int failCount) {
    for (ShuffleServer server : grpcShuffleServers) {
      ((MockedGrpcServer) server.getServer()).getService().enableFirstNReadRequestToFail(failCount);
    }
  }

  @Override
  public Map runTest(SparkSession spark, String fileName) throws Exception {
    List<Row> rows =
        spark.range(0, 1000, 1, 4).repartition(2).groupBy("id").count().collectAsList();
    Map<String, Long> result = Maps.newHashMap();
    for (Row row : rows) {
      result.put(row.get(0).toString(), row.getLong(1));
    }
    return result;
  }

  @Override
  public void updateSparkConfCustomer(SparkConf sparkConf) {
    super.updateSparkConfCustomer(sparkConf);
    sparkConf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX
            + RssSparkConfig.RSS_RESUBMIT_STAGE_WITH_FETCH_FAILURE_ENABLED,
        "true");
  }

  @Test
  public void testRSSStageResubmit() throws Exception {
    run();
  }
}
