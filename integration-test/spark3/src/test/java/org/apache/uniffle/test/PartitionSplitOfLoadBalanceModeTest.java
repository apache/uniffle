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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssSparkConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.PartitionSplitMode;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_CLIENT_TYPE;
import static org.apache.uniffle.client.util.RssClientConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER;
import static org.apache.uniffle.client.util.RssClientConfig.RSS_CLIENT_RETRY_MAX;
import static org.apache.uniffle.common.config.RssClientConf.RSS_CLIENT_PARTITION_SPLIT_LOAD_BALANCE_SERVER_NUMBER;
import static org.apache.uniffle.common.config.RssClientConf.RSS_CLIENT_PARTITION_SPLIT_MODE;
import static org.apache.uniffle.common.config.RssClientConf.RSS_CLIENT_REASSIGN_ENABLED;

/** This class is to simulate test partition split on load balance mode. */
public class PartitionSplitOfLoadBalanceModeTest extends SparkSQLTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PartitionSplitOfLoadBalanceModeTest.class);

  @BeforeAll
  public static void setupServers() throws Exception {
    LOGGER.info("Setup servers");

    // for coordinator
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    coordinatorConf.setLong("rss.coordinator.app.expired", 5000);
    Map<String, String> dynamicConf = Maps.newHashMap();
    dynamicConf.put(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE.name());
    addDynamicConf(coordinatorConf, dynamicConf);
    storeCoordinatorConf(coordinatorConf);

    // starting 3 nodes with grpc
    for (int i = 0; i < 3; i++) {
      storeShuffleServerConf(buildShuffleServerConf(ServerType.GRPC, i));
    }
    // starting 3 nodes with grpc-netty
    for (int i = 0; i < 3; i++) {
      storeShuffleServerConf(buildShuffleServerConf(ServerType.GRPC_NETTY, i));
    }
    startServersWithRandomPorts();
  }

  private static ShuffleServerConf buildShuffleServerConf(ServerType serverType, int index)
      throws IOException {
    Path tempDir = Files.createTempDirectory(serverType + "-" + index);
    String dataPath = tempDir.toAbsolutePath().toString();

    ShuffleServerConf shuffleServerConf = shuffleServerConfWithoutPort(0, null, serverType);
    shuffleServerConf.setLong("rss.server.heartbeat.interval", 5000);
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 4000);
    shuffleServerConf.setString("rss.storage.basePath", dataPath);
    shuffleServerConf.setString("rss.storage.type", StorageType.MEMORY_LOCALFILE.name());
    // this will always trigger the partition split. and then reassign another servers
    shuffleServerConf.setString("rss.server.huge-partition.split.limit", "-1");
    return shuffleServerConf;
  }

  @Override
  public void updateSparkConfCustomer(SparkConf sparkConf) {
    sparkConf.set("spark.sql.shuffle.partitions", "4");

    sparkConf.set("spark." + RSS_CLIENT_RETRY_MAX, "1");
    sparkConf.set("spark." + RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER, "1");
    sparkConf.set("spark." + RSS_CLIENT_REASSIGN_ENABLED.key(), "true");
    sparkConf.set(
        "spark." + RSS_CLIENT_PARTITION_SPLIT_MODE, PartitionSplitMode.LOAD_BALANCE.name());
    // Once one shuffle-server partition split trigger, it will assign 2 nodes for the following
    // writing
    sparkConf.set("spark." + RSS_CLIENT_PARTITION_SPLIT_LOAD_BALANCE_SERVER_NUMBER, "2");
  }

  @Override
  public void checkRunState(SparkConf sparkConf) {
    if (sparkConf.getBoolean("spark." + RSS_CLIENT_REASSIGN_ENABLED.key(), false)) {
      // All servers will be assigned for one app due to the partition split
      if (sparkConf.get(RSS_CLIENT_TYPE).equals("GRPC")) {
        for (ShuffleServer shuffleServer : grpcShuffleServers) {
          Assertions.assertEquals(1, shuffleServer.getAppInfos().size());
        }
      } else {
        for (ShuffleServer shuffleServer : nettyShuffleServers) {
          Assertions.assertEquals(1, shuffleServer.getAppInfos().size());
        }
      }
    }
  }

  @Override
  public void updateRssStorage(SparkConf sparkConf) {
    // ignore
  }

  @Override
  public void checkShuffleData() throws Exception {
    // ignore
  }
}
