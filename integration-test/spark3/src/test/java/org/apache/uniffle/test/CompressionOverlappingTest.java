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
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_WRITE_OVERLAPPING_COMPRESSION_ENABLED;

public class CompressionOverlappingTest extends SparkSQLTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompressionOverlappingTest.class);

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
    return shuffleServerConf;
  }

  @Override
  public void updateSparkConfCustomer(SparkConf sparkConf) {
    sparkConf.set("spark.sql.shuffle.partitions", "4");
    String overlappingOptionKey = RSS_WRITE_OVERLAPPING_COMPRESSION_ENABLED.key();
    sparkConf.set("spark." + overlappingOptionKey, "true");
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
