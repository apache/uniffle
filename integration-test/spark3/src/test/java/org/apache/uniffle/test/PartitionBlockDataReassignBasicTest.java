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
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssSparkConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.buffer.ShuffleBufferManager;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.client.util.RssClientConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER;
import static org.apache.uniffle.client.util.RssClientConfig.RSS_CLIENT_RETRY_MAX;
import static org.apache.uniffle.common.config.RssClientConf.RSS_CLIENT_REASSIGN_ENABLED;

/** This class is to basic test the mechanism of partition block data reassignment */
public class PartitionBlockDataReassignBasicTest extends SparkSQLTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PartitionBlockDataReassignBasicTest.class);

  protected static String basePath;

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    LOGGER.info("Setup servers");

    // for coordinator
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    coordinatorConf.setLong("rss.coordinator.app.expired", 5000);
    Map<String, String> dynamicConf = Maps.newHashMap();
    dynamicConf.put(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE.name());
    addDynamicConf(coordinatorConf, dynamicConf);
    storeCoordinatorConf(coordinatorConf);

    // for shuffle-server
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();

    for (int i = 0; i < 3; i++) {
      storeShuffleServerConf(buildShuffleServerConf(ServerType.GRPC));
    }
    for (int i = 0; i < 2; i++) {
      storeShuffleServerConf(buildShuffleServerConf(ServerType.GRPC_NETTY));
    }
    startServersWithRandomPorts();

    // simulate one server without enough buffer for grpc
    ShuffleServer grpcServer = grpcShuffleServers.get(0);
    ShuffleBufferManager bufferManager = grpcServer.getShuffleBufferManager();
    bufferManager.setUsedMemory(bufferManager.getCapacity() + 100);

    // simulate one server without enough buffer for netty server
    ShuffleServer nettyServer = nettyShuffleServers.get(0);
    bufferManager = nettyServer.getShuffleBufferManager();
    bufferManager.setUsedMemory(bufferManager.getCapacity() + 100);
  }

  protected static ShuffleServerConf buildShuffleServerConf(ServerType serverType) {
    ShuffleServerConf shuffleServerConf = shuffleServerConfWithoutPort(0, null, serverType);
    shuffleServerConf.setLong("rss.server.heartbeat.interval", 5000);
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 4000);
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    shuffleServerConf.setString("rss.storage.type", StorageType.MEMORY_LOCALFILE.name());
    return shuffleServerConf;
  }

  @Override
  public void updateSparkConfCustomer(SparkConf sparkConf) {
    sparkConf.set("spark.sql.shuffle.partitions", "4");
    sparkConf.set("spark." + RSS_CLIENT_RETRY_MAX, "2");
    sparkConf.set(
        "spark." + RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER,
        String.valueOf(grpcShuffleServers.size()));
    sparkConf.set("spark." + RSS_CLIENT_REASSIGN_ENABLED.key(), "true");
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
