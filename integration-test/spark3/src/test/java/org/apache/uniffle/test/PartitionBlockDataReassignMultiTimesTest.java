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

import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.strategy.assignment.AssignmentStrategyFactory;
import org.apache.uniffle.server.MockedGrpcServer;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.buffer.ShuffleBufferManager;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_PARTITION_REASSIGN_BLOCK_RETRY_MAX_TIMES;
import static org.apache.uniffle.client.util.RssClientConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER;
import static org.apache.uniffle.client.util.RssClientConfig.RSS_CLIENT_RETRY_MAX;
import static org.apache.uniffle.common.config.RssClientConf.RSS_CLIENT_REASSIGN_ENABLED;
import static org.apache.uniffle.coordinator.CoordinatorConf.COORDINATOR_ASSIGNMENT_STRATEGY;

/** This class is to test the partition reassign mechanism of multiple retries. */
public class PartitionBlockDataReassignMultiTimesTest extends PartitionBlockDataReassignBasicTest {
  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    // for coordinator
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    coordinatorConf.setLong("rss.coordinator.app.expired", 5000);
    coordinatorConf.set(
        COORDINATOR_ASSIGNMENT_STRATEGY, AssignmentStrategyFactory.StrategyName.BASIC);

    Map<String, String> dynamicConf = Maps.newHashMap();
    dynamicConf.put(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE.name());
    addDynamicConf(coordinatorConf, dynamicConf);
    storeCoordinatorConf(coordinatorConf);

    // for shuffle-server
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();

    for (int i = 0; i < 3; i++) {
      storeMockShuffleServerConf(buildShuffleServerConf(ServerType.GRPC));
    }
    for (int i = 0; i < 2; i++) {
      storeShuffleServerConf(buildShuffleServerConf(ServerType.GRPC_NETTY));
    }
    startServersWithRandomPorts();
  }

  @Override
  public void updateSparkConfCustomer(SparkConf sparkConf) {
    sparkConf.set("spark.sql.shuffle.partitions", "4");
    sparkConf.set("spark." + RSS_CLIENT_RETRY_MAX, "2");
    sparkConf.set("spark." + RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER, "1");
    sparkConf.set("spark." + RSS_CLIENT_REASSIGN_ENABLED.key(), "true");
    sparkConf.set("spark." + RSS_PARTITION_REASSIGN_BLOCK_RETRY_MAX_TIMES.key(), "10");

    // simulate the grpc servers has different free memory
    // and make the assign priority seq: g1 -> g2 -> g3
    ShuffleServer g1 = grpcShuffleServers.get(0);
    ShuffleBufferManager bufferManager = g1.getShuffleBufferManager();
    bufferManager.setUsedMemory(bufferManager.getCapacity() - 3000000);
    g1.sendHeartbeat();

    ShuffleServer g2 = grpcShuffleServers.get(1);
    bufferManager = g2.getShuffleBufferManager();
    bufferManager.setUsedMemory(bufferManager.getCapacity() - 2000000);
    g2.sendHeartbeat();

    ShuffleServer g3 = grpcShuffleServers.get(2);
    bufferManager = g3.getShuffleBufferManager();
    bufferManager.setUsedMemory(bufferManager.getCapacity() - 1000000);
    g3.sendHeartbeat();

    // This will make the partition of g1 reassign to g2 servers.
    ((MockedGrpcServer) g1.getServer()).getService().enableMockRequireBufferFailWithNoBuffer();

    // And then reassign to g3
    ((MockedGrpcServer) g2.getServer()).getService().enableMockRequireBufferFailWithNoBuffer();
  }
}
