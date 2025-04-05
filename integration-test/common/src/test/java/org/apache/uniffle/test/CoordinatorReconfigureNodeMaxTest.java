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
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.ReconfigurableConfManager;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CoordinatorReconfigureNodeMaxTest extends CoordinatorTestBase {
  private static final int DEFAULT_SHUFFLE_NODES_MAX = 10;
  private static final int SERVER_NUM = 10;
  private static final HashSet<String> TAGS = Sets.newHashSet("t1");

  private static String tempConfFilePath;

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf1 = coordinatorConfWithoutPort();
    coordinatorConf1.setLong(CoordinatorConf.COORDINATOR_APP_EXPIRED, 2000);
    coordinatorConf1.setInteger(
        CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX, DEFAULT_SHUFFLE_NODES_MAX);
    coordinatorConf1.setLong(RssBaseConf.RSS_RECONFIGURE_INTERVAL_SEC, 1L);
    tempConfFilePath = tmpDir.getPath() + "/conf.file";
    ReconfigurableConfManager.init(coordinatorConf1, tempConfFilePath);
    storeCoordinatorConf(coordinatorConf1);

    CoordinatorConf coordinatorConf2 = coordinatorConfWithoutPort();
    coordinatorConf2.setLong(CoordinatorConf.COORDINATOR_APP_EXPIRED, 2000);
    coordinatorConf2.setInteger(
        CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX, DEFAULT_SHUFFLE_NODES_MAX);
    coordinatorConf2.setLong(RssBaseConf.RSS_RECONFIGURE_INTERVAL_SEC, 1L);
    storeCoordinatorConf(coordinatorConf2);

    for (int i = 0; i < SERVER_NUM; i++) {
      ShuffleServerConf shuffleServerConf =
          shuffleServerConfWithoutPort(i, tmpDir, ServerType.GRPC_NETTY);
      shuffleServerConf.setString(
          ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
      shuffleServerConf.set(RssBaseConf.RPC_METRICS_ENABLED, true);
      shuffleServerConf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 2000L);
      shuffleServerConf.set(ShuffleServerConf.SERVER_PRE_ALLOCATION_EXPIRED, 5000L);
      shuffleServerConf.set(ShuffleServerConf.TAGS, new ArrayList<>(TAGS));
      storeShuffleServerConf(shuffleServerConf);
    }
    startServersWithRandomPorts();

    Thread.sleep(1000 * 5);
  }

  @Test
  public void testReconfigureNodeMax() throws Exception {
    // case1: check the initial node max val = 10
    ReconfigurableConfManager.Reconfigurable<Integer> nodeMax =
        new RssConf().getReconfigurableConf(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX);
    assertEquals(DEFAULT_SHUFFLE_NODES_MAX, nodeMax.get());

    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(3)
            .retryIntervalMax(1000)
            .heartBeatThreadNum(1)
            .replica(1)
            .replicaWrite(1)
            .replicaRead(1)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(10)
            .unregisterRequestTimeSec(10)
            .build();
    shuffleWriteClient.registerCoordinators(getQuorum());
    ShuffleAssignmentsInfo info =
        shuffleWriteClient.getShuffleAssignments("app1", 0, 10, 1, TAGS, SERVER_NUM + 10, -1);
    assertEquals(DEFAULT_SHUFFLE_NODES_MAX, info.getServerToPartitionRanges().keySet().size());

    // case2: reset its value to 5
    try (FileWriter fileWriter = new FileWriter(tempConfFilePath)) {
      fileWriter.append(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX.key() + " " + 5);
    }
    Awaitility.await().timeout(2, TimeUnit.SECONDS).until(() -> nodeMax.get() == 5);
    info = shuffleWriteClient.getShuffleAssignments("app1", 0, 10, 1, TAGS, SERVER_NUM + 10, -1);
    assertEquals(5, info.getServerToPartitionRanges().keySet().size());

    // case3: recover its value to 10
    try (FileWriter fileWriter = new FileWriter(tempConfFilePath)) {
      fileWriter.append(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX.key() + " " + 10);
    }
    Awaitility.await().timeout(2, TimeUnit.SECONDS).until(() -> nodeMax.get() == 10);
    info = shuffleWriteClient.getShuffleAssignments("app1", 0, 10, 1, TAGS, SERVER_NUM + 10, -1);
    assertEquals(10, info.getServerToPartitionRanges().keySet().size());
  }
}
