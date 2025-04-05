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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * This class is to test the conf of {@code org.apache.uniffle.server.ShuffleServerConf.Tags} and
 * {@code RssClientConfig.RSS_CLIENT_ASSIGNMENT_TAGS}
 */
public class AssignmentWithTagsTest extends CoordinatorTestBase {

  // KV: tag -> shuffle server id
  private static Map<String, List<Integer>> tagOfShufflePorts = new HashMap<>();
  private static final String tag1 = "fixed";
  private static final String tag2 = "elastic";

  private static void prepareShuffleServerConf(int subDirIndex, Set<String> tags, File tmpDir) {
    ShuffleServerConf shuffleServerConf =
        shuffleServerConfWithoutPort(subDirIndex, tmpDir, ServerType.GRPC_NETTY);
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 4000);
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    shuffleServerConf.setString("rss.server.tags", StringUtils.join(tags, ","));
    storeShuffleServerConf(shuffleServerConf);
  }

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    storeCoordinatorConf(coordinatorConf);

    for (int i = 0; i < 2; i++) {
      prepareShuffleServerConf(i, Sets.newHashSet(), tmpDir);
    }

    for (int i = 0; i < 2; i++) {
      prepareShuffleServerConf(2 + i, Sets.newHashSet(tag1), tmpDir);
    }

    for (int i = 0; i < 2; i++) {
      prepareShuffleServerConf(4 + i, Sets.newHashSet(tag2), tmpDir);
    }

    startServersWithRandomPorts();
    List<Integer> collect =
        nettyShuffleServers.stream().map(ShuffleServer::getNettyPort).collect(Collectors.toList());
    tagOfShufflePorts.put(Constants.SHUFFLE_SERVER_VERSION, collect);
    tagOfShufflePorts.put(
        tag1,
        Arrays.asList(
            nettyShuffleServers.get(2).getNettyPort(), nettyShuffleServers.get(3).getNettyPort()));
    tagOfShufflePorts.put(
        tag2,
        Arrays.asList(
            nettyShuffleServers.get(4).getNettyPort(), nettyShuffleServers.get(5).getNettyPort()));

    // Wait all shuffle servers registering to coordinator
    long startTimeMS = System.currentTimeMillis();
    while (true) {
      int nodeSum = coordinators.get(0).getClusterManager().getNodesNum();
      if (nodeSum == 6) {
        break;
      }
      if (System.currentTimeMillis() - startTimeMS > 1000 * 5) {
        throw new Exception("Timeout of waiting shuffle servers registry, timeout: 5s.");
      }
    }
  }

  @Test
  public void testTags() {
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
            .unregisterTimeSec(10)
            .unregisterRequestTimeSec(10)
            .build();
    shuffleWriteClient.registerCoordinators(getQuorum());

    // Case1 : only set the single default shuffle version tag
    ShuffleAssignmentsInfo assignmentsInfo =
        shuffleWriteClient.getShuffleAssignments(
            "app-1", 1, 1, 1, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION), 1, -1);

    List<Integer> assignedServerPorts =
        assignmentsInfo.getPartitionToServers().values().stream()
            .flatMap(x -> x.stream())
            .map(ShuffleServerInfo::getNettyPort)
            .collect(Collectors.toList());
    assertEquals(1, assignedServerPorts.size());
    assertTrue(
        tagOfShufflePorts
            .get(Constants.SHUFFLE_SERVER_VERSION)
            .contains(assignedServerPorts.get(0)));

    // Case2: Set the single non-exist shuffle server tag
    try {
      assignmentsInfo =
          shuffleWriteClient.getShuffleAssignments(
              "app-2", 1, 1, 1, Sets.newHashSet("non-exist"), 1, -1);
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Error happened when getShuffleAssignments with"));
    }

    // Case3: Set the single fixed tag
    assignmentsInfo =
        shuffleWriteClient.getShuffleAssignments("app-3", 1, 1, 1, Sets.newHashSet(tag1), 1, -1);
    assignedServerPorts =
        assignmentsInfo.getPartitionToServers().values().stream()
            .flatMap(x -> x.stream())
            .map(ShuffleServerInfo::getNettyPort)
            .collect(Collectors.toList());
    assertEquals(1, assignedServerPorts.size());
    assertTrue(tagOfShufflePorts.get(tag1).contains(assignedServerPorts.get(0)));

    // case4: Set the multiple tags if exists
    assignmentsInfo =
        shuffleWriteClient.getShuffleAssignments(
            "app-4", 1, 1, 1, Sets.newHashSet(tag1, Constants.SHUFFLE_SERVER_VERSION), 1, -1);
    assignedServerPorts =
        assignmentsInfo.getPartitionToServers().values().stream()
            .flatMap(x -> x.stream())
            .map(ShuffleServerInfo::getNettyPort)
            .collect(Collectors.toList());
    assertEquals(1, assignedServerPorts.size());
    assertTrue(tagOfShufflePorts.get(tag1).contains(assignedServerPorts.get(0)));

    // case5: Set the multiple tags if non-exist
    try {
      assignmentsInfo =
          shuffleWriteClient.getShuffleAssignments(
              "app-5",
              1,
              1,
              1,
              Sets.newHashSet(tag1, tag2, Constants.SHUFFLE_SERVER_VERSION),
              1,
              -1);
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Error happened when getShuffleAssignments with"));
    }
  }
}
