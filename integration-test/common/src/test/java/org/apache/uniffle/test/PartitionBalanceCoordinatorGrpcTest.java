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

import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.client.request.RssGetShuffleAssignmentsRequest;
import org.apache.uniffle.client.response.RssGetShuffleAssignmentsResponse;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServerConf;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PartitionBalanceCoordinatorGrpcTest extends CoordinatorTestBase {

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    coordinatorConf.setLong("rss.coordinator.app.expired", 2000);
    coordinatorConf.setLong("rss.coordinator.server.heartbeat.timeout", 30000);
    storeCoordinatorConf(coordinatorConf);

    ShuffleServerConf shuffleServerConf0 = shuffleServerConfWithoutPort(0, tmpDir, ServerType.GRPC);
    shuffleServerConf0.setString("rss.server.buffer.capacity", "204800000");
    storeShuffleServerConf(shuffleServerConf0);

    ShuffleServerConf shuffleServerConf1 = shuffleServerConfWithoutPort(1, tmpDir, ServerType.GRPC);
    shuffleServerConf1.setString("rss.server.buffer.capacity", "512000000");
    storeShuffleServerConf(shuffleServerConf1);

    ShuffleServerConf shuffleServerConf2 = shuffleServerConfWithoutPort(2, tmpDir, ServerType.GRPC);
    shuffleServerConf2.setString("rss.server.buffer.capacity", "102400000");
    storeShuffleServerConf(shuffleServerConf2);

    startServersWithRandomPorts();
  }

  @Test
  public void getShuffleAssignmentsTest() throws Exception {
    CoordinatorTestUtils.waitForRegister(coordinatorClient, 3);
    RssGetShuffleAssignmentsRequest request =
        new RssGetShuffleAssignmentsRequest(
            "app1", 1, 1, 1, 1, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));
    RssGetShuffleAssignmentsResponse response = coordinatorClient.getShuffleAssignments(request);
    assertEquals(1, response.getPartitionToServers().size());
    for (Map.Entry<Integer, List<ShuffleServerInfo>> entry :
        response.getPartitionToServers().entrySet()) {
      assertEquals(1, entry.getValue().size());
      assertEquals(grpcShuffleServers.get(1).getGrpcPort(), entry.getValue().get(0).getGrpcPort());
    }
    request =
        new RssGetShuffleAssignmentsRequest(
            "app1", 2, 1, 1, 1, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));
    response = coordinatorClient.getShuffleAssignments(request);
    assertEquals(1, response.getPartitionToServers().size());
    for (Map.Entry<Integer, List<ShuffleServerInfo>> entry :
        response.getPartitionToServers().entrySet()) {
      assertEquals(1, entry.getValue().size());
      assertEquals(grpcShuffleServers.get(1).getGrpcPort(), entry.getValue().get(0).getGrpcPort());
    }
    request =
        new RssGetShuffleAssignmentsRequest(
            "app1", 2, 1, 1, 1, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));
    response = coordinatorClient.getShuffleAssignments(request);
    assertEquals(1, response.getPartitionToServers().size());
    for (Map.Entry<Integer, List<ShuffleServerInfo>> entry :
        response.getPartitionToServers().entrySet()) {
      assertEquals(1, entry.getValue().size());
      assertEquals(grpcShuffleServers.get(0).getGrpcPort(), entry.getValue().get(0).getGrpcPort());
    }
  }
}
