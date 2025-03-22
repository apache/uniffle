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
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.client.request.RssGetShuffleAssignmentsRequest;
import org.apache.uniffle.client.response.RssGetShuffleAssignmentsResponse;
import org.apache.uniffle.common.ServerStatus;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.ServerNode;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

public class HealthCheckCoordinatorGrpcTest extends CoordinatorTestBase {

  private static File tempDataFile;
  private static int writeDataSize;

  @BeforeAll
  public static void setupServers(@TempDir File serverTmpDir) throws Exception {
    tempDataFile = new File(serverTmpDir, "data");
    File data1 = new File(serverTmpDir, "data1");
    data1.mkdirs();
    File data2 = new File(serverTmpDir, "data2");
    data2.mkdirs();
    long freeSize = serverTmpDir.getUsableSpace();
    double maxUsage;
    double healthUsage;
    if (freeSize > 400 * 1024 * 1024) {
      writeDataSize = 200 * 1024 * 1024;
    } else {
      writeDataSize = (int) freeSize / 2;
    }
    long totalSize = serverTmpDir.getTotalSpace();
    long usedSize = serverTmpDir.getTotalSpace() - serverTmpDir.getUsableSpace();
    maxUsage = (writeDataSize * 0.75 + usedSize) * 100.0 / totalSize;
    healthUsage = (writeDataSize * 0.5 + usedSize) * 100.0 / totalSize;
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    coordinatorConf.setLong(CoordinatorConf.COORDINATOR_APP_EXPIRED, 2000);
    coordinatorConf.setLong(CoordinatorConf.COORDINATOR_HEARTBEAT_TIMEOUT, 3000);
    storeCoordinatorConf(coordinatorConf);

    ShuffleServerConf shuffleServerConf1 =
        shuffleServerConfWithoutPort(0, serverTmpDir, ServerType.GRPC);
    shuffleServerConf1.setBoolean(ShuffleServerConf.HEALTH_CHECK_ENABLE, true);
    shuffleServerConf1.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE.name());
    shuffleServerConf1.set(
        ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(data1.getAbsolutePath()));
    shuffleServerConf1.setDouble(
        ShuffleServerConf.HEALTH_STORAGE_RECOVERY_USAGE_PERCENTAGE, healthUsage);
    shuffleServerConf1.setDouble(ShuffleServerConf.HEALTH_STORAGE_MAX_USAGE_PERCENTAGE, maxUsage);
    shuffleServerConf1.setLong(ShuffleServerConf.HEALTH_CHECK_INTERVAL, 1000L);
    storeShuffleServerConf(shuffleServerConf1);

    ShuffleServerConf shuffleServerConf2 =
        shuffleServerConfWithoutPort(1, serverTmpDir, ServerType.GRPC);
    shuffleServerConf2.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE.name());
    shuffleServerConf2.set(
        ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(data2.getAbsolutePath()));
    shuffleServerConf2.setDouble(
        ShuffleServerConf.HEALTH_STORAGE_RECOVERY_USAGE_PERCENTAGE, healthUsage);
    shuffleServerConf2.setDouble(ShuffleServerConf.HEALTH_STORAGE_MAX_USAGE_PERCENTAGE, maxUsage);
    shuffleServerConf2.setLong(ShuffleServerConf.HEALTH_CHECK_INTERVAL, 1000L);
    shuffleServerConf2.setBoolean(ShuffleServerConf.HEALTH_CHECK_ENABLE, true);
    storeShuffleServerConf(shuffleServerConf2);

    startServersWithRandomPorts();
  }

  @Test
  public void healthCheckTest() throws Exception {
    Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
    assertEquals(2, coordinatorClient.getShuffleServerList().getServersCount());
    List<ServerNode> nodes =
        coordinators
            .get(0)
            .getClusterManager()
            .getServerList(Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));
    assertEquals(2, coordinatorClient.getShuffleServerList().getServersCount());
    assertEquals(2, nodes.size());

    RssGetShuffleAssignmentsRequest request =
        new RssGetShuffleAssignmentsRequest(
            "1", 1, 1, 1, 1, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));
    RssGetShuffleAssignmentsResponse response = coordinatorClient.getShuffleAssignments(request);
    assertFalse(response.getPartitionToServers().isEmpty());
    for (ServerNode node : nodes) {
      assertEquals(ServerStatus.ACTIVE, node.getStatus());
    }
    byte[] bytes = new byte[writeDataSize];
    new Random().nextBytes(bytes);
    try (FileOutputStream out = new FileOutputStream(tempDataFile)) {
      out.write(bytes);
    }
    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);

    nodes = coordinators.get(0).getClusterManager().list();
    assertEquals(2, nodes.size());
    for (ServerNode node : nodes) {
      assertEquals(ServerStatus.UNHEALTHY, node.getStatus());
    }
    nodes =
        coordinators
            .get(0)
            .getClusterManager()
            .getServerList(Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));
    assertEquals(0, nodes.size());
    response = coordinatorClient.getShuffleAssignments(request);
    assertEquals(StatusCode.INTERNAL_ERROR, response.getStatusCode());

    tempDataFile.delete();
    int i = 0;
    do {
      Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
      nodes =
          coordinators
              .get(0)
              .getClusterManager()
              .getServerList(Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));
      i++;
      if (i == 10) {
        fail();
      }
    } while (nodes.size() != 2);
    for (ServerNode node : nodes) {
      assertEquals(ServerStatus.ACTIVE, node.getStatus());
    }
    assertEquals(2, nodes.size());
    response = coordinatorClient.getShuffleAssignments(request);
    assertFalse(response.getPartitionToServers().isEmpty());
  }
}
