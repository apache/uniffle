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
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.client.request.RssApplicationInfoRequest;
import org.apache.uniffle.client.request.RssGetShuffleAssignmentsRequest;
import org.apache.uniffle.client.response.RssApplicationInfoResponse;
import org.apache.uniffle.client.response.RssGetShuffleAssignmentsResponse;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ShuffleRegisterInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.storage.StorageInfo;
import org.apache.uniffle.common.storage.StorageMedia;
import org.apache.uniffle.common.storage.StorageStatus;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.CoordinatorServer;
import org.apache.uniffle.coordinator.ServerNode;
import org.apache.uniffle.coordinator.SimpleClusterManager;
import org.apache.uniffle.coordinator.metric.CoordinatorGrpcMetrics;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.proto.RssProtos.GetShuffleAssignmentsResponse;
import org.apache.uniffle.proto.RssProtos.PartitionRangeAssignment;
import org.apache.uniffle.proto.RssProtos.ShuffleServerId;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;

import static org.apache.uniffle.common.metrics.GRPCMetrics.GRPC_SERVER_CONNECTION_NUMBER_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.org.webcompere.systemstubs.SystemStubs.withEnvironmentVariables;

public class CoordinatorGrpcTest extends CoordinatorTestBase {

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    coordinatorConf.set(RssBaseConf.RPC_METRICS_ENABLED, true);
    coordinatorConf.setString(CoordinatorConf.COORDINATOR_ASSIGNMENT_STRATEGY.key(), "BASIC");
    coordinatorConf.setLong("rss.coordinator.app.expired", 2000);
    coordinatorConf.setLong("rss.coordinator.server.heartbeat.timeout", 3000);
    storeCoordinatorConf(coordinatorConf);

    storeShuffleServerConf(shuffleServerConfWithoutPort(0, tmpDir, ServerType.GRPC));
    storeShuffleServerConf(shuffleServerConfWithoutPort(1, tmpDir, ServerType.GRPC));

    startServersWithRandomPorts();
  }

  @Test
  public void testGetPartitionToServers() {
    GetShuffleAssignmentsResponse testResponse = generateShuffleAssignmentsResponse();

    Map<Integer, List<ShuffleServerInfo>> partitionToServers =
        coordinatorClient.getPartitionToServers(testResponse);

    assertEquals(
        Arrays.asList(
            new ShuffleServerInfo("id1", "0.0.0.1", 100),
            new ShuffleServerInfo("id2", "0.0.0.2", 100)),
        partitionToServers.get(0));
    assertEquals(
        Arrays.asList(
            new ShuffleServerInfo("id1", "0.0.0.1", 100),
            new ShuffleServerInfo("id2", "0.0.0.2", 100)),
        partitionToServers.get(1));
    assertEquals(
        Arrays.asList(
            new ShuffleServerInfo("id3", "0.0.0.3", 100),
            new ShuffleServerInfo("id4", "0.0.0.4", 100)),
        partitionToServers.get(2));
    assertEquals(
        Arrays.asList(
            new ShuffleServerInfo("id3", "0.0.0.3", 100),
            new ShuffleServerInfo("id4", "0.0.0.4", 100)),
        partitionToServers.get(3));
    assertNull(partitionToServers.get(4));
  }

  @Test
  public void getShuffleRegisterInfoTest() {
    GetShuffleAssignmentsResponse testResponse = generateShuffleAssignmentsResponse();
    Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges =
        coordinatorClient.getServerToPartitionRanges(testResponse);
    List<ShuffleRegisterInfo> expected =
        Arrays.asList(
            new ShuffleRegisterInfo(
                new ShuffleServerInfo("id1", "0.0.0.1", 100),
                Lists.newArrayList(new PartitionRange(0, 1))),
            new ShuffleRegisterInfo(
                new ShuffleServerInfo("id2", "0.0.0.2", 100),
                Lists.newArrayList(new PartitionRange(0, 1))),
            new ShuffleRegisterInfo(
                new ShuffleServerInfo("id3", "0.0.0.3", 100),
                Lists.newArrayList(new PartitionRange(2, 3))),
            new ShuffleRegisterInfo(
                new ShuffleServerInfo("id4", "0.0.0.4", 100),
                Lists.newArrayList(new PartitionRange(2, 3))));
    assertEquals(4, serverToPartitionRanges.size());
    for (ShuffleRegisterInfo sri : expected) {
      List<PartitionRange> partitionRanges =
          serverToPartitionRanges.get(sri.getShuffleServerInfo());
      assertEquals(sri.getPartitionRanges(), partitionRanges);
    }
  }

  @Test
  public void getShuffleAssignmentsTest(@TempDir File tmpDir) throws Exception {
    final String appId = "getShuffleAssignmentsTest";
    CoordinatorTestUtils.waitForRegister(coordinatorClient, 2);

    grpcShuffleServers.get(0).stopServer();
    ShuffleServerConf shuffleServerConf = shuffleServerConfWithoutPort(0, tmpDir, ServerType.GRPC);
    shuffleServerConf.set(ShuffleServerConf.STORAGE_MEDIA_PROVIDER_ENV_KEY, "RSS_ENV_KEY");
    String baseDir = shuffleServerConf.get(ShuffleServerConf.RSS_STORAGE_BASE_PATH).get(0);
    shuffleServerConf.setString(ShuffleServerConf.RSS_STORAGE_BASE_PATH.key(), baseDir);
    String storageTypeJsonSource = String.format("{\"%s\": \"ssd\"}", baseDir);
    withEnvironmentVariables("RSS_ENV_KEY", storageTypeJsonSource)
        .execute(
            () -> {
              shuffleServerConf.setString("rss.coordinator.quorum", getQuorum());
              shuffleServerConf.setInteger(RssBaseConf.RPC_SERVER_PORT, 0);
              shuffleServerConf.setInteger(RssBaseConf.JETTY_HTTP_PORT, 0);
              ShuffleServer ss = new ShuffleServer(shuffleServerConf);
              ss.start();
              grpcShuffleServers.set(0, ss);
            });
    Thread.sleep(4000);
    CoordinatorServer coordinatorServer = coordinators.get(0);
    ((SimpleClusterManager) (coordinatorServer.getClusterManager())).nodesCheckTest();

    // add tag when ClientType is `GRPC`
    RssGetShuffleAssignmentsRequest request =
        new RssGetShuffleAssignmentsRequest(
            appId,
            1,
            10,
            4,
            1,
            Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION, ClientType.GRPC.name()));
    RssGetShuffleAssignmentsResponse response = coordinatorClient.getShuffleAssignments(request);
    Set<Integer> expectedStart = Sets.newHashSet(0, 4, 8);

    Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges =
        response.getServerToPartitionRanges();
    assertEquals(2, serverToPartitionRanges.size());
    List<PartitionRange> partitionRanges = Lists.newArrayList();
    for (List<PartitionRange> ranges : serverToPartitionRanges.values()) {
      partitionRanges.addAll(ranges);
    }
    for (PartitionRange pr : partitionRanges) {
      switch (pr.getStart()) {
        case 0:
          assertEquals(3, pr.getEnd());
          expectedStart.remove(0);
          break;
        case 4:
          assertEquals(7, pr.getEnd());
          expectedStart.remove(4);
          break;
        case 8:
          assertEquals(11, pr.getEnd());
          expectedStart.remove(8);
          break;
        default:
          fail("Shouldn't be here");
      }
    }
    assertTrue(expectedStart.isEmpty());

    request =
        new RssGetShuffleAssignmentsRequest(
            appId,
            1,
            10,
            4,
            2,
            Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION, ClientType.GRPC.name()));
    response = coordinatorClient.getShuffleAssignments(request);
    serverToPartitionRanges = response.getServerToPartitionRanges();
    assertEquals(2, serverToPartitionRanges.size());
    partitionRanges = Lists.newArrayList();
    for (List<PartitionRange> ranges : serverToPartitionRanges.values()) {
      partitionRanges.addAll(ranges);
    }
    assertEquals(6, partitionRanges.size());
    int range0To3 = 0;
    int range4To7 = 0;
    int range8To11 = 0;
    for (PartitionRange pr : partitionRanges) {
      switch (pr.getStart()) {
        case 0:
          assertEquals(3, pr.getEnd());
          range0To3++;
          break;
        case 4:
          assertEquals(7, pr.getEnd());
          range4To7++;
          break;
        case 8:
          assertEquals(11, pr.getEnd());
          range8To11++;
          break;
        default:
          fail("Shouldn't be here");
      }
    }
    assertEquals(2, range0To3);
    assertEquals(2, range4To7);
    assertEquals(2, range8To11);

    request =
        new RssGetShuffleAssignmentsRequest(appId, 3, 2, 1, 1, Sets.newHashSet("fake_version"));
    try {
      coordinatorClient.getShuffleAssignments(request);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Empty assignment"));
    }
  }

  @Test
  public void appHeartbeatTest() throws Exception {
    RssApplicationInfoResponse response =
        coordinatorClient.registerApplicationInfo(
            new RssApplicationInfoRequest("application_appHeartbeatTest1", 1000, "user"));
    assertEquals(StatusCode.SUCCESS, response.getStatusCode());
    assertEquals(
        Sets.newHashSet("application_appHeartbeatTest1"),
        coordinators.get(0).getApplicationManager().getAppIds());
    coordinatorClient.registerApplicationInfo(
        new RssApplicationInfoRequest("application_appHeartbeatTest2", 1000, "user"));
    assertEquals(
        Sets.newHashSet("application_appHeartbeatTest1", "application_appHeartbeatTest2"),
        coordinators.get(0).getApplicationManager().getAppIds());
    int retry = 0;
    while (retry < 5) {
      coordinatorClient.registerApplicationInfo(
          new RssApplicationInfoRequest("application_appHeartbeatTest1", 1000, "user"));
      retry++;
      Thread.sleep(1000);
    }
    // appHeartbeatTest2 was removed because of expired
    assertEquals(
        Sets.newHashSet("application_appHeartbeatTest1"),
        coordinators.get(0).getApplicationManager().getAppIds());
  }

  @Test
  public void shuffleServerHeartbeatTest(@TempDir File tempDir) throws Exception {
    CoordinatorTestUtils.waitForRegister(coordinatorClient, 2);
    grpcShuffleServers.get(0).stopServer();
    Thread.sleep(5000);
    SimpleClusterManager scm = (SimpleClusterManager) coordinators.get(0).getClusterManager();
    List<ServerNode> nodes = scm.getServerList(Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));
    assertEquals(1, nodes.size());
    ServerNode node = nodes.get(0);
    assertEquals(1, node.getStorageInfo().size());
    StorageInfo infoHead = node.getStorageInfo().values().iterator().next();
    final StorageInfo expectedStorageInfo =
        grpcShuffleServers.get(1).getStorageManager().getStorageInfo().values().iterator().next();
    assertEquals(expectedStorageInfo, infoHead);
    assertEquals(StorageStatus.NORMAL, infoHead.getStatus());
    assertTrue(node.getTags().contains(Constants.SHUFFLE_SERVER_VERSION));
    assertTrue(scm.getTagToNodes().get(Constants.SHUFFLE_SERVER_VERSION).contains(node));

    ShuffleServerConf shuffleServerConf = shuffleServerConfWithoutPort(0, tempDir, ServerType.GRPC);
    shuffleServerConf.set(ShuffleServerConf.STORAGE_MEDIA_PROVIDER_ENV_KEY, "RSS_ENV_KEY");
    String baseDir = shuffleServerConf.get(ShuffleServerConf.RSS_STORAGE_BASE_PATH).get(0);
    shuffleServerConf.setString(ShuffleServerConf.RSS_STORAGE_BASE_PATH.key(), baseDir);
    String storageTypeJsonSource = String.format("{\"%s\": \"ssd\"}", baseDir);
    withEnvironmentVariables("RSS_ENV_KEY", storageTypeJsonSource)
        .execute(
            () -> {
              // set this server's tag to ssd
              shuffleServerConf.set(ShuffleServerConf.TAGS, Lists.newArrayList("SSD"));
              shuffleServerConf.setString("rss.coordinator.quorum", getQuorum());
              shuffleServerConf.setInteger(RssBaseConf.RPC_SERVER_PORT, 0);
              shuffleServerConf.setInteger(RssBaseConf.JETTY_HTTP_PORT, 0);
              ShuffleServer ss = new ShuffleServer(shuffleServerConf);
              ss.start();
              grpcShuffleServers.set(0, ss);
            });
    Thread.sleep(3000);
    assertEquals(2, coordinators.get(0).getClusterManager().getNodesNum());
    nodes = scm.getServerList(Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION, "SSD"));
    assertEquals(1, nodes.size());
    ServerNode ssdNode = nodes.get(0);
    infoHead = ssdNode.getStorageInfo().values().iterator().next();
    assertEquals(StorageMedia.SSD, infoHead.getType());

    scm.close();
  }

  @Test
  public void rpcMetricsTest() throws Exception {
    CoordinatorTestUtils.waitForRegister(coordinatorClient, 2);
    double newValue =
        coordinators
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(CoordinatorGrpcMetrics.HEARTBEAT_METHOD)
            .get();
    assertTrue(newValue > 1);

    String appId = "rpcMetricsTest";
    RssGetShuffleAssignmentsRequest request =
        new RssGetShuffleAssignmentsRequest(
            appId, 1, 10, 4, 1, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));
    double oldValue =
        coordinators
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(CoordinatorGrpcMetrics.GET_SHUFFLE_ASSIGNMENTS_METHOD)
            .get();
    coordinatorClient.getShuffleAssignments(request);
    newValue =
        coordinators
            .get(0)
            .getGrpcMetrics()
            .getCounterMap()
            .get(CoordinatorGrpcMetrics.GET_SHUFFLE_ASSIGNMENTS_METHOD)
            .get();
    assertEquals(oldValue + 1, newValue, 0.5);

    double connectionSize =
        coordinators
            .get(0)
            .getGrpcMetrics()
            .getGaugeMap()
            .get(GRPC_SERVER_CONNECTION_NUMBER_KEY)
            .get();
    assertTrue(connectionSize > 0);
  }

  private GetShuffleAssignmentsResponse generateShuffleAssignmentsResponse() {
    ShuffleServerId ss1 =
        RssProtos.ShuffleServerId.newBuilder()
            .setIp("0.0.0.1")
            .setPort(100)
            .setNettyPort(-1)
            .setId("id1")
            .build();

    ShuffleServerId ss2 =
        RssProtos.ShuffleServerId.newBuilder()
            .setIp("0.0.0.2")
            .setPort(100)
            .setNettyPort(-1)
            .setId("id2")
            .build();

    ShuffleServerId ss3 =
        RssProtos.ShuffleServerId.newBuilder()
            .setIp("0.0.0.3")
            .setPort(100)
            .setNettyPort(-1)
            .setId("id3")
            .build();

    ShuffleServerId ss4 =
        RssProtos.ShuffleServerId.newBuilder()
            .setIp("0.0.0.4")
            .setPort(100)
            .setNettyPort(-1)
            .setId("id4")
            .build();

    PartitionRangeAssignment assignment1 =
        RssProtos.PartitionRangeAssignment.newBuilder()
            .setStartPartition(0)
            .setEndPartition(1)
            .addAllServer(Arrays.asList(ss1, ss2))
            .build();

    PartitionRangeAssignment assignment2 =
        RssProtos.PartitionRangeAssignment.newBuilder()
            .setStartPartition(2)
            .setEndPartition(3)
            .addAllServer(Arrays.asList(ss3, ss4))
            .build();

    return RssProtos.GetShuffleAssignmentsResponse.newBuilder()
        .addAllAssignments(Arrays.asList(assignment1, assignment2))
        .build();
  }
}
