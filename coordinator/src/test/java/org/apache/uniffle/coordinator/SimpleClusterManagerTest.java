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

package org.apache.uniffle.coordinator;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.ServerStatus;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleClusterManagerTest {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleClusterManagerTest.class);

  private final Set<String> testTags = Sets.newHashSet("test");
  private final Set<String> nettyTags = Sets.newHashSet("test", ClientType.GRPC_NETTY.name());
  private final Set<String> grpcTags = Sets.newHashSet("test", ClientType.GRPC.name());

  @BeforeEach
  public void setUp() {
    CoordinatorMetrics.register();
  }

  @AfterEach
  public void clear() {
    CoordinatorMetrics.clear();
  }

  @Test
  public void startupSilentPeriodTest() throws Exception {
    CoordinatorConf coordinatorConf = new CoordinatorConf();
    coordinatorConf.set(CoordinatorConf.COORDINATOR_START_SILENT_PERIOD_ENABLED, true);
    coordinatorConf.set(CoordinatorConf.COORDINATOR_START_SILENT_PERIOD_DURATION, 20 * 1000L);
    try (SimpleClusterManager manager =
        new SimpleClusterManager(coordinatorConf, new Configuration())) {
      assertFalse(manager.isReadyForServe());

      manager.setStartTime(System.currentTimeMillis() - 30 * 1000L);
      assertTrue(manager.isReadyForServe());
    }
  }

  @Test
  public void getServerListTest() throws Exception {
    CoordinatorConf ssc = new CoordinatorConf();
    ssc.setLong(CoordinatorConf.COORDINATOR_HEARTBEAT_TIMEOUT, 30 * 1000L);
    try (SimpleClusterManager clusterManager = new SimpleClusterManager(ssc, new Configuration())) {

      ServerNode sn1 = new ServerNode("sn1", "ip", 0, 100L, 50L, 20, 10, grpcTags);
      ServerNode sn2 = new ServerNode("sn2", "ip", 0, 100L, 50L, 21, 10, grpcTags);
      ServerNode sn3 = new ServerNode("sn3", "ip", 0, 100L, 50L, 20, 11, grpcTags);
      clusterManager.add(sn1);
      clusterManager.add(sn2);
      clusterManager.add(sn3);
      List<ServerNode> serverNodes = clusterManager.getServerList(grpcTags);
      assertEquals(3, serverNodes.size());
      Set<String> expectedIds = Sets.newHashSet("sn1", "sn2", "sn3");
      assertEquals(
          expectedIds, serverNodes.stream().map(ServerNode::getId).collect(Collectors.toSet()));

      // tag changes
      sn1 = new ServerNode("sn1", "ip", 0, 100L, 50L, 20, 10, Sets.newHashSet("new_tag"));
      sn2 = new ServerNode("sn2", "ip", 0, 100L, 50L, 21, 10, Sets.newHashSet("test", "new_tag"));
      ServerNode sn4 = new ServerNode("sn4", "ip", 0, 100L, 51L, 20, 10, grpcTags);
      clusterManager.add(sn1);
      clusterManager.add(sn2);
      clusterManager.add(sn4);
      serverNodes = clusterManager.getServerList(testTags);
      assertEquals(3, serverNodes.size());
      assertTrue(serverNodes.contains(sn2));
      assertTrue(serverNodes.contains(sn3));
      assertTrue(serverNodes.contains(sn4));

      Map<String, Set<ServerNode>> tagToNodes = clusterManager.getTagToNodes();
      assertEquals(3, tagToNodes.size());

      Set<ServerNode> newTagNodes = tagToNodes.get("new_tag");
      assertEquals(2, newTagNodes.size());
      assertTrue(newTagNodes.contains(sn1));
      assertTrue(newTagNodes.contains(sn2));

      Set<ServerNode> testTagNodes = tagToNodes.get("test");
      assertEquals(3, testTagNodes.size());
      assertTrue(testTagNodes.contains(sn2));
      assertTrue(testTagNodes.contains(sn3));
      assertTrue(testTagNodes.contains(sn4));
    }
  }

  @Test
  public void getLostServerListTest() throws Exception {
    CoordinatorConf coordinatorConf = new CoordinatorConf();
    // Shorten the heartbeat time
    coordinatorConf.setLong(CoordinatorConf.COORDINATOR_HEARTBEAT_TIMEOUT, 600L);
    try (SimpleClusterManager clusterManager =
        new SimpleClusterManager(coordinatorConf, new Configuration())) {
      ServerNode sn1 = new ServerNode("sn1", "ip", 0, 100L, 50L, 20, 10, grpcTags);
      ServerNode sn2 = new ServerNode("sn2", "ip", 0, 100L, 50L, 21, 10, grpcTags);
      ServerNode sn3 = new ServerNode("sn3", "ip", 0, 100L, 50L, 20, 11, grpcTags);
      clusterManager.add(sn1);
      clusterManager.add(sn2);
      clusterManager.add(sn3);
      Set<String> expectedIds = Sets.newHashSet("sn1", "sn2", "sn3");
      await()
          .atMost(1, TimeUnit.SECONDS)
          .until(
              () -> {
                Set<String> lostServerList =
                    clusterManager.getLostServerList().stream()
                        .map(ServerNode::getId)
                        .collect(Collectors.toSet());
                return CollectionUtils.isEqualCollection(lostServerList, expectedIds);
              });
      // re-register sn3
      sn3 = new ServerNode("sn3", "ip", 0, 100L, 50L, 20, 11, grpcTags);
      clusterManager.add(sn3);
      Set<String> expectedIdsre = Sets.newHashSet("sn1", "sn2");
      await()
          .atMost(1, TimeUnit.SECONDS)
          .until(
              () -> {
                // Retrieve listed ServerNode List
                Set<String> lostServerListre =
                    clusterManager.getLostServerList().stream()
                        .map(ServerNode::getId)
                        .collect(Collectors.toSet());
                return CollectionUtils.isEqualCollection(lostServerListre, expectedIdsre);
              });
    }
  }

  @Test
  public void getUnhealthyServerList() throws Exception {
    CoordinatorConf coordinatorConf = new CoordinatorConf();
    // Shorten the heartbeat time
    coordinatorConf.setLong(CoordinatorConf.COORDINATOR_HEARTBEAT_TIMEOUT, 600L);
    try (SimpleClusterManager clusterManager =
        new SimpleClusterManager(coordinatorConf, new Configuration())) {
      ServerNode sn1 = new ServerNode("sn1", "ip", 0, 100L, 50L, 20, 10, grpcTags);
      ServerNode sn2 = new ServerNode("sn2", "ip", 0, 100L, 50L, 21, 10, grpcTags);
      ServerNode sn3 =
          new ServerNode("sn3", "ip", 0, 100L, 50L, 20, 11, grpcTags, ServerStatus.UNHEALTHY);
      ServerNode sn4 =
          new ServerNode("sn4", "ip", 0, 100L, 50L, 20, 11, grpcTags, ServerStatus.UNHEALTHY);
      clusterManager.add(sn1);
      clusterManager.add(sn2);
      clusterManager.add(sn3);
      clusterManager.add(sn4);
      // Analog timeout registration
      Set<String> expectedIds = Sets.newHashSet("sn3", "sn4");
      await()
          .atMost(1, TimeUnit.SECONDS)
          .until(
              () -> {
                Set<String> unhealthyServerList =
                    clusterManager.getUnhealthyServerList().stream()
                        .map(ServerNode::getId)
                        .collect(Collectors.toSet());
                return CollectionUtils.isEqualCollection(unhealthyServerList, expectedIds);
              });
      // Register unhealthy node sn3 again
      sn3 = new ServerNode("sn3", "ip", 0, 100L, 50L, 20, 11, grpcTags, ServerStatus.UNHEALTHY);
      clusterManager.add(sn3);
      Set<String> expectedIdsre = Sets.newHashSet("sn3");
      await()
          .atMost(1, TimeUnit.SECONDS)
          .until(
              () -> {
                Set<String> unhealthyServerListre =
                    clusterManager.getUnhealthyServerList().stream()
                        .map(ServerNode::getId)
                        .collect(Collectors.toSet());
                return CollectionUtils.isEqualCollection(unhealthyServerListre, expectedIdsre);
              });
      // At this point verify that sn4 is in the lost list
      List<ServerNode> lostremoveunhealthy = clusterManager.getLostServerList();
      Set<String> expectedIdlostremoveunhealthy = Sets.newHashSet("sn1", "sn2", "sn4");
      assertEquals(
          expectedIdlostremoveunhealthy,
          lostremoveunhealthy.stream().map(ServerNode::getId).collect(Collectors.toSet()));
    }
  }

  @Test
  public void getServerListForNettyTest() throws Exception {
    CoordinatorConf ssc = new CoordinatorConf();
    ssc.setLong(CoordinatorConf.COORDINATOR_HEARTBEAT_TIMEOUT, 30 * 1000L);
    try (SimpleClusterManager clusterManager = new SimpleClusterManager(ssc, new Configuration())) {

      ServerNode sn1 =
          new ServerNode(
              "sn1",
              "ip",
              0,
              100L,
              50L,
              20,
              10,
              nettyTags,
              ServerStatus.ACTIVE,
              JavaUtils.newConcurrentMap(),
              1);
      ServerNode sn2 =
          new ServerNode(
              "sn2",
              "ip",
              0,
              100L,
              50L,
              21,
              10,
              nettyTags,
              ServerStatus.ACTIVE,
              JavaUtils.newConcurrentMap(),
              1);
      ServerNode sn3 =
          new ServerNode(
              "sn3",
              "ip",
              0,
              100L,
              50L,
              20,
              11,
              nettyTags,
              ServerStatus.ACTIVE,
              JavaUtils.newConcurrentMap(),
              1);
      ServerNode sn4 = new ServerNode("sn4", "ip", 0, 100L, 50L, 20, 11, grpcTags);
      clusterManager.add(sn1);
      clusterManager.add(sn2);
      clusterManager.add(sn3);
      clusterManager.add(sn4);

      List<ServerNode> serverNodes2 = clusterManager.getServerList(nettyTags);
      assertEquals(3, serverNodes2.size());

      List<ServerNode> serverNodes3 = clusterManager.getServerList(grpcTags);
      assertEquals(1, serverNodes3.size());

      List<ServerNode> serverNodes4 = clusterManager.getServerList(testTags);
      assertEquals(4, serverNodes4.size());

      Map<String, Set<ServerNode>> tagToNodes = clusterManager.getTagToNodes();
      assertEquals(3, tagToNodes.size());

      // tag changes
      sn1 =
          new ServerNode(
              "sn1",
              "ip",
              0,
              100L,
              50L,
              20,
              10,
              Sets.newHashSet("new_tag"),
              ServerStatus.ACTIVE,
              JavaUtils.newConcurrentMap(),
              1);
      sn2 =
          new ServerNode(
              "sn2",
              "ip",
              0,
              100L,
              50L,
              21,
              10,
              Sets.newHashSet("test", "new_tag"),
              ServerStatus.ACTIVE,
              JavaUtils.newConcurrentMap(),
              1);
      sn4 = new ServerNode("sn4", "ip", 0, 100L, 51L, 20, 10, grpcTags);
      clusterManager.add(sn1);
      clusterManager.add(sn2);
      clusterManager.add(sn4);
      Set<ServerNode> testTagNodesForNetty = tagToNodes.get(ClientType.GRPC_NETTY.name());
      assertEquals(1, testTagNodesForNetty.size());

      List<ServerNode> serverNodes = clusterManager.getServerList(grpcTags);
      assertEquals(1, serverNodes.size());
      assertTrue(serverNodes.contains(sn4));

      Set<ServerNode> newTagNodes = tagToNodes.get("new_tag");
      assertEquals(2, newTagNodes.size());
      assertTrue(newTagNodes.contains(sn1));
      assertTrue(newTagNodes.contains(sn2));
      Set<ServerNode> testTagNodes = tagToNodes.get("test");
      assertEquals(3, testTagNodes.size());
      assertTrue(testTagNodes.contains(sn2));
      assertTrue(testTagNodes.contains(sn3));
      assertTrue(testTagNodes.contains(sn4));
    }
  }

  @Test
  public void testGetCorrectServerNodesWhenOneNodeRemovedAndUnhealthyNodeFound() throws Exception {
    CoordinatorConf ssc = new CoordinatorConf();
    ssc.setLong(CoordinatorConf.COORDINATOR_HEARTBEAT_TIMEOUT, 30 * 1000L);
    try (SimpleClusterManager clusterManager = new SimpleClusterManager(ssc, new Configuration())) {
      ServerNode sn1 =
          new ServerNode("sn1", "ip", 0, 100L, 50L, 20, 10, testTags, ServerStatus.UNHEALTHY);
      ServerNode sn2 = new ServerNode("sn2", "ip", 0, 100L, 50L, 21, 10, testTags);
      ServerNode sn3 = new ServerNode("sn3", "ip", 0, 100L, 50L, 20, 11, testTags);
      clusterManager.add(sn1);
      clusterManager.add(sn2);
      clusterManager.add(sn3);

      List<ServerNode> serverNodes = clusterManager.getServerList(testTags);
      assertEquals(2, serverNodes.size());
      assertEquals(0, CoordinatorMetrics.gaugeUnhealthyServerNum.get());
      clusterManager.nodesCheck();

      List<ServerNode> serverList = clusterManager.getServerList(testTags);
      Assertions.assertEquals(2, serverList.size());
      assertEquals(1, CoordinatorMetrics.gaugeUnhealthyServerNum.get());

      sn3.setTimestamp(System.currentTimeMillis() - 60 * 1000L);
      clusterManager.nodesCheck();

      List<ServerNode> serverList2 = clusterManager.getServerList(testTags);
      Assertions.assertEquals(1, serverList2.size());
      assertEquals(1, CoordinatorMetrics.gaugeUnhealthyServerNum.get());
    }
  }

  private void addNode(String id, SimpleClusterManager clusterManager) {
    ServerNode node = new ServerNode(id, "ip", 0, 100L, 50L, 30L, 10, testTags);
    LOG.info("Add node {} {}", node.getId(), node.getTimestamp());
    clusterManager.add(node);
  }

  @Test
  public void heartbeatTimeoutTest() throws Exception {
    CoordinatorConf ssc = new CoordinatorConf();
    ssc.setLong(CoordinatorConf.COORDINATOR_HEARTBEAT_TIMEOUT, 600L);
    try (SimpleClusterManager clusterManager = new SimpleClusterManager(ssc, new Configuration())) {
      addNode("sn0", clusterManager);
      addNode("sn1", clusterManager);
      List<ServerNode> serverNodes = clusterManager.getServerList(testTags);
      assertEquals(2, serverNodes.size());
      Set<String> expectedIds = Sets.newHashSet("sn0", "sn1");
      assertEquals(
          expectedIds, serverNodes.stream().map(ServerNode::getId).collect(Collectors.toSet()));
      await()
          .atMost(1, TimeUnit.SECONDS)
          .until(() -> clusterManager.getServerList(testTags).isEmpty());

      addNode("sn2", clusterManager);
      serverNodes = clusterManager.getServerList(testTags);
      assertEquals(1, serverNodes.size());
      assertEquals("sn2", serverNodes.get(0).getId());
      await()
          .atMost(1, TimeUnit.SECONDS)
          .until(() -> clusterManager.getServerList(testTags).isEmpty());
    }
  }

  @Test
  public void testGetCorrectServerNodesWhenOneNodeRemoved() throws Exception {
    CoordinatorConf ssc = new CoordinatorConf();
    ssc.setLong(CoordinatorConf.COORDINATOR_HEARTBEAT_TIMEOUT, 30 * 1000L);
    try (SimpleClusterManager clusterManager = new SimpleClusterManager(ssc, new Configuration())) {
      ServerNode sn1 = new ServerNode("sn1", "ip", 0, 100L, 50L, 20, 10, testTags);
      ServerNode sn2 = new ServerNode("sn2", "ip", 0, 100L, 50L, 21, 10, testTags);
      ServerNode sn3 = new ServerNode("sn3", "ip", 0, 100L, 50L, 20, 11, testTags);
      clusterManager.add(sn1);
      clusterManager.add(sn2);
      clusterManager.add(sn3);
      List<ServerNode> serverNodes = clusterManager.getServerList(testTags);
      assertEquals(3, serverNodes.size());

      sn3.setTimestamp(System.currentTimeMillis() - 60 * 1000L);
      clusterManager.nodesCheck();

      Map<String, Set<ServerNode>> tagToNodes = clusterManager.getTagToNodes();
      List<ServerNode> serverList = clusterManager.getServerList(testTags);
      Assertions.assertEquals(2, tagToNodes.get(testTags.iterator().next()).size());
      Assertions.assertEquals(2, serverList.size());
    }
  }

  @Test
  public void updateExcludeNodesTest() throws Exception {
    String excludeNodesFolder =
        (new File(ClassLoader.getSystemResource("empty").getFile())).getParent();
    String excludeNodesPath = Paths.get(excludeNodesFolder, "excludeNodes").toString();
    CoordinatorConf ssc = new CoordinatorConf();
    File excludeNodesFile = new File(excludeNodesPath);
    URI excludeNodesUri = excludeNodesFile.toURI();
    ssc.setString(
        CoordinatorConf.COORDINATOR_EXCLUDE_NODES_FILE_PATH, excludeNodesUri.toURL().toString());
    ssc.setLong(CoordinatorConf.COORDINATOR_EXCLUDE_NODES_CHECK_INTERVAL, 1000);

    try (SimpleClusterManager scm = new SimpleClusterManager(ssc, new Configuration())) {
      scm.add(new ServerNode("node1-1999", "ip", 0, 100L, 50L, 20, 10, testTags));
      scm.add(new ServerNode("node2-1999", "ip", 0, 100L, 50L, 20, 10, testTags));
      scm.add(new ServerNode("node3-1999", "ip", 0, 100L, 50L, 20, 10, testTags));
      scm.add(new ServerNode("node4-1999", "ip", 0, 100L, 50L, 20, 10, testTags));
      assertTrue(scm.getExcludedNodes().isEmpty());

      final Set<String> nodes = Sets.newHashSet("node1-1999", "node2-1999");
      writeExcludeHosts(excludeNodesPath, nodes);
      await().atMost(3, TimeUnit.SECONDS).until(() -> scm.getExcludedNodes().equals(nodes));
      List<ServerNode> availableNodes = scm.getServerList(testTags);
      assertEquals(2, availableNodes.size());
      Set<String> remainNodes = Sets.newHashSet("node3-1999", "node4-1999");
      assertEquals(
          remainNodes, availableNodes.stream().map(ServerNode::getId).collect(Collectors.toSet()));

      final Set<String> nodes2 = Sets.newHashSet("node3-1999", "node4-1999");
      writeExcludeHosts(excludeNodesPath, nodes2);
      await().atMost(3, TimeUnit.SECONDS).until(() -> scm.getExcludedNodes().equals(nodes2));
      assertEquals(nodes2, scm.getExcludedNodes());

      final Set<String> comments =
          Sets.newHashSet(
              "# The contents of the first comment",
              "node3-1999",
              "# The contents of the second comment",
              "node4-1999",
              "# The content of the third comment");
      final Set<String> noComments = Sets.newHashSet("node3-1999", "node4-1999");
      writeExcludeHosts(excludeNodesPath, comments);
      await().atMost(3, TimeUnit.SECONDS).until(() -> scm.getExcludedNodes().equals(noComments));
      assertEquals(noComments, scm.getExcludedNodes());

      Set<String> excludeNodes = scm.getExcludedNodes();
      Thread.sleep(3000);
      // excludeNodes shouldn't be updated if file has no change
      assertEquals(excludeNodes, scm.getExcludedNodes());

      writeExcludeHosts(excludeNodesPath, Sets.newHashSet());
      // excludeNodes is an empty file, set should be empty
      await().atMost(3, TimeUnit.SECONDS).until(() -> scm.getExcludedNodes().isEmpty());

      final Set<String> nodes3 = Sets.newHashSet("node1-1999");
      writeExcludeHosts(excludeNodesPath, nodes3);
      await().atMost(3, TimeUnit.SECONDS).until(() -> scm.getExcludedNodes().equals(nodes3));

      File blacklistFile = new File(excludeNodesPath);
      assertTrue(blacklistFile.delete());
      // excludeNodes is deleted, set should be empty
      await().atMost(3, TimeUnit.SECONDS).until(() -> scm.getExcludedNodes().isEmpty());

      remainNodes = Sets.newHashSet("node1-1999", "node2-1999", "node3-1999", "node4-1999");
      availableNodes = scm.getServerList(testTags);
      assertEquals(
          remainNodes, availableNodes.stream().map(ServerNode::getId).collect(Collectors.toSet()));
    }
  }

  @Test
  public void excludeNodesNoDelayTest() throws Exception {
    String excludeNodesFolder =
        (new File(ClassLoader.getSystemResource("empty").getFile())).getParent();
    String excludeNodesPath = Paths.get(excludeNodesFolder, "excludeNodes").toString();
    CoordinatorConf ssc = new CoordinatorConf();
    File excludeNodesFile = new File(excludeNodesPath);
    URI excludeNodesUri = excludeNodesFile.toURI();
    ssc.setString(
        CoordinatorConf.COORDINATOR_EXCLUDE_NODES_FILE_PATH, excludeNodesUri.toURL().toString());
    ssc.setLong(CoordinatorConf.COORDINATOR_EXCLUDE_NODES_CHECK_INTERVAL, 5000);

    final Set<String> nodes = Sets.newHashSet("node1-1999", "node2-1999");
    writeExcludeHosts(excludeNodesPath, nodes);

    try (SimpleClusterManager scm = new SimpleClusterManager(ssc, new Configuration())) {
      // waiting for excludeNode file parse.
      Uninterruptibles.sleepUninterruptibly(20, TimeUnit.MILLISECONDS);
      scm.add(new ServerNode("node1-1999", "ip", 0, 100L, 50L, 20, 10, testTags));
      scm.add(new ServerNode("node2-1999", "ip", 0, 100L, 50L, 20, 10, testTags));
      scm.add(new ServerNode("node3-1999", "ip", 0, 100L, 50L, 20, 10, testTags));
      scm.add(new ServerNode("node4-1999", "ip", 0, 100L, 50L, 20, 10, testTags));
      assertEquals(4, scm.getNodesNum());
      assertEquals(2, scm.getExcludedNodes().size());
    }
    File blacklistFile = new File(excludeNodesPath);
    assertTrue(blacklistFile.delete());
  }

  private void writeExcludeHosts(String path, Set<String> values) throws Exception {
    try (PrintWriter pw = new PrintWriter(new FileWriter(path))) {
      // have empty line as value
      pw.write("\n");
      for (String value : values) {
        pw.write(value + "\n");
      }
    }
  }
}
