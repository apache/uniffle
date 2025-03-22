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

package org.apache.uniffle.coordinator.strategy.assignment;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.ServerNode;
import org.apache.uniffle.coordinator.SimpleClusterManager;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class PartitionBalanceAssignmentStrategyTest {

  private int shuffleNodesMax = 5;
  private Set<String> tags = Sets.newHashSet("test");

  @BeforeEach
  public void setUp() {
    CoordinatorMetrics.register();
  }

  @AfterEach
  public void clear() {
    CoordinatorMetrics.clear();
  }

  @Test
  public void testAssign() throws Exception {
    CoordinatorConf ssc = new CoordinatorConf();
    ssc.set(
        CoordinatorConf.COORDINATOR_SELECT_PARTITION_STRATEGY,
        AbstractAssignmentStrategy.SelectPartitionStrategyName.ROUND);
    ssc.setInteger(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX, shuffleNodesMax);
    try (SimpleClusterManager clusterManager = new SimpleClusterManager(ssc, new Configuration())) {
      PartitionBalanceAssignmentStrategy strategy =
          new PartitionBalanceAssignmentStrategy(clusterManager, ssc);

      List<Long> list = Lists.newArrayList();
      for (int i = 0; i < 20; i++) {
        list.add(10L);
      }
      updateServerResource(clusterManager, list);
      boolean isThrown = false;
      try {
        strategy.assign(100, 2, 1, tags, -1, -1);
      } catch (Exception e) {
        isThrown = true;
      }
      assertTrue(isThrown);
      try {
        strategy.assign(0, 1, 1, tags, -1, -1);
      } catch (Exception e) {
        fail();
      }
      isThrown = false;
      try {
        strategy.assign(10, 1, 1, Sets.newHashSet("fake"), 1, -1);
      } catch (Exception e) {
        isThrown = true;
      }
      assertTrue(isThrown);
      strategy.assign(100, 1, 1, tags, -1, -1);
      List<Long> expect =
          Lists.newArrayList(
              20L, 20L, 20L, 20L, 20L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
      valid(clusterManager, strategy, expect);
      strategy.assign(75, 1, 1, tags, -1, -1);
      expect =
          Lists.newArrayList(
              20L, 20L, 20L, 20L, 20L, 15L, 15L, 15L, 15L, 15L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
              0L);
      valid(clusterManager, strategy, expect);
      strategy.assign(100, 1, 1, tags, -1, -1);
      expect =
          Lists.newArrayList(
              20L, 20L, 20L, 20L, 20L, 15L, 15L, 15L, 15L, 15L, 20L, 20L, 20L, 20L, 20L, 0L, 0L, 0L,
              0L, 0L);
      valid(clusterManager, strategy, expect);

      Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
      list =
          Lists.newArrayList(
              7L, 18L, 7L, 3L, 19L, 15L, 11L, 10L, 16L, 11L, 14L, 17L, 15L, 17L, 8L, 1L, 3L, 3L, 6L,
              12L);
      updateServerResource(clusterManager, list);
      strategy.assign(100, 1, 1, tags, -1, -1);
      expect =
          Lists.newArrayList(
              0L, 20L, 0L, 0L, 20L, 0L, 0L, 0L, 20L, 0L, 0L, 20L, 0L, 20L, 0L, 0L, 0L, 0L, 0L, 0L);
      valid(clusterManager, strategy, expect);
      strategy.assign(50, 1, 1, tags, -1, -1);
      expect =
          Lists.newArrayList(
              0L, 20L, 0L, 0L, 20L, 10L, 10L, 0L, 20L, 0L, 10L, 20L, 10L, 20L, 0L, 0L, 0L, 0L, 0L,
              10L);
      valid(clusterManager, strategy, expect);

      strategy.assign(75, 1, 1, tags, -1, -1);
      expect =
          Lists.newArrayList(
              0L, 20L, 0L, 0L, 20L, 25L, 10L, 15L, 20L, 15L, 25L, 20L, 25L, 20L, 0L, 0L, 0L, 0L, 0L,
              10L);
      valid(clusterManager, strategy, expect);

      Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
      list =
          Lists.newArrayList(
              7L, 18L, 7L, 3L, 19L, 15L, 11L, 10L, 16L, 11L, 14L, 17L, 15L, 17L, 8L, 1L, 3L, 3L, 6L,
              12L);
      updateServerResource(clusterManager, list);
      strategy.assign(50, 1, 2, tags, -1, -1);
      expect =
          Lists.newArrayList(
              0L, 20L, 0L, 0L, 20L, 0L, 0L, 0L, 20L, 0L, 0L, 20L, 0L, 20L, 0L, 0L, 0L, 0L, 0L, 0L);
      valid(clusterManager, strategy, expect);
      strategy.assign(75, 1, 2, tags, -1, -1);
      expect =
          Lists.newArrayList(
              0L, 20L, 0L, 0L, 50L, 30L, 0L, 0L, 20L, 0L, 30L, 20L, 30L, 20L, 0L, 0L, 0L, 0L, 0L,
              30L);
      valid(clusterManager, strategy, expect);
      strategy.assign(33, 1, 2, tags, -1, -1);
      expect =
          Lists.newArrayList(
              0L, 33L, 0L, 0L, 50L, 30L, 14L, 13L, 20L, 13L, 30L, 20L, 30L, 20L, 13L, 0L, 0L, 0L,
              0L, 30L);
      valid(clusterManager, strategy, expect);

      list = Lists.newArrayList();
      for (int i = 0; i < 20; i++) {
        if (i % 2 == 0) {
          list.add(10L);
        } else {
          list.add(20L);
        }
      }

      Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
      updateServerResource(clusterManager, list);
      strategy.assign(33, 1, 1, tags, -1, -1);
      expect =
          Lists.newArrayList(
              0L, 7L, 0L, 7L, 0L, 7L, 0L, 6L, 0L, 6L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
      valid(clusterManager, strategy, expect);
      strategy.assign(41, 1, 2, tags, -1, -1);
      expect =
          Lists.newArrayList(
              0L, 7L, 0L, 7L, 0L, 7L, 0L, 6L, 0L, 6L, 0L, 17L, 0L, 17L, 0L, 16L, 0L, 16L, 0L, 16L);
      valid(clusterManager, strategy, expect);
      strategy.assign(23, 1, 1, tags, -1, -1);
      expect =
          Lists.newArrayList(
              5L, 7L, 5L, 7L, 5L, 7L, 4L, 6L, 4L, 6L, 0L, 17L, 0L, 17L, 0L, 16L, 0L, 16L, 0L, 16L);
      valid(clusterManager, strategy, expect);
      strategy.assign(11, 1, 3, tags, -1, -1);
      expect =
          Lists.newArrayList(
              5L, 7L, 5L, 7L, 5L, 7L, 4L, 13L, 4L, 13L, 7L, 17L, 6L, 17L, 6L, 16L, 0L, 16L, 0L,
              16L);
      valid(clusterManager, strategy, expect);
    }
  }

  private void valid(
      SimpleClusterManager clusterManager,
      PartitionBalanceAssignmentStrategy strategy,
      List<Long> expect) {
    assertEquals(20, expect.size());
    int i = 0;
    List<ServerNode> list = clusterManager.getServerList(tags);
    list.sort(
        new Comparator<ServerNode>() {
          @Override
          public int compare(ServerNode o1, ServerNode o2) {
            return o1.getId().compareTo(o2.getId());
          }
        });
    for (ServerNode node : list) {
      assertEquals(
          expect.get(i).intValue(), strategy.getServerToPartitions().get(node).getPartitionNum());
      i++;
    }
  }

  void updateServerResource(SimpleClusterManager clusterManager, List<Long> resources) {
    for (int i = 0; i < 20; i++) {
      ServerNode node =
          new ServerNode(
              String.valueOf((char) ('a' + i)),
              "127.0.0." + i,
              0,
              10L,
              5L,
              resources.get(i),
              5,
              tags);
      clusterManager.add(node);
    }
  }

  @Test
  public void testAssignmentShuffleNodesNum() throws Exception {
    CoordinatorConf ssc = new CoordinatorConf();
    ssc.set(
        CoordinatorConf.COORDINATOR_SELECT_PARTITION_STRATEGY,
        AbstractAssignmentStrategy.SelectPartitionStrategyName.ROUND);
    ssc.setInteger(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX, shuffleNodesMax);
    try (SimpleClusterManager clusterManager = new SimpleClusterManager(ssc, new Configuration())) {
      PartitionBalanceAssignmentStrategy strategy =
          new PartitionBalanceAssignmentStrategy(clusterManager, ssc);

      Set<String> serverTags = Sets.newHashSet("tag-1");

      for (int i = 0; i < 20; ++i) {
        clusterManager.add(
            new ServerNode("t1-" + i, "127.0.0." + i, 0, 0, 0, 20 - i, 0, serverTags));
      }

      /**
       * case1: user specify the illegal shuffle node num(<0) it will use the default shuffle nodes
       * num when having enough servers.
       */
      PartitionRangeAssignment pra = strategy.assign(100, 1, 1, serverTags, -1, -1);
      assertEquals(
          shuffleNodesMax,
          pra.getAssignments().values().stream()
              .flatMap(Collection::stream)
              .collect(Collectors.toSet())
              .size());

      /**
       * case2: user specify the illegal shuffle node num(==0) it will use the default shuffle nodes
       * num when having enough servers.
       */
      pra = strategy.assign(100, 1, 1, serverTags, 0, -1);
      assertEquals(
          shuffleNodesMax,
          pra.getAssignments().values().stream()
              .flatMap(Collection::stream)
              .collect(Collectors.toSet())
              .size());

      /**
       * case3: user specify the illegal shuffle node num(>default max limitation) it will use the
       * default shuffle nodes num when having enough servers
       */
      pra = strategy.assign(100, 1, 1, serverTags, shuffleNodesMax + 10, -1);
      assertEquals(
          shuffleNodesMax,
          pra.getAssignments().values().stream()
              .flatMap(Collection::stream)
              .collect(Collectors.toSet())
              .size());

      /**
       * case4: user specify the legal shuffle node num, it will use the customized shuffle nodes
       * num when having enough servers
       */
      pra = strategy.assign(100, 1, 1, serverTags, shuffleNodesMax - 1, -1);
      assertEquals(
          shuffleNodesMax - 1,
          pra.getAssignments().values().stream()
              .flatMap(Collection::stream)
              .collect(Collectors.toSet())
              .size());

      /**
       * case5: user specify the legal shuffle node num, but cluster don't have enough servers, it
       * will return the remaining servers.
       */
      serverTags = Sets.newHashSet("tag-2");
      for (int i = 0; i < shuffleNodesMax - 1; ++i) {
        clusterManager.add(
            new ServerNode("t2-" + i, "127.0.0." + i, 0, 0, 0, 20 - i, 0, serverTags));
      }
      pra = strategy.assign(100, 1, 1, serverTags, shuffleNodesMax, -1);
      assertEquals(
          shuffleNodesMax - 1,
          pra.getAssignments().values().stream()
              .flatMap(Collection::stream)
              .collect(Collectors.toSet())
              .size());
    }
  }

  @Test
  public void testAssignmentWithMustDiff() throws Exception {
    CoordinatorConf ssc = new CoordinatorConf();
    ssc.setInteger(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX, shuffleNodesMax);
    ssc.set(
        CoordinatorConf.COORDINATOR_ASSIGNMENT_HOST_STRATEGY,
        AbstractAssignmentStrategy.HostAssignmentStrategyName.MUST_DIFF);
    try (SimpleClusterManager clusterManager = new SimpleClusterManager(ssc, new Configuration())) {
      AssignmentStrategy strategy = new PartitionBalanceAssignmentStrategy(clusterManager, ssc);

      Set<String> serverTags = Sets.newHashSet("tag-1");

      for (int i = 0; i < 5; ++i) {
        clusterManager.add(
            new ServerNode("t1-" + i, "127.0.0." + i, 0, 0, 0, 20 - i, 0, serverTags));
      }
      for (int i = 0; i < 5; ++i) {
        clusterManager.add(
            new ServerNode("t2-" + i, "127.0.0." + i, 1, 0, 0, 20 - i, 0, serverTags));
      }
      PartitionRangeAssignment pra = strategy.assign(100, 1, 5, serverTags, -1, -1);
      pra.getAssignments()
          .values()
          .forEach(
              (nodeList) -> {
                Map<String, ServerNode> nodeMap = new HashMap<>();
                nodeList.forEach(
                    (node) -> {
                      ServerNode serverNode = nodeMap.get(node.getIp());
                      assertNull(serverNode);
                      nodeMap.put(node.getIp(), node);
                    });
              });

      pra = strategy.assign(100, 1, 6, serverTags, -1, -1);
      pra.getAssignments()
          .values()
          .forEach(
              (nodeList) -> {
                Map<String, ServerNode> nodeMap = new HashMap<>();
                boolean hasSameHost = false;
                for (ServerNode node : nodeList) {
                  ServerNode serverNode = nodeMap.get(node.getIp());
                  if (serverNode != null) {
                    hasSameHost = true;
                    break;
                  }
                  assertNull(serverNode);
                  nodeMap.put(node.getIp(), node);
                }
                assertTrue(hasSameHost);
              });
    }
  }

  @Test
  public void testAssignmentWithPreferDiff() throws Exception {
    CoordinatorConf ssc = new CoordinatorConf();
    ssc.setInteger(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX, shuffleNodesMax);
    ssc.set(
        CoordinatorConf.COORDINATOR_ASSIGNMENT_HOST_STRATEGY,
        AbstractAssignmentStrategy.HostAssignmentStrategyName.PREFER_DIFF);
    Set<String> serverTags = Sets.newHashSet("tag-1");
    try (SimpleClusterManager clusterManager = new SimpleClusterManager(ssc, new Configuration())) {
      AssignmentStrategy strategy = new PartitionBalanceAssignmentStrategy(clusterManager, ssc);
      for (int i = 0; i < 3; ++i) {
        clusterManager.add(
            new ServerNode("t1-" + i, "127.0.0." + i, 0, 0, 0, 20 - i, 0, serverTags));
      }
      for (int i = 0; i < 2; ++i) {
        clusterManager.add(
            new ServerNode("t2-" + i, "127.0.0." + i, 1, 0, 0, 20 - i, 0, serverTags));
      }
      PartitionRangeAssignment pra = strategy.assign(100, 1, 5, serverTags, -1, -1);
      pra.getAssignments()
          .values()
          .forEach(
              (nodeList) -> {
                assertEquals(5, nodeList.size());
              });
    }

    ssc.setInteger(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX, 3);
    try (SimpleClusterManager clusterManager = new SimpleClusterManager(ssc, new Configuration())) {
      for (int i = 0; i < 3; ++i) {
        clusterManager.add(
            new ServerNode("t1-" + i, "127.0.0." + i, 0, 0, 0, 20 - i, 0, serverTags));
      }
      for (int i = 0; i < 2; ++i) {
        clusterManager.add(
            new ServerNode("t2-" + i, "127.0.0." + i, 1, 0, 0, 20 - i, 0, serverTags));
      }
      AssignmentStrategy strategy = new PartitionBalanceAssignmentStrategy(clusterManager, ssc);
      PartitionRangeAssignment pra = strategy.assign(100, 1, 3, serverTags, -1, -1);
      pra.getAssignments()
          .values()
          .forEach(
              (nodeList) -> {
                Map<String, ServerNode> nodeMap = new HashMap<>();
                nodeList.forEach(
                    (node) -> {
                      ServerNode serverNode = nodeMap.get(node.getIp());
                      assertNull(serverNode);
                      nodeMap.put(node.getIp(), node);
                    });
              });
    }
  }

  @Test
  public void testAssignmentWithNone() throws Exception {
    CoordinatorConf ssc = new CoordinatorConf();
    ssc.setInteger(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX, shuffleNodesMax);
    ssc.set(
        CoordinatorConf.COORDINATOR_ASSIGNMENT_HOST_STRATEGY,
        AbstractAssignmentStrategy.HostAssignmentStrategyName.NONE);
    try (SimpleClusterManager clusterManager = new SimpleClusterManager(ssc, new Configuration())) {
      AssignmentStrategy strategy = new PartitionBalanceAssignmentStrategy(clusterManager, ssc);
      Set<String> serverTags = Sets.newHashSet("tag-1");

      for (int i = 0; i < 3; ++i) {
        clusterManager.add(
            new ServerNode("t1-" + i, "127.0.0." + i, 0, 0, 0, 20 - i, 0, serverTags));
      }
      for (int i = 0; i < 2; ++i) {
        clusterManager.add(
            new ServerNode("t2-" + i, "127.0.0." + i, 1, 0, 0, 20 - i, 0, serverTags));
      }
      PartitionRangeAssignment pra = strategy.assign(100, 1, 5, serverTags, -1, -1);
      pra.getAssignments()
          .values()
          .forEach(
              (nodeList) -> {
                assertEquals(5, nodeList.size());
              });
    }
  }

  @Test
  public void testWithContinuousSelectPartitionStrategy() throws Exception {
    CoordinatorConf ssc = new CoordinatorConf();
    ssc.set(
        CoordinatorConf.COORDINATOR_SELECT_PARTITION_STRATEGY,
        AbstractAssignmentStrategy.SelectPartitionStrategyName.CONTINUOUS);
    ssc.setInteger(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX, shuffleNodesMax);
    try (SimpleClusterManager clusterManager = new SimpleClusterManager(ssc, new Configuration())) {
      PartitionBalanceAssignmentStrategy strategy =
          new PartitionBalanceAssignmentStrategy(clusterManager, ssc);
      List<Long> list =
          Lists.newArrayList(
              20L, 20L, 20L, 20L, 20L, 20L, 20L, 20L, 20L, 20L, 20L, 20L, 20L, 20L, 20L, 20L, 20L,
              20L, 20L, 20L);
      updateServerResource(clusterManager, list);
      strategy.assign(100, 1, 2, tags, 5, 20);
      List<Long> expect =
          Lists.newArrayList(
              40L, 40L, 40L, 40L, 40L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
      valid(clusterManager, strategy, expect);

      strategy.assign(28, 1, 2, tags, 5, 20);
      expect =
          Lists.newArrayList(
              40L, 40L, 40L, 40L, 40L, 11L, 12L, 12L, 11L, 10L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
              0L);
      valid(clusterManager, strategy, expect);

      strategy.assign(29, 1, 2, tags, 5, 4);
      expect =
          Lists.newArrayList(
              40L, 40L, 40L, 40L, 40L, 11L, 12L, 12L, 11L, 10L, 11L, 12L, 12L, 12L, 11L, 0L, 0L, 0L,
              0L, 0L);
      valid(clusterManager, strategy, expect);
    }
  }
}
