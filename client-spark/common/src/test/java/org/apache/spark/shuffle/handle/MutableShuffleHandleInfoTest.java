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

package org.apache.spark.shuffle.handle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleServerInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MutableShuffleHandleInfoTest {

  private ShuffleServerInfo createFakeServerInfo(String id) {
    return new ShuffleServerInfo(id, id, 1);
  }

  @Test
  public void testUpdateAssignment() {
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    partitionToServers.put(1, Arrays.asList(createFakeServerInfo("a"), createFakeServerInfo("b")));
    partitionToServers.put(2, Arrays.asList(createFakeServerInfo("c")));

    MutableShuffleHandleInfo handleInfo =
        new MutableShuffleHandleInfo(1, partitionToServers, new RemoteStorageInfo(""));

    // case1: update the replacement servers but has existing servers
    Set<ShuffleServerInfo> updated =
        handleInfo.updateAssignment(
            1, "a", Sets.newHashSet(createFakeServerInfo("a"), createFakeServerInfo("d")));
    assertTrue(updated.stream().findFirst().get().getId().equals("d"));

    // case2: update when having multiple servers
    Map<Integer, Map<Integer, List<ShuffleServerInfo>>> partitionReplicaAssignedServers =
        new HashMap<>();
    List<ShuffleServerInfo> servers =
        new ArrayList<>(
            Arrays.asList(
                createFakeServerInfo("a"),
                createFakeServerInfo("b"),
                createFakeServerInfo("c"),
                createFakeServerInfo("d")));
    partitionReplicaAssignedServers
        .computeIfAbsent(1, x -> new HashMap<>())
        .computeIfAbsent(0, x -> servers);
    handleInfo =
        new MutableShuffleHandleInfo(1, new RemoteStorageInfo(""), partitionReplicaAssignedServers);
    int partitionId = 1;
    updated =
        handleInfo.updateAssignment(
            partitionId,
            "a",
            Sets.newHashSet(
                createFakeServerInfo("b"),
                createFakeServerInfo("d"),
                createFakeServerInfo("e"),
                createFakeServerInfo("f")));
    assertEquals(updated, Sets.newHashSet(createFakeServerInfo("e"), createFakeServerInfo("f")));
  }

  @Test
  public void testListAllPartitionAssignmentServers() {
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    partitionToServers.put(1, Arrays.asList(createFakeServerInfo("a"), createFakeServerInfo("b")));
    partitionToServers.put(2, Arrays.asList(createFakeServerInfo("c")));

    MutableShuffleHandleInfo handleInfo =
        new MutableShuffleHandleInfo(1, partitionToServers, new RemoteStorageInfo(""));

    // case1
    int partitionId = 2;
    handleInfo.updateAssignment(partitionId, "c", Sets.newHashSet(createFakeServerInfo("d")));

    Map<Integer, List<ShuffleServerInfo>> partitionAssignment =
        handleInfo.getAllPartitionServersForReader();
    assertEquals(2, partitionAssignment.size());
    assertEquals(
        Arrays.asList(createFakeServerInfo("c"), createFakeServerInfo("d")),
        partitionAssignment.get(2));

    // case2: reassign multiple times for one partition, it will not append the same replacement
    // servers
    handleInfo.updateAssignment(partitionId, "c", Sets.newHashSet(createFakeServerInfo("d")));
    partitionAssignment = handleInfo.getAllPartitionServersForReader();
    assertEquals(
        Arrays.asList(createFakeServerInfo("c"), createFakeServerInfo("d")),
        partitionAssignment.get(2));

    // case3: reassign multiple times for one partition, it will append the non-existing replacement
    // servers
    handleInfo.updateAssignment(
        partitionId, "c", Sets.newHashSet(createFakeServerInfo("d"), createFakeServerInfo("e")));
    partitionAssignment = handleInfo.getAllPartitionServersForReader();
    assertEquals(
        Arrays.asList(
            createFakeServerInfo("c"), createFakeServerInfo("d"), createFakeServerInfo("e")),
        partitionAssignment.get(2));
  }

  @Test
  public void testCreatePartitionReplicaTracking() {
    ShuffleServerInfo a = createFakeServerInfo("a");
    ShuffleServerInfo b = createFakeServerInfo("b");
    ShuffleServerInfo c = createFakeServerInfo("c");

    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    partitionToServers.put(1, Arrays.asList(a, b));
    partitionToServers.put(2, Arrays.asList(c));

    MutableShuffleHandleInfo handleInfo =
        new MutableShuffleHandleInfo(1, partitionToServers, new RemoteStorageInfo(""));

    // not any replacements
    PartitionDataReplicaRequirementTracking tracking = handleInfo.createPartitionReplicaTracking();
    Map<Integer, Map<Integer, List<ShuffleServerInfo>>> inventory = tracking.getInventory();
    assertEquals(a, inventory.get(1).get(0).get(0));
    assertEquals(b, inventory.get(1).get(1).get(0));
    assertEquals(c, inventory.get(2).get(0).get(0));
  }

  @Test
  public void testUpdateAssignmentOnPartitionSplit() {
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    partitionToServers.put(1, Arrays.asList(createFakeServerInfo("a"), createFakeServerInfo("b")));
    partitionToServers.put(2, Arrays.asList(createFakeServerInfo("c")));

    MutableShuffleHandleInfo handleInfo =
        new MutableShuffleHandleInfo(1, partitionToServers, new RemoteStorageInfo(""));

    // case1: update the replacement servers but has existing servers
    Set<ShuffleServerInfo> updated =
        handleInfo.updateAssignment(
            1, "a", Sets.newHashSet(createFakeServerInfo("a"), createFakeServerInfo("d")));
    assertTrue(updated.stream().findFirst().get().getId().equals("d"));

    // case2: update when having multiple servers
    Map<Integer, Map<Integer, List<ShuffleServerInfo>>> partitionReplicaAssignedServers =
        new HashMap<>();
    List<ShuffleServerInfo> servers =
        new ArrayList<>(
            Arrays.asList(
                createFakeServerInfo("a"),
                createFakeServerInfo("b"),
                createFakeServerInfo("c"),
                createFakeServerInfo("d")));
    partitionReplicaAssignedServers
        .computeIfAbsent(1, x -> new HashMap<>())
        .computeIfAbsent(0, x -> servers);
    handleInfo =
        new MutableShuffleHandleInfo(1, new RemoteStorageInfo(""), partitionReplicaAssignedServers);

    Map<Integer, List<ShuffleServerInfo>> availablePartitionServers =
        handleInfo.getAvailablePartitionServersForWriter();
    assertEquals("d", availablePartitionServers.get(1).get(0).getHost());
    Map<Integer, List<ShuffleServerInfo>> assignment = handleInfo.getAllPartitionServersForReader();
    assertEquals(4, assignment.get(1).size());

    int partitionId = 1;

    handleInfo.getReplacementsForPartition(1, "a");
    HashSet<ShuffleServerInfo> replacements =
        Sets.newHashSet(
            createFakeServerInfo("b"),
            createFakeServerInfo("d"),
            createFakeServerInfo("e"),
            createFakeServerInfo("f"));
    updated = handleInfo.updateAssignmentOnPartitionSplit(partitionId, "a", replacements);
    assertEquals(updated, Sets.newHashSet(createFakeServerInfo("e"), createFakeServerInfo("f")));

    Set<String> excludedServers = handleInfo.listExcludedServersForPartition(partitionId);
    assertEquals(1, excludedServers.size());
    assertEquals("a", excludedServers.iterator().next());
    assertEquals(replacements, handleInfo.getReplacementsForPartition(1, "a"));
    availablePartitionServers = handleInfo.getAvailablePartitionServersForWriter();
    // The current writer is the last one
    assertEquals("f", availablePartitionServers.get(1).get(0).getHost());
    assignment = handleInfo.getAllPartitionServersForReader();
    // All the servers were selected as writer are available as reader
    assertEquals(6, assignment.get(1).size());
  }
}
