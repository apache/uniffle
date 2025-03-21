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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.handle.split.PartitionSplitInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.common.PartitionSplitMode;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.proto.RssProtos;

/** This class holds the dynamic partition assignment for partition reassign mechanism. */
public class MutableShuffleHandleInfo extends ShuffleHandleInfoBase {
  private static final long serialVersionUID = 0L;
  private static final Logger LOGGER = LoggerFactory.getLogger(MutableShuffleHandleInfo.class);

  /**
   * partitionId -> replica -> assigned servers.
   *
   * <p>The first index of list<ShuffleServerInfo> is the initial static assignment server.
   *
   * <p>The remaining indexes are the replacement servers if exists.
   */
  private Map<Integer, Map<Integer, List<ShuffleServerInfo>>> partitionReplicaAssignedServers;

  private Map<String, Set<ShuffleServerInfo>> excludedServerToReplacements;
  /**
   * partitionId -> excluded server -> replacement servers. The replacement servers for exclude
   * server of specific partition.
   */
  private Map<Integer, Map<String, Set<ShuffleServerInfo>>>
      excludedServerForPartitionToReplacements;

  private PartitionSplitMode partitionSplitMode = PartitionSplitMode.PIPELINE;

  public MutableShuffleHandleInfo(
      int shuffleId,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      RemoteStorageInfo storageInfo) {
    this(shuffleId, storageInfo, toPartitionReplicaMapping(partitionToServers));
  }

  public MutableShuffleHandleInfo(
      int shuffleId,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      RemoteStorageInfo storageInfo,
      PartitionSplitMode partitionSplitMode) {
    this(shuffleId, storageInfo, toPartitionReplicaMapping(partitionToServers));
    this.partitionSplitMode = partitionSplitMode;
  }

  @VisibleForTesting
  protected MutableShuffleHandleInfo(
      int shuffleId,
      RemoteStorageInfo storageInfo,
      Map<Integer, Map<Integer, List<ShuffleServerInfo>>> partitionReplicaAssignedServers) {
    super(shuffleId, storageInfo);
    this.excludedServerToReplacements = new HashMap<>();
    this.excludedServerForPartitionToReplacements = new HashMap<>();
    this.partitionReplicaAssignedServers = partitionReplicaAssignedServers;
  }

  public MutableShuffleHandleInfo(int shuffleId, RemoteStorageInfo storageInfo) {
    super(shuffleId, storageInfo);
  }

  private static Map<Integer, Map<Integer, List<ShuffleServerInfo>>> toPartitionReplicaMapping(
      Map<Integer, List<ShuffleServerInfo>> partitionToServers) {
    Map<Integer, Map<Integer, List<ShuffleServerInfo>>> partitionReplicaAssignedServers =
        new HashMap<>();
    for (Map.Entry<Integer, List<ShuffleServerInfo>> partitionEntry :
        partitionToServers.entrySet()) {
      int partitionId = partitionEntry.getKey();
      Map<Integer, List<ShuffleServerInfo>> replicaMapping =
          partitionReplicaAssignedServers.computeIfAbsent(partitionId, x -> new HashMap<>());

      List<ShuffleServerInfo> replicaServers = partitionEntry.getValue();
      for (int i = 0; i < replicaServers.size(); i++) {
        int replicaIdx = i;
        replicaMapping
            .computeIfAbsent(replicaIdx, x -> new ArrayList<>())
            .add(replicaServers.get(i));
      }
    }
    return partitionReplicaAssignedServers;
  }

  public Set<ShuffleServerInfo> getReplacements(String faultyServerId) {
    return excludedServerToReplacements.get(faultyServerId);
  }

  public Set<ShuffleServerInfo> getReplacementsForPartition(
      int partitionId, String excludedServerId) {
    return excludedServerForPartitionToReplacements
        .getOrDefault(partitionId, Collections.emptyMap())
        .getOrDefault(excludedServerId, Collections.emptySet());
  }

  /**
   * Update the assignment for the receiving failure server of the given partition.
   *
   * @param partitionId the partition id
   * @param receivingFailureServerId the id of the receiving failure server
   * @param replacements the new assigned servers for replacing the receiving failure server
   * @return the updated server list for receiving data
   */
  public Set<ShuffleServerInfo> updateAssignment(
      int partitionId, String receivingFailureServerId, Set<ShuffleServerInfo> replacements) {
    if (replacements == null || StringUtils.isEmpty(receivingFailureServerId)) {
      return Collections.emptySet();
    }
    excludedServerToReplacements.put(receivingFailureServerId, replacements);

    return updateAssignmentInternal(partitionId, receivingFailureServerId, replacements);
  }

  private Set<ShuffleServerInfo> updateAssignmentInternal(
      int partitionId, String receivingFailureServerId, Set<ShuffleServerInfo> replacements) {
    Set<ShuffleServerInfo> updatedServers = new HashSet<>();
    Map<Integer, List<ShuffleServerInfo>> replicaServers =
        partitionReplicaAssignedServers.get(partitionId);
    for (Map.Entry<Integer, List<ShuffleServerInfo>> serverEntry : replicaServers.entrySet()) {
      List<ShuffleServerInfo> servers = serverEntry.getValue();
      if (servers.stream()
          .map(x -> x.getId())
          .collect(Collectors.toSet())
          .contains(receivingFailureServerId)) {
        Set<ShuffleServerInfo> tempSet = new HashSet<>();
        tempSet.addAll(replacements);
        tempSet.removeAll(servers);

        if (CollectionUtils.isNotEmpty(tempSet)) {
          updatedServers.addAll(tempSet);
          servers.addAll(tempSet);
        }
      }
    }
    return updatedServers;
  }

  /**
   * Update the assignment for the receiving failure server of the need split partition.
   *
   * @param partitionId the partition id
   * @param receivingFailureServerId the id of the receiving failure server
   * @param replacements the new assigned servers for replacing the receiving failure server
   * @return the updated server list for receiving data
   */
  public Set<ShuffleServerInfo> updateAssignmentOnPartitionSplit(
      int partitionId, String receivingFailureServerId, Set<ShuffleServerInfo> replacements) {
    if (replacements == null || StringUtils.isEmpty(receivingFailureServerId)) {
      return Collections.emptySet();
    }
    excludedServerForPartitionToReplacements
        .computeIfAbsent(partitionId, x -> new HashMap<>())
        .put(receivingFailureServerId, replacements);

    return updateAssignmentInternal(partitionId, receivingFailureServerId, replacements);
  }

  @Override
  public Set<ShuffleServerInfo> getServers() {
    return partitionReplicaAssignedServers.values().stream()
        .flatMap(x -> x.values().stream().flatMap(k -> k.stream()))
        .collect(Collectors.toSet());
  }

  @Override
  public Map<Integer, List<ShuffleServerInfo>> getAvailablePartitionServersForWriter() {
    Map<Integer, List<ShuffleServerInfo>> assignment = new HashMap<>();
    for (Map.Entry<Integer, Map<Integer, List<ShuffleServerInfo>>> entry :
        partitionReplicaAssignedServers.entrySet()) {
      int partitionId = entry.getKey();
      Map<Integer, List<ShuffleServerInfo>> replicaServers = entry.getValue();
      PartitionSplitInfo splitInfo = this.getPartitionSplitInfo(partitionId);
      for (Map.Entry<Integer, List<ShuffleServerInfo>> replicaServerEntry :
          replicaServers.entrySet()) {
        ShuffleServerInfo candidate;
        int candidateSize = replicaServerEntry.getValue().size();
        // Use the last one for each replica writing
        candidate = replicaServerEntry.getValue().get(candidateSize - 1);

        long taskAttemptId =
            Optional.ofNullable(TaskContext.get()).map(x -> x.taskAttemptId()).orElse(-1L);
        if (taskAttemptId != -1
            && splitInfo.isSplit()
            && splitInfo.getMode() == PartitionSplitMode.LOAD_BALANCE) {
          // 1. exclude the problem nodes to pick up
          List<ShuffleServerInfo> servers =
              replicaServerEntry.getValue().stream()
                  .filter(x -> !excludedServerToReplacements.containsKey(x.getId()))
                  .collect(Collectors.toList());

          // 2. exclude the first partition split triggered node.
          int serverSize = servers.size();
          if (serverSize > 1) {
            // 0, 1, 2
            int idx = (int) (taskAttemptId % (serverSize - 1)) + 1;
            candidate = servers.get(idx);
          }
        }

        assignment.computeIfAbsent(partitionId, x -> new ArrayList<>()).add(candidate);
      }
    }
    return assignment;
  }

  @Override
  public Map<Integer, List<ShuffleServerInfo>> getAllPartitionServersForReader() {
    Map<Integer, List<ShuffleServerInfo>> assignment = new HashMap<>();
    for (Map.Entry<Integer, Map<Integer, List<ShuffleServerInfo>>> entry :
        partitionReplicaAssignedServers.entrySet()) {
      int partitionId = entry.getKey();
      Map<Integer, List<ShuffleServerInfo>> replicaServers = entry.getValue();
      for (Map.Entry<Integer, List<ShuffleServerInfo>> replicaServerEntry :
          replicaServers.entrySet()) {
        assignment
            .computeIfAbsent(partitionId, x -> new ArrayList<>())
            .addAll(replicaServerEntry.getValue());
      }
    }
    return assignment;
  }

  @Override
  public PartitionDataReplicaRequirementTracking createPartitionReplicaTracking() {
    PartitionDataReplicaRequirementTracking replicaRequirement =
        new PartitionDataReplicaRequirementTracking(shuffleId, partitionReplicaAssignedServers);
    return replicaRequirement;
  }

  @Override
  public PartitionSplitInfo getPartitionSplitInfo(int partitionId) {
    boolean isSplit = excludedServerForPartitionToReplacements.containsKey(partitionId);
    return new PartitionSplitInfo(
        partitionId,
        isSplit,
        partitionSplitMode,
        new ArrayList<>(partitionReplicaAssignedServers.get(partitionId).values()));
  }

  public Set<String> listExcludedServers() {
    return excludedServerToReplacements.keySet();
  }

  public void checkPartitionReassignServerNum(
      Set<Integer> partitionIds, int legalReassignServerNum) {
    for (int partitionId : partitionIds) {
      Map<Integer, List<ShuffleServerInfo>> replicas =
          partitionReplicaAssignedServers.get(partitionId);
      for (List<ShuffleServerInfo> servers : replicas.values()) {
        if (servers.size() - 1 > legalReassignServerNum) {
          throw new RssException(
              "Illegal reassignment servers for partitionId: "
                  + partitionId
                  + " that exceeding the max legal reassign server num: "
                  + legalReassignServerNum);
        }
      }
    }
  }

  public static RssProtos.MutableShuffleHandleInfo toProto(MutableShuffleHandleInfo handleInfo) {
    synchronized (handleInfo) {
      Map<Integer, RssProtos.PartitionReplicaServers> partitionToServers = new HashMap<>();
      for (Map.Entry<Integer, Map<Integer, List<ShuffleServerInfo>>> entry :
          handleInfo.partitionReplicaAssignedServers.entrySet()) {
        int partitionId = entry.getKey();

        Map<Integer, RssProtos.ReplicaServersItem> replicaServersProto = new HashMap<>();
        Map<Integer, List<ShuffleServerInfo>> replicaServers = entry.getValue();
        for (Map.Entry<Integer, List<ShuffleServerInfo>> replicaServerEntry :
            replicaServers.entrySet()) {
          RssProtos.ReplicaServersItem item =
              RssProtos.ReplicaServersItem.newBuilder()
                  .addAllServerId(ShuffleServerInfo.toProto(replicaServerEntry.getValue()))
                  .build();
          replicaServersProto.put(replicaServerEntry.getKey(), item);
        }

        RssProtos.PartitionReplicaServers partitionReplicaServerProto =
            RssProtos.PartitionReplicaServers.newBuilder()
                .putAllReplicaServers(replicaServersProto)
                .build();
        partitionToServers.put(partitionId, partitionReplicaServerProto);
      }

      Map<String, RssProtos.ReplacementServers> excludeToReplacements = new HashMap<>();
      for (Map.Entry<String, Set<ShuffleServerInfo>> entry :
          handleInfo.excludedServerToReplacements.entrySet()) {
        RssProtos.ReplacementServers replacementServers =
            RssProtos.ReplacementServers.newBuilder()
                .addAllServerId(ShuffleServerInfo.toProto(new ArrayList<>(entry.getValue())))
                .build();
        excludeToReplacements.put(entry.getKey(), replacementServers);
      }

      RssProtos.PartitionSplitMode mode = RssProtos.PartitionSplitMode.PIPELINE;
      if (handleInfo.partitionSplitMode == PartitionSplitMode.LOAD_BALANCE) {
        mode = RssProtos.PartitionSplitMode.LOAD_BALANCE;
      }

      RssProtos.MutableShuffleHandleInfo handleProto =
          RssProtos.MutableShuffleHandleInfo.newBuilder()
              .setShuffleId(handleInfo.shuffleId)
              .setRemoteStorageInfo(
                  RssProtos.RemoteStorageInfo.newBuilder()
                      .setPath(handleInfo.remoteStorage.getPath())
                      .putAllConfItems(handleInfo.remoteStorage.getConfItems())
                      .build())
              .putAllPartitionToServers(partitionToServers)
              .putAllExcludedServerToReplacements(excludeToReplacements)
              .setPartitionSplitMode(mode)
              .addAllSplitPartitionId(handleInfo.excludedServerForPartitionToReplacements.keySet())
              .build();
      return handleProto;
    }
  }

  public static MutableShuffleHandleInfo fromProto(RssProtos.MutableShuffleHandleInfo handleProto) {
    if (handleProto == null) {
      return null;
    }
    Map<Integer, Map<Integer, List<ShuffleServerInfo>>> partitionToServers = new HashMap<>();
    for (Map.Entry<Integer, RssProtos.PartitionReplicaServers> entry :
        handleProto.getPartitionToServersMap().entrySet()) {
      Map<Integer, List<ShuffleServerInfo>> replicaServers =
          partitionToServers.computeIfAbsent(entry.getKey(), x -> new HashMap<>());
      for (Map.Entry<Integer, RssProtos.ReplicaServersItem> serverEntry :
          entry.getValue().getReplicaServersMap().entrySet()) {
        int replicaIdx = serverEntry.getKey();
        List<ShuffleServerInfo> shuffleServerInfos =
            ShuffleServerInfo.fromProto(serverEntry.getValue().getServerIdList());
        replicaServers.put(replicaIdx, shuffleServerInfos);
      }
    }

    Map<String, Set<ShuffleServerInfo>> excludeToReplacements = new HashMap<>();
    for (Map.Entry<String, RssProtos.ReplacementServers> entry :
        handleProto.getExcludedServerToReplacementsMap().entrySet()) {
      excludeToReplacements.put(
          entry.getKey(),
          new HashSet<>(ShuffleServerInfo.fromProto(entry.getValue().getServerIdList())));
    }

    Map<Integer, Map<String, Set<ShuffleServerInfo>>> partitionSplitToServers = new HashMap<>();
    for (int partitionId : handleProto.getSplitPartitionIdList()) {
      partitionSplitToServers.computeIfAbsent(partitionId, x -> new HashMap<>());
    }

    RemoteStorageInfo remoteStorageInfo =
        new RemoteStorageInfo(
            handleProto.getRemoteStorageInfo().getPath(),
            handleProto.getRemoteStorageInfo().getConfItemsMap());
    MutableShuffleHandleInfo handle =
        new MutableShuffleHandleInfo(handleProto.getShuffleId(), remoteStorageInfo);
    handle.partitionReplicaAssignedServers = partitionToServers;
    handle.partitionSplitMode =
        handleProto.getPartitionSplitMode() == RssProtos.PartitionSplitMode.LOAD_BALANCE
            ? PartitionSplitMode.LOAD_BALANCE
            : PartitionSplitMode.PIPELINE;
    handle.excludedServerForPartitionToReplacements = partitionSplitToServers;
    handle.excludedServerToReplacements = excludeToReplacements;
    return handle;
  }

  public Set<String> listExcludedServersForPartition(int partitionId) {
    return excludedServerForPartitionToReplacements
        .getOrDefault(partitionId, Collections.emptyMap())
        .keySet();
  }
}
