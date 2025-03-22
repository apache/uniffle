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

package org.apache.uniffle.client.record.reader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.proto.RssProtos;

public class MockedShuffleWriteClient implements ShuffleWriteClient {

  private Map<String, Map<Integer, Map<Integer, Roaring64NavigableMap>>> blockIds = new HashMap<>();

  @Override
  public SendShuffleDataResult sendShuffleData(
      String appId,
      List<ShuffleBlockInfo> shuffleBlockInfoList,
      Supplier<Boolean> needCancelRequest) {
    return null;
  }

  @Override
  public void sendAppHeartbeat(String appId, long timeoutMs) {}

  @Override
  public void registerApplicationInfo(String appId, long timeoutMs, String user) {}

  @Override
  public void registerShuffle(
      ShuffleServerInfo shuffleServerInfo,
      String appId,
      int shuffleId,
      List<PartitionRange> partitionRanges,
      RemoteStorageInfo remoteStorage,
      ShuffleDataDistributionType dataDistributionType,
      int maxConcurrencyPerPartitionToWrite,
      int stageAttemptNumber,
      RssProtos.MergeContext mergeContext,
      Map<String, String> properties) {}

  @Override
  public boolean sendCommit(
      Set<ShuffleServerInfo> shuffleServerInfoSet, String appId, int shuffleId, int numMaps) {
    return false;
  }

  @Override
  public void registerCoordinators(String coordinators, long retryIntervalMs, int retryTimes) {}

  @Override
  public Map<String, String> fetchClientConf(int timeoutMs) {
    return null;
  }

  @Override
  public RemoteStorageInfo fetchRemoteStorage(String appId) {
    return null;
  }

  @Override
  public void reportShuffleResult(
      Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds,
      String appId,
      int shuffleId,
      long taskAttemptId,
      int bitmapNum) {
    this.blockIds.putIfAbsent(appId, new HashMap<>());
    this.blockIds.get(appId).putIfAbsent(shuffleId, new HashMap<>());

    for (Map<Integer, Set<Long>> partitionToBlockIds : serverToPartitionToBlockIds.values()) {
      for (Map.Entry<Integer, Set<Long>> entry : partitionToBlockIds.entrySet()) {
        int partitionId = entry.getKey();
        this.blockIds
            .get(appId)
            .get(shuffleId)
            .putIfAbsent(partitionId, Roaring64NavigableMap.bitmapOf());
        for (long blockId : entry.getValue()) {
          this.blockIds.get(appId).get(shuffleId).get(partitionId).add(blockId);
        }
      }
    }
  }

  @Override
  public ShuffleAssignmentsInfo getShuffleAssignments(
      String appId,
      int shuffleId,
      int partitionNum,
      int partitionNumPerRange,
      Set<String> requiredTags,
      int assignmentShuffleServerNumber,
      int estimateTaskConcurrency,
      Set<String> faultyServerIds,
      int stageId,
      int stageAttemptNumber,
      boolean reassign,
      long retryIntervalMs,
      int retryTimes) {
    return null;
  }

  @Override
  public ShuffleAssignmentsInfo getShuffleAssignments(
      String appId,
      int shuffleId,
      int partitionNum,
      int partitionNumPerRange,
      Set<String> requiredTags,
      int assignmentShuffleServerNumber,
      int estimateTaskConcurrency) {
    return null;
  }

  @Override
  public void startSortMerge(
      Set<ShuffleServerInfo> serverInfos,
      String appId,
      int shuffleId,
      int partitionId,
      Roaring64NavigableMap expectedTaskIds) {}

  @Override
  public Roaring64NavigableMap getShuffleResult(
      String clientType,
      Set<ShuffleServerInfo> shuffleServerInfoSet,
      String appId,
      int shuffleId,
      int partitionId) {
    return this.blockIds.get(appId).get(shuffleId).get(partitionId);
  }

  @Override
  public Roaring64NavigableMap getShuffleResultForMultiPart(
      String clientType,
      Map<ShuffleServerInfo, Set<Integer>> serverToPartitions,
      String appId,
      int shuffleId,
      Set<Integer> failedPartitions,
      PartitionDataReplicaRequirementTracking replicaRequirementTracking) {
    return null;
  }

  @Override
  public void close() {}

  @Override
  public void unregisterShuffle(String appId, int shuffleId) {}

  @Override
  public void unregisterShuffle(String appId) {}
}
