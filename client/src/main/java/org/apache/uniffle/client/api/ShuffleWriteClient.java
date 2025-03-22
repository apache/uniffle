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

package org.apache.uniffle.client.api;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.proto.RssProtos.MergeContext;

public interface ShuffleWriteClient {

  default SendShuffleDataResult sendShuffleData(
      String appId,
      int stageAttemptNumber,
      List<ShuffleBlockInfo> shuffleBlockInfoList,
      Supplier<Boolean> needCancelRequest) {
    throw new UnsupportedOperationException(
        this.getClass().getName()
            + " doesn't implement getShuffleAssignments with faultyServerIds");
  }

  SendShuffleDataResult sendShuffleData(
      String appId,
      List<ShuffleBlockInfo> shuffleBlockInfoList,
      Supplier<Boolean> needCancelRequest);

  void sendAppHeartbeat(String appId, long timeoutMs);

  void registerApplicationInfo(String appId, long timeoutMs, String user);

  default void registerShuffle(
      ShuffleServerInfo shuffleServerInfo,
      String appId,
      int shuffleId,
      List<PartitionRange> partitionRanges,
      RemoteStorageInfo remoteStorage,
      ShuffleDataDistributionType dataDistributionType,
      int maxConcurrencyPerPartitionToWrite) {
    registerShuffle(
        shuffleServerInfo,
        appId,
        shuffleId,
        partitionRanges,
        remoteStorage,
        dataDistributionType,
        maxConcurrencyPerPartitionToWrite,
        0,
        null,
        Collections.emptyMap());
  }

  default void registerShuffle(
      ShuffleServerInfo shuffleServerInfo,
      String appId,
      int shuffleId,
      List<PartitionRange> partitionRanges,
      RemoteStorageInfo remoteStorage,
      ShuffleDataDistributionType dataDistributionType,
      int maxConcurrencyPerPartitionToWrite,
      Map<String, String> properties) {
    registerShuffle(
        shuffleServerInfo,
        appId,
        shuffleId,
        partitionRanges,
        remoteStorage,
        dataDistributionType,
        maxConcurrencyPerPartitionToWrite,
        0,
        null,
        properties);
  }

  default void registerShuffle(
      ShuffleServerInfo shuffleServerInfo,
      String appId,
      int shuffleId,
      List<PartitionRange> partitionRanges,
      RemoteStorageInfo remoteStorage,
      ShuffleDataDistributionType dataDistributionType,
      int maxConcurrencyPerPartitionToWrite,
      int stageAttemptNumber,
      MergeContext mergeContext) {
    registerShuffle(
        shuffleServerInfo,
        appId,
        shuffleId,
        partitionRanges,
        remoteStorage,
        dataDistributionType,
        maxConcurrencyPerPartitionToWrite,
        stageAttemptNumber,
        mergeContext,
        Collections.emptyMap());
  }

  void registerShuffle(
      ShuffleServerInfo shuffleServerInfo,
      String appId,
      int shuffleId,
      List<PartitionRange> partitionRanges,
      RemoteStorageInfo remoteStorage,
      ShuffleDataDistributionType dataDistributionType,
      int maxConcurrencyPerPartitionToWrite,
      int stageAttemptNumber,
      MergeContext mergeContext,
      Map<String, String> properties);

  boolean sendCommit(
      Set<ShuffleServerInfo> shuffleServerInfoSet, String appId, int shuffleId, int numMaps);

  @Deprecated
  default void registerCoordinators(String coordinators) {
    registerCoordinators(coordinators, 0, 0);
  }

  void registerCoordinators(String coordinators, long retryIntervalMs, int retryTimes);

  Map<String, String> fetchClientConf(int timeoutMs);

  RemoteStorageInfo fetchRemoteStorage(String appId);

  void reportShuffleResult(
      Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds,
      String appId,
      int shuffleId,
      long taskAttemptId,
      int bitmapNum);

  default void reportShuffleResult(
      Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds,
      String appId,
      int shuffleId,
      long taskAttemptId,
      int bitmapNum,
      Set<ShuffleServerInfo> reportFailureServers,
      boolean enableWriteFailureRetry) {}

  ShuffleAssignmentsInfo getShuffleAssignments(
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
      int retryTimes);

  default ShuffleAssignmentsInfo getShuffleAssignments(
      String appId,
      int shuffleId,
      int partitionNum,
      int partitionNumPerRange,
      Set<String> requiredTags,
      int assignmentShuffleServerNumber,
      int estimateTaskConcurrency,
      Set<String> faultyServerIds) {
    return getShuffleAssignments(
        appId,
        shuffleId,
        partitionNum,
        partitionNumPerRange,
        requiredTags,
        assignmentShuffleServerNumber,
        estimateTaskConcurrency,
        faultyServerIds,
        -1,
        0,
        false,
        0,
        0);
  }

  default ShuffleAssignmentsInfo getShuffleAssignments(
      String appId,
      int shuffleId,
      int partitionNum,
      int partitionNumPerRange,
      Set<String> requiredTags,
      int assignmentShuffleServerNumber,
      int estimateTaskConcurrency) {
    return getShuffleAssignments(
        appId,
        shuffleId,
        partitionNum,
        partitionNumPerRange,
        requiredTags,
        assignmentShuffleServerNumber,
        estimateTaskConcurrency,
        Collections.emptySet());
  }

  Roaring64NavigableMap getShuffleResult(
      String clientType,
      Set<ShuffleServerInfo> shuffleServerInfoSet,
      String appId,
      int shuffleId,
      int partitionId);

  Roaring64NavigableMap getShuffleResultForMultiPart(
      String clientType,
      Map<ShuffleServerInfo, Set<Integer>> serverToPartitions,
      String appId,
      int shuffleId,
      Set<Integer> failedPartitions,
      PartitionDataReplicaRequirementTracking replicaRequirementTracking);

  void close();

  void unregisterShuffle(String appId, int shuffleId);

  void unregisterShuffle(String appId);

  void startSortMerge(
      Set<ShuffleServerInfo> serverInfos,
      String appId,
      int shuffleId,
      int partitionId,
      Roaring64NavigableMap expectedTaskIds);
}
