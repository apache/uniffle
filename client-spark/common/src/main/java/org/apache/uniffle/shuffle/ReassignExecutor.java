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

package org.apache.uniffle.shuffle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.handle.MutableShuffleHandleInfo;
import org.apache.spark.shuffle.writer.TaskAttemptAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.impl.FailedBlockSendTracker;
import org.apache.uniffle.client.impl.TrackingBlockStatus;
import org.apache.uniffle.client.request.RssReassignOnBlockSendFailureRequest;
import org.apache.uniffle.client.response.RssReassignOnBlockSendFailureResponse;
import org.apache.uniffle.common.ReceivingFailureServer;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.exception.RssSendFailedException;
import org.apache.uniffle.common.rpc.StatusCode;

/**
 * this class is responsible for the reassignment, including the partition split and the block
 * resend after reassignment.
 */
public class ReassignExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(ReassignExecutor.class);
  private static final Set<StatusCode> STATUS_CODE_WITHOUT_BLOCK_RESEND =
      Sets.newHashSet(StatusCode.NO_REGISTER);

  private final FailedBlockSendTracker failedBlockSendTracker;
  private final TaskAttemptAssignment taskAttemptAssignment;

  private final Consumer<ShuffleBlockInfo> removeBlockStatsFunction;
  private final Consumer<List<ShuffleBlockInfo>> resendBlocksFunction;
  private final Supplier<ShuffleManagerClient> managerClientSupplier;

  private final TaskContext taskContext;
  private final int shuffleId;
  private int blockFailSentRetryMaxTimes;

  public ReassignExecutor(
      FailedBlockSendTracker failedBlockSendTracker,
      TaskAttemptAssignment taskAttemptAssignment,
      Consumer<ShuffleBlockInfo> removeBlockStatsFunction,
      Consumer<List<ShuffleBlockInfo>> resendBlocksFunction,
      Supplier<ShuffleManagerClient> managerClientSupplier,
      TaskContext taskContext,
      int shuffleId,
      int blockFailSentRetryMaxTimes) {
    this.failedBlockSendTracker = failedBlockSendTracker;
    this.taskAttemptAssignment = taskAttemptAssignment;
    this.removeBlockStatsFunction = removeBlockStatsFunction;
    this.resendBlocksFunction = resendBlocksFunction;
    this.managerClientSupplier = managerClientSupplier;
    this.taskContext = taskContext;
    this.shuffleId = shuffleId;
    this.blockFailSentRetryMaxTimes = blockFailSentRetryMaxTimes;
  }

  public void reassign() {
    if (failedBlockSendTracker == null) {
      return;
    }
    // 1. reassign for split partitions.
    reassignOnPartitionNeedSplit();
    // 2. reassign for failed blocks
    reassignAndResendForFailedBlocks();
  }

  @VisibleForTesting
  public void resetBlockRetryMaxTimes(int times) {
    this.blockFailSentRetryMaxTimes = times;
  }

  private void releaseResources(Set<Long> blockIds) {
    for (Long blockId : blockIds) {
      List<TrackingBlockStatus> failedBlockStatus =
          failedBlockSendTracker.getFailedBlockStatus(blockId);
      if (CollectionUtils.isNotEmpty(failedBlockStatus)) {
        TrackingBlockStatus blockStatus = failedBlockStatus.get(0);
        blockStatus.getShuffleBlockInfo().executeCompletionCallback(true);
      }
    }
  }

  private void reassignAndResendForFailedBlocks() {
    Set<Long> failedBlockIds = failedBlockSendTracker.getFailedBlockIds();
    if (CollectionUtils.isEmpty(failedBlockIds)) {
      return;
    }

    Set<TrackingBlockStatus> resendBlocks = new HashSet<>();
    for (Long blockId : failedBlockIds) {
      List<TrackingBlockStatus> failedBlockStatus =
          failedBlockSendTracker.getFailedBlockStatus(blockId);
      synchronized (failedBlockStatus) {
        int retryCnt =
            failedBlockStatus.stream()
                .filter(
                    x -> {
                      // If statusCode is null, the block was resent due to a stale assignment.
                      // In this case, the retry count checking should be ignored.
                      return x.getStatusCode() != null;
                    })
                .map(x -> x.getShuffleBlockInfo().getRetryCnt())
                .max(Comparator.comparing(Integer::valueOf))
                .orElse(-1);
        if (retryCnt >= blockFailSentRetryMaxTimes) {
          releaseResources(failedBlockIds);
          String message =
              String.format(
                  "Block send retry exceeded max retries. blockId=%d, retryCount=%d, maxRetry=%d, faultyServers=%s",
                  blockId,
                  retryCnt,
                  blockFailSentRetryMaxTimes,
                  failedBlockStatus.stream()
                      .map(TrackingBlockStatus::getShuffleServerInfo)
                      .collect(Collectors.toSet()));
          throw new RssSendFailedException(message);
        }

        for (TrackingBlockStatus status : failedBlockStatus) {
          StatusCode code = status.getStatusCode();
          if (STATUS_CODE_WITHOUT_BLOCK_RESEND.contains(code)) {
            releaseResources(failedBlockIds);
            String message =
                String.format(
                    "Block send failed with status code [%s] which does not trigger block resend. blockId=%d, retryCount=%d, maxRetry=%d, faultyServer=%s",
                    code,
                    blockId,
                    retryCnt,
                    blockFailSentRetryMaxTimes,
                    status.getShuffleServerInfo());
            throw new RssSendFailedException(message);
          }
        }

        // todo: if setting multi replica and another replica is succeed to send, no need to resend
        resendBlocks.addAll(failedBlockStatus);
      }
    }
    reassignAndResendBlocks(resendBlocks);
  }

  private void reassignOnPartitionNeedSplit() {
    final FailedBlockSendTracker failedTracker = failedBlockSendTracker;
    Map<Integer, List<ReceivingFailureServer>> failurePartitionToServers = new HashMap<>();

    failedTracker
        .removeAllTrackedPartitions()
        .forEach(
            partitionStatus -> {
              List<ReceivingFailureServer> servers =
                  failurePartitionToServers.computeIfAbsent(
                      partitionStatus.getPartitionId(), x -> new ArrayList<>());
              String serverId = partitionStatus.getShuffleServerInfo().getId();
              // todo: use better data structure to filter
              if (!servers.stream()
                  .map(x -> x.getServerId())
                  .collect(Collectors.toSet())
                  .contains(serverId)) {
                servers.add(new ReceivingFailureServer(serverId, StatusCode.SUCCESS));
              }
            });

    if (failurePartitionToServers.isEmpty()) {
      return;
    }

    //
    // For the [load balance] mode
    // Once partition has been split, the following split trigger will be ignored.
    //
    // For the [pipeline] mode
    // The split request will be always response
    //
    Map<Integer, List<ReceivingFailureServer>> reassignPartitionServers = new HashMap<>();
    for (Map.Entry<Integer, List<ReceivingFailureServer>> entry :
        failurePartitionToServers.entrySet()) {
      int partitionId = entry.getKey();
      List<ReceivingFailureServer> failureServers = entry.getValue();
      if (!taskAttemptAssignment.updatePartitionSplitAssignment(
          partitionId,
          failureServers.stream()
              .map(x -> ShuffleServerInfo.from(x.getServerId()))
              .collect(Collectors.toList()))) {
        reassignPartitionServers.put(partitionId, failureServers);
      }
    }

    if (reassignPartitionServers.isEmpty()) {
      LOG.info(
          "[partition-split] Switch to another shuffle-server for split partitions (maybe has been load balanced). partitionIds: {}",
          failurePartitionToServers.keySet());
      return;
    }

    doReassignOnBlockSendFailure(reassignPartitionServers, true);

    StringBuilder builder = new StringBuilder();
    builder.append("=partition-split=");
    for (Map.Entry<Integer, List<ReceivingFailureServer>> entry :
        reassignPartitionServers.entrySet()) {
      builder.append("partitionId");
      builder.append(entry.getKey());
      builder.append(": ");
      builder.append(
          entry.getValue().stream().map(x -> x.getServerId()).collect(Collectors.toList()));
      builder.append(" -> ");
      builder.append(taskAttemptAssignment.retrieve(entry.getKey()));
    }
    builder.append("==");
    LOG.info(builder.toString());
  }

  @VisibleForTesting
  protected void doReassignOnBlockSendFailure(
      Map<Integer, List<ReceivingFailureServer>> failurePartitionToServers,
      boolean partitionSplit) {
    LOG.info(
        "Initiate reassignOnBlockSendFailure of taskId[{}]. partition split: {}. failure partition servers: {}. ",
        taskContext.taskAttemptId(),
        partitionSplit,
        failurePartitionToServers);
    String executorId = SparkEnv.get().executorId();
    long taskAttemptId = taskContext.taskAttemptId();
    int stageId = taskContext.stageId();
    int stageAttemptNum = taskContext.stageAttemptNumber();
    try {
      RssReassignOnBlockSendFailureRequest request =
          new RssReassignOnBlockSendFailureRequest(
              shuffleId,
              failurePartitionToServers,
              executorId,
              taskAttemptId,
              stageId,
              stageAttemptNum,
              partitionSplit);
      RssReassignOnBlockSendFailureResponse response =
          managerClientSupplier.get().reassignOnBlockSendFailure(request);
      if (response.getStatusCode() != StatusCode.SUCCESS) {
        String msg =
            String.format(
                "Reassign failed. statusCode: %s, msg: %s",
                response.getStatusCode(), response.getMessage());
        throw new RssException(msg);
      }
      MutableShuffleHandleInfo handle = MutableShuffleHandleInfo.fromProto(response.getHandle());
      taskAttemptAssignment.update(handle);

      // print the lastest assignment for those reassignment partition ids
      Map<Integer, List<String>> reassignments = new HashMap<>();
      for (Map.Entry<Integer, List<ReceivingFailureServer>> entry :
          failurePartitionToServers.entrySet()) {
        int partitionId = entry.getKey();
        List<ShuffleServerInfo> servers = taskAttemptAssignment.retrieve(partitionId);
        reassignments.put(
            partitionId, servers.stream().map(x -> x.getId()).collect(Collectors.toList()));
      }
      LOG.info("Succeed to reassign that the latest assignment is {}", reassignments);
    } catch (Exception e) {
      throw new RssException(
          "Errors on reassign on block send failure. failure partition->servers : "
              + failurePartitionToServers,
          e);
    }
  }

  private void reassignAndResendBlocks(Set<TrackingBlockStatus> blocks) {
    List<ShuffleBlockInfo> resendCandidates = Lists.newArrayList();
    Map<Integer, List<TrackingBlockStatus>> partitionedFailedBlocks =
        blocks.stream()
            .collect(Collectors.groupingBy(d -> d.getShuffleBlockInfo().getPartitionId()));

    Map<Integer, List<ReceivingFailureServer>> failurePartitionToServers = new HashMap<>();
    for (Map.Entry<Integer, List<TrackingBlockStatus>> entry : partitionedFailedBlocks.entrySet()) {
      int partitionId = entry.getKey();
      List<TrackingBlockStatus> partitionBlocks = entry.getValue();
      Map<ShuffleServerInfo, TrackingBlockStatus> serverBlocks =
          partitionBlocks.stream()
              .collect(Collectors.groupingBy(d -> d.getShuffleServerInfo()))
              .entrySet()
              .stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey, x -> x.getValue().stream().findFirst().get()));
      for (Map.Entry<ShuffleServerInfo, TrackingBlockStatus> blockStatusEntry :
          serverBlocks.entrySet()) {
        String serverId = blockStatusEntry.getKey().getId();
        // avoid duplicate reassign for the same failure server.
        // todo: getting the replacement should support multi replica.
        List<ShuffleServerInfo> servers = taskAttemptAssignment.retrieve(partitionId);
        // Gets the first replica for this partition for now.
        // It can not work if we want to use multiple replicas.
        ShuffleServerInfo replacement = servers.get(0);
        String latestServerId = replacement.getId();
        if (!serverId.equals(latestServerId)) {
          continue;
        }
        StatusCode code = blockStatusEntry.getValue().getStatusCode();
        failurePartitionToServers
            .computeIfAbsent(partitionId, x -> new ArrayList<>())
            .add(new ReceivingFailureServer(serverId, code));
      }
    }

    if (!failurePartitionToServers.isEmpty()) {
      doReassignOnBlockSendFailure(failurePartitionToServers, false);
    }

    for (TrackingBlockStatus blockStatus : blocks) {
      ShuffleBlockInfo block = blockStatus.getShuffleBlockInfo();
      // todo: getting the replacement should support multi replica.
      List<ShuffleServerInfo> servers = taskAttemptAssignment.retrieve(block.getPartitionId());
      // Gets the first replica for this partition for now.
      // It can not work if we want to use multiple replicas.
      ShuffleServerInfo replacement = servers.get(0);
      if (blockStatus.getShuffleServerInfo().getId().equals(replacement.getId())) {
        LOG.warn(
            "PartitionId:{} has the following assigned servers: {}. But currently the replacement server:{} is the same with previous one!",
            block.getPartitionId(),
            taskAttemptAssignment.list(block.getPartitionId()),
            replacement);
        throw new RssException(
            "No available replacement server for: " + blockStatus.getShuffleServerInfo().getId());
      }
      // clear the previous retry state of block
      removeBlockStatsFunction.accept(block);
      final ShuffleBlockInfo newBlock = block;
      newBlock.incrRetryCnt();
      newBlock.reassignShuffleServers(Arrays.asList(replacement));
      resendCandidates.add(newBlock);
    }
    resendBlocksFunction.accept(resendCandidates);
    LOG.info(
        "Failed blocks have been resent to data pusher queue since reassignment has been finished successfully");
  }
}
