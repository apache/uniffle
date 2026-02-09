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

package org.apache.spark.shuffle.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.spark.shuffle.RssSparkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.common.ShuffleServerPushCostTracker;
import org.apache.uniffle.client.impl.FailedBlockSendTracker;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.ThreadUtils;

/**
 * A {@link DataPusher} that is responsible for sending data to remote shuffle servers
 * asynchronously.
 */
public class DataPusher implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataPusher.class);

  private final ExecutorService executorService;

  private final ShuffleWriteClient shuffleWriteClient;
  // Must be thread safe
  private final Map<String, Set<Long>> taskToSuccessBlockIds;
  // Must be thread safe
  Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker;
  private String rssAppId;
  // Must be thread safe
  private final Set<String> failedTaskIds;

  // Whether to fast-switch for those stale assignment to avoid backpressure.
  // This is only valid if partition-reassign is enabled and single replica is used.
  private final boolean staleAssignmentFastSwitchEnabled;

  public DataPusher(
      ShuffleWriteClient shuffleWriteClient,
      Map<String, Set<Long>> taskToSuccessBlockIds,
      Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker,
      Set<String> failedTaskIds,
      int threadPoolSize,
      int threadKeepAliveTime,
      RssConf rssConf) {
    this.shuffleWriteClient = shuffleWriteClient;
    this.taskToSuccessBlockIds = taskToSuccessBlockIds;
    this.taskToFailedBlockSendTracker = taskToFailedBlockSendTracker;
    this.failedTaskIds = failedTaskIds;
    this.executorService =
        new ThreadPoolExecutor(
            threadPoolSize,
            threadPoolSize * 2,
            threadKeepAliveTime,
            TimeUnit.SECONDS,
            Queues.newLinkedBlockingQueue(Integer.MAX_VALUE),
            ThreadUtils.getThreadFactory(this.getClass().getName()));
    this.staleAssignmentFastSwitchEnabled =
        rssConf.get(RssClientConf.RSS_CLIENT_REASSIGN_ENABLED)
            && rssConf.get(
                RssSparkConfig.RSS_PARTITION_REASSIGN_STALE_ASSIGNMENT_FAST_SWITCH_ENABLED);
  }

  @VisibleForTesting
  public DataPusher(
      ShuffleWriteClient shuffleWriteClient,
      Map<String, Set<Long>> taskToSuccessBlockIds,
      Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker,
      Set<String> failedTaskIds,
      int threadPoolSize,
      int threadKeepAliveTime) {
    this(
        shuffleWriteClient,
        taskToSuccessBlockIds,
        taskToFailedBlockSendTracker,
        failedTaskIds,
        threadPoolSize,
        threadKeepAliveTime,
        new RssConf());
  }

  public CompletableFuture<Long> send(AddBlockEvent event) {
    if (rssAppId == null) {
      throw new RssException("RssAppId should be set.");
    }
    return CompletableFuture.supplyAsync(
            () -> {
              String taskId = event.getTaskId();
              List<ShuffleBlockInfo> blocks = event.getShuffleDataInfoList();
              List<ShuffleBlockInfo> validBlocks = filterOutStaleAssignmentBlocks(taskId, blocks);
              if (CollectionUtils.isEmpty(validBlocks)) {
                return 0L;
              }

              SendShuffleDataResult result = null;
              try {
                result =
                    shuffleWriteClient.sendShuffleData(
                        rssAppId,
                        event.getStageAttemptNumber(),
                        validBlocks,
                        () -> !isValidTask(taskId));
                // completionCallback should be executed before updating taskToSuccessBlockIds
                // structure to avoid side effect
                Set<Long> succeedBlockIds = getSucceedBlockIds(result);
                for (ShuffleBlockInfo block : validBlocks) {
                  block.executeCompletionCallback(succeedBlockIds.contains(block.getBlockId()));
                }
                putBlockId(taskToSuccessBlockIds, taskId, result.getSuccessBlockIds());
                putFailedBlockSendTracker(
                    taskToFailedBlockSendTracker, taskId, result.getFailedBlockSendTracker());
              } finally {
                WriteBufferManager bufferManager = event.getBufferManager();
                if (bufferManager != null && result != null) {
                  ShuffleServerPushCostTracker shuffleServerPushCostTracker =
                      result.getShuffleServerPushCostTracker();
                  bufferManager.merge(shuffleServerPushCostTracker);
                }

                List<Runnable> callbackChain =
                    Optional.of(event.getProcessedCallbackChain()).orElse(Collections.EMPTY_LIST);
                for (Runnable runnable : callbackChain) {
                  runnable.run();
                }
              }
              Set<Long> succeedBlockIds = getSucceedBlockIds(result);
              return validBlocks.stream()
                  .filter(x -> succeedBlockIds.contains(x.getBlockId()))
                  .map(x -> x.getFreeMemory())
                  .reduce((a, b) -> a + b)
                  .orElse(0L);
            },
            executorService)
        .exceptionally(
            ex -> {
              LOGGER.error("Unexpected exceptions occurred while sending shuffle data", ex);
              return null;
            });
  }

  /**
   * This method is only valid for the single replica. If the block info's assignment is stale, it
   * will be filtered out and make it retry. If the partition reassignment is disabled, this method
   * always will not filter out any blocks.
   *
   * @param taskId
   * @param blocks
   * @return the valid shuffle blocks
   */
  private List<ShuffleBlockInfo> filterOutStaleAssignmentBlocks(
      String taskId, List<ShuffleBlockInfo> blocks) {
    if (!staleAssignmentFastSwitchEnabled) {
      return blocks;
    }
    FailedBlockSendTracker staleBlockTracker = new FailedBlockSendTracker();
    List<ShuffleBlockInfo> validBlocks = new ArrayList<>();
    for (ShuffleBlockInfo block : blocks) {
      List<ShuffleServerInfo> servers = block.getShuffleServerInfos();
      // skip the multi replica cases.
      if (servers == null || servers.size() != 1) {
        validBlocks.add(block);
      } else {
        ShuffleServerInfo server = servers.get(0);
        if (block.isStaleAssignment()) {
          // It means the block failed due to the stale assignment fast-switch when status code is
          // null.
          staleBlockTracker.add(block, server, null);
        } else {
          validBlocks.add(block);
        }
      }
    }
    putFailedBlockSendTracker(taskToFailedBlockSendTracker, taskId, staleBlockTracker);
    return validBlocks;
  }

  private Set<Long> getSucceedBlockIds(SendShuffleDataResult result) {
    if (result == null || result.getSuccessBlockIds() == null) {
      return Collections.emptySet();
    }
    return result.getSuccessBlockIds();
  }

  private synchronized void putBlockId(
      Map<String, Set<Long>> taskToBlockIds, String taskAttemptId, Set<Long> blockIds) {
    if (blockIds == null || blockIds.isEmpty()) {
      return;
    }
    taskToBlockIds
        .computeIfAbsent(taskAttemptId, x -> Sets.newConcurrentHashSet())
        .addAll(blockIds);
  }

  private synchronized void putFailedBlockSendTracker(
      Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker,
      String taskAttemptId,
      FailedBlockSendTracker failedBlockSendTracker) {
    if (failedBlockSendTracker == null || failedBlockSendTracker.isEmpty()) {
      return;
    }
    taskToFailedBlockSendTracker
        .computeIfAbsent(taskAttemptId, x -> new FailedBlockSendTracker())
        .merge(failedBlockSendTracker);
  }

  public boolean isValidTask(String taskId) {
    return !failedTaskIds.contains(taskId);
  }

  public void setRssAppId(String rssAppId) {
    this.rssAppId = rssAppId;
  }

  @Override
  public void close() throws IOException {
    if (executorService != null) {
      try {
        ThreadUtils.shutdownThreadPool(executorService, 5);
      } catch (InterruptedException interruptedException) {
        LOGGER.error("Errors on shutdown thread pool of [{}].", this.getClass().getSimpleName());
      }
    }
  }
}
