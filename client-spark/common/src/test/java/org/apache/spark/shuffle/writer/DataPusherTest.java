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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.spark.shuffle.RssSparkConfig;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.common.ShuffleServerPushCostTracker;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.impl.FailedBlockSendTracker;
import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.JavaUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataPusherTest {

  static class FakedShuffleWriteClient extends ShuffleWriteClientImpl {
    private SendShuffleDataResult fakedShuffleDataResult;

    FakedShuffleWriteClient() {
      super(
          ShuffleClientFactory.newWriteBuilder()
              .clientType("GRPC")
              .retryMax(1)
              .retryIntervalMax(1)
              .heartBeatThreadNum(10)
              .replica(1)
              .replicaWrite(1)
              .replicaRead(1)
              .replicaSkipEnabled(true)
              .dataTransferPoolSize(1)
              .dataCommitPoolSize(1)
              .unregisterThreadPoolSize(1)
              .unregisterTimeSec(1)
              .unregisterRequestTimeSec(1));
    }

    @Override
    public SendShuffleDataResult sendShuffleData(
        String appId,
        List<ShuffleBlockInfo> shuffleBlockInfoList,
        Supplier<Boolean> needCancelRequest) {
      return sendShuffleData(appId, 0, shuffleBlockInfoList, needCancelRequest);
    }

    @Override
    public SendShuffleDataResult sendShuffleData(
        String appId,
        int stageAttemptNumber,
        List<ShuffleBlockInfo> shuffleBlockInfoList,
        Supplier<Boolean> needCancelRequest) {
      return fakedShuffleDataResult;
    }

    public void setFakedShuffleDataResult(SendShuffleDataResult fakedShuffleDataResult) {
      this.fakedShuffleDataResult = fakedShuffleDataResult;
    }
  }

  @Test
  public void testFilterOutStaleAssignmentBlocks() {
    FakedShuffleWriteClient shuffleWriteClient = new FakedShuffleWriteClient();

    Map<String, Set<Long>> taskToSuccessBlockIds = Maps.newConcurrentMap();
    Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker = JavaUtils.newConcurrentMap();
    Set<String> failedTaskIds = new HashSet<>();

    RssConf rssConf = new RssConf();
    rssConf.set(RssClientConf.RSS_CLIENT_REASSIGN_ENABLED, true);
    rssConf.set(RssSparkConfig.RSS_PARTITION_REASSIGN_STALE_ASSIGNMENT_FAST_SWITCH_ENABLED, true);
    DataPusher dataPusher =
        new DataPusher(
            shuffleWriteClient,
            taskToSuccessBlockIds,
            taskToFailedBlockSendTracker,
            failedTaskIds,
            1,
            2,
            rssConf);
    dataPusher.setRssAppId("testFilterOutStaleAssignmentBlocks");

    String taskId = "taskId1";
    List<ShuffleServerInfo> server1 =
        Collections.singletonList(new ShuffleServerInfo("0", "localhost", 1234));
    ShuffleBlockInfo staleBlock1 =
        new ShuffleBlockInfo(
            1, 1, 3, 1, 1, new byte[1], server1, 1, 100, 1, integer -> Collections.emptyList());

    // case1: will fast fail due to the stale assignment
    AddBlockEvent event = new AddBlockEvent(taskId, Arrays.asList(staleBlock1));
    CompletableFuture<Long> f1 = dataPusher.send(event);
    assertEquals(f1.join(), 0);
    Set<Long> failedBlockIds = taskToFailedBlockSendTracker.get(taskId).getFailedBlockIds();
    assertEquals(1, failedBlockIds.size());
    assertEquals(3, failedBlockIds.stream().findFirst().get());
  }

  /**
   * Test that when all blocks in a batch are stale (filtered out by fast-switch), the
   * processedCallbackChain is still executed. Before the fix, if all blocks were stale, the early
   * return skipped the finally block, causing the callback (which notifies checkBlockSendResult via
   * finishEventQueue) to never run. This led to checkBlockSendResult blocking indefinitely on
   * poll(), unable to call reassign() to resend the stale blocks, ultimately timing out.
   */
  @Test
  public void testProcessedCallbackChainExecutedWhenAllBlocksAreStale()
      throws ExecutionException, InterruptedException {
    FakedShuffleWriteClient shuffleWriteClient = new FakedShuffleWriteClient();

    Map<String, Set<Long>> taskToSuccessBlockIds = Maps.newConcurrentMap();
    Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker = JavaUtils.newConcurrentMap();
    Set<String> failedTaskIds = new HashSet<>();

    RssConf rssConf = new RssConf();
    rssConf.set(RssClientConf.RSS_CLIENT_REASSIGN_ENABLED, true);
    rssConf.set(RssSparkConfig.RSS_PARTITION_REASSIGN_STALE_ASSIGNMENT_FAST_SWITCH_ENABLED, true);
    DataPusher dataPusher =
        new DataPusher(
            shuffleWriteClient,
            taskToSuccessBlockIds,
            taskToFailedBlockSendTracker,
            failedTaskIds,
            1,
            2,
            rssConf);
    dataPusher.setRssAppId("testCallbackWhenAllStale");

    String taskId = "taskId1";
    List<ShuffleServerInfo> server1 =
        Collections.singletonList(new ShuffleServerInfo("0", "localhost", 1234));
    // Create a stale block: isStaleAssignment() returns true because the
    // partitionAssignmentRetrieveFunc returns an empty list (different from the block's servers).
    ShuffleBlockInfo staleBlock =
        new ShuffleBlockInfo(
            1, 1, 10, 1, 1, new byte[1], server1, 1, 100, 1, integer -> Collections.emptyList());

    // Track whether processedCallbackChain is invoked
    AtomicBoolean callbackExecuted = new AtomicBoolean(false);
    AddBlockEvent event = new AddBlockEvent(taskId, Arrays.asList(staleBlock));
    event.addCallback(() -> callbackExecuted.set(true));

    CompletableFuture<Long> future = dataPusher.send(event);
    long result = future.get();

    // The block is stale, so no data is actually sent (0 bytes freed)
    assertEquals(0L, result);

    // The stale block should be tracked in the FailedBlockSendTracker
    Set<Long> failedBlockIds = taskToFailedBlockSendTracker.get(taskId).getFailedBlockIds();
    assertEquals(1, failedBlockIds.size());
    assertEquals(10, failedBlockIds.stream().findFirst().get());

    // The processedCallbackChain MUST be executed even when all blocks are stale.
    // Before the fix, this assertion would fail because the early return (return 0L)
    // was placed before the try-finally that executes the callback chain.
    assertTrue(
        callbackExecuted.get(),
        "processedCallbackChain must be executed even when all blocks are stale, "
            + "otherwise checkBlockSendResult will block on finishEventQueue.poll() indefinitely");
  }

  @Test
  public void testSendData() throws ExecutionException, InterruptedException {
    FakedShuffleWriteClient shuffleWriteClient = new FakedShuffleWriteClient();

    Map<String, Set<Long>> taskToSuccessBlockIds = Maps.newConcurrentMap();
    Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker = JavaUtils.newConcurrentMap();
    Set<String> failedTaskIds = new HashSet<>();

    DataPusher dataPusher =
        new DataPusher(
            shuffleWriteClient,
            taskToSuccessBlockIds,
            taskToFailedBlockSendTracker,
            failedTaskIds,
            1,
            2);
    dataPusher.setRssAppId("testSendData_appId");
    FailedBlockSendTracker failedBlockSendTracker = new FailedBlockSendTracker();
    ShuffleBlockInfo failedBlock1 =
        new ShuffleBlockInfo(1, 1, 3, 1, 1, new byte[1], null, 1, 100, 1);
    ShuffleBlockInfo failedBlock2 =
        new ShuffleBlockInfo(1, 1, 4, 1, 1, new byte[1], null, 1, 100, 1);
    failedBlockSendTracker.add(
        failedBlock1, new ShuffleServerInfo("host", 39998), StatusCode.NO_BUFFER);
    failedBlockSendTracker.add(
        failedBlock2, new ShuffleServerInfo("host", 39998), StatusCode.NO_BUFFER);
    shuffleWriteClient.setFakedShuffleDataResult(
        new SendShuffleDataResult(
            Sets.newHashSet(1L, 2L), failedBlockSendTracker, new ShuffleServerPushCostTracker()));
    ShuffleBlockInfo shuffleBlockInfo =
        new ShuffleBlockInfo(1, 1, 1, 1, 1, new byte[1], null, 1, 100, 1);
    AddBlockEvent event = new AddBlockEvent("taskId", Arrays.asList(shuffleBlockInfo));
    // sync send
    CompletableFuture<Long> future = dataPusher.send(event);
    long memoryFree = future.get();
    assertEquals(100, memoryFree);
    assertTrue(taskToSuccessBlockIds.get("taskId").contains(1L));
    assertTrue(taskToSuccessBlockIds.get("taskId").contains(2L));
    assertTrue(taskToFailedBlockSendTracker.get("taskId").getFailedBlockIds().contains(3L));
    assertTrue(taskToFailedBlockSendTracker.get("taskId").getFailedBlockIds().contains(4L));
  }
}
