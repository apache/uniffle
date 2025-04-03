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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.impl.FailedBlockSendTracker;
import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
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
        new SendShuffleDataResult(Sets.newHashSet(1L, 2L), failedBlockSendTracker));
    ShuffleBlockInfo shuffleBlockInfo =
        new ShuffleBlockInfo(1, 1, 1, 1, 1, new byte[1], null, 1, 100, 1);
    AddBlockEvent event = new AddBlockEvent("taskId", Arrays.asList(shuffleBlockInfo));
    // sync send
    Future<Long> future = dataPusher.send(event);
    long memoryFree = future.get();
    assertEquals(100, memoryFree);
    assertTrue(taskToSuccessBlockIds.get("taskId").contains(1L));
    assertTrue(taskToSuccessBlockIds.get("taskId").contains(2L));
    assertTrue(taskToFailedBlockSendTracker.get("taskId").getFailedBlockIds().contains(3L));
    assertTrue(taskToFailedBlockSendTracker.get("taskId").getFailedBlockIds().contains(4L));
  }
}
