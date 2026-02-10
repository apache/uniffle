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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;

import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.writer.TaskAttemptAssignment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.impl.FailedBlockSendTracker;
import org.apache.uniffle.client.impl.TrackingBlockStatus;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.exception.RssSendFailedException;
import org.apache.uniffle.common.rpc.StatusCode;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReassignExecutorTest {

  @Mock private FailedBlockSendTracker failedBlockSendTracker = mock(FailedBlockSendTracker.class);

  @Mock private TaskAttemptAssignment taskAttemptAssignment = mock(TaskAttemptAssignment.class);

  @Mock private ShuffleManagerClient shuffleManagerClient = mock(ShuffleManagerClient.class);

  @Mock private TaskContext taskContext = mock(TaskContext.class);

  @Mock private Consumer<ShuffleBlockInfo> removeBlockStatsFunction = mock(Consumer.class);

  @Mock private Consumer<List<ShuffleBlockInfo>> resendBlocksFunction = mock(Consumer.class);

  private ReassignExecutor executor = mock(ReassignExecutor.class);

  @BeforeEach
  void setUp() {
    when(taskContext.taskAttemptId()).thenReturn(1L);
    when(taskContext.stageId()).thenReturn(1);
    when(taskContext.stageAttemptNumber()).thenReturn(0);

    executor =
        new ReassignExecutor(
            failedBlockSendTracker,
            taskAttemptAssignment,
            removeBlockStatsFunction,
            resendBlocksFunction,
            () -> shuffleManagerClient,
            taskContext,
            1,
            3);
  }

  @Test
  void testRetryExceededShouldFailAndReleaseResources() {
    long blockId = 100L;

    ShuffleBlockInfo blockInfo = org.mockito.Mockito.mock(ShuffleBlockInfo.class);
    when(blockInfo.getRetryCnt()).thenReturn(3);

    TrackingBlockStatus status = org.mockito.Mockito.mock(TrackingBlockStatus.class);
    when(status.getShuffleBlockInfo()).thenReturn(blockInfo);
    when(status.getStatusCode()).thenReturn(StatusCode.INTERNAL_ERROR);

    when(failedBlockSendTracker.getFailedBlockIds())
        .thenReturn(new HashSet<>(Arrays.asList(blockId)));
    when(failedBlockSendTracker.getFailedBlockStatus(blockId)).thenReturn(Arrays.asList(status));

    assertThrows(RssSendFailedException.class, executor::reassign);

    verify(blockInfo).executeCompletionCallback(true);
  }
}
