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

package org.apache.uniffle.client.request;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.uniffle.common.ReceivingFailureServer;
import org.apache.uniffle.proto.RssProtos;

public class RssReassignOnBlockSendFailureRequest {
  private final boolean partitionSplit;
  private int shuffleId;
  private Map<Integer, List<ReceivingFailureServer>> failurePartitionToServers;
  private String executorId;
  private long taskAttemptId;
  private int stageId;
  private int stageAttemptNumber;

  public RssReassignOnBlockSendFailureRequest(
      int shuffleId,
      Map<Integer, List<ReceivingFailureServer>> failurePartitionToServers,
      String executorId,
      long taskAttemptId,
      int stageId,
      int stageAttemptNum,
      boolean partitionSplit) {
    this.shuffleId = shuffleId;
    this.failurePartitionToServers = failurePartitionToServers;
    this.executorId = executorId;
    this.taskAttemptId = taskAttemptId;
    this.stageId = stageId;
    this.stageAttemptNumber = stageAttemptNum;
    this.partitionSplit = partitionSplit;
  }

  public static RssProtos.RssReassignOnBlockSendFailureRequest toProto(
      RssReassignOnBlockSendFailureRequest request) {
    return RssProtos.RssReassignOnBlockSendFailureRequest.newBuilder()
        .setShuffleId(request.shuffleId)
        .putAllFailurePartitionToServerIds(
            request.failurePartitionToServers.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey, x -> ReceivingFailureServer.toProto(x.getValue()))))
        .setExecutorId(request.executorId)
        .setStageId(request.stageId)
        .setStageAttemptNumber(request.stageAttemptNumber)
        .setTaskAttemptId(request.taskAttemptId)
        .setPartitionSplit(request.partitionSplit)
        .build();
  }
}
