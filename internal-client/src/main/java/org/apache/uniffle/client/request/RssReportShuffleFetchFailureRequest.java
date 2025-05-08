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

import org.apache.uniffle.proto.RssProtos.ReportShuffleFetchFailureRequest;

public class RssReportShuffleFetchFailureRequest {
  private String appId;
  private int shuffleId;
  private int uniffleShuffleId;
  private int stageAttemptId;
  private int stageAttemptNumber;
  private int partitionId;
  private String exception;

  public RssReportShuffleFetchFailureRequest(
      String appId,
      int shuffleId,
      int uniffleShuffleId,
      int stageAttemptId,
      int stageAttemptNumber,
      int partitionId,
      String exception) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.uniffleShuffleId = uniffleShuffleId;
    this.stageAttemptId = stageAttemptId;
    this.stageAttemptNumber = stageAttemptNumber;
    this.partitionId = partitionId;
    this.exception = exception;
  }

  public ReportShuffleFetchFailureRequest toProto() {
    ReportShuffleFetchFailureRequest.Builder builder =
        ReportShuffleFetchFailureRequest.newBuilder();
    builder
        .setAppId(appId)
        .setShuffleId(shuffleId)
        .setUniffleShuffleId(uniffleShuffleId)
        .setStageAttemptId(stageAttemptId)
        .setStageAttemptNumber(stageAttemptNumber)
        .setPartitionId(partitionId);
    if (exception != null) {
      builder.setException(exception);
    }
    return builder.build();
  }
}
