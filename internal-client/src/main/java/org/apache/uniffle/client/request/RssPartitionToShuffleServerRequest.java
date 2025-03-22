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

import org.apache.uniffle.proto.RssProtos;

public class RssPartitionToShuffleServerRequest {
  private int stageAttemptId;
  private int stageAttemptNumber;
  private int shuffleId;
  private boolean isWritePhase;

  public RssPartitionToShuffleServerRequest(
      int stageAttemptId, int stageAttemptNumber, int shuffleId, boolean isWritePhase) {
    this.stageAttemptId = stageAttemptId;
    this.stageAttemptNumber = stageAttemptNumber;
    this.shuffleId = shuffleId;
    this.isWritePhase = isWritePhase;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getStageAttemptNumber() {
    return stageAttemptNumber;
  }

  public RssProtos.PartitionToShuffleServerRequest toProto() {
    RssProtos.PartitionToShuffleServerRequest.Builder builder =
        RssProtos.PartitionToShuffleServerRequest.newBuilder();
    builder.setStageAttemptId(stageAttemptId);
    builder.setStageAttemptNumber(stageAttemptNumber);
    builder.setShuffleId(shuffleId);
    builder.setIsWritePhase(isWritePhase);
    return builder.build();
  }
}
