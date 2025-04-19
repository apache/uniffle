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

public class RssAppUniffleShuffleIdRequest {
  private final int shuffleId;
  private final int stageAttemptId;
  private final int stageAttemptNumber;
  private final boolean isWriter;

  public RssAppUniffleShuffleIdRequest(
      int shuffleId, int stageAttemptId, int stageAttemptNumber, boolean isWriter) {
    this.shuffleId = shuffleId;
    this.stageAttemptId = stageAttemptId;
    this.stageAttemptNumber = stageAttemptNumber;
    this.isWriter = isWriter;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getStageAttemptNumber() {
    return stageAttemptNumber;
  }

  public boolean isWriter() {
    return isWriter;
  }

  public RssProtos.AppUniffleShuffleIdRequest toProto() {
    RssProtos.AppUniffleShuffleIdRequest.Builder builder =
        RssProtos.AppUniffleShuffleIdRequest.newBuilder()
            .setShuffleId(shuffleId)
            .setStageAttemptId(stageAttemptId)
            .setStageAttemptNumber(stageAttemptNumber)
            .setIsWriter(isWriter);
    return builder.build();
  }
}
