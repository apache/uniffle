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

import com.google.common.collect.Lists;

import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.proto.RssProtos.ReportShuffleWriteFailureRequest;
import org.apache.uniffle.proto.RssProtos.ShuffleServerId;

public class RssReportShuffleWriteFailureRequest {
  private String appId;
  private int shuffleId;
  private int stageAttemptId;
  private int stageAttemptNumber;
  private List<ShuffleServerInfo> shuffleServerInfos;
  private String exception;

  public RssReportShuffleWriteFailureRequest(
      String appId,
      int shuffleId,
      int stageAttemptId,
      int stageAttemptNumber,
      List<ShuffleServerInfo> shuffleServerInfos,
      String exception) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.stageAttemptId = stageAttemptId;
    this.stageAttemptNumber = stageAttemptNumber;
    this.shuffleServerInfos = shuffleServerInfos;
    this.exception = exception;
  }

  public ReportShuffleWriteFailureRequest toProto() {
    List<ShuffleServerId> shuffleServerIds = Lists.newArrayList();
    for (ShuffleServerInfo shuffleServerInfo : shuffleServerInfos) {
      shuffleServerIds.add(
          ShuffleServerId.newBuilder()
              .setId(shuffleServerInfo.getId())
              .setPort(shuffleServerInfo.getGrpcPort())
              .setIp(shuffleServerInfo.getHost())
              .build());
    }
    ReportShuffleWriteFailureRequest.Builder builder =
        ReportShuffleWriteFailureRequest.newBuilder();
    builder
        .setAppId(appId)
        .setShuffleId(shuffleId)
        .setStageAttemptId(stageAttemptId)
        .setStageAttemptNumber(stageAttemptNumber)
        .addAllShuffleServerIds(shuffleServerIds);
    if (exception != null) {
      builder.setException(exception);
    }
    return builder.build();
  }
}
