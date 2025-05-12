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

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.uniffle.proto.RssProtos;

public class RssReportShuffleWriteMetricRequest {
  private int stageId;
  private int shuffleId;
  private long taskId;
  private Map<String, TaskShuffleWriteMetric> metrics;

  public RssReportShuffleWriteMetricRequest(
      int stageId, int shuffleId, long taskId, Map<String, TaskShuffleWriteMetric> metrics) {
    this.stageId = stageId;
    this.shuffleId = shuffleId;
    this.taskId = taskId;
    this.metrics = metrics;
  }

  public RssProtos.ReportShuffleWriteMetricRequest toProto() {
    RssReportShuffleWriteMetricRequest request = this;
    RssProtos.ReportShuffleWriteMetricRequest.Builder builder =
        RssProtos.ReportShuffleWriteMetricRequest.newBuilder();
    builder
        .setShuffleId(request.shuffleId)
        .setStageId(request.stageId)
        .setTaskId(request.taskId)
        .putAllMetrics(
            request.metrics.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        x -> {
                          TaskShuffleWriteMetric metric = x.getValue();
                          return RssProtos.ShuffleWriteMetric.newBuilder()
                              .setByteSize(metric.getByteSize())
                              .setDurationMillis(metric.getDurationMillis())
                              .setPushFailureNumber(metric.getPushFailureNumber())
                              .setRequireBufferFailureNumber(metric.getRequireBufferFailureNumber())
                              .setLastPushFailureReason(
                                  Optional.ofNullable(metric.getLastFailureReason()).orElse(""))
                              .build();
                        })));
    return builder.build();
  }

  public static class TaskShuffleWriteMetric {
    private long durationMillis;
    private long byteSize;

    private long requireBufferFailureNumber;
    private long pushFailureNumber;

    private String lastFailureReason;

    public TaskShuffleWriteMetric(
        long durationMillis,
        long byteSize,
        long requireBufferFailureNumber,
        long pushFailureNumber,
        String lastFailureReason) {
      this.durationMillis = durationMillis;
      this.byteSize = byteSize;
      this.requireBufferFailureNumber = requireBufferFailureNumber;
      this.pushFailureNumber = pushFailureNumber;
      this.lastFailureReason = lastFailureReason;
    }

    public long getDurationMillis() {
      return durationMillis;
    }

    public long getByteSize() {
      return byteSize;
    }

    public long getRequireBufferFailureNumber() {
      return requireBufferFailureNumber;
    }

    public long getPushFailureNumber() {
      return pushFailureNumber;
    }

    public String getLastFailureReason() {
      return lastFailureReason;
    }
  }
}
