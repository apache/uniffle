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
import java.util.stream.Collectors;

import org.apache.uniffle.proto.RssProtos;

public class RssReportShuffleReadMetricRequest {
  private int stageId;
  private int shuffleId;
  private long taskId;
  private Map<String, TaskShuffleReadMetric> metrics;

  public RssReportShuffleReadMetricRequest(
      int stageId, int shuffleId, long taskId, Map<String, TaskShuffleReadMetric> metrics) {
    this.stageId = stageId;
    this.shuffleId = shuffleId;
    this.taskId = taskId;
    this.metrics = metrics;
  }

  public RssProtos.ReportShuffleReadMetricRequest toProto() {
    RssReportShuffleReadMetricRequest request = this;
    RssProtos.ReportShuffleReadMetricRequest.Builder builder =
        RssProtos.ReportShuffleReadMetricRequest.newBuilder();
    builder
        .setShuffleId(request.shuffleId)
        .setStageId(request.stageId)
        .setTaskId(request.taskId)
        .putAllMetrics(
            request.metrics.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        x ->
                            RssProtos.ShuffleReadMetric.newBuilder()
                                .setByteSize(x.getValue().getByteSize())
                                .setDurationMillis(x.getValue().getDurationMillis())
                                .build())));
    return builder.build();
  }

  public static class TaskShuffleReadMetric {
    private long durationMillis;
    private long byteSize;

    TaskShuffleReadMetric(long durationMillis, long byteSize) {
      this.durationMillis = durationMillis;
      this.byteSize = byteSize;
    }

    public long getDurationMillis() {
      return durationMillis;
    }

    public long getByteSize() {
      return byteSize;
    }
  }
}
