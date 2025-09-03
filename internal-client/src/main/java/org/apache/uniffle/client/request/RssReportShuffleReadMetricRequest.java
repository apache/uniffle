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

import org.apache.uniffle.common.ShuffleReadTimes;
import org.apache.uniffle.proto.RssProtos;

public class RssReportShuffleReadMetricRequest {
  private int stageId;
  private int shuffleId;
  private long taskId;
  private Map<String, TaskShuffleReadMetric> metrics;
  private boolean isShuffleReadFailed;
  private Optional<String> shuffleReadReason;
  private ShuffleReadTimes shuffleReadTimes;

  public RssReportShuffleReadMetricRequest(
      int stageId,
      int shuffleId,
      long taskId,
      Map<String, TaskShuffleReadMetric> metrics,
      boolean isShuffleReadFailed,
      Optional<String> shuffleReadReason,
      ShuffleReadTimes shuffleReadTimes) {
    this.stageId = stageId;
    this.shuffleId = shuffleId;
    this.taskId = taskId;
    this.metrics = metrics;
    this.isShuffleReadFailed = isShuffleReadFailed;
    this.shuffleReadReason = shuffleReadReason;
    this.shuffleReadTimes = shuffleReadTimes;
  }

  public RssProtos.ReportShuffleReadMetricRequest toProto() {
    RssReportShuffleReadMetricRequest request = this;
    RssProtos.ReportShuffleReadMetricRequest.Builder builder =
        RssProtos.ReportShuffleReadMetricRequest.newBuilder();
    builder
        .setShuffleId(request.shuffleId)
        .setStageId(request.stageId)
        .setTaskId(request.taskId)
        .setIsTaskReadFailed(request.isShuffleReadFailed)
        .setShuffleReadFailureReason(request.shuffleReadReason.orElse(""))
        .setShuffleReadTimes(shuffleReadTimes.toProto())
        .putAllMetrics(
            request.metrics.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        x ->
                            RssProtos.ShuffleReadMetric.newBuilder()
                                .setByteSize(x.getValue().getByteSize())
                                .setDurationMillis(x.getValue().getDurationMillis())
                                .setMemoryByteSize(x.getValue().getMemoryByteSize())
                                .setMemoryDurationMillis(x.getValue().getMemoryDurationMillis())
                                .setLocalfileByteSize(x.getValue().getLocalfileByteSize())
                                .setLocalfileDurationMillis(
                                    x.getValue().getLocalfileDurationMillis())
                                .setHadoopByteSize(x.getValue().getHadoopByteSize())
                                .setHadoopDurationMillis(x.getValue().getHadoopDurationMillis())
                                .build())));
    return builder.build();
  }

  public static class TaskShuffleReadMetric {
    private long durationMillis;
    private long byteSize;

    private long localfileByteSize;
    private long localfileDurationMillis;

    private long memoryByteSize;
    private long memoryDurationMillis;

    private long hadoopByteSize;
    private long hadoopDurationMillis;

    public TaskShuffleReadMetric(
        long durationMillis,
        long byteSize,
        long memoryReadDurationMillis,
        long memoryReadBytes,
        long localfileReadDurationMillis,
        long localfileReadBytes,
        long hadoopReadLocalFileDurationMillis,
        long hadoopReadLocalFileBytes) {
      this.durationMillis = durationMillis;
      this.byteSize = byteSize;

      this.localfileByteSize = localfileReadBytes;
      this.localfileDurationMillis = localfileReadDurationMillis;

      this.memoryByteSize = memoryReadBytes;
      this.memoryDurationMillis = memoryReadDurationMillis;

      this.hadoopByteSize = hadoopReadLocalFileBytes;
      this.hadoopDurationMillis = hadoopReadLocalFileDurationMillis;
    }

    public long getDurationMillis() {
      return durationMillis;
    }

    public long getByteSize() {
      return byteSize;
    }

    public long getLocalfileByteSize() {
      return localfileByteSize;
    }

    public long getLocalfileDurationMillis() {
      return localfileDurationMillis;
    }

    public long getMemoryByteSize() {
      return memoryByteSize;
    }

    public long getMemoryDurationMillis() {
      return memoryDurationMillis;
    }

    public long getHadoopByteSize() {
      return hadoopByteSize;
    }

    public long getHadoopDurationMillis() {
      return hadoopDurationMillis;
    }
  }
}
