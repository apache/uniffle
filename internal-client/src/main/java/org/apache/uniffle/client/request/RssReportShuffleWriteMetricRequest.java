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
  private TaskShuffleWriteTimes writeTimes;

  private boolean isShuffleWriteFailed;
  private Optional<String> shuffleWriteFailureReason;

  public RssReportShuffleWriteMetricRequest(
      int stageId,
      int shuffleId,
      long taskId,
      Map<String, TaskShuffleWriteMetric> metrics,
      TaskShuffleWriteTimes writeTimes,
      boolean isShuffleWriteFailed,
      Optional<String> shuffleWriteFailureReason) {
    this.stageId = stageId;
    this.shuffleId = shuffleId;
    this.taskId = taskId;
    this.metrics = metrics;
    this.writeTimes = writeTimes;
    this.isShuffleWriteFailed = isShuffleWriteFailed;
    this.shuffleWriteFailureReason = shuffleWriteFailureReason;
  }

  public RssProtos.ReportShuffleWriteMetricRequest toProto() {
    RssReportShuffleWriteMetricRequest request = this;
    RssProtos.ReportShuffleWriteMetricRequest.Builder builder =
        RssProtos.ReportShuffleWriteMetricRequest.newBuilder();
    builder
        .setShuffleId(request.shuffleId)
        .setStageId(request.stageId)
        .setTaskId(request.taskId)
        .setShuffleWriteTimes(writeTimes.toProto())
        .setIsTaskWriteFailed(isShuffleWriteFailed)
        .setShuffleWriteFailureReason(shuffleWriteFailureReason.orElse(""))
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

  public static class TaskShuffleWriteTimes {
    private long totalTime;

    private long copyTime = 0;
    private long serializeTime = 0;
    private long compressTime = 0;
    private long sortTime = 0;
    private long requireMemoryTime = 0;
    private long waitFinishTime = 0;

    public TaskShuffleWriteTimes(
        long totalTime,
        long copyTime,
        long serializeTime,
        long compressTime,
        long sortTime,
        long requireMemoryTime,
        long waitFinishTime) {
      this.totalTime = totalTime;
      this.copyTime = copyTime;
      this.serializeTime = serializeTime;
      this.compressTime = compressTime;
      this.sortTime = sortTime;
      this.requireMemoryTime = requireMemoryTime;
      this.waitFinishTime = waitFinishTime;
    }

    public long getTotalTime() {
      return totalTime;
    }

    public long getCopyTime() {
      return copyTime;
    }

    public long getSerializeTime() {
      return serializeTime;
    }

    public long getCompressTime() {
      return compressTime;
    }

    public long getSortTime() {
      return sortTime;
    }

    public long getRequireMemoryTime() {
      return requireMemoryTime;
    }

    public long getWaitFinishTime() {
      return waitFinishTime;
    }

    public RssProtos.ShuffleWriteTimes toProto() {
      return RssProtos.ShuffleWriteTimes.newBuilder()
          .setTotal(this.totalTime)
          .setCopy(this.copyTime)
          .setSerialize(this.serializeTime)
          .setCompress(this.compressTime)
          .setSort(this.sortTime)
          .setRequireMemory(this.requireMemoryTime)
          .setWaitFinish(this.waitFinishTime)
          .build();
    }
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
