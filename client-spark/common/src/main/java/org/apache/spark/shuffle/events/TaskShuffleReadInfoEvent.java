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

package org.apache.spark.shuffle.events;

import java.util.Map;

public class TaskShuffleReadInfoEvent extends UniffleEvent {
  private int stageId;
  private int shuffleId;
  private long taskId;
  private Map<String, ShuffleReadMetric> metrics;
  private boolean isShuffleReadFailed;
  private String failureReason;

  public TaskShuffleReadInfoEvent(
      int stageId,
      int shuffleId,
      long taskId,
      Map<String, ShuffleReadMetric> metrics,
      boolean isShuffleReadFailed,
      String failureReason) {
    this.stageId = stageId;
    this.shuffleId = shuffleId;
    this.taskId = taskId;
    this.metrics = metrics;
    this.isShuffleReadFailed = isShuffleReadFailed;
    this.failureReason = failureReason;
  }

  public int getStageId() {
    return stageId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public long getTaskId() {
    return taskId;
  }

  public Map<String, ShuffleReadMetric> getMetrics() {
    return metrics;
  }

  public boolean isShuffleReadFailed() {
    return isShuffleReadFailed;
  }

  public String getFailureReason() {
    return failureReason;
  }
}
