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

package org.apache.spark.shuffle.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.apache.uniffle.common.ShuffleBlockInfo;

public class AddBlockEvent {

  private Long eventId;
  private String taskId;
  private int stageAttemptNumber;
  private List<ShuffleBlockInfo> shuffleDataInfoList;
  private List<Runnable> processedCallbackChain;

  private Consumer<Future> prepare;

  public AddBlockEvent(String taskId, List<ShuffleBlockInfo> shuffleDataInfoList) {
    this(-1L, taskId, 0, shuffleDataInfoList);
  }

  public AddBlockEvent(
      Long eventId,
      String taskId,
      int stageAttemptNumber,
      List<ShuffleBlockInfo> shuffleDataInfoList) {
    this.eventId = eventId;
    this.taskId = taskId;
    this.stageAttemptNumber = stageAttemptNumber;
    this.shuffleDataInfoList = shuffleDataInfoList;
    this.processedCallbackChain = new ArrayList<>();
  }

  /** @param callback, should not throw any exception and execute fast. */
  public void addCallback(Runnable callback) {
    processedCallbackChain.add(callback);
  }

  public void addPrepare(Consumer<Future> prepare) {
    this.prepare = prepare;
  }

  public void doPrepare(Future future) {
    if (prepare != null) {
      prepare.accept(future);
    }
  }

  public String getTaskId() {
    return taskId;
  }

  public Long getEventId() {
    return eventId;
  }

  public int getStageAttemptNumber() {
    return stageAttemptNumber;
  }

  public List<ShuffleBlockInfo> getShuffleDataInfoList() {
    return shuffleDataInfoList;
  }

  public List<Runnable> getProcessedCallbackChain() {
    return processedCallbackChain;
  }

  @Override
  public String toString() {
    return "AddBlockEvent: TaskId[" + taskId + "], " + shuffleDataInfoList;
  }
}
