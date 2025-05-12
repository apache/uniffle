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

package org.apache.uniffle.client.common;

import java.util.concurrent.atomic.AtomicLong;

public class ShuffleServerPushCost {
  private final String shuffleServerId;
  private final AtomicLong sentBytes;
  private final AtomicLong sentDurationMs;

  private final AtomicLong requireBufferFailureCounter;
  private final AtomicLong pushFailureCounter;

  private String lastPushFailureReason;

  public ShuffleServerPushCost(String shuffleServerId) {
    this.shuffleServerId = shuffleServerId;
    this.sentBytes = new AtomicLong();
    this.sentDurationMs = new AtomicLong();
    this.requireBufferFailureCounter = new AtomicLong();
    this.pushFailureCounter = new AtomicLong();
    this.lastPushFailureReason = null;
  }

  public void incRequiredBufferFailure(long delta) {
    this.requireBufferFailureCounter.addAndGet(delta);
  }

  public void incSentFailure(long delta, String failureReason) {
    this.pushFailureCounter.addAndGet(delta);
    this.lastPushFailureReason = failureReason;
  }

  public void incSentBytes(long bytes) {
    this.sentBytes.addAndGet(bytes);
  }

  public void incDurationMs(long duration) {
    this.sentDurationMs.addAndGet(duration);
  }

  public void merge(ShuffleServerPushCost cost) {
    if (!cost.shuffleServerId.equals(this.shuffleServerId)) {
      return;
    }

    this.incSentBytes(cost.sentBytes.get());
    this.incDurationMs(cost.sentDurationMs.get());
    this.incRequiredBufferFailure(cost.requireBufferFailureCounter.get());
    this.incSentFailure(cost.pushFailureCounter.get(), cost.lastPushFailureReason);
  }

  public long speed() {
    if (sentDurationMs.get() == 0) {
      return 0L;
    }
    return sentBytes.get() / sentDurationMs.get();
  }

  public long sentBytes() {
    return sentBytes.get();
  }

  public long sentDurationMillis() {
    return sentDurationMs.get();
  }

  public long requiredBufferFailureNumber() {
    return requireBufferFailureCounter.get();
  }

  public long pushFailureNumber() {
    return pushFailureCounter.get();
  }

  public String getLastPushFailureReason() {
    return lastPushFailureReason;
  }

  @Override
  public String toString() {
    return "ShuffleServerPushCost{"
        + "shuffleServerId='"
        + shuffleServerId
        + ", sentBytes="
        + sentBytes
        + ", sentDurationMs="
        + sentDurationMs
        + ", speed="
        + speed()
        + "}";
  }
}
