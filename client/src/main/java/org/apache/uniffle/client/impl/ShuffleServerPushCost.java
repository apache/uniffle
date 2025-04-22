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

package org.apache.uniffle.client.impl;

import java.util.concurrent.atomic.AtomicLong;

public class ShuffleServerPushCost {
  private final String shuffleServerId;
  private final AtomicLong sentBytes;
  private final AtomicLong sentDurationMs;

  public ShuffleServerPushCost(String shuffleServerId) {
    this.shuffleServerId = shuffleServerId;
    this.sentBytes = new AtomicLong();
    this.sentDurationMs = new AtomicLong();
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
