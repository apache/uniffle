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

package org.apache.uniffle.server;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.server.buffer.ShuffleBuffer;
import org.apache.uniffle.server.storage.StorageManager;

public class ShuffleDataFlushEvent {

  private final long eventId;
  private final String appId;
  private final int shuffleId;
  private final int startPartition;
  private final int endPartition;
  private final long size;
  private final List<ShufflePartitionedBlock> shuffleBlocks;
  private final Supplier<Boolean> valid;
  private final ShuffleBuffer shuffleBuffer;
  private final AtomicInteger retryTimes = new AtomicInteger();
  private StorageManager storageManager;

  public ShuffleDataFlushEvent(
      long eventId,
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      long size,
      List<ShufflePartitionedBlock> shuffleBlocks,
      Supplier<Boolean> valid,
      ShuffleBuffer shuffleBuffer) {
    this.eventId = eventId;
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.startPartition = startPartition;
    this.endPartition = endPartition;
    this.size = size;
    this.shuffleBlocks = shuffleBlocks;
    this.valid = valid;
    this.shuffleBuffer = shuffleBuffer;
  }

  public List<ShufflePartitionedBlock> getShuffleBlocks() {
    return shuffleBlocks;
  }

  public long getEventId() {
    return eventId;
  }

  public long getSize() {
    return size;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getStartPartition() {
    return startPartition;
  }

  public int getEndPartition() {
    return endPartition;
  }

  public ShuffleBuffer getShuffleBuffer() {
    return shuffleBuffer;
  }

  public StorageManager getStorageManager() {
    return storageManager;
  }

  public void setStorageManager(StorageManager storageManager) {
    this.storageManager = storageManager;
  }

  public boolean isValid() {
    if (valid == null) {
      return true;
    }
    return valid.get();
  }

  public int getRetryTimes() {
    return retryTimes.get();
  }

  public void increaseRetryTimes() {
    retryTimes.incrementAndGet();
  }

  @Override
  public String toString() {
    return "ShuffleDataFlushEvent: eventId=" + eventId
        + ", appId=" + appId
        + ", shuffleId=" + shuffleId
        + ", startPartition=" + startPartition
        + ", endPartition=" + endPartition;
  }
}
