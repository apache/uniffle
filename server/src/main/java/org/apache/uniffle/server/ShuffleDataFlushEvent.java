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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.server.buffer.ShuffleBuffer;
import org.apache.uniffle.storage.common.Storage;

public class ShuffleDataFlushEvent {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleDataFlushEvent.class);

  private final long eventId;
  private final String appId;
  private final int shuffleId;
  private final int startPartition;
  private final int endPartition;
  /** The memory cost size include encoded length */
  private final long encodedLength;
  /** The data size of this shuffle block */
  private final long dataLength;

  private final Collection<ShufflePartitionedBlock> shuffleBlocks;
  private final Supplier<Boolean> valid;
  private final ShuffleBuffer shuffleBuffer;
  private final AtomicInteger retryTimes = new AtomicInteger();

  private boolean isPended = false;
  private Storage underStorage;
  private final List<Runnable> cleanupCallbackChains;

  private boolean ownedByHugePartition = false;
  private long startPendingTime;

  @VisibleForTesting
  public ShuffleDataFlushEvent(
      long eventId,
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      long encodedLength,
      Collection<ShufflePartitionedBlock> shuffleBlocks,
      Supplier<Boolean> valid,
      ShuffleBuffer shuffleBuffer) {
    this(
        eventId,
        appId,
        shuffleId,
        startPartition,
        endPartition,
        encodedLength,
        encodedLength,
        shuffleBlocks,
        valid,
        shuffleBuffer);
  }

  public ShuffleDataFlushEvent(
      long eventId,
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      long encodedLength,
      long dataLength,
      Collection<ShufflePartitionedBlock> shuffleBlocks,
      Supplier<Boolean> valid,
      ShuffleBuffer shuffleBuffer) {
    this.eventId = eventId;
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.startPartition = startPartition;
    this.endPartition = endPartition;
    this.encodedLength = encodedLength;
    this.shuffleBlocks = shuffleBlocks;
    this.valid = valid;
    this.shuffleBuffer = shuffleBuffer;
    this.cleanupCallbackChains = new ArrayList<>();
    this.dataLength = dataLength;
  }

  public Collection<ShufflePartitionedBlock> getShuffleBlocks() {
    return shuffleBlocks;
  }

  public long getEventId() {
    return eventId;
  }

  public long getEncodedLength() {
    return encodedLength;
  }

  public long getDataLength() {
    return dataLength;
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

  public boolean isPended() {
    return isPended;
  }

  public void markPended() {
    isPended = true;
    startPendingTime = System.currentTimeMillis();
  }

  public Storage getUnderStorage() {
    return underStorage;
  }

  public void setUnderStorage(Storage underStorage) {
    this.underStorage = underStorage;
  }

  public boolean doCleanup() {
    boolean ret = true;
    for (Runnable cleanupCallback : cleanupCallbackChains) {
      try {
        cleanupCallback.run();
      } catch (Exception e) {
        ret = false;
        LOGGER.error("Errors doing cleanup callback. event: {}", this, e);
      }
    }
    return ret;
  }

  public void addCleanupCallback(Runnable cleanupCallback) {
    if (cleanupCallback != null) {
      cleanupCallbackChains.add(cleanupCallback);
    }
  }

  @Override
  public String toString() {
    return "ShuffleDataFlushEvent: eventId="
        + eventId
        + ", appId="
        + appId
        + ", shuffleId="
        + shuffleId
        + ", startPartition="
        + startPartition
        + ", endPartition="
        + endPartition
        + ", retryTimes="
        + retryTimes
        + ", underStorage="
        + (underStorage == null ? null : underStorage.getClass().getSimpleName())
        + ", isPended="
        + isPended
        + ", ownedByHugePartition="
        + ownedByHugePartition;
  }

  public boolean isOwnedByHugePartition() {
    return ownedByHugePartition;
  }

  public void markOwnedByHugePartition() {
    this.ownedByHugePartition = true;
  }

  public long getStartPendingTime() {
    return startPendingTime;
  }
}
