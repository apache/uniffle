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

package org.apache.uniffle.storage.handler.impl;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.exception.RssException;

public abstract class PrefetchableClientReadHandler extends AbstractClientReadHandler {
  private static final Logger LOG = LoggerFactory.getLogger(PrefetchableClientReadHandler.class);

  private boolean prefetchEnabled;
  private int prefetchQueueCapacity;
  private int prefetchTimeoutSec;
  private LinkedBlockingQueue<Optional<ShuffleDataResult>> prefetchResultQueue;
  private ExecutorService prefetchExecutors;
  private AtomicBoolean abnormalFetchTag;
  private AtomicBoolean finishedTag;
  private AtomicInteger queueingNumber;
  private AtomicLong fetchTime;

  public PrefetchableClientReadHandler(Optional<PrefetchOption> prefetchOptional) {
    if (prefetchOptional.isPresent()) {
      PrefetchOption option = prefetchOptional.get();
      if (option.capacity <= 0) {
        throw new RssException("Illegal prefetch capacity: " + option.capacity);
      }
      LOG.info("Prefetch is enabled, capacity: {}", option.capacity);
      this.prefetchEnabled = true;
      this.prefetchQueueCapacity = option.capacity;
      this.prefetchTimeoutSec = option.timeoutSec;
      this.prefetchResultQueue = new LinkedBlockingQueue<>(option.capacity);
      // todo: support multi threads to prefetch
      this.prefetchExecutors = Executors.newFixedThreadPool(1);
      this.abnormalFetchTag = new AtomicBoolean(false);
      this.finishedTag = new AtomicBoolean(false);
      this.queueingNumber = new AtomicInteger(0);
      this.fetchTime = new AtomicLong(0);
    } else {
      this.prefetchEnabled = false;
    }
  }

  public static class PrefetchOption {
    private int capacity;
    private int timeoutSec;

    public PrefetchOption(int capacity, int timeoutSec) {
      this.capacity = capacity;
      this.timeoutSec = timeoutSec;
    }
  }

  protected abstract ShuffleDataResult doReadShuffleData();

  @Override
  public ShuffleDataResult readShuffleData() {
    if (!prefetchEnabled) {
      return doReadShuffleData();
    }

    int free = prefetchQueueCapacity - prefetchResultQueue.size() - queueingNumber.get();
    for (int i = 0; i < free; i++) {
      queueingNumber.incrementAndGet();
      prefetchExecutors.submit(
          () -> {
            long start = System.currentTimeMillis();
            try {
              if (abnormalFetchTag.get() || finishedTag.get()) {
                return;
              }
              ShuffleDataResult result = doReadShuffleData();
              if (result == null) {
                this.finishedTag.set(true);
              }
              prefetchResultQueue.offer(Optional.ofNullable(result));
            } catch (Exception e) {
              abnormalFetchTag.set(true);
              LOG.error("Errors on doing readShuffleData", e);
            } finally {
              queueingNumber.decrementAndGet();
              fetchTime.addAndGet(System.currentTimeMillis() - start);
            }
          });
    }

    long start = System.currentTimeMillis();
    while (true) {
      if (abnormalFetchTag.get()) {
        throw new RssException("Fast fail due to the fetch failure");
      }

      try {
        Optional<ShuffleDataResult> optionalShuffleDataResult =
            prefetchResultQueue.poll(10, TimeUnit.MILLISECONDS);
        if (optionalShuffleDataResult != null) {
          if (optionalShuffleDataResult.isPresent()) {
            return optionalShuffleDataResult.get();
          } else {
            return null;
          }
        }
      } catch (InterruptedException e) {
        return null;
      }

      if (System.currentTimeMillis() - start > prefetchTimeoutSec * 1000) {
        throw new RssException("Unexpected duration of reading shuffle data. Fast fail!");
      }
    }
  }

  @Override
  public void close() {
    super.close();
    if (prefetchExecutors != null) {
      prefetchExecutors.shutdown();
    }
  }

  @Override
  public void logConsumedBlockInfo() {
    LOG.info(
        "Metrics for shuffleId[{}], partitionId[{}], background fetch cost {} ms",
        shuffleId,
        partitionId,
        fetchTime);
    super.logConsumedBlockInfo();
  }
}
