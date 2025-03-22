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

import java.util.Collection;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.storage.handler.api.ShuffleWriteHandler;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;

/**
 * The {@link PooledHadoopShuffleWriteHandler} is a wrapper of underlying multiple {@link
 * HadoopShuffleWriteHandler} to support concurrency control of writing single partition to multi
 * files.
 *
 * <p>By leveraging {@link LinkedBlockingDeque}, it will always write the same file when no race
 * condition, which is good for reducing file numbers for Hadoop FS.
 */
public class PooledHadoopShuffleWriteHandler implements ShuffleWriteHandler {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PooledHadoopShuffleWriteHandler.class);

  private final LinkedBlockingDeque<ShuffleWriteHandler> queue;
  private final int maxConcurrency;
  private final String basePath;
  private Function<Integer, ShuffleWriteHandler> createWriterFunc;
  private AtomicInteger initializedHandlerCntRef = new AtomicInteger(0);

  // Only for tests
  @VisibleForTesting
  public PooledHadoopShuffleWriteHandler(LinkedBlockingDeque<ShuffleWriteHandler> queue) {
    this.queue = queue;
    this.maxConcurrency = queue.size();
    this.basePath = StringUtils.EMPTY;
  }

  @VisibleForTesting
  public PooledHadoopShuffleWriteHandler(
      LinkedBlockingDeque<ShuffleWriteHandler> queue,
      int maxConcurrency,
      Function<Integer, ShuffleWriteHandler> createWriterFunc) {
    this.queue = queue;
    this.maxConcurrency = maxConcurrency;
    this.basePath = StringUtils.EMPTY;
    this.createWriterFunc = createWriterFunc;
  }

  public PooledHadoopShuffleWriteHandler(
      RssBaseConf rssBaseConf,
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      String storageBasePath,
      String fileNamePrefix,
      Configuration hadoopConf,
      String user,
      int concurrency) {
    this.maxConcurrency = concurrency;
    this.queue = new LinkedBlockingDeque<>(maxConcurrency);
    this.basePath =
        ShuffleStorageUtils.getFullShuffleDataFolder(
            storageBasePath,
            ShuffleStorageUtils.getShuffleDataPath(appId, shuffleId, startPartition, endPartition));

    this.createWriterFunc =
        index -> {
          try {
            return new HadoopShuffleWriteHandler(
                rssBaseConf,
                appId,
                shuffleId,
                startPartition,
                endPartition,
                storageBasePath,
                fileNamePrefix + "_" + index,
                hadoopConf,
                user);
          } catch (Exception e) {
            throw new RssException("Errors on initializing Hadoop FS writer handler.", e);
          }
        };
  }

  @Override
  public void write(Collection<ShufflePartitionedBlock> shuffleBlocks) throws Exception {
    if (queue.isEmpty() && initializedHandlerCntRef.get() < maxConcurrency) {
      synchronized (this) {
        if (initializedHandlerCntRef.get() < maxConcurrency) {
          queue.add(createWriterFunc.apply(initializedHandlerCntRef.get()));
          initializedHandlerCntRef.addAndGet(1);
        }
      }
    }

    if (queue.isEmpty()) {
      LOGGER.warn("No free Hadoop FS writer handler, it will wait. storage path: {}", basePath);
    }
    ShuffleWriteHandler writeHandler = queue.take();
    try {
      writeHandler.write(shuffleBlocks);
    } finally {
      // Use addFirst() here because we are sure the capacity will not be exceeded.
      // Note: addFirst() throws IllegalStateException when queue is full.
      queue.addFirst(writeHandler);
    }
  }

  @VisibleForTesting
  protected int getInitializedHandlerCnt() {
    return initializedHandlerCntRef.get();
  }
}
