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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.shuffle.RssSparkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.impl.FailedBlockSendTracker;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.ThreadUtils;

/**
 * The extension of {@link DataPusher} is used only when the overlapping compression is activated.
 */
public class OverlappingCompressionDataPusher extends DataPusher {
  private static final Logger LOG = LoggerFactory.getLogger(OverlappingCompressionDataPusher.class);

  private final ExecutorService compressionThreadPool;

  public OverlappingCompressionDataPusher(
      ShuffleWriteClient shuffleWriteClient,
      Map<String, Set<Long>> taskToSuccessBlockIds,
      Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker,
      Set<String> failedTaskIds,
      int threadPoolSize,
      int threadKeepAliveTime,
      int compressionThreads) {
    super(
        shuffleWriteClient,
        taskToSuccessBlockIds,
        taskToFailedBlockSendTracker,
        failedTaskIds,
        threadPoolSize,
        threadKeepAliveTime);
    if (compressionThreads <= 0) {
      throw new RssException(
          "Invalid rss configuration of "
              + RssSparkConfig.RSS_WRITE_OVERLAPPING_COMPRESSION_THREADS_PER_VCORE.key()
              + ": "
              + compressionThreads);
    }
    this.compressionThreadPool =
        Executors.newFixedThreadPool(
            compressionThreads, ThreadUtils.getThreadFactory("compression-thread"));
  }

  @Override
  public CompletableFuture<Long> send(AddBlockEvent event) {
    // Step 1: process event data in a separate thread (e.g., trigger compression)
    return CompletableFuture.supplyAsync(
            () -> {
              event.getShuffleDataInfoList().forEach(shuffleDataInfo -> shuffleDataInfo.getData());
              return event;
            },
            compressionThreadPool)
        .thenCompose(
            processedEvent -> {
              // Step 2: forward to the parent class's send method
              return super.send(processedEvent);
            });
  }
}
