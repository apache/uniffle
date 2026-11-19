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

package org.apache.uniffle.shuffle.manager;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.JavaUtils;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_EAGER_SHUFFLE_DELETION_DELAYED_MINUTES;

/**
 * This class tracks the dependencies between stages. It maintains a mapping of stage IDs to their
 * parent stage IDs and reference counts to manage the lifecycle of stages and their associated
 * shuffle data. But for the shuffle-reuse scenario, we have to introduce the delayed deletion
 * mechanism to avoid the too-early deletion issue.
 *
 * <p>ATTENTION: This is still an experimental feature and may not cover all edge cases.
 */
public class StageDependencyTracker {
  private static final Logger LOG = LoggerFactory.getLogger(StageDependencyTracker.class);

  // key: stageId, value: shuffleId of writer
  private Map<Integer, Integer> stageIdToShuffleIdOfWriters = JavaUtils.newConcurrentMap();

  // key: shuffleId, value: stageIds of readers
  private Map<Integer, Set<Integer>> shuffleIdToStageIdsOfReaders = JavaUtils.newConcurrentMap();
  // reverse link by the stageId
  private Map<Integer, Set<Integer>> stageIdToShuffleIdOfReaders = JavaUtils.newConcurrentMap();

  private final ExecutorService deletionExecutor;
  private final DelayQueue<ShuffleDeletionItem> deletionDelayQueue = new DelayQueue<>();

  private long deletionDelayMs = -1L;

  // for the test cases
  private Set<Integer> cleanedShuffles = ConcurrentHashMap.newKeySet();

  public StageDependencyTracker(RssConf rssConf, Consumer<Integer> deletionFunc) {
    this.deletionExecutor = Executors.newFixedThreadPool(1);
    deletionExecutor.execute(
        () -> {
          while (true) {
            try {
              ShuffleDeletionItem item = deletionDelayQueue.take();
              int shuffleId = item.getShuffleId();
              // to check references again
              Set<Integer> readers = shuffleIdToStageIdsOfReaders.get(shuffleId);
              if (readers == null || readers.isEmpty()) {
                LOG.info("Deleting shuffle data for shuffleId: {}", shuffleId);
                deletionFunc.accept(shuffleId);
                cleanedShuffles.add(shuffleId);
                shuffleIdToStageIdsOfReaders.remove(shuffleId);
              } else {
                LOG.info("Skipping deletion for shuffleId: {} as it has new readers", shuffleId);
              }
            } catch (InterruptedException e) {
              LOG.info("Interrupted while waiting for deletion delay queue", e);
              Thread.currentThread().interrupt();
              break;
            } catch (Exception e) {
              LOG.error("Errors on deleting", e);
            }
          }
        });
    if (rssConf != null) {
      int delayMinutes = rssConf.get(RSS_EAGER_SHUFFLE_DELETION_DELAYED_MINUTES);
      if (delayMinutes > 0) {
        this.deletionDelayMs = delayMinutes * 60L * 1000L;
        LOG.info("Set deletion delay to {} ms", this.deletionDelayMs);
      }
    }
  }

  public StageDependencyTracker(Consumer<Integer> deletionFunc) {
    this(null, deletionFunc);
  }

  public int getShuffleIdByStageIdOfWriter(int stageId) {
    Integer shuffleId = stageIdToShuffleIdOfWriters.get(stageId);
    if (shuffleId == null) {
      // ignore this.
      return -1;
    }
    return shuffleId;
  }

  public void linkWriter(int shuffleId, int writerStageId) {
    stageIdToShuffleIdOfWriters.put(writerStageId, shuffleId);
  }

  public void linkReader(int shuffleId, int readerStageId) {
    shuffleIdToStageIdsOfReaders
        .computeIfAbsent(shuffleId, k -> ConcurrentHashMap.newKeySet())
        .add(readerStageId);
    stageIdToShuffleIdOfReaders
        .computeIfAbsent(readerStageId, k -> ConcurrentHashMap.newKeySet())
        .add(shuffleId);
  }

  public void removeStage(int stageId) {
    Set<Integer> allUpstreamShuffleIdsOfRead = stageIdToShuffleIdOfReaders.get(stageId);
    if (allUpstreamShuffleIdsOfRead != null) {
      for (int shuffleId : allUpstreamShuffleIdsOfRead) {
        Set<Integer> readers = shuffleIdToStageIdsOfReaders.get(shuffleId);
        if (readers != null) {
          readers.remove(stageId);
          if (readers.isEmpty()) {
            // add into the delayed deletion queue
            deletionDelayQueue.offer(new ShuffleDeletionItem(shuffleId, deletionDelayMs));
          }
        }
      }
    }
  }

  public int getActiveShuffleCount() {
    return shuffleIdToStageIdsOfReaders.size();
  }

  public int getCleanedShuffleCount() {
    return cleanedShuffles.size();
  }
}
