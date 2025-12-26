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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tracks the dependencies between stages. It maintains a mapping of stage IDs to their
 * parent stage IDs and reference counts to manage the lifecycle of stages and their associated
 * shuffle data.
 */
public class StageDependencyTracker {
  private static final Logger LOG = LoggerFactory.getLogger(StageDependencyTracker.class);

  // key: stageId, value: shuffleId of writer
  private Map<Integer, Integer> stageIdToShuffleIdOfWriters = new HashMap<>();

  // key: shuffleId, value: stageIds of readers
  private Map<Integer, Set<Integer>> shuffleIdToStageIdsOfReaders = new HashMap<>();
  // reverse link by the stageId
  private Map<Integer, Set<Integer>> stageIdToShuffleIdOfReaders = new HashMap<>();

  private final ExecutorService cleanupExecutor;
  private final LinkedBlockingQueue<Integer> cleanupQueue;

  // for the test cases
  private List<Integer> cleanedShuffles = new ArrayList<>();

  public StageDependencyTracker(Consumer<Integer> cleanupFunction) {
    this.cleanupQueue = new LinkedBlockingQueue<>();
    this.cleanupExecutor = Executors.newFixedThreadPool(1);
    cleanupExecutor.execute(
        () -> {
          while (true) {
            try {
              int shuffleId = cleanupQueue.take();
              LOG.info("Cleaning the shuffle data for shuffleId: {}", shuffleId);
              cleanedShuffles.add(shuffleId);
              cleanupFunction.accept(shuffleId);
            } catch (InterruptedException e) {
              LOG.error("Cleanup executor interrupted", e);
              Thread.currentThread().interrupt();
              break;
            } catch (Exception e) {
              LOG.error("Error during cleanup", e);
            }
          }
        });
  }

  public synchronized int getShuffleIdByStageIdOfWriter(int stageId) {
    Integer shuffleId = stageIdToShuffleIdOfWriters.get(stageId);
    if (shuffleId == null) {
      // ignore this.
      return -1;
    }
    return shuffleId;
  }

  public synchronized void linkWriter(int shuffleId, int writerStageId) {
    stageIdToShuffleIdOfWriters.put(writerStageId, shuffleId);
  }

  public synchronized void linkReader(int shuffleId, int readerStageId) {
    shuffleIdToStageIdsOfReaders
        .computeIfAbsent(shuffleId, k -> new HashSet<>())
        .add(readerStageId);
    stageIdToShuffleIdOfReaders.computeIfAbsent(readerStageId, k -> new HashSet<>()).add(shuffleId);
  }

  public synchronized void removeStage(int stageId) {
    Set<Integer> allUpstreamShuffleIdsOfRead = stageIdToShuffleIdOfReaders.get(stageId);
    if (allUpstreamShuffleIdsOfRead != null) {
      for (int shuffleId : allUpstreamShuffleIdsOfRead) {
        Set<Integer> readers = shuffleIdToStageIdsOfReaders.get(shuffleId);
        if (readers != null) {
          readers.remove(stageId);
          if (readers.isEmpty()) {
            // for this shuffle, all readers have been removed
            cleanupQueue.offer(shuffleId);
            shuffleIdToStageIdsOfReaders.remove(shuffleId);
          }
        }
      }
    }
  }

  public synchronized int getActiveShuffleCount() {
    return shuffleIdToStageIdsOfReaders.size();
  }

  public int getCleanedShuffleCount() {
    return cleanedShuffles.size();
  }
}
