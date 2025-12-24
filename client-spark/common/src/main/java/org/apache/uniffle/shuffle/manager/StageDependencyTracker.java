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

import java.util.HashMap;
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

  // key: stageId, value: shuffleId
  private final Map<Integer, Integer> stageIdToShuffleId;

  // key: stageId, value: parent stageIds
  private final Map<Integer, Set<Integer>> dependencies;

  // key: stageId, value: reference count
  private final Map<Integer, Integer> stageIdToRefCount;

  private final ExecutorService cleanupExecutor;
  private final LinkedBlockingQueue<Integer> cleanupQueue;

  public StageDependencyTracker(Consumer<Integer> cleanupFunction) {
    this.stageIdToShuffleId = new HashMap<>();
    this.dependencies = new HashMap<>();
    this.stageIdToRefCount = new HashMap<>();

    this.cleanupQueue = new LinkedBlockingQueue<>();
    this.cleanupExecutor = Executors.newFixedThreadPool(1);
    cleanupExecutor.execute(
        () -> {
          while (true) {
            try {
              Integer stageId = cleanupQueue.take();
              Integer shuffleId = stageIdToShuffleId.get(stageId);
              if (shuffleId != null) {
                cleanupFunction.accept(shuffleId);
              } else {
                LOG.warn("Unable to find shuffleId for stageId {}!", stageId);
              }
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

  public synchronized void linkStageToShuffle(int stageId, int shuffleId) {
    try {
      stageIdToShuffleId.computeIfAbsent(
          stageId,
          x -> {
            LOG.info("Linked stageId-{} to shuffleId-{}", stageId, shuffleId);
            return shuffleId;
          });
    } catch (Exception e) {
      LOG.error("Unable to link stage {} to shuffle {}", stageId, shuffleId, e);
    }
  }

  public synchronized void addStageDependency(int stageId, Set<Integer> parentStageIds) {
    try {
      dependencies.put(stageId, parentStageIds);
      addRef(stageId);
      for (int parentStageId : parentStageIds) {
        addRef(parentStageId);
      }
      LOG.info("Add stage dependency for stage {} with parents {}", stageId, parentStageIds);
    } catch (Exception e) {
      LOG.error("Unable to add stage dependency for stage {}", stageId, e);
    }
  }

  public synchronized void removeStageDependency(int stageId) {
    try {
      removeRef(stageId);
      Set<Integer> parentStageIds = dependencies.remove(stageId);
      if (parentStageIds != null) {
        for (int parentStageId : parentStageIds) {
          removeRef(parentStageId);
        }
      }
      LOG.info("Removed stage dependency for stage {}", stageId);
    } catch (Exception e) {
      LOG.error("Unable to remove stage dependency for stage {}", stageId, e);
    }
  }

  private void addRef(int stageId) {
    stageIdToRefCount.compute(stageId, (k, v) -> v == null ? 1 : v + 1);
  }

  private void removeRef(int stageId) {
    int refCount = stageIdToRefCount.get(stageId) - 1;
    if (refCount <= 0) {
      // cleanup this stageId
      LOG.info("Removing stage dependency for stage {} due to 0 reference count", stageId);
      cleanupQueue.offer(stageId);
      stageIdToRefCount.remove(stageId);
    } else {
      stageIdToRefCount.put(stageId, refCount);
    }
  }
}
