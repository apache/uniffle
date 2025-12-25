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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Sets;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StageDependencyTrackerTest {

  @Test
  public void testSingleReaderCleanupTriggered() throws Exception {
    AtomicInteger cleanedShuffleId = new AtomicInteger(-1);

    StageDependencyTracker tracker =
        new StageDependencyTracker(shuffleId -> cleanedShuffleId.set(shuffleId));

    int shuffleId = 1;
    int writerStageId = 100;
    int readerStageId = 200;

    tracker.linkWriter(shuffleId, writerStageId);
    tracker.linkReader(shuffleId, readerStageId);

    tracker.removeStage(readerStageId);

    Awaitility.await()
        .timeout(1, TimeUnit.SECONDS)
        .until(() -> cleanedShuffleId.get() == shuffleId);
  }

  @Test
  public void testMultipleReadersCleanupAfterLastRemoved() throws Exception {
    AtomicInteger cleanupCount = new AtomicInteger(0);

    StageDependencyTracker tracker =
        new StageDependencyTracker(shuffleId -> cleanupCount.incrementAndGet());

    int shuffleId = 2;
    int writerStageId = 101;

    int reader1 = 201;
    int reader2 = 202;

    tracker.linkWriter(shuffleId, writerStageId);
    tracker.linkReader(shuffleId, reader1);
    tracker.linkReader(shuffleId, reader2);

    tracker.removeStage(reader1);

    // Should not cleanup yet
    Thread.sleep(200);
    Assertions.assertEquals(0, cleanupCount.get());

    tracker.removeStage(reader2);

    Awaitility.await().timeout(1, TimeUnit.SECONDS).until(() -> cleanupCount.get() == 1);
  }

  @Test
  public void testRemoveUnknownStageDoesNothing() throws Exception {
    AtomicInteger cleanupCount = new AtomicInteger(0);

    StageDependencyTracker tracker =
        new StageDependencyTracker(shuffleId -> cleanupCount.incrementAndGet());

    tracker.removeStage(999);

    // Give async thread a bit of time
    Thread.sleep(200);

    Assertions.assertEquals(0, cleanupCount.get());
  }

  @Test
  public void testMultipleShufflesIndependentCleanup() throws Exception {
    Set<Integer> cleaned = Collections.synchronizedSet(new HashSet<>());

    StageDependencyTracker tracker =
        new StageDependencyTracker(shuffleId -> cleaned.add(shuffleId));

    tracker.linkWriter(1, 10);
    tracker.linkWriter(2, 20);

    tracker.linkReader(1, 101);
    tracker.linkReader(2, 201);

    tracker.removeStage(101);
    tracker.removeStage(201);

    Awaitility.await()
        .timeout(1, TimeUnit.SECONDS)
        .until(() -> cleaned.equals(Sets.newHashSet(1, 2)));
  }
}
