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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

public class StageDependencyTrackerTest {

  @Test
  public void basicTest() throws InterruptedException {
    List<Integer> cleaned = new ArrayList<>();
    Consumer<Integer> cleanupFunction = cleaned::add;

    StageDependencyTracker tracker = new StageDependencyTracker(cleanupFunction);

    // Link parent stage 1 to shuffle 100
    tracker.linkStageToShuffle(1, 100);
    tracker.linkStageToShuffle(2, 200);

    // add 1
    tracker.addStageDependency(1, Collections.emptySet());

    // Child stage 2 depends on parent 1
    Set<Integer> parentStageIds = Collections.singleton(1);
    tracker.addStageDependency(2, parentStageIds);

    // Removing the dependency should schedule cleanup for parent 1's shuffle (100)
    tracker.removeStageDependency(2);
    tracker.removeStageDependency(1);

    Awaitility.await().timeout(1, TimeUnit.SECONDS).until(() -> cleaned.size() == 2);
  }
}
