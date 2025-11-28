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

package org.apache.uniffle.client.impl;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MergedPartitionStatsTest {

  private Map<Integer, Map<Long, Long>> generateRecords(
      int partitionId, long taskId, long records) {
    Map<Integer, Map<Long, Long>> recordsMap = new HashMap<>();
    recordsMap.computeIfAbsent(partitionId, x -> new HashMap<>()).put(taskId, records);
    return recordsMap;
  }

  @Test
  public void testMerge() {
    MergedPartitionStats stats = new MergedPartitionStats();

    // case1
    Map<Integer, Map<Long, Long>> r1 = generateRecords(1, 1, 100);
    stats.merge(r1);

    Map<Integer, Map<Long, Long>> r2 = generateRecords(1, 1, 200);
    stats.merge(r2);

    Roaring64NavigableMap taskIds = new Roaring64NavigableMap();
    taskIds.add(1);
    long records = stats.getExpectedRecordNumberByTaskIds(taskIds);
    assertEquals(300, records);

    // case2
    stats.merge(generateRecords(1, 2, 100));
    taskIds.add(2);
    assertEquals(400, stats.getExpectedRecordNumberByTaskIds(taskIds));

    // case3: filter out some taskIds
    taskIds.clear();
    taskIds.add(2);
    assertEquals(100, stats.getExpectedRecordNumberByTaskIds(taskIds));

    // case4: different partitionIds
    stats.merge(generateRecords(2, 3, 100));
    taskIds.clear();
    taskIds.add(1);
    taskIds.add(2);
    taskIds.add(3);
    assertEquals(500, stats.getExpectedRecordNumberByTaskIds(taskIds));
  }
}
