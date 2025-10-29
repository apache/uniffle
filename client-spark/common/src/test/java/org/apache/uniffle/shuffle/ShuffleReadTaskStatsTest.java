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

package org.apache.uniffle.shuffle;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShuffleReadTaskStatsTest {

  @Test
  public void testReadStats() {
    ShuffleReadTaskStats readTaskStats = new ShuffleReadTaskStats();
    readTaskStats.incPartitionRecord(0, 1);
    readTaskStats.incPartitionRecord(0, 2);
    readTaskStats.incPartitionRecord(0, 3);
    readTaskStats.incPartitionRecord(0, 3);

    java.util.Iterator<Pair<Long, Long>> iter = readTaskStats.get(0);
    Map<Long, Long> records = getRecords(iter);
    assertEquals(1, records.get(1L));
    assertEquals(1, records.get(2L));
    assertEquals(2, records.get(3L));
  }

  private Map<Long, Long> getRecords(java.util.Iterator<Pair<Long, Long>> iter) {
    Map<Long, Long> records = new HashMap<>();
    while (iter.hasNext()) {
      Pair<Long, Long> pair = iter.next();
      records.compute(pair.getLeft(), (k, v) -> v == null ? pair.getRight() : v + 1);
    }
    return records;
  }
}
