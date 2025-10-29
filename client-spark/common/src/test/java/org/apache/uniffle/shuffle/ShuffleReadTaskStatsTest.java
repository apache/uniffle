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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShuffleReadTaskStatsTest {

  @Test
  public void testReadStats() {
    ShuffleReadTaskStats readTaskStats = new ShuffleReadTaskStats();
    readTaskStats.incPartitionRecord(0, 1);
    readTaskStats.incPartitionRecord(0, 2);
    readTaskStats.incPartitionRecord(0, 3);
    readTaskStats.incPartitionRecord(0, 3);

    Map<Long, Long> records = readTaskStats.getPartitionRecords(0);
    assertEquals(1, records.get(1L));
    assertEquals(1, records.get(2L));
    assertEquals(2, records.get(3L));

    readTaskStats.incPartitionBlock(0, 1);
    readTaskStats.incPartitionBlock(0, 2);
    Map<Long, Long> blocks = readTaskStats.getPartitionBlocks(0);
    assertEquals(2, blocks.size());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testDiffWithInconsistentStats(boolean inconsistent) {
    ShuffleReadTaskStats readTaskStats = new ShuffleReadTaskStats();
    int partitionId = 0;
    long taskId = 10;
    long taskAttemptId = 1001L;
    int expectedRecords = 10;
    int expectedBlocks = 2;

    int readRecords = inconsistent ? expectedRecords + 1 : expectedRecords;
    // 10 records from 1 upstream tasks
    for (int i = 0; i < readRecords; i++) {
      readTaskStats.incPartitionRecord(partitionId, taskAttemptId);
    }
    // 2 blocks from 1 upstream tasks
    for (int i = 0; i < expectedBlocks; i++) {
      readTaskStats.incPartitionBlock(partitionId, taskAttemptId);
    }

    ShuffleWriteTaskStats writeStat = new ShuffleWriteTaskStats(1, taskAttemptId, taskId);
    for (int i = 0; i < expectedRecords; i++) {
      writeStat.incPartitionRecord(partitionId);
    }
    for (int i = 0; i < expectedBlocks; i++) {
      writeStat.incPartitionBlock(partitionId);
    }
    Map<Long, ShuffleWriteTaskStats> writeStats = new HashMap<>();
    writeStats.put(taskAttemptId, writeStat);
    boolean result = readTaskStats.diff(writeStats, 0, 1);
    assertEquals(!inconsistent, result);
  }
}
