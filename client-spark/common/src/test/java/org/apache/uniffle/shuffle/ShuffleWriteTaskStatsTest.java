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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShuffleWriteTaskStatsTest {

  @Test
  public void testValidValidationInfo() {
    long taskId = 10;
    long taskAttemptId = 12345L;
    ShuffleWriteTaskStats stats = new ShuffleWriteTaskStats(2, taskAttemptId, taskId);
    stats.incPartitionRecord(0);
    stats.incPartitionRecord(1);

    stats.incPartitionBlock(0);
    stats.incPartitionBlock(1);

    String encoded = stats.encode();
    ShuffleWriteTaskStats decoded = ShuffleWriteTaskStats.decode(encoded);

    assertEquals(10, stats.getTaskId());

    assertEquals(taskAttemptId, decoded.getTaskAttemptId());
    assertEquals(1, decoded.getRecordsWritten(0));
    assertEquals(1, decoded.getRecordsWritten(1));

    assertEquals(1, decoded.getBlocksWritten(0));
    assertEquals(1, decoded.getBlocksWritten(1));
  }
}
