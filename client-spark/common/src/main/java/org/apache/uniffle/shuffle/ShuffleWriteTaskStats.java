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

import java.nio.ByteBuffer;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

/**
 * ShuffleWriteTaskStats stores statistics for a shuffle write task attempt, including the task
 * attempt ID and the number of records written for each partition.
 */
public class ShuffleWriteTaskStats {
  private long taskAttemptId;
  private long[] partitionRecordsWritten;
  private long[] partitionBlocksWritten;

  public ShuffleWriteTaskStats(int partitions, long taskAttemptId) {
    this.partitionRecordsWritten = new long[partitions];
    this.partitionBlocksWritten = new long[partitions];
    this.taskAttemptId = taskAttemptId;

    Arrays.fill(this.partitionRecordsWritten, 0L);
    Arrays.fill(this.partitionBlocksWritten, 0L);
  }

  public long getRecordsWritten(int partitionId) {
    return partitionRecordsWritten[partitionId];
  }

  public void incPartitionRecord(int partitionId) {
    partitionRecordsWritten[partitionId]++;
  }

  public void incPartitionBlock(int partitionId) {
    partitionBlocksWritten[partitionId]++;
  }

  public long getBlocksWritten(int partitionId) {
    return partitionBlocksWritten[partitionId];
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  public String encode() {
    int partitions = partitionRecordsWritten.length;
    ByteBuffer buffer =
        ByteBuffer.allocate(Long.BYTES + Integer.BYTES + partitions * Long.BYTES * 2);
    buffer.putLong(taskAttemptId);
    buffer.putInt(partitions);
    for (long records : partitionRecordsWritten) {
      buffer.putLong(records);
    }
    for (long blocks : partitionBlocksWritten) {
      buffer.putLong(blocks);
    }
    return new String(buffer.array(), ISO_8859_1);
  }

  public static ShuffleWriteTaskStats decode(String raw) {
    byte[] bytes = raw.getBytes(ISO_8859_1);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    long taskAttemptId = buffer.getLong();
    int partitions = buffer.getInt();
    ShuffleWriteTaskStats stats = new ShuffleWriteTaskStats(partitions, taskAttemptId);
    for (int i = 0; i < partitions; i++) {
      stats.partitionRecordsWritten[i] = buffer.getLong();
    }
    for (int i = 0; i < partitions; i++) {
      stats.partitionBlocksWritten[i] = buffer.getLong();
    }
    return stats;
  }
}
