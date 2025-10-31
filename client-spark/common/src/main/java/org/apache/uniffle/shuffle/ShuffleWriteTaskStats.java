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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.RssException;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

/**
 * ShuffleWriteTaskStats stores statistics for a shuffle write task attempt, including the task
 * attempt ID and the number of records written for each partition.
 */
public class ShuffleWriteTaskStats {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleWriteTaskStats.class);

  // the unique task id across all stages
  private long taskId;
  // this is only unique for one stage and defined in uniffle side instead of spark
  private long taskAttemptId;
  private long[] partitionRecordsWritten;
  private long[] partitionBlocksWritten;

  public ShuffleWriteTaskStats(int partitions, long taskAttemptId, long taskId) {
    this.partitionRecordsWritten = new long[partitions];
    this.partitionBlocksWritten = new long[partitions];
    this.taskAttemptId = taskAttemptId;
    this.taskId = taskId;

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
        ByteBuffer.allocate(2 * Long.BYTES + Integer.BYTES + partitions * Long.BYTES * 2);
    buffer.putLong(taskId);
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
    long taskId = buffer.getLong();
    long taskAttemptId = buffer.getLong();
    int partitions = buffer.getInt();
    ShuffleWriteTaskStats stats = new ShuffleWriteTaskStats(partitions, taskAttemptId, taskId);
    for (int i = 0; i < partitions; i++) {
      stats.partitionRecordsWritten[i] = buffer.getLong();
    }
    for (int i = 0; i < partitions; i++) {
      stats.partitionBlocksWritten[i] = buffer.getLong();
    }
    return stats;
  }

  public long getTaskId() {
    return taskId;
  }

  public void log() {
    StringBuilder infoBuilder = new StringBuilder();
    int partitions = partitionRecordsWritten.length;
    for (int i = 0; i < partitions; i++) {
      long records = partitionRecordsWritten[i];
      long blocks = partitionBlocksWritten[i];
      infoBuilder.append(i).append("/").append(records).append("/").append(blocks).append(",");
    }
    LOGGER.info(
        "Partition records/blocks written for taskId[{}]: {}", taskId, infoBuilder.toString());
  }

  public void check(long[] partitionLens) {
    int partitions = partitionRecordsWritten.length;
    for (int idx = 0; idx < partitions; idx++) {
      long records = partitionRecordsWritten[idx];
      long blocks = partitionBlocksWritten[idx];
      long length = partitionLens[idx];
      if (records > 0) {
        if (blocks <= 0 || length <= 0) {
          throw new RssException(
              "Illegal partition:"
                  + idx
                  + " stats. records/blocks/length: "
                  + records
                  + "/"
                  + blocks
                  + "/"
                  + length);
        }
      }
    }
  }
}
