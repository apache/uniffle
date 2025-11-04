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

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.apache.spark.shuffle.RssSparkConfig.RSS_DATA_INTEGRATION_VALIDATION_BLOCK_CHECK_ENABLED;

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
  protected long[] partitionRecordsWritten;
  private long[] partitionBlocksWritten;

  private final boolean blockCheckEnabled;

  public ShuffleWriteTaskStats(RssConf rssConf, int partitions, long taskAttemptId, long taskId) {
    this.partitionRecordsWritten = new long[partitions];
    this.partitionBlocksWritten = new long[partitions];
    this.taskAttemptId = taskAttemptId;
    this.taskId = taskId;

    Arrays.fill(this.partitionRecordsWritten, 0L);
    Arrays.fill(this.partitionBlocksWritten, 0L);

    this.blockCheckEnabled = rssConf.get(RSS_DATA_INTEGRATION_VALIDATION_BLOCK_CHECK_ENABLED);
  }

  public ShuffleWriteTaskStats(int partitions, long taskAttemptId, long taskId) {
    this(new RssConf(), partitions, taskAttemptId, taskId);
  }

  public long getRecordsWritten(int partitionId) {
    return partitionRecordsWritten[partitionId];
  }

  public boolean isBlockCheckEnabled() {
    return blockCheckEnabled;
  }

  public void incPartitionRecord(int partitionId) {
    partitionRecordsWritten[partitionId]++;
  }

  public void setPartitionRecordsWritten(int partitionId, long recordsWritten) {
    partitionRecordsWritten[partitionId] = recordsWritten;
  }

  public void incPartitionBlock(int partitionId) {
    partitionBlocksWritten[partitionId]++;
  }

  public void setPartitionBlocksWritten(int partitionId, long blocksWritten) {
    partitionBlocksWritten[partitionId] = blocksWritten;
  }

  public long getBlocksWritten(int partitionId) {
    return partitionBlocksWritten[partitionId];
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  private static void pack(ByteBuffer buffer, long[] array) {
    int partitionLength = array.length;
    int flagLength = (array.length + 7) / 8;

    // step1: zero skip flags
    byte[] zeroSkipFlags = new byte[flagLength];
    for (int i = 0; i < partitionLength; i++) {
      if (array[i] == 0) {
        int byteIdx = i / 8;
        int bitIdx = i % 8;
        zeroSkipFlags[byteIdx] |= (1 << bitIdx);
      }
    }
    buffer.put(zeroSkipFlags);

    // step2: variable int flags
    byte[] variableIntFlags = new byte[flagLength];
    for (int i = 0; i < partitionLength; i++) {
      if (array[i] > Integer.MAX_VALUE) {
        int byteIdx = i / 8;
        int bitIdx = i % 8;
        variableIntFlags[byteIdx] |= (1 << bitIdx);
      }
    }
    buffer.put(variableIntFlags);

    // step3: add
    for (long record : array) {
      if (record == 0L) {
        continue;
      }
      if (record <= Integer.MAX_VALUE) {
        buffer.putInt((int) record);
      } else {
        buffer.putLong(record);
      }
    }
  }

  private static void unpack(ByteBuffer buffer, long[] array) {
    int partitionLength = array.length;
    int flagLength = (partitionLength + 7) / 8;

    byte[] zeroSkipFlags = new byte[flagLength];
    buffer.get(zeroSkipFlags);

    byte[] varIntFlags = new byte[flagLength];
    buffer.get(varIntFlags);

    for (int i = 0; i < partitionLength; i++) {
      int byteIdx = i / 8;
      int bitIdx = i % 8;

      boolean isZero = (zeroSkipFlags[byteIdx] & (1 << bitIdx)) != 0;
      if (isZero) {
        array[i] = 0L;
        continue;
      }

      boolean isLong = (varIntFlags[byteIdx] & (1 << bitIdx)) != 0;
      if (isLong) {
        array[i] = buffer.getLong();
      } else {
        array[i] = buffer.getInt();
      }
    }
  }

  public String encode() {
    long start = System.currentTimeMillis();
    int partitions = partitionRecordsWritten.length;
    int flagBytes = (partitions + 7) / 8;

    // estimated max size to set the buffer capacity
    // taskId + taskAttemptId + partitionNumber
    int header = 2 * Long.BYTES + Integer.BYTES;
    // records flagBytes (zero skip + variable int) + real bytes
    int records = flagBytes * 2 + partitions * Long.BYTES;
    int blocks = blockCheckEnabled ? flagBytes + partitions * Long.BYTES : 0;
    ByteBuffer buffer = ByteBuffer.allocate(header + records + blocks);

    buffer.putLong(taskId);
    buffer.putLong(taskAttemptId);
    buffer.putInt(partitions);

    // 1. records
    pack(buffer, partitionRecordsWritten);

    // 2. blocks
    if (blockCheckEnabled) {
      pack(buffer, partitionBlocksWritten);
    }

    buffer.flip();
    byte[] finalBytes = new byte[buffer.remaining()];
    buffer.get(finalBytes);

    String encoded = new String(finalBytes, ISO_8859_1);
    LOGGER.info(
        "Encoded task stats with {} bytes in {} ms",
        encoded.length(),
        System.currentTimeMillis() - start);
    return encoded;
  }

  public static ShuffleWriteTaskStats decode(RssConf rssConf, String raw) {
    byte[] bytes = raw.getBytes(ISO_8859_1);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    long taskId = buffer.getLong();
    long taskAttemptId = buffer.getLong();
    int partitions = buffer.getInt();

    ShuffleWriteTaskStats stats =
        new ShuffleWriteTaskStats(rssConf, partitions, taskAttemptId, taskId);
    unpack(buffer, stats.partitionRecordsWritten);
    if (rssConf.get(RSS_DATA_INTEGRATION_VALIDATION_BLOCK_CHECK_ENABLED)) {
      unpack(buffer, stats.partitionBlocksWritten);
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
