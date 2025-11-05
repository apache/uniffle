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

import org.apache.uniffle.common.compression.ZstdCodec;
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
  private final RssConf rssConf;

  public ShuffleWriteTaskStats(RssConf rssConf, int partitions, long taskAttemptId, long taskId) {
    this.partitionRecordsWritten = new long[partitions];
    this.partitionBlocksWritten = new long[partitions];
    this.taskAttemptId = taskAttemptId;
    this.taskId = taskId;

    Arrays.fill(this.partitionRecordsWritten, 0L);
    Arrays.fill(this.partitionBlocksWritten, 0L);

    this.rssConf = rssConf;
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
    int typeFlagLength = (partitionLength + 3) / 4;
    byte[] typeFlags = new byte[typeFlagLength];

    for (int i = 0; i < partitionLength; i++) {
      long value = array[i];
      int typeCode;
      if (value <= Short.MAX_VALUE) {
        typeCode = 0; // short
      } else if (value <= Integer.MAX_VALUE) {
        typeCode = 1; // int
      } else {
        typeCode = 2; // long
      }
      int byteIdx = i / 4;
      int bitShift = (i % 4) * 2;
      typeFlags[byteIdx] |= (typeCode << bitShift);
    }
    buffer.put(typeFlags);

    for (long value : array) {
      if (value <= Short.MAX_VALUE) {
        buffer.putShort((short) value);
      } else if (value <= Integer.MAX_VALUE) {
        buffer.putInt((int) value);
      } else {
        buffer.putLong(value);
      }
    }
  }

  private static void unpack(ByteBuffer buffer, long[] array) {
    int partitionLength = array.length;
    int typeFlagLength = (partitionLength + 3) / 4;
    byte[] typeFlags = new byte[typeFlagLength];
    buffer.get(typeFlags);

    for (int i = 0; i < partitionLength; i++) {
      int byteIdx = i / 4;
      int bitShift = (i % 4) * 2;
      int typeCode = (typeFlags[byteIdx] >> bitShift) & 0b11;

      switch (typeCode) {
        case 0:
          array[i] = buffer.getShort();
          break;
        case 1:
          array[i] = buffer.getInt();
          break;
        case 2:
          array[i] = buffer.getLong();
          break;
        default:
          throw new RssException("Unsupported type code: " + typeCode);
      }
    }
  }

  public String encode() {
    final long start = System.currentTimeMillis();
    int partitions = partitionRecordsWritten.length;
    int flagBytes = (partitions + 3) / 4;

    // estimated max size to set the buffer capacity
    // taskId + taskAttemptId + partitionNumber
    int header = 2 * Long.BYTES + Integer.BYTES;
    // records flagBytes + real bytes
    int records = flagBytes + partitions * Long.BYTES;
    int blocks = blockCheckEnabled ? flagBytes + partitions * Long.BYTES : 0;
    int capacity = header + records + blocks;
    ByteBuffer buffer = ByteBuffer.allocate(capacity);

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
    byte[] uncompressed = new byte[buffer.remaining()];
    buffer.get(uncompressed);
    // 1. Compress the uncompressed data
    byte[] compressed = ZstdCodec.getInstance(rssConf).compress(uncompressed);
    // 2. New ByteBuffer with 4 bytes for length + compressed.length
    ByteBuffer outBuffer = ByteBuffer.allocate(Integer.BYTES + compressed.length);
    // 3. Write compressed length
    outBuffer.putInt(uncompressed.length);
    // 4. Write compressed data
    outBuffer.put(compressed);
    String encoded = new String(outBuffer.array(), ISO_8859_1);
    LOGGER.info(
        "Encoded task stats for {} partitions with {} bytes (original: {} bytes) in {} ms",
        partitions,
        encoded.length(),
        capacity,
        System.currentTimeMillis() - start);
    return encoded;
  }

  public static ShuffleWriteTaskStats decode(RssConf rssConf, String raw) {
    byte[] bytes = raw.getBytes(ISO_8859_1);
    ByteBuffer source = ByteBuffer.wrap(bytes);
    int uncompressedLength = source.getInt();
    ByteBuffer buffer = ByteBuffer.allocate(uncompressedLength);
    ZstdCodec.getInstance(rssConf).decompress(source, uncompressedLength, buffer, 0);

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
