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
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.apache.spark.shuffle.RssSparkConfig.RSS_CLIENT_INTEGRITY_VALIDATION_STATS_COMPRESSION_TYPE;
import static org.apache.spark.shuffle.RssSparkConfig.RSS_DATA_INTEGRITY_VALIDATION_BLOCK_NUMBER_CHECK_ENABLED;

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
  private int partitions;
  private long[] partitionRecordsWritten;
  private long[] partitionBlocksWritten;
  private boolean blockNumberCheckEnabled;
  private RssConf rssConf;

  public ShuffleWriteTaskStats(RssConf rssConf, int partitions, long taskAttemptId, long taskId) {
    this.partitionRecordsWritten = new long[partitions];
    Arrays.fill(this.partitionRecordsWritten, 0L);

    this.partitions = partitions;
    this.taskAttemptId = taskAttemptId;
    this.taskId = taskId;
    this.rssConf = rssConf;
    this.blockNumberCheckEnabled =
        rssConf.get(RSS_DATA_INTEGRITY_VALIDATION_BLOCK_NUMBER_CHECK_ENABLED);

    if (blockNumberCheckEnabled) {
      this.partitionBlocksWritten = new long[partitions];
      Arrays.fill(this.partitionBlocksWritten, 0L);
    }
  }

  public ShuffleWriteTaskStats(int partitions, long taskAttemptId, long taskId) {
    this(new RssConf(), partitions, taskAttemptId, taskId);
  }

  public long getRecordsWritten(int partitionId) {
    return partitionRecordsWritten[partitionId];
  }

  public void incPartitionRecord(int partitionId) {
    partitionRecordsWritten[partitionId]++;
  }

  public void incPartitionBlock(int partitionId) {
    if (blockNumberCheckEnabled) {
      partitionBlocksWritten[partitionId]++;
    }
  }

  public long getBlocksWritten(int partitionId) {
    if (blockNumberCheckEnabled) {
      return partitionBlocksWritten[partitionId];
    }
    return -1L;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  public String encode() {
    final long start = System.currentTimeMillis();
    int partitions = partitionRecordsWritten.length;
    int capacity = 2 * Long.BYTES + Integer.BYTES + partitions * Long.BYTES;
    if (blockNumberCheckEnabled) {
      capacity += partitions * Long.BYTES;
    }
    ByteBuffer buffer = ByteBuffer.allocate(capacity);
    buffer.putLong(taskId);
    buffer.putLong(taskAttemptId);
    buffer.putInt(partitions);
    for (long records : partitionRecordsWritten) {
      buffer.putLong(records);
    }
    if (blockNumberCheckEnabled) {
      for (long blocks : partitionBlocksWritten) {
        buffer.putLong(blocks);
      }
    }
    Optional<Codec> optionalCodec = getCodec(rssConf);
    if (optionalCodec.isPresent()) {
      Codec codec = optionalCodec.get();
      byte[] compressed = codec.compress(buffer.array());
      ByteBuffer compositedBuffer = ByteBuffer.allocate(Integer.BYTES + compressed.length);
      compositedBuffer.putInt(capacity);
      compositedBuffer.put(compressed);
      LOGGER.info(
          "Encoded task stats for {} partitions with {} bytes (original: {} bytes) in {} ms",
          partitions,
          compositedBuffer.capacity(),
          capacity,
          System.currentTimeMillis() - start);
      return new String(compositedBuffer.array(), ISO_8859_1);
    } else {
      return new String(buffer.array(), ISO_8859_1);
    }
  }

  private static Optional<Codec> getCodec(RssConf rssConf) {
    return Codec.newInstance(
        rssConf.get(RSS_CLIENT_INTEGRITY_VALIDATION_STATS_COMPRESSION_TYPE), rssConf);
  }

  public static ShuffleWriteTaskStats decode(RssConf rssConf, String raw) {
    byte[] rawBytes = raw.getBytes(ISO_8859_1);
    ByteBuffer outBuffer = ByteBuffer.wrap(rawBytes);

    Optional<Codec> optionalCodec = getCodec(rssConf);
    if (optionalCodec.isPresent()) {
      ByteBuffer inBuffer = ByteBuffer.wrap(rawBytes);
      int capacity = inBuffer.getInt();
      outBuffer = ByteBuffer.allocate(capacity);
      optionalCodec.get().decompress(inBuffer, capacity, outBuffer, 0);
    }

    long taskId = outBuffer.getLong();
    long taskAttemptId = outBuffer.getLong();
    int partitions = outBuffer.getInt();
    ShuffleWriteTaskStats stats =
        new ShuffleWriteTaskStats(rssConf, partitions, taskAttemptId, taskId);
    for (int i = 0; i < partitions; i++) {
      stats.partitionRecordsWritten[i] = outBuffer.getLong();
    }
    if (rssConf.get(RSS_DATA_INTEGRITY_VALIDATION_BLOCK_NUMBER_CHECK_ENABLED)) {
      for (int i = 0; i < partitions; i++) {
        stats.partitionBlocksWritten[i] = outBuffer.getLong();
      }
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
      long records = getRecordsWritten(i);
      long blocks = getBlocksWritten(i);
      infoBuilder.append(i).append("/").append(records).append("/").append(blocks).append(",");
    }
    LOGGER.info(
        "Partition records/blocks written for taskId[{}]: {}", taskId, infoBuilder.toString());
  }

  public void check(long[] partitionLens) {
    for (int idx = 0; idx < partitions; idx++) {
      long records = getRecordsWritten(idx);
      long blocks = getBlocksWritten(idx);
      long length = partitionLens[idx];
      if (records > 0 && length <= 0) {
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
