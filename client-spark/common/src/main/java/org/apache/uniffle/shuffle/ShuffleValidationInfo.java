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

import com.google.common.annotations.VisibleForTesting;

/**
 * This class records partition writing statistics and leverages them to verify the integrity of
 * read data. This will use the number of records and row-based checksum for per-partition to
 * validate.
 */
public class ShuffleValidationInfo {
  private long[] partitionRecordsWritten;

  public ShuffleValidationInfo(int partitions) {
    partitionRecordsWritten = new long[partitions];
  }

  public long getRecordsWritten(int partitionId) {
    return partitionRecordsWritten[partitionId];
  }

  public void incPartitionRecord(int partitionId) {
    partitionRecordsWritten[partitionId]++;
  }

  @VisibleForTesting
  public ByteBuffer encode() {
    byte[] bytes = new byte[Long.BYTES * partitionRecordsWritten.length];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    for (long v : partitionRecordsWritten) {
      buffer.putLong(v);
    }
    return buffer;
  }

  public static ShuffleValidationInfo decode(ByteBuffer raw) {
    if (raw == null || raw.remaining() < Long.BYTES) {
      return null;
    }

    ByteBuffer buffer = raw.duplicate();
    int partitions = buffer.remaining() / Long.BYTES;
    ShuffleValidationInfo info = new ShuffleValidationInfo(partitions);

    for (int i = 0; i < partitions; i++) {
      info.partitionRecordsWritten[i] = buffer.getLong();
    }

    return info;
  }
}
