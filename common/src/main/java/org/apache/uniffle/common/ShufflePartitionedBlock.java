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

package org.apache.uniffle.common;

import java.util.Objects;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ShufflePartitionedBlock {

  private int dataLength;
  private long crc;
  private long blockId;
  private int uncompressLength;
  private ByteBuf data;
  private long taskAttemptId;

  public ShufflePartitionedBlock(
      int dataLength,
      int uncompressLength,
      long crc,
      long blockId,
      long taskAttemptId,
      byte[] data) {
    this.dataLength = dataLength;
    this.crc = crc;
    this.blockId = blockId;
    this.uncompressLength = uncompressLength;
    this.taskAttemptId = taskAttemptId;
    this.data = data == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(data);
  }

  public ShufflePartitionedBlock(
      int dataLength,
      int uncompressLength,
      long crc,
      long blockId,
      long taskAttemptId,
      ByteBuf data) {
    this.dataLength = dataLength;
    this.crc = crc;
    this.blockId = blockId;
    this.uncompressLength = uncompressLength;
    this.taskAttemptId = taskAttemptId;
    this.data = data;
  }

  // calculate the data size for this block in memory including metadata which are
  // blockId, crc, taskAttemptId, length, uncompressLength
  public long getEncodedLength() {
    return dataLength + 3 * 8 + 2 * 4;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ShufflePartitionedBlock that = (ShufflePartitionedBlock) o;
    return dataLength == that.dataLength
        && crc == that.crc
        && blockId == that.blockId
        && data.equals(that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataLength, crc, blockId, data);
  }

  public int getDataLength() {
    return dataLength;
  }

  public void setDataLength(int dataLength) {
    this.dataLength = dataLength;
  }

  public long getCrc() {
    return crc;
  }

  public void setCrc(long crc) {
    this.crc = crc;
  }

  public long getBlockId() {
    return blockId;
  }

  public void setBlockId(long blockId) {
    this.blockId = blockId;
  }

  public ByteBuf getData() {
    return data;
  }

  public void setData(ByteBuf data) {
    this.data = data;
  }

  public int getUncompressLength() {
    return uncompressLength;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  public void setTaskAttemptId(long taskAttemptId) {
    this.taskAttemptId = taskAttemptId;
  }

  @Override
  public String toString() {
    return "ShufflePartitionedBlock{blockId["
        + blockId
        + "], length["
        + dataLength
        + "], uncompressLength["
        + uncompressLength
        + "], crc["
        + crc
        + "], taskAttemptId["
        + taskAttemptId
        + "]}";
  }
}
