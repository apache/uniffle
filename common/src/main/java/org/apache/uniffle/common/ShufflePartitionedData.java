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

import java.util.Arrays;

import com.google.common.annotations.VisibleForTesting;

public class ShufflePartitionedData {

  private static final ShufflePartitionedBlock[] EMPTY_BLOCK_LIST =
      new ShufflePartitionedBlock[] {};
  private int partitionId;
  private final ShufflePartitionedBlock[] blockList;
  private final long totalBlockEncodedLength;
  private final long totalBlockDataLength;

  public ShufflePartitionedData(
      int partitionId, long encodedLength, long dataLength, ShufflePartitionedBlock[] blockList) {
    this.partitionId = partitionId;
    this.blockList = blockList == null ? EMPTY_BLOCK_LIST : blockList;
    totalBlockEncodedLength = encodedLength;
    totalBlockDataLength = dataLength;
  }

  @VisibleForTesting
  public ShufflePartitionedData(int partitionId, ShufflePartitionedBlock[] blockList) {
    this.partitionId = partitionId;
    this.blockList = blockList == null ? EMPTY_BLOCK_LIST : blockList;
    long encodedLength = 0L;
    long dataLength = 0L;
    for (ShufflePartitionedBlock block : this.blockList) {
      encodedLength += block.getEncodedLength();
      dataLength += block.getDataLength();
    }
    totalBlockEncodedLength = encodedLength;
    totalBlockDataLength = dataLength;
  }

  @Override
  public String toString() {
    return "ShufflePartitionedData{partitionId="
        + partitionId
        + ", blockList="
        + Arrays.toString(blockList)
        + '}';
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public ShufflePartitionedBlock[] getBlockList() {
    return blockList;
  }

  public long getTotalBlockEncodedLength() {
    return totalBlockEncodedLength;
  }

  public long getTotalBlockDataLength() {
    return totalBlockDataLength;
  }
}
