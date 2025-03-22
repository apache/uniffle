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

import org.apache.uniffle.common.util.UnitConverter;

public class PartitionInfo {
  private int id;
  private int shuffleId;
  private long size;
  private long blockCount;

  public long getSize() {
    return size;
  }

  public long getBlockCount() {
    return blockCount;
  }

  public int getId() {
    return id;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public void update(int id, int shuffleId, long size, long blockCount) {
    this.id = id;
    this.shuffleId = shuffleId;
    this.size = size;
    this.blockCount = blockCount;
  }

  @Override
  public String toString() {
    return String.format(
        "[id=%s, shuffleId=%s, size=%s, blockCount=%s]",
        id, shuffleId, UnitConverter.formatSize(size), blockCount);
  }

  public boolean isCurrentPartition(int shuffleId, int partitionId) {
    return this.shuffleId == shuffleId && this.id == partitionId;
  }

  public void setBlockCount(long blockCount) {
    this.blockCount = blockCount;
  }
}
