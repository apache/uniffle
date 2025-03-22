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

package org.apache.uniffle.server;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.time.DateFormatUtils;

import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.UnitConverter;

public class ShuffleDetailInfo {
  private int id;
  private AtomicLong dataSize;
  private AtomicLong blockCount;
  private AtomicLong partitionCount;
  private long startTime;

  public ShuffleDetailInfo(int id, long startTime) {
    this.id = id;
    this.dataSize = new AtomicLong();
    this.blockCount = new AtomicLong();
    this.partitionCount = new AtomicLong();
    this.startTime = startTime;
  }

  public int getId() {
    return id;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getBlockCount() {
    return blockCount.get();
  }

  public long getDataSize() {
    return dataSize.get();
  }

  public void incrDataSize(long size) {
    dataSize.addAndGet(size);
  }

  public void incrBlockCount(long count) {
    blockCount.addAndGet(count);
  }

  public void incrPartitionCount() {
    partitionCount.addAndGet(1);
  }

  @Override
  public String toString() {
    return String.format(
        "ShuffleDetail [%d: partitionCount=%s, blockCount=%s, size=%s, startTime=%s]",
        id,
        partitionCount,
        blockCount,
        UnitConverter.formatSize(dataSize.get()),
        DateFormatUtils.format(startTime, Constants.DATE_PATTERN));
  }
}
