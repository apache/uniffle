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

package org.apache.uniffle.storage.handler.impl;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.uniffle.common.StorageType;

public class ShuffleServerReadCost {
  private final String shuffleServerId;
  private final AtomicLong durationMillis;
  private final AtomicLong readBytes;

  // hybrid storage statistics
  private final AtomicLong memoryReadBytes;
  private final AtomicLong memoryReadDurationMillis;

  private final AtomicLong localfileReadBytes;
  private final AtomicLong localfileReadDurationMillis;

  private final AtomicLong hadoopReadLocalFileBytes;
  private final AtomicLong hadoopReadLocalFileDurationMillis;

  public ShuffleServerReadCost(String shuffleServerId) {
    this.shuffleServerId = shuffleServerId;

    this.durationMillis = new AtomicLong(0);
    this.readBytes = new AtomicLong(0);

    this.memoryReadBytes = new AtomicLong(0);
    this.memoryReadDurationMillis = new AtomicLong(0);

    this.localfileReadBytes = new AtomicLong(0);
    this.localfileReadDurationMillis = new AtomicLong(0);

    this.hadoopReadLocalFileBytes = new AtomicLong(0);
    this.hadoopReadLocalFileDurationMillis = new AtomicLong(0);
  }

  public void inc(StorageType storageType, long bytes, long durationMillis) {
    this.durationMillis.addAndGet(durationMillis);
    this.readBytes.addAndGet(bytes);

    switch (storageType) {
      case MEMORY:
        this.memoryReadBytes.addAndGet(bytes);
        this.memoryReadDurationMillis.addAndGet(durationMillis);
        break;
      case LOCALFILE:
        this.localfileReadBytes.addAndGet(bytes);
        this.localfileReadDurationMillis.addAndGet(durationMillis);
        break;
      case HDFS:
        this.hadoopReadLocalFileBytes.addAndGet(bytes);
        this.hadoopReadLocalFileDurationMillis.addAndGet(durationMillis);
        break;
      default:
        break;
    }
  }

  public long getDurationMillis() {
    return durationMillis.get();
  }

  public long getReadBytes() {
    return readBytes.get();
  }

  public long getMemoryReadBytes() {
    return memoryReadBytes.get();
  }

  public long getMemoryReadDurationMillis() {
    return memoryReadDurationMillis.get();
  }

  public long getLocalfileReadBytes() {
    return localfileReadBytes.get();
  }

  public long getHadoopReadLocalFileBytes() {
    return hadoopReadLocalFileBytes.get();
  }

  public long getLocalfileReadDurationMillis() {
    return localfileReadDurationMillis.get();
  }

  public long getHadoopReadLocalFileDurationMillis() {
    return hadoopReadLocalFileDurationMillis.get();
  }
}
