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

package org.apache.spark.shuffle.events;

public class ShuffleReadMetric extends ShuffleMetric {
  private final long memoryDurationMillis;
  private final long memoryByteSize;

  private final long localfileDurationMillis;
  private final long localfileByteSize;

  private final long hadoopDurationMillis;
  private final long hadoopByteSize;

  public ShuffleReadMetric(
      long durationMillis,
      long byteSize,
      long memoryDurationMillis,
      long memoryByteSize,
      long localfileDurationMillis,
      long localfileByteSize,
      long hadoopDurationMillis,
      long hadoopByteSize) {
    super(durationMillis, byteSize);

    this.memoryDurationMillis = memoryDurationMillis;
    this.memoryByteSize = memoryByteSize;

    this.localfileDurationMillis = localfileDurationMillis;
    this.localfileByteSize = localfileByteSize;

    this.hadoopDurationMillis = hadoopDurationMillis;
    this.hadoopByteSize = hadoopByteSize;
  }

  public long getMemoryDurationMillis() {
    return memoryDurationMillis;
  }

  public long getMemoryByteSize() {
    return memoryByteSize;
  }

  public long getLocalfileDurationMillis() {
    return localfileDurationMillis;
  }

  public long getLocalfileByteSize() {
    return localfileByteSize;
  }

  public long getHadoopDurationMillis() {
    return hadoopDurationMillis;
  }

  public long getHadoopByteSize() {
    return hadoopByteSize;
  }
}
