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

package org.apache.uniffle.server.bench;

import org.apache.uniffle.server.ShuffleServerMetrics;

public class BenchTrace {
  private double totalWriteTime;
  private double totalWriteBlockSize;
  private double totalWriteDataSize;
  private boolean isEnd = false;

  public void start() {
    totalWriteTime = ShuffleServerMetrics.counterTotalWriteTime.get();
    totalWriteBlockSize = ShuffleServerMetrics.counterTotalWriteBlockSize.get();
    totalWriteDataSize = ShuffleServerMetrics.counterTotalWriteDataSize.get();
  }

  public void end() {
    totalWriteTime = ShuffleServerMetrics.counterTotalWriteTime.get() - totalWriteTime;
    totalWriteBlockSize =
        ShuffleServerMetrics.counterTotalWriteBlockSize.get() - totalWriteBlockSize;
    totalWriteDataSize = ShuffleServerMetrics.counterTotalWriteDataSize.get() - totalWriteDataSize;
    isEnd = true;
  }

  public boolean isEnd() {
    return isEnd;
  }

  public void setEnd(boolean end) {
    isEnd = end;
  }

  public double getTotalWriteBlockSize() {
    return totalWriteBlockSize;
  }

  public double getTotalWriteDataSize() {
    return totalWriteDataSize;
  }

  public double getTotalWriteTime() {
    return totalWriteTime;
  }

  public void setTotalWriteBlockSize(double totalWriteBlockSize) {
    this.totalWriteBlockSize = totalWriteBlockSize;
  }

  public void setTotalWriteDataSize(double totalWriteDataSize) {
    this.totalWriteDataSize = totalWriteDataSize;
  }

  public void setTotalWriteTime(double totalWriteTime) {
    this.totalWriteTime = totalWriteTime;
  }
}
