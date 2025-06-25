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

import org.apache.uniffle.proto.RssProtos;

public class ShuffleWriteTimes {
  private long total;

  private long copy = 0;
  private long serialize = 0;
  private long compress = 0;
  private long sort = 0;
  private long requireMemory = 0;
  private long waitFinish = 0;

  public static ShuffleWriteTimes fromProto(RssProtos.ShuffleWriteTimes times) {
    ShuffleWriteTimes writeTimes = new ShuffleWriteTimes();
    writeTimes.copy = times.getCopy();
    writeTimes.serialize = times.getSerialize();
    writeTimes.compress = times.getCompress();
    writeTimes.sort = times.getSort();
    writeTimes.requireMemory = times.getRequireMemory();
    writeTimes.waitFinish = times.getWaitFinish();
    writeTimes.total = times.getTotal();
    return writeTimes;
  }

  public long getTotal() {
    return total;
  }

  public long getCopy() {
    return copy;
  }

  public long getSerialize() {
    return serialize;
  }

  public long getCompress() {
    return compress;
  }

  public long getSort() {
    return sort;
  }

  public long getRequireMemory() {
    return requireMemory;
  }

  public long getWaitFinish() {
    return waitFinish;
  }

  public void inc(ShuffleWriteTimes times) {
    if (times == null) {
      return;
    }
    total += times.getTotal();
    copy += times.getCopy();
    serialize += times.getSerialize();
    compress += times.getCompress();
    sort += times.getSort();
    requireMemory += times.getRequireMemory();
    waitFinish += times.getWaitFinish();
  }
}
