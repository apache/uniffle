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

package org.apache.uniffle.shuffle.manager;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class ShuffleDeletionItem implements Delayed {
  private final int shuffleId;
  private final long expireAtMs;

  public ShuffleDeletionItem(int shuffleId, long delayMs) {
    this.shuffleId = shuffleId;
    this.expireAtMs = System.currentTimeMillis() + delayMs;
  }

  @Override
  public long getDelay(TimeUnit unit) {
    long remainingMs = expireAtMs - System.currentTimeMillis();
    return unit.convert(remainingMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public int compareTo(Delayed o) {
    if (this.equals(o)) {
      return 0;
    }
    long diff = this.getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS);
    return diff < 0 ? -1 : (diff > 0 ? 1 : 0);
  }

  public int getShuffleId() {
    return shuffleId;
  }
}
