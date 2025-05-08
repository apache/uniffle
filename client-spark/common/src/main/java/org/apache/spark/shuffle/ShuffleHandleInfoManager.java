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

package org.apache.spark.shuffle;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import org.apache.spark.shuffle.handle.MutableShuffleHandleInfo;

import org.apache.uniffle.common.util.JavaUtils;

public class ShuffleHandleInfoManager implements Closeable {
  private Map<Integer, MutableShuffleHandleInfo> shuffleIdToShuffleHandleInfo;

  public ShuffleHandleInfoManager() {
    this.shuffleIdToShuffleHandleInfo = JavaUtils.newConcurrentMap();
  }

  public MutableShuffleHandleInfo get(int shuffleId) {
    return shuffleIdToShuffleHandleInfo.get(shuffleId);
  }

  public void remove(int shuffleId) {
    shuffleIdToShuffleHandleInfo.remove(shuffleId);
  }

  public void register(int shuffleId, MutableShuffleHandleInfo handle) {
    shuffleIdToShuffleHandleInfo.put(shuffleId, handle);
  }

  @Override
  public void close() throws IOException {
    if (shuffleIdToShuffleHandleInfo == null) {
      return;
    }
    shuffleIdToShuffleHandleInfo.clear();
  }
}
