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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.StorageType;

public class ShuffleServerReadCostTracker {
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServerReadCostTracker.class);

  private Map<String, ShuffleServerReadCost> tracking;

  public ShuffleServerReadCostTracker() {
    this.tracking = new ConcurrentHashMap<>();
  }

  public void record(String serverId, StorageType storageType, long bytes, long durationMillis) {
    if (serverId == null) {
      return;
    }
    ShuffleServerReadCost readCost =
        tracking.computeIfAbsent(serverId, x -> new ShuffleServerReadCost(x));
    readCost.inc(storageType, bytes, durationMillis);
  }

  public Map<String, ShuffleServerReadCost> list() {
    return tracking;
  }
}
