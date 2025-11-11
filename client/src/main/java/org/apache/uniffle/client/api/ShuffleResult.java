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

package org.apache.uniffle.client.api;

import java.util.Map;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class ShuffleResult {
  private Roaring64NavigableMap blockIds;
  // partitionId -> taskAttemptId -> recordNumber
  private Map<Integer, Map<Long, Long>> partitionToTaskAttemptIdToRecordNumbers;

  public ShuffleResult(
      Roaring64NavigableMap blockIds,
      Map<Integer, Map<Long, Long>> partitionToTaskAttemptIdToRecordNumbers) {
    this.blockIds = blockIds;
    this.partitionToTaskAttemptIdToRecordNumbers = partitionToTaskAttemptIdToRecordNumbers;
  }

  public Roaring64NavigableMap getBlockIds() {
    return blockIds;
  }

  public Map<Integer, Map<Long, Long>> getPartitionToTaskAttemptIdToRecordNumbers() {
    return partitionToTaskAttemptIdToRecordNumbers;
  }
}
