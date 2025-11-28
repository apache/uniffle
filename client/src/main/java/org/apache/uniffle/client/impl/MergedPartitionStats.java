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

package org.apache.uniffle.client.impl;

import java.util.HashMap;
import java.util.Map;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

/**
 * Merge the partition stats (now includes the partition records number from the different upstream
 * taskAttemptIds) from the shuffle-servers. The partition records checksum also can be supported in
 * this class.
 */
public class MergedPartitionStats {
  // partitionId -> taskAttemptId -> records
  private final Map<Integer, Map<Long, Long>> partitionToTaskAttemptIdToRecordNumbers;

  public MergedPartitionStats() {
    this.partitionToTaskAttemptIdToRecordNumbers = new HashMap<>();
  }

  public void merge(Map<Integer, Map<Long, Long>> partitionToTaskAttemptIdToRecordNumbers) {
    if (partitionToTaskAttemptIdToRecordNumbers == null) {
      return;
    }

    for (Map.Entry<Integer, Map<Long, Long>> entry :
        partitionToTaskAttemptIdToRecordNumbers.entrySet()) {
      int partitionId = entry.getKey();
      Map<Long, Long> incomingTaskMap = entry.getValue();

      Map<Long, Long> currentTaskMap =
          this.partitionToTaskAttemptIdToRecordNumbers.computeIfAbsent(
              partitionId, k -> new HashMap<>());

      for (Map.Entry<Long, Long> taskEntry : incomingTaskMap.entrySet()) {
        long taskAttemptId = taskEntry.getKey();
        long recordCount = taskEntry.getValue();
        currentTaskMap.merge(taskAttemptId, recordCount, Long::sum);
      }
    }
  }

  // get the expected total record number filter by the upstream taskIds
  public long getExpectedRecordNumberByTaskIds(Roaring64NavigableMap taskIdBitmap) {
    long total =
        partitionToTaskAttemptIdToRecordNumbers.values().stream()
            .flatMap(x -> x.entrySet().stream())
            .filter(x -> taskIdBitmap.contains(x.getKey()))
            .mapToLong(Map.Entry::getValue)
            .sum();
    return total;
  }
}
