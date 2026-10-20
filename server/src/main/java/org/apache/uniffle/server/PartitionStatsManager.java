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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.proto.RssProtos;

/** Stores partition record counts for one application. */
public class PartitionStatsManager {
  /** shuffleId -> partitionId -> taskAttemptId -> record number */
  private final Map<Integer, Map<Integer, Map<Long, Long>>> partitionStats =
      JavaUtils.newConcurrentMap();

  public void add(int shuffleId, List<RssProtos.PartitionStats> stats) {
    for (RssProtos.PartitionStats partition : stats) {
      Map<Long, Long> records =
          partitionStats
              .computeIfAbsent(shuffleId, key -> JavaUtils.newConcurrentMap())
              .computeIfAbsent(partition.getPartitionId(), key -> JavaUtils.newConcurrentMap());
      for (RssProtos.TaskAttemptIdToRecords record : partition.getTaskAttemptIdToRecordsList()) {
        // Reports contain totals, so retries must replace rather than accumulate them.
        records.put(record.getTaskAttemptId(), record.getRecordNumber());
      }
    }
  }

  public List<RssProtos.PartitionStats> get(int shuffleId, Set<Integer> partitions) {
    Map<Integer, Map<Long, Long>> stats =
        partitionStats.getOrDefault(shuffleId, Collections.emptyMap());
    List<RssProtos.PartitionStats> result = new ArrayList<>();
    for (int partitionId : partitions) {
      Map<Long, Long> records = stats.get(partitionId);
      if (records == null) {
        continue;
      }
      RssProtos.PartitionStats.Builder partition =
          RssProtos.PartitionStats.newBuilder().setPartitionId(partitionId);
      records.forEach(
          (taskAttemptId, recordNumber) ->
              partition.addTaskAttemptIdToRecords(
                  RssProtos.TaskAttemptIdToRecords.newBuilder()
                      .setTaskAttemptId(taskAttemptId)
                      .setRecordNumber(recordNumber)));
      result.add(partition.build());
    }
    return result;
  }

  public void remove(int shuffleId) {
    partitionStats.remove(shuffleId);
  }
}
