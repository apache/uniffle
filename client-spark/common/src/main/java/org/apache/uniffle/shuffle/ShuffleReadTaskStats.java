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

package org.apache.uniffle.shuffle;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleReadTaskStats {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleReadTaskStats.class);

  // partition_id -> upstream_map_id -> records_read
  private Map<Integer, Map<Long, Long>> partitionRecordsReadPerMap = new HashMap<>();
  // partition_id -> upstream_map_id -> blocks_read
  private Map<Integer, Map<Long, Long>> partitionBlocksReadPerMap = new HashMap<>();

  public void incPartitionRecord(int partitionId, long taskAttemptId) {
    Map<Long, Long> records =
        partitionRecordsReadPerMap.computeIfAbsent(partitionId, k -> new HashMap<>());
    records.compute(taskAttemptId, (k, v) -> v == null ? 1 : v + 1);
  }

  public void incPartitionBlock(int partitionId, long taskAttemptId) {
    Map<Long, Long> records =
        partitionBlocksReadPerMap.computeIfAbsent(partitionId, k -> new HashMap<>());
    records.compute(taskAttemptId, (k, v) -> v == null ? 1 : v + 1);
  }

  public Map<Long, Long> getPartitionRecords(int partitionId) {
    return partitionRecordsReadPerMap.get(partitionId);
  }

  public Map<Long, Long> getPartitionBlocks(int partitionId) {
    return partitionBlocksReadPerMap.get(partitionId);
  }

  /**
   * Compared with the upstream shuffleWriteTaskStats that is re-built from the client
   * mapOutputTracker metadata
   *
   * @param writeStats
   * @param startPartition
   * @param endPartition
   * @return
   */
  public boolean diff(
      Map<Long, ShuffleWriteTaskStats> writeStats, int startPartition, int endPartition) {
    StringBuilder infoBuilder = new StringBuilder();
    for (int idx = startPartition; idx < endPartition; idx++) {
      for (Map.Entry<Long, Long> recordEntry : partitionRecordsReadPerMap.get(idx).entrySet()) {
        long taskAttemptId = recordEntry.getKey();
        long recordsRead = recordEntry.getValue();
        long blocksRead =
            Optional.ofNullable(partitionBlocksReadPerMap.get(idx))
                .map(m -> m.getOrDefault(taskAttemptId, 0L))
                .orElse(0L);

        ShuffleWriteTaskStats stats = writeStats.get(taskAttemptId);
        if (stats == null) {
          LOGGER.warn("Should not happen that task attempt {} has no write stats", taskAttemptId);
          continue;
        }
        long recordsUpstream = stats.getRecordsWritten(idx);
        // If blocksUpstream is less than 0, it indicates that the block number check is disabled.
        long blocksUpstream = stats.getBlocksWritten(idx);
        if (recordsRead != recordsUpstream
            || (blocksUpstream >= 0 && blocksRead != blocksUpstream)) {
          infoBuilder.append(idx);
          infoBuilder.append("/");
          infoBuilder.append(stats.getTaskId());
          infoBuilder.append("/");
          infoBuilder.append(recordsRead);
          infoBuilder.append("-");
          infoBuilder.append(recordsUpstream);
          infoBuilder.append("/");
          infoBuilder.append(blocksRead);
          infoBuilder.append("-");
          infoBuilder.append(blocksUpstream);
          infoBuilder.append(",");
        }
      }
    }
    if (infoBuilder.length() > 0) {
      infoBuilder.insert(
          0,
          "Errors on integrity validating. Details(partitionId/upstreamTaskId/recordsRead-recordsUpstream/blocksRead-blocksUpstream): ");
      LOGGER.warn(infoBuilder.toString());
      return false;
    }
    return true;
  }
}
