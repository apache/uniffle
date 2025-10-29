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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

public class ShuffleReadTaskStats {
  // partition_id -> upstream_map_id -> records_read
  private Map<Integer, Map<Long, Long>> partitionRecordsReadPerMap = new HashMap<>();

  public void incPartitionRecord(int partitionId, long taskAttemptId) {
    Map<Long, Long> records =
        partitionRecordsReadPerMap.computeIfAbsent(partitionId, k -> new HashMap<>());
    records.compute(taskAttemptId, (k, v) -> v == null ? 1 : v + 1);
  }

  public Iterator<Pair<Long, Long>> get(int partitionId) {
    Map<Long, Long> records = partitionRecordsReadPerMap.get(partitionId);
    if (records == null) {
      return Collections.emptyIterator();
    }
    return records.entrySet().stream().map(x -> Pair.of(x.getKey(), x.getValue())).iterator();
  }
}
