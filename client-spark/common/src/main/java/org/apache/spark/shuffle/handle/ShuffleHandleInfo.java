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

package org.apache.spark.shuffle.handle;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.shuffle.handle.split.PartitionSplitInfo;

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleServerInfo;

public interface ShuffleHandleInfo {
  /** Get all the assigned servers including the excluded servers. */
  Set<ShuffleServerInfo> getServers();

  /**
   * Get the assignment of available servers for writer to write partitioned blocks to corresponding
   * shuffleServers. Implementations might return dynamic, up-to-date information here. Returns
   * partitionId -> [replica1, replica2, ...]
   */
  Map<Integer, List<ShuffleServerInfo>> getAvailablePartitionServersForWriter();

  /**
   * Get all servers ever assigned to writers group by partitionId for reader to get the data
   * written to these servers
   */
  Map<Integer, List<ShuffleServerInfo>> getAllPartitionServersForReader();

  /** Create the partition replicas tracker for the writer to check data replica requirements */
  PartitionDataReplicaRequirementTracking createPartitionReplicaTracking();

  int getShuffleId();

  RemoteStorageInfo getRemoteStorage();

  PartitionSplitInfo getPartitionSplitInfo(int partitionId);
}
