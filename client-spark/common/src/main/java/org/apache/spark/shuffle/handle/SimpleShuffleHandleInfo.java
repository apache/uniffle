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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.shuffle.handle.split.PartitionSplitInfo;

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleServerInfo;

/**
 * Class for holding, 1. partition ID -> shuffle servers mapping. 2. remote storage info
 *
 * <p>It's to be broadcast to executors and referenced by shuffle tasks.
 */
public class SimpleShuffleHandleInfo extends ShuffleHandleInfoBase implements Serializable {
  private static final long serialVersionUID = 0L;
  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;

  public SimpleShuffleHandleInfo(
      int shuffleId,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      RemoteStorageInfo storageInfo) {
    super(shuffleId, storageInfo);
    this.partitionToServers = partitionToServers;
  }

  @Override
  public Set<ShuffleServerInfo> getServers() {
    return partitionToServers.values().stream()
        .flatMap(x -> x.stream())
        .collect(Collectors.toSet());
  }

  @Override
  public Map<Integer, List<ShuffleServerInfo>> getAvailablePartitionServersForWriter() {
    return partitionToServers;
  }

  @Override
  public Map<Integer, List<ShuffleServerInfo>> getAllPartitionServersForReader() {
    return partitionToServers;
  }

  @Override
  public PartitionDataReplicaRequirementTracking createPartitionReplicaTracking() {
    return new PartitionDataReplicaRequirementTracking(partitionToServers, shuffleId);
  }

  public RemoteStorageInfo getRemoteStorage() {
    return remoteStorage;
  }

  @Override
  public PartitionSplitInfo getPartitionSplitInfo(int partitionId) {
    return null;
  }

  public int getShuffleId() {
    return shuffleId;
  }
}
