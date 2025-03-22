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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.rpc.StatusCode;

public class FailedBlockSendTracker {

  /**
   * blockId -> list(trackingStatus)
   *
   * <p>This indicates the blockId latest sending status, and it will not store the resending
   * history. The list data structure is to describe the multiple servers for the multiple replica
   */
  private Map<Long, List<TrackingBlockStatus>> trackingBlockStatusMap;

  private final BlockingQueue<TrackingPartitionStatus> trackingNeedSplitPartitionStatusQueue;

  public FailedBlockSendTracker() {
    this.trackingBlockStatusMap = Maps.newConcurrentMap();
    this.trackingNeedSplitPartitionStatusQueue = new LinkedBlockingQueue<>();
  }

  public void add(
      ShuffleBlockInfo shuffleBlockInfo,
      ShuffleServerInfo shuffleServerInfo,
      StatusCode statusCode) {
    trackingBlockStatusMap
        .computeIfAbsent(
            shuffleBlockInfo.getBlockId(), s -> Collections.synchronizedList(Lists.newArrayList()))
        .add(new TrackingBlockStatus(shuffleBlockInfo, shuffleServerInfo, statusCode));
  }

  public void merge(FailedBlockSendTracker failedBlockSendTracker) {
    this.trackingBlockStatusMap.putAll(failedBlockSendTracker.trackingBlockStatusMap);
    this.trackingNeedSplitPartitionStatusQueue.addAll(
        failedBlockSendTracker.trackingNeedSplitPartitionStatusQueue);
  }

  public void remove(long blockId) {
    trackingBlockStatusMap.remove(blockId);
  }

  public void clearAndReleaseBlockResources() {
    trackingBlockStatusMap
        .values()
        .forEach(
            l -> {
              synchronized (l) {
                l.forEach(x -> x.getShuffleBlockInfo().executeCompletionCallback(false));
              }
            });
    trackingBlockStatusMap.clear();
    trackingNeedSplitPartitionStatusQueue.clear();
  }

  public Set<Long> getFailedBlockIds() {
    return trackingBlockStatusMap.keySet();
  }

  public List<TrackingBlockStatus> getFailedBlockStatus(Long blockId) {
    return trackingBlockStatusMap.get(blockId);
  }

  public Set<ShuffleServerInfo> getFaultyShuffleServers() {
    Set<ShuffleServerInfo> shuffleServerInfos = Sets.newHashSet();
    trackingBlockStatusMap.values().stream()
        .forEach(
            l -> {
              synchronized (l) {
                l.stream()
                    .forEach((status) -> shuffleServerInfos.add(status.getShuffleServerInfo()));
              }
            });
    return shuffleServerInfos;
  }

  public void addNeedSplitPartition(int partitionId, ShuffleServerInfo shuffleServerInfo) {
    try {
      trackingNeedSplitPartitionStatusQueue.put(
          new TrackingPartitionStatus(partitionId, shuffleServerInfo));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RssException(e);
    }
  }

  public List<TrackingPartitionStatus> removeAllTrackedPartitions() {
    List<TrackingPartitionStatus> trackingPartitionStatusList = Lists.newArrayList();
    trackingNeedSplitPartitionStatusQueue.drainTo(trackingPartitionStatusList);
    return trackingPartitionStatusList;
  }
}
