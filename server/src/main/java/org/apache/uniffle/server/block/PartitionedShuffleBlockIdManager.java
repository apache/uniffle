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

package org.apache.uniffle.server.block;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.server.ShuffleTaskInfo;

/** Manage the block ids individual for each partition. */
public class PartitionedShuffleBlockIdManager implements ShuffleBlockIdManager {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionedShuffleBlockIdManager.class);

  // appId -> shuffleId -> partitionId -> blockIds
  private Map<String, Map<Integer, Map<Integer, Roaring64NavigableMap>>> partitionsToBlockIds;

  public PartitionedShuffleBlockIdManager() {
    this.partitionsToBlockIds = new ConcurrentHashMap<>();
  }

  public void registerAppId(String appId) {
    partitionsToBlockIds.computeIfAbsent(appId, key -> JavaUtils.newConcurrentMap());
  }

  @Override
  public int addFinishedBlockIds(
      ShuffleTaskInfo taskInfo,
      String appId,
      Integer shuffleId,
      Map<Integer, long[]> partitionToBlockIds,
      int bitmapNum) {
    Map<Integer, Map<Integer, Roaring64NavigableMap>> shuffleIdToPartitions =
        partitionsToBlockIds.get(appId);
    if (shuffleIdToPartitions == null) {
      throw new RssException("appId[" + appId + "] is expired!");
    }
    shuffleIdToPartitions.computeIfAbsent(shuffleId, key -> new ConcurrentHashMap<>());

    Map<Integer, Roaring64NavigableMap> partitions = shuffleIdToPartitions.get(shuffleId);
    int totalUpdatedBlockCount = 0;
    for (Map.Entry<Integer, long[]> entry : partitionToBlockIds.entrySet()) {
      Integer partitionId = entry.getKey();
      partitions.computeIfAbsent(partitionId, k -> Roaring64NavigableMap.bitmapOf());
      Roaring64NavigableMap bitmap = partitions.get(partitionId);
      int updatedBlockCount = 0;
      synchronized (bitmap) {
        for (long blockId : entry.getValue()) {
          if (!bitmap.contains(blockId)) {
            bitmap.addLong(blockId);
            updatedBlockCount++;
            totalUpdatedBlockCount++;
          }
        }
      }
      taskInfo.incBlockNumber(shuffleId, partitionId, updatedBlockCount);
    }
    return totalUpdatedBlockCount;
  }

  @Override
  public byte[] getFinishedBlockIds(
      ShuffleTaskInfo taskInfo,
      String appId,
      Integer shuffleId,
      Set<Integer> partitions,
      BlockIdLayout blockIdLayout)
      throws IOException {
    Map<Integer, Map<Integer, Roaring64NavigableMap>> shuffleIdToPartitions =
        partitionsToBlockIds.get(appId);
    if (shuffleIdToPartitions == null) {
      LOG.warn("Empty blockIds for app: {}. This should not happen", appId);
      return null;
    }

    Map<Integer, Roaring64NavigableMap> partitionToBlockId = shuffleIdToPartitions.get(shuffleId);

    long expectedBlockNumber = 0;
    Roaring64NavigableMap res = Roaring64NavigableMap.bitmapOf();
    for (int partitionId : partitions) {
      expectedBlockNumber += taskInfo.getBlockNumber(shuffleId, partitionId);
      Roaring64NavigableMap bitmap = partitionToBlockId.get(partitionId);
      res.or(bitmap);
    }

    if (res.getLongCardinality() != expectedBlockNumber) {
      throw new RssException(
          "Inconsistent block number for partitions: "
              + partitions
              + ". Excepted: "
              + expectedBlockNumber
              + ", actual: "
              + res.getLongCardinality());
    }

    return RssUtils.serializeBitMap(res);
  }

  @Override
  public void removeBlockIdByAppId(String appId) {
    partitionsToBlockIds.remove(appId);
  }

  @Override
  public void removeBlockIdByShuffleId(String appId, List<Integer> shuffleIds) {
    Optional.ofNullable(partitionsToBlockIds.get(appId))
        .ifPresent(
            x -> {
              for (Integer shuffleId : shuffleIds) {
                x.remove(shuffleId);
              }
            });
  }

  @Override
  public long getTotalBlockCount() {
    return partitionsToBlockIds.values().stream()
        .flatMap(innerMap -> innerMap.values().stream())
        .flatMap(innerMap -> innerMap.values().stream())
        .mapToLong(roaringMap -> roaringMap.getLongCardinality())
        .sum();
  }

  @Override
  public long getBlockCountByShuffleId(String appId, List<Integer> shuffleIds) {

    return partitionsToBlockIds.values().stream()
        .filter(k -> shuffleIds.contains(k.keySet()))
        .flatMap(innerMap -> innerMap.values().stream())
        .flatMap(innerMap -> innerMap.values().stream())
        .mapToLong(roaringMap -> roaringMap.getLongCardinality())
        .sum();
  }

  @Override
  public boolean contains(String appId) {
    return partitionsToBlockIds.containsKey(appId);
  }

  @Override
  public long getBitmapNum(String appId, int shuffleId) {
    return partitionsToBlockIds.get(appId).get(shuffleId).size();
  }
}
