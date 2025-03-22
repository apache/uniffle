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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.InvalidRequestException;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.server.ShuffleTaskInfo;

/**
 * The default implementation of ShuffleBlockIdManager, manage block id of all partitions together.
 */
public class DefaultShuffleBlockIdManager implements ShuffleBlockIdManager {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultShuffleBlockIdManager.class);

  // appId -> shuffleId -> blockIds to avoid too many appId
  // store taskAttemptId info to filter speculation task
  // Roaring64NavigableMap instance will cost much memory,
  // merge different blockId of partition to one bitmap can reduce memory cost,
  // but when get blockId, performance will degrade a little which can be optimized by client
  // configuration
  // appId -> shuffleId -> hashId -> blockIds
  private Map<String, Map<Integer, Roaring64NavigableMap[]>> partitionsToBlockIds;

  public DefaultShuffleBlockIdManager() {
    this.partitionsToBlockIds = JavaUtils.newConcurrentMap();
  }

  @VisibleForTesting
  /** Filter the specific partition blockId in the bitmap to the resultBitmap. */
  public static Roaring64NavigableMap getBlockIdsByPartitionId(
      Set<Integer> requestPartitions,
      Roaring64NavigableMap bitmap,
      Roaring64NavigableMap resultBitmap,
      BlockIdLayout blockIdLayout) {
    bitmap.forEach(
        blockId -> {
          int partitionId = blockIdLayout.getPartitionId(blockId);
          if (requestPartitions.contains(partitionId)) {
            resultBitmap.addLong(blockId);
          }
        });
    return resultBitmap;
  }

  public void registerAppId(String appId) {
    partitionsToBlockIds.computeIfAbsent(appId, key -> JavaUtils.newConcurrentMap());
  }

  /**
   * Add finished blockIds from client
   *
   * @param appId
   * @param shuffleId
   * @param partitionToBlockIds
   * @param bitmapNum
   * @return the number of added blockIds
   */
  public int addFinishedBlockIds(
      ShuffleTaskInfo taskInfo,
      String appId,
      Integer shuffleId,
      Map<Integer, long[]> partitionToBlockIds,
      int bitmapNum) {
    Map<Integer, Roaring64NavigableMap[]> shuffleIdToPartitions = partitionsToBlockIds.get(appId);
    if (shuffleIdToPartitions == null) {
      throw new RssException("appId[" + appId + "] is expired!");
    }
    shuffleIdToPartitions.computeIfAbsent(
        shuffleId,
        key -> {
          Roaring64NavigableMap[] blockIds = new Roaring64NavigableMap[bitmapNum];
          for (int i = 0; i < bitmapNum; i++) {
            blockIds[i] = Roaring64NavigableMap.bitmapOf();
          }
          return blockIds;
        });
    Roaring64NavigableMap[] blockIds = shuffleIdToPartitions.get(shuffleId);
    if (blockIds.length != bitmapNum) {
      throw new InvalidRequestException(
          "Request expects "
              + bitmapNum
              + " bitmaps, but there are "
              + blockIds.length
              + " bitmaps!");
    }

    int totalUpdatedBlockCount = 0;
    for (Map.Entry<Integer, long[]> entry : partitionToBlockIds.entrySet()) {
      Integer partitionId = entry.getKey();
      Roaring64NavigableMap bitmap = blockIds[partitionId % bitmapNum];
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

  public byte[] getFinishedBlockIds(
      ShuffleTaskInfo taskInfo,
      String appId,
      Integer shuffleId,
      Set<Integer> partitions,
      BlockIdLayout blockIdLayout)
      throws IOException {
    Map<Integer, Roaring64NavigableMap[]> shuffleIdToPartitions = partitionsToBlockIds.get(appId);
    if (shuffleIdToPartitions == null) {
      LOG.warn("Empty blockIds for app: {}. This should not happen", appId);
      return null;
    }
    Roaring64NavigableMap[] blockIds = shuffleIdToPartitions.get(shuffleId);
    if (blockIds == null) {
      LOG.warn("Empty blockIds for app: {}, shuffleId: {}", appId, shuffleId);
      return new byte[] {};
    }
    long expectedBlockNumber = 0;
    Map<Integer, Set<Integer>> bitmapIndexToPartitions = Maps.newHashMap();
    for (int partitionId : partitions) {
      int bitmapIndex = partitionId % blockIds.length;
      if (bitmapIndexToPartitions.containsKey(bitmapIndex)) {
        bitmapIndexToPartitions.get(bitmapIndex).add(partitionId);
      } else {
        HashSet<Integer> newHashSet = Sets.newHashSet(partitionId);
        bitmapIndexToPartitions.put(bitmapIndex, newHashSet);
      }
      expectedBlockNumber += taskInfo.getBlockNumber(shuffleId, partitionId);
    }
    Roaring64NavigableMap res = Roaring64NavigableMap.bitmapOf();
    for (Map.Entry<Integer, Set<Integer>> entry : bitmapIndexToPartitions.entrySet()) {
      Set<Integer> requestPartitions = entry.getValue();
      Roaring64NavigableMap bitmap = blockIds[entry.getKey()];
      getBlockIdsByPartitionId(requestPartitions, bitmap, res, blockIdLayout);
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
  public void removeBlockIdByAppId(String appId) {
    partitionsToBlockIds.remove(appId);
  }

  @Override
  public long getTotalBlockCount() {
    return partitionsToBlockIds.values().stream()
        .flatMap(innerMap -> innerMap.values().stream())
        .flatMapToLong(
            arr ->
                java.util.Arrays.stream(arr).mapToLong(Roaring64NavigableMap::getLongCardinality))
        .sum();
  }

  @Override
  public long getBlockCountByShuffleId(String appId, List<Integer> shuffleIds) {

    return partitionsToBlockIds.values().stream()
        .filter(k -> shuffleIds.contains(k.keySet()))
        .flatMap(innerMap -> innerMap.values().stream())
        .flatMapToLong(
            arr ->
                java.util.Arrays.stream(arr).mapToLong(Roaring64NavigableMap::getLongCardinality))
        .sum();
  }

  @Override
  public boolean contains(String appId) {
    return partitionsToBlockIds.containsKey(appId);
  }

  @Override
  public long getBitmapNum(String appId, int shuffleId) {
    return partitionsToBlockIds.get(appId).get(shuffleId).length;
  }
}
