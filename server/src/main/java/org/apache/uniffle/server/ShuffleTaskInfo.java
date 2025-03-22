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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.PartitionInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.UnitConverter;
import org.apache.uniffle.server.block.ShuffleBlockIdManager;
import org.apache.uniffle.server.block.ShuffleBlockIdManagerFactory;

/**
 * ShuffleTaskInfo contains the information of submitting the shuffle, the information of the cache
 * block, user and timestamp corresponding to the app
 */
public class ShuffleTaskInfo {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleTaskInfo.class);

  private final String appId;
  private Long currentTimes;
  /** shuffleId -> commit count */
  private Map<Integer, AtomicInteger> commitCounts;

  private Map<Integer, Object> commitLocks;
  /** shuffleId -> blockIds */
  private Map<Integer, Roaring64NavigableMap> cachedBlockIds;

  private AtomicReference<String> user;

  private final AtomicLong totalDataSize = new AtomicLong(0);
  private final AtomicLong inMemoryDataSize = new AtomicLong(0);
  private final AtomicLong onLocalFileNum = new AtomicLong(0);
  private final AtomicLong onLocalFileDataSize = new AtomicLong(0);
  private final AtomicLong onHadoopFileNum = new AtomicLong(0);
  private final AtomicLong onHadoopDataSize = new AtomicLong(0);

  /** shuffleId, partitionId, partitionSize */
  private final PartitionInfo maxSizePartitionInfo = new PartitionInfo();
  /** shuffleId -> partitionId -> partition shuffle data size */
  private Map<Integer, Map<Integer, Long>> partitionDataSizes;
  /** shuffleId -> huge partitionIds set */
  private final Map<Integer, Set<Integer>> hugePartitionTags;

  private final AtomicBoolean existHugePartition;

  private final AtomicReference<ShuffleSpecification> specification;

  /** shuffleId -> partitionId -> block counter */
  private final Map<Integer, Map<Integer, AtomicLong>> partitionBlockCounters;
  /** shuffleId -> shuffleDetailInfo */
  private final Map<Integer, ShuffleDetailInfo> shuffleDetailInfos;

  private final Map<Integer, Integer> latestStageAttemptNumbers;
  private Map<String, String> properties;
  private ShuffleBlockIdManager shuffleBlockIdManager;

  public ShuffleTaskInfo(String appId) {
    this.appId = appId;
    this.currentTimes = System.currentTimeMillis();
    this.commitCounts = JavaUtils.newConcurrentMap();
    this.commitLocks = JavaUtils.newConcurrentMap();
    this.cachedBlockIds = JavaUtils.newConcurrentMap();
    this.user = new AtomicReference<>();
    this.partitionDataSizes = JavaUtils.newConcurrentMap();
    this.hugePartitionTags = JavaUtils.newConcurrentMap();
    this.existHugePartition = new AtomicBoolean(false);
    this.specification = new AtomicReference<>();
    this.partitionBlockCounters = JavaUtils.newConcurrentMap();
    this.latestStageAttemptNumbers = JavaUtils.newConcurrentMap();
    this.shuffleDetailInfos = JavaUtils.newConcurrentMap();
  }

  public Long getCurrentTimes() {
    return currentTimes;
  }

  public void setCurrentTimes(Long currentTimes) {
    this.currentTimes = currentTimes;
  }

  public Map<Integer, AtomicInteger> getCommitCounts() {
    return commitCounts;
  }

  public Map<Integer, Object> getCommitLocks() {
    return commitLocks;
  }

  public Map<Integer, Roaring64NavigableMap> getCachedBlockIds() {
    return cachedBlockIds;
  }

  public String getUser() {
    return user.get();
  }

  public void setUser(String user) {
    this.user.set(user);
  }

  public int getMaxConcurrencyPerPartitionToWrite() {
    return specification.get().getMaxConcurrencyPerPartitionToWrite();
  }

  public ShuffleDataDistributionType getDataDistType() {
    return specification.get().getDistributionType();
  }

  public void setSpecification(ShuffleSpecification specification) {
    this.specification.set(specification);
  }

  public long addPartitionDataSize(int shuffleId, int partitionId, long delta) {
    totalDataSize.addAndGet(delta);
    inMemoryDataSize.addAndGet(delta);
    ShuffleDetailInfo shuffleDetailInfo =
        shuffleDetailInfos.computeIfAbsent(
            shuffleId, key -> new ShuffleDetailInfo(shuffleId, System.currentTimeMillis()));
    shuffleDetailInfo.incrDataSize(delta);
    partitionDataSizes.computeIfAbsent(shuffleId, key -> JavaUtils.newConcurrentMap());
    Map<Integer, Long> partitions = partitionDataSizes.get(shuffleId);
    partitions.computeIfAbsent(
        partitionId,
        k -> {
          shuffleDetailInfo.incrPartitionCount();
          return 0L;
        });
    return partitions.computeIfPresent(
        partitionId,
        (k, v) -> {
          long size = v + delta;
          if (size > maxSizePartitionInfo.getSize()) {
            maxSizePartitionInfo.update(
                partitionId, shuffleId, size, getBlockNumber(shuffleId, partitionId));
          }
          return size;
        });
  }

  public long getTotalDataSize() {
    return totalDataSize.get();
  }

  public long getInMemoryDataSize() {
    return inMemoryDataSize.get();
  }

  public long addOnLocalFileDataSize(long delta, boolean isNewlyCreated) {
    if (isNewlyCreated) {
      onLocalFileNum.incrementAndGet();
    }
    inMemoryDataSize.addAndGet(-delta);
    return onLocalFileDataSize.addAndGet(delta);
  }

  public long getOnLocalFileDataSize() {
    return onLocalFileDataSize.get();
  }

  public long addOnHadoopDataSize(long delta, boolean isNewlyCreated) {
    if (isNewlyCreated) {
      onHadoopDataSize.incrementAndGet();
    }
    inMemoryDataSize.addAndGet(-delta);
    return onHadoopDataSize.addAndGet(delta);
  }

  public long getOnHadoopDataSize() {
    return onHadoopDataSize.get();
  }

  public long getPartitionDataSize(int shuffleId, int partitionId) {
    Map<Integer, Long> partitions = partitionDataSizes.get(shuffleId);
    if (partitions == null) {
      return 0;
    }
    Long size = partitions.get(partitionId);
    if (size == null) {
      return 0L;
    }
    return size;
  }

  public boolean hasHugePartition() {
    return existHugePartition.get();
  }

  public int getHugePartitionSize() {
    return hugePartitionTags.values().stream().map(x -> x.size()).reduce((x, y) -> x + y).orElse(0);
  }

  public void markHugePartition(int shuffleId, int partitionId) {
    if (!existHugePartition.get()) {
      boolean markedWithCAS = existHugePartition.compareAndSet(false, true);
      if (markedWithCAS) {
        ShuffleServerMetrics.gaugeAppWithHugePartitionNum.inc();
        ShuffleServerMetrics.counterTotalAppWithHugePartitionNum.inc();
      }
    }

    Set<Integer> partitions =
        hugePartitionTags.computeIfAbsent(shuffleId, key -> Sets.newConcurrentHashSet());
    if (partitions.add(partitionId)) {
      ShuffleServerMetrics.counterTotalHugePartitionNum.inc();
      ShuffleServerMetrics.gaugeHugePartitionNum.inc();
      LOGGER.warn(
          "Huge partition occurs, appId: {}, shuffleId: {}, partitionId: {}",
          appId,
          shuffleId,
          partitionId);
    }
  }

  public boolean isHugePartition(int shuffleId, int partitionId) {
    return existHugePartition.get()
        && hugePartitionTags.containsKey(shuffleId)
        && hugePartitionTags.get(shuffleId).contains(partitionId);
  }

  public Set<Integer> getShuffleIds() {
    return partitionDataSizes.keySet();
  }

  public Set<Integer> getPartitionIds(int shuffleId) {
    return partitionDataSizes.get(shuffleId).keySet();
  }

  public void incBlockNumber(int shuffleId, int partitionId, int delta) {
    long blockCount =
        this.partitionBlockCounters
            .computeIfAbsent(shuffleId, x -> JavaUtils.newConcurrentMap())
            .computeIfAbsent(partitionId, x -> new AtomicLong())
            .addAndGet(delta);
    if (maxSizePartitionInfo.isCurrentPartition(shuffleId, partitionId)) {
      maxSizePartitionInfo.setBlockCount(blockCount);
    }
    shuffleDetailInfos
        .computeIfAbsent(
            shuffleId, key -> new ShuffleDetailInfo(shuffleId, System.currentTimeMillis()))
        .incrBlockCount(delta);
  }

  public long getBlockNumber(int shuffleId, int partitionId) {
    Map<Integer, AtomicLong> partitionBlockCounters = this.partitionBlockCounters.get(shuffleId);
    if (partitionBlockCounters == null) {
      return 0L;
    }
    AtomicLong counter = partitionBlockCounters.get(partitionId);
    if (counter == null) {
      return 0L;
    }
    return counter.get();
  }

  public Integer getLatestStageAttemptNumber(int shuffleId) {
    return latestStageAttemptNumbers.getOrDefault(shuffleId, 0);
  }

  public void refreshLatestStageAttemptNumber(int shuffleId, int stageAttemptNumber) {
    latestStageAttemptNumbers.put(shuffleId, stageAttemptNumber);
  }

  public PartitionInfo getMaxSizePartitionInfo() {
    return maxSizePartitionInfo;
  }

  public ShuffleDetailInfo getShuffleDetailInfo(int shuffleId) {
    return shuffleDetailInfos.get(shuffleId);
  }

  public long getPartitionNum() {
    return partitionDataSizes.values().stream().mapToLong(Map::size).sum();
  }

  @Override
  public String toString() {
    return "ShuffleTaskInfo{"
        + "appId='"
        + appId
        + '\''
        + ", totalDataSize="
        + UnitConverter.formatSize(totalDataSize.get())
        + ", inMemoryDataSize="
        + UnitConverter.formatSize(inMemoryDataSize.get())
        + ", onLocalFileDataSize="
        + UnitConverter.formatSize(onLocalFileDataSize.get())
        + ", onHadoopDataSize="
        + UnitConverter.formatSize(onHadoopDataSize.get())
        + ", maxSizePartitionInfo="
        + maxSizePartitionInfo
        + ", shuffleDetailInfo="
        + shuffleDetailInfos
        + '}';
  }

  public void setProperties(Map<String, String> properties) {
    Map<String, String> filteredProperties =
        properties.entrySet().stream()
            .filter(entry -> entry.getKey().contains(".rss."))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    this.properties = filteredProperties;
    LOGGER.info("{} set properties to {}", appId, filteredProperties);
    String keyName = RssClientConf.RSS_CLIENT_BLOCK_ID_MANAGER_CLASS.key();
    String className = properties.get(keyName);
    if (StringUtils.isEmpty(className)) {
      keyName =
          Constants.SPARK_RSS_CONFIG_PREFIX + RssClientConf.RSS_CLIENT_BLOCK_ID_MANAGER_CLASS.key();
      className =
          properties.get(
              Constants.SPARK_RSS_CONFIG_PREFIX
                  + RssClientConf.RSS_CLIENT_BLOCK_ID_MANAGER_CLASS.key());
    }
    if (StringUtils.isNotEmpty(className)) {
      shuffleBlockIdManager =
          ShuffleBlockIdManagerFactory.createShuffleBlockIdManager(className, keyName);
      LOGGER.info(
          "{} use app configured ShuffleBlockIdManager to {}", appId, shuffleBlockIdManager);
    }
  }

  public ShuffleBlockIdManager getShuffleBlockIdManager() {
    return shuffleBlockIdManager;
  }

  public void setShuffleBlockIdManagerIfNeeded(ShuffleBlockIdManager shuffleBlockIdManager) {
    if (this.shuffleBlockIdManager == null) {
      this.shuffleBlockIdManager = shuffleBlockIdManager;
    }
  }
}
