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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.exception.FileNotFoundException;
import org.apache.uniffle.common.exception.InvalidRequestException;
import org.apache.uniffle.common.exception.NoBufferException;
import org.apache.uniffle.common.exception.NoBufferForHugePartitionException;
import org.apache.uniffle.common.exception.NoRegisterException;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.future.CompletableFutureExtension;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.OutputUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.common.util.UnitConverter;
import org.apache.uniffle.server.block.ShuffleBlockIdManager;
import org.apache.uniffle.server.block.ShuffleBlockIdManagerFactory;
import org.apache.uniffle.server.buffer.PreAllocatedBufferInfo;
import org.apache.uniffle.server.buffer.ShuffleBuffer;
import org.apache.uniffle.server.buffer.ShuffleBufferManager;
import org.apache.uniffle.server.event.AppPurgeEvent;
import org.apache.uniffle.server.event.AppUnregisterPurgeEvent;
import org.apache.uniffle.server.event.PurgeEvent;
import org.apache.uniffle.server.event.ShufflePurgeEvent;
import org.apache.uniffle.server.merge.ShuffleMergeManager;
import org.apache.uniffle.server.storage.StorageManager;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.common.StorageReadMetrics;
import org.apache.uniffle.storage.request.CreateShuffleReadHandlerRequest;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.server.ShuffleServerConf.CLIENT_MAX_CONCURRENCY_LIMITATION_OF_ONE_PARTITION;
import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MAX_CONCURRENCY_OF_ONE_PARTITION;
import static org.apache.uniffle.server.ShuffleServerMetrics.CACHED_BLOCK_COUNT;
import static org.apache.uniffle.server.ShuffleServerMetrics.REPORTED_BLOCK_COUNT;
import static org.apache.uniffle.server.ShuffleServerMetrics.REQUIRE_BUFFER_COUNT;

public class ShuffleTaskManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleTaskManager.class);
  private final boolean storageTypeWithMemory;
  private ShuffleFlushManager shuffleFlushManager;
  private final ScheduledExecutorService scheduledExecutorService;
  private final ScheduledExecutorService expiredAppCleanupExecutorService;
  private final ScheduledExecutorService leakShuffleDataCheckExecutorService;
  private ScheduledExecutorService triggerFlushExecutorService;
  private final TopNShuffleDataSizeOfAppCalcTask topNShuffleDataSizeOfAppCalcTask;
  private StorageManager storageManager;
  private AtomicLong requireBufferId = new AtomicLong(0);
  private ShuffleServerConf conf;
  private long appExpiredWithoutHB;
  private long preAllocationExpired;
  private long commitCheckIntervalMax;
  private long leakShuffleDataCheckInterval;
  private long triggerFlushInterval;
  private final ShuffleBufferManager shuffleBufferManager;
  private Map<String, ShuffleTaskInfo> shuffleTaskInfos = JavaUtils.newConcurrentMap();
  private Map<Long, PreAllocatedBufferInfo> requireBufferIds = JavaUtils.newConcurrentMap();
  private Thread clearResourceThread;
  private BlockingQueue<PurgeEvent> expiredAppIdQueue = Queues.newLinkedBlockingQueue();
  private final Cache<String, ReentrantReadWriteLock> appLocks;
  private final long storageRemoveOperationTimeoutSec;
  private ShuffleMergeManager shuffleMergeManager;
  private ShuffleBlockIdManager shuffleBlockIdManager;

  public ShuffleTaskManager(
      ShuffleServerConf conf,
      ShuffleFlushManager shuffleFlushManager,
      ShuffleBufferManager shuffleBufferManager,
      StorageManager storageManager) {
    this(conf, shuffleFlushManager, shuffleBufferManager, storageManager, null);
  }

  public ShuffleTaskManager(
      ShuffleServerConf conf,
      ShuffleFlushManager shuffleFlushManager,
      ShuffleBufferManager shuffleBufferManager,
      StorageManager storageManager,
      ShuffleMergeManager shuffleMergeManager) {
    this.conf = conf;
    this.shuffleFlushManager = shuffleFlushManager;
    this.shuffleBufferManager = shuffleBufferManager;
    this.storageManager = storageManager;
    this.shuffleMergeManager = shuffleMergeManager;
    org.apache.uniffle.common.StorageType storageType =
        conf.get(ShuffleServerConf.RSS_STORAGE_TYPE);
    this.storageTypeWithMemory =
        storageType == null
            ? false
            : StorageType.withMemory(StorageType.valueOf(storageType.name()));
    this.appExpiredWithoutHB = conf.getLong(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT);
    this.commitCheckIntervalMax = conf.getLong(ShuffleServerConf.SERVER_COMMIT_CHECK_INTERVAL_MAX);
    this.preAllocationExpired = conf.getLong(ShuffleServerConf.SERVER_PRE_ALLOCATION_EXPIRED);
    this.storageRemoveOperationTimeoutSec =
        conf.getLong(ShuffleServerConf.STORAGE_REMOVE_RESOURCE_OPERATION_TIMEOUT_SEC);
    this.leakShuffleDataCheckInterval =
        conf.getLong(ShuffleServerConf.SERVER_LEAK_SHUFFLE_DATA_CHECK_INTERVAL);
    this.triggerFlushInterval = conf.getLong(ShuffleServerConf.SERVER_TRIGGER_FLUSH_CHECK_INTERVAL);
    // the thread for checking application status
    this.scheduledExecutorService =
        ThreadUtils.getDaemonSingleThreadScheduledExecutor("checkResource");
    scheduledExecutorService.scheduleAtFixedRate(
        this::preAllocatedBufferCheck,
        preAllocationExpired / 2,
        preAllocationExpired / 2,
        TimeUnit.MILLISECONDS);
    this.expiredAppCleanupExecutorService =
        ThreadUtils.getDaemonSingleThreadScheduledExecutor("expiredAppCleaner");
    expiredAppCleanupExecutorService.scheduleAtFixedRate(
        this::checkResourceStatus,
        appExpiredWithoutHB / 2,
        appExpiredWithoutHB / 2,
        TimeUnit.MILLISECONDS);
    this.leakShuffleDataCheckExecutorService =
        ThreadUtils.getDaemonSingleThreadScheduledExecutor("leakShuffleDataChecker");
    leakShuffleDataCheckExecutorService.scheduleAtFixedRate(
        this::checkLeakShuffleData,
        leakShuffleDataCheckInterval,
        leakShuffleDataCheckInterval,
        TimeUnit.MILLISECONDS);
    if (triggerFlushInterval > 0) {
      triggerFlushExecutorService =
          ThreadUtils.getDaemonSingleThreadScheduledExecutor("triggerShuffleBufferManagerFlush");
      triggerFlushExecutorService.scheduleWithFixedDelay(
          this::triggerFlush,
          triggerFlushInterval / 2,
          triggerFlushInterval,
          TimeUnit.MILLISECONDS);
    }
    if (shuffleBufferManager != null) {
      shuffleBufferManager.setShuffleTaskManager(this);
    }

    shuffleBlockIdManager = ShuffleBlockIdManagerFactory.createShuffleBlockIdManager(conf);

    appLocks =
        CacheBuilder.newBuilder()
            .expireAfterAccess(3600, TimeUnit.SECONDS)
            .maximumSize(Integer.MAX_VALUE)
            .build();

    // the thread for clear expired resources
    Runnable clearResourceRunnable =
        () -> {
          while (true) {
            PurgeEvent event = null;
            try {
              event = expiredAppIdQueue.take();
              long startTime = System.currentTimeMillis();
              if (event instanceof AppPurgeEvent) {
                removeResources(event.getAppId(), true);
                double usedTime =
                    (System.currentTimeMillis() - startTime) / Constants.MILLION_SECONDS_PER_SECOND;
                ShuffleServerMetrics.summaryTotalRemoveResourceTime.observe(usedTime);
              }
              if (event instanceof AppUnregisterPurgeEvent) {
                removeResources(event.getAppId(), false);
                double usedTime =
                    (System.currentTimeMillis() - startTime) / Constants.MILLION_SECONDS_PER_SECOND;
                ShuffleServerMetrics.summaryTotalRemoveResourceTime.observe(usedTime);
              }
              if (event instanceof ShufflePurgeEvent) {
                removeResourcesByShuffleIds(event.getAppId(), event.getShuffleIds());
                double usedTime =
                    (System.currentTimeMillis() - startTime) / Constants.MILLION_SECONDS_PER_SECOND;
                ShuffleServerMetrics.summaryTotalRemoveResourceByShuffleIdsTime.observe(usedTime);
              }
            } catch (Exception e) {
              StringBuilder diagnosticMessageBuilder =
                  new StringBuilder(
                      "Exception happened when clearing resource for expired application");
              if (event != null) {
                diagnosticMessageBuilder.append(" for appId: ");
                diagnosticMessageBuilder.append(event.getAppId());

                if (CollectionUtils.isNotEmpty(event.getShuffleIds())) {
                  diagnosticMessageBuilder.append(", shuffleIds: ");
                  diagnosticMessageBuilder.append(event.getShuffleIds());
                }
              }
              LOG.error("{}", diagnosticMessageBuilder, e);
            }
          }
        };
    clearResourceThread = new Thread(clearResourceRunnable);
    clearResourceThread.setName("clearResourceThread");
    clearResourceThread.setDaemon(true);

    topNShuffleDataSizeOfAppCalcTask = new TopNShuffleDataSizeOfAppCalcTask(this, conf);
    topNShuffleDataSizeOfAppCalcTask.start();

    ShuffleServerMetrics.addLabeledGauge(REQUIRE_BUFFER_COUNT, requireBufferIds::size);
    ShuffleServerMetrics.addLabeledCacheGauge(
        REPORTED_BLOCK_COUNT,
        () ->
            shuffleBlockIdManager.getTotalBlockCount()
                + shuffleTaskInfos.values().stream()
                    .map(ShuffleTaskInfo::getShuffleBlockIdManager)
                    .filter(manager -> manager != null && manager != shuffleBlockIdManager)
                    .mapToLong(ShuffleBlockIdManager::getTotalBlockCount)
                    .sum(),
        2 * 60 * 1000L /* 2 minutes */);
    ShuffleServerMetrics.addLabeledCacheGauge(
        CACHED_BLOCK_COUNT,
        () ->
            shuffleTaskInfos.values().stream()
                .map(ShuffleTaskInfo::getCachedBlockIds)
                .flatMap(map -> map.values().stream())
                .mapToLong(Roaring64NavigableMap::getLongCardinality)
                .sum(),
        2 * 60 * 1000L /* 2 minutes */);
  }

  public ReentrantReadWriteLock.WriteLock getAppWriteLock(String appId) {
    try {
      return appLocks.get(appId, ReentrantReadWriteLock::new).writeLock();
    } catch (ExecutionException e) {
      LOG.error("Failed to get App lock.", e);
      throw new RssException(e);
    }
  }

  public ReentrantReadWriteLock.ReadLock getAppReadLock(String appId) {
    try {
      return appLocks.get(appId, ReentrantReadWriteLock::new).readLock();
    } catch (ExecutionException e) {
      LOG.error("Failed to get App lock.", e);
      throw new RssException(e);
    }
  }

  /** Only for test */
  @VisibleForTesting
  public StatusCode registerShuffle(
      String appId,
      int shuffleId,
      List<PartitionRange> partitionRanges,
      RemoteStorageInfo remoteStorageInfo,
      String user) {
    return registerShuffle(
        appId,
        shuffleId,
        0,
        partitionRanges,
        remoteStorageInfo,
        user,
        ShuffleDataDistributionType.NORMAL,
        -1,
        Collections.emptyMap());
  }

  public StatusCode registerShuffle(
      String appId,
      int shuffleId,
      int stageAttemptNumber,
      List<PartitionRange> partitionRanges,
      RemoteStorageInfo remoteStorageInfo,
      String user,
      ShuffleDataDistributionType dataDistType,
      int maxConcurrencyPerPartitionToWrite,
      Map<String, String> properties) {
    ReentrantReadWriteLock.WriteLock lock = getAppWriteLock(appId);
    lock.lock();
    try {
      refreshAppId(appId);

      ShuffleTaskInfo taskInfo = shuffleTaskInfos.get(appId);
      taskInfo.setProperties(properties);
      taskInfo.setUser(user);
      taskInfo.setSpecification(
          ShuffleSpecification.builder()
              .maxConcurrencyPerPartitionToWrite(
                  getMaxConcurrencyWriting(maxConcurrencyPerPartitionToWrite, conf))
              .dataDistributionType(dataDistType)
              .build());
      taskInfo.setShuffleBlockIdManagerIfNeeded(shuffleBlockIdManager);
      taskInfo.refreshLatestStageAttemptNumber(shuffleId, stageAttemptNumber);
      taskInfo.getShuffleBlockIdManager().registerAppId(appId);
      for (PartitionRange partitionRange : partitionRanges) {
        shuffleBufferManager.registerBuffer(
            appId, shuffleId, partitionRange.getStart(), partitionRange.getEnd());
      }
      if (!remoteStorageInfo.isEmpty()) {
        storageManager.registerRemoteStorage(appId, remoteStorageInfo);
      }
      return StatusCode.SUCCESS;
    } finally {
      lock.unlock();
    }
  }

  @VisibleForTesting
  protected static int getMaxConcurrencyWriting(
      int maxConcurrencyPerPartitionToWrite, ShuffleServerConf conf) {
    if (maxConcurrencyPerPartitionToWrite > 0) {
      return Math.min(
          maxConcurrencyPerPartitionToWrite,
          conf.get(CLIENT_MAX_CONCURRENCY_LIMITATION_OF_ONE_PARTITION));
    }
    return conf.get(SERVER_MAX_CONCURRENCY_OF_ONE_PARTITION);
  }

  public StatusCode cacheShuffleData(
      String appId, int shuffleId, boolean isPreAllocated, ShufflePartitionedData spd) {
    refreshAppId(appId);
    long partitionSize = getPartitionDataSize(appId, shuffleId, spd.getPartitionId());
    long deltaSize = spd.getTotalBlockEncodedLength();
    partitionSize += deltaSize;
    // We do not need to check the huge partition size here, after old client upgraded to this
    // version,
    // since huge partition size is limited when requireBuffer is called.
    HugePartitionUtils.checkExceedPartitionHardLimit(
        "cacheShuffleData", shuffleBufferManager, partitionSize, deltaSize);
    return shuffleBufferManager.cacheShuffleData(appId, shuffleId, isPreAllocated, spd);
  }

  public PreAllocatedBufferInfo getAndRemovePreAllocatedBuffer(long requireBufferId) {
    return requireBufferIds.remove(requireBufferId);
  }

  public void releasePreAllocatedSize(long requireSize) {
    shuffleBufferManager.releasePreAllocatedSize(requireSize);
  }

  @VisibleForTesting
  void removeAndReleasePreAllocatedBuffer(long requireBufferId) {
    PreAllocatedBufferInfo info = getAndRemovePreAllocatedBuffer(requireBufferId);
    if (info != null) {
      releasePreAllocatedSize(info.getRequireSize());
    }
  }

  public StatusCode commitShuffle(String appId, int shuffleId) throws Exception {
    long start = System.currentTimeMillis();
    refreshAppId(appId);
    Roaring64NavigableMap cachedBlockIds = getCachedBlockIds(appId, shuffleId);
    Roaring64NavigableMap cloneBlockIds;
    ShuffleTaskInfo shuffleTaskInfo =
        shuffleTaskInfos.computeIfAbsent(appId, x -> new ShuffleTaskInfo(appId));
    Object lock = shuffleTaskInfo.getCommitLocks().computeIfAbsent(shuffleId, x -> new Object());
    synchronized (lock) {
      long commitTimeout = conf.get(ShuffleServerConf.SERVER_COMMIT_TIMEOUT);
      if (System.currentTimeMillis() - start > commitTimeout) {
        throw new RssException("Shuffle data commit timeout for " + commitTimeout + " ms");
      }
      synchronized (cachedBlockIds) {
        cloneBlockIds = RssUtils.cloneBitMap(cachedBlockIds);
      }
      long expectedCommitted = cloneBlockIds.getLongCardinality();
      shuffleBufferManager.commitShuffleTask(appId, shuffleId);
      Roaring64NavigableMap committedBlockIds;
      Roaring64NavigableMap cloneCommittedBlockIds;
      long checkInterval = 1000L;
      while (true) {
        committedBlockIds = shuffleFlushManager.getCommittedBlockIds(appId, shuffleId);
        synchronized (committedBlockIds) {
          cloneCommittedBlockIds = RssUtils.cloneBitMap(committedBlockIds);
        }
        cloneBlockIds.andNot(cloneCommittedBlockIds);
        if (cloneBlockIds.isEmpty()) {
          break;
        }
        Thread.sleep(checkInterval);
        if (System.currentTimeMillis() - start > commitTimeout) {
          throw new RssException("Shuffle data commit timeout for " + commitTimeout + " ms");
        }
        LOG.info(
            "Checking commit result for appId["
                + appId
                + "], shuffleId["
                + shuffleId
                + "], expect committed["
                + expectedCommitted
                + "], remain["
                + cloneBlockIds.getLongCardinality()
                + "]");
        checkInterval = Math.min(checkInterval * 2, commitCheckIntervalMax);
      }
      LOG.info(
          "Finish commit for appId["
              + appId
              + "], shuffleId["
              + shuffleId
              + "] with expectedCommitted["
              + expectedCommitted
              + "], cost "
              + (System.currentTimeMillis() - start)
              + " ms to check");
    }
    return StatusCode.SUCCESS;
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
      String appId, Integer shuffleId, Map<Integer, long[]> partitionToBlockIds, int bitmapNum) {
    refreshAppId(appId);
    ShuffleTaskInfo taskInfo = getShuffleTaskInfo(appId);
    if (taskInfo == null) {
      throw new InvalidRequestException(
          "ShuffleTaskInfo is not found that should not happen for appId: " + appId);
    }
    ShuffleBlockIdManager manager = taskInfo.getShuffleBlockIdManager();
    if (manager == null) {
      throw new RssException("appId[" + appId + "] is expired!");
    }
    return manager.addFinishedBlockIds(taskInfo, appId, shuffleId, partitionToBlockIds, bitmapNum);
  }

  public int updateAndGetCommitCount(String appId, int shuffleId) {
    ShuffleTaskInfo shuffleTaskInfo =
        shuffleTaskInfos.computeIfAbsent(appId, x -> new ShuffleTaskInfo(appId));
    AtomicInteger commitNum =
        shuffleTaskInfo.getCommitCounts().computeIfAbsent(shuffleId, x -> new AtomicInteger(0));
    return commitNum.incrementAndGet();
  }

  // Only for tests
  public void updateCachedBlockIds(String appId, int shuffleId, ShufflePartitionedBlock[] spbs) {
    updateCachedBlockIds(appId, shuffleId, 0, spbs);
  }

  public void updateCachedBlockIds(
      String appId, int shuffleId, int partitionId, ShufflePartitionedBlock[] spbs) {
    if (spbs == null || spbs.length == 0) {
      return;
    }
    ShuffleTaskInfo shuffleTaskInfo =
        shuffleTaskInfos.computeIfAbsent(appId, x -> new ShuffleTaskInfo(appId));
    long size = 0L;
    // With memory storage type should never need cachedBlockIds,
    // since client do not need call finish shuffle rpc
    if (!storageTypeWithMemory) {
      Roaring64NavigableMap bitmap =
          shuffleTaskInfo
              .getCachedBlockIds()
              .computeIfAbsent(shuffleId, x -> Roaring64NavigableMap.bitmapOf());

      synchronized (bitmap) {
        for (ShufflePartitionedBlock spb : spbs) {
          bitmap.addLong(spb.getBlockId());
          size += spb.getEncodedLength();
        }
      }
    } else {
      for (ShufflePartitionedBlock spb : spbs) {
        size += spb.getEncodedLength();
      }
    }
    long partitionSize = shuffleTaskInfo.addPartitionDataSize(shuffleId, partitionId, size);
    HugePartitionUtils.markHugePartition(
        shuffleBufferManager, shuffleTaskInfo, shuffleId, partitionId, partitionSize);
  }

  public Roaring64NavigableMap getCachedBlockIds(String appId, int shuffleId) {
    Map<Integer, Roaring64NavigableMap> shuffleIdToBlockIds =
        shuffleTaskInfos.getOrDefault(appId, new ShuffleTaskInfo(appId)).getCachedBlockIds();
    Roaring64NavigableMap blockIds = shuffleIdToBlockIds.get(shuffleId);
    if (blockIds == null) {
      LOG.warn(
          "Unexpected value when getCachedBlockIds for appId["
              + appId
              + "], shuffleId["
              + shuffleId
              + "]");
      return Roaring64NavigableMap.bitmapOf();
    }
    return blockIds;
  }

  public long getPartitionDataSize(String appId, int shuffleId, int partitionId) {
    ShuffleTaskInfo shuffleTaskInfo = shuffleTaskInfos.get(appId);
    if (shuffleTaskInfo == null) {
      return 0L;
    }
    return shuffleTaskInfo.getPartitionDataSize(shuffleId, partitionId);
  }

  /**
   * Require buffer for shuffle data
   *
   * @param appId the appId
   * @param shuffleId the shuffleId
   * @param partitionIds the partitionIds
   * @param partitionRequireSizes the partitionRequireSizes
   * @param requireSize the requireSize
   * @return returns (requireId, splitPartitionIds)
   */
  public Pair<Long, List<Integer>> requireBufferReturnPair(
      String appId,
      int shuffleId,
      List<Integer> partitionIds,
      List<Integer> partitionRequireSizes,
      int requireSize) {
    ShuffleTaskInfo shuffleTaskInfo = shuffleTaskInfos.get(appId);
    if (null == shuffleTaskInfo) {
      LOG.error("No such app is registered. appId: {}, shuffleId: {}", appId, shuffleId);
      throw new NoRegisterException("No such app is registered. appId: " + appId);
    }
    List<Integer> splitPartitionIds = new ArrayList<>();
    // To be compatible with legacy clients which have empty partitionRequireSizes
    if (partitionIds.size() == partitionRequireSizes.size()) {
      for (int i = 0; i < partitionIds.size(); i++) {
        int partitionId = partitionIds.get(i);
        int partitionRequireSize = partitionRequireSizes.get(i);
        long partitionUsedDataSize =
            getPartitionDataSize(appId, shuffleId, partitionId) + partitionRequireSize;
        if (HugePartitionUtils.limitHugePartition(
            shuffleBufferManager, appId, shuffleId, partitionId, partitionUsedDataSize)) {
          String errorMessage =
              String.format(
                  "Huge partition is limited to writing. appId: %s, shuffleId: %s, partitionIds: %s, partitionUsedDataSize: %s",
                  appId,
                  shuffleId,
                  OutputUtils.listToSegment(partitionIds, 10),
                  partitionUsedDataSize);
          LOG.error(errorMessage);
          throw new NoBufferForHugePartitionException(errorMessage);
        }
        HugePartitionUtils.checkExceedPartitionHardLimit(
            "requireBuffer", shuffleBufferManager, partitionUsedDataSize, partitionRequireSize);
        if (HugePartitionUtils.hasExceedPartitionSplitLimit(
            shuffleBufferManager, partitionUsedDataSize)) {
          LOG.info(
              "Need split partition. appId: {}, shuffleId: {}, partitionIds: {}, partitionUsedDataSize: {}",
              appId,
              shuffleId,
              partitionIds,
              partitionUsedDataSize);
          splitPartitionIds.add(partitionId);
          // We do not mind to reduce the partitionRequireSize from the requireSize for soft
          // partition split
        }
      }
    }
    return Pair.of(requireBuffer(appId, requireSize), splitPartitionIds);
  }

  @VisibleForTesting
  public long requireBuffer(
      String appId,
      int shuffleId,
      List<Integer> partitionIds,
      List<Integer> partitionRequireSizes,
      int requireSize) {
    return requireBufferReturnPair(
            appId, shuffleId, partitionIds, partitionRequireSizes, requireSize)
        .getLeft();
  }

  public long requireBuffer(String appId, int requireSize) {
    if (shuffleBufferManager.requireMemory(requireSize, true)) {
      long requireId = requireBufferId.incrementAndGet();
      requireBufferIds.put(
          requireId,
          new PreAllocatedBufferInfo(appId, requireId, System.currentTimeMillis(), requireSize));
      return requireId;
    } else {
      LOG.warn("Failed to require buffer, require size: {}", requireSize);
      throw new NoBufferException("No Buffer For Regular Partition, requireSize: " + requireSize);
    }
  }

  public long requireBuffer(int requireSize) {
    // appId of EMPTY means the client uses the old version that should be upgraded.
    return requireBuffer("EMPTY", requireSize);
  }

  public boolean requireMemory(int requireSize, boolean isPreAllocated) {
    return shuffleBufferManager.requireMemory(requireSize, isPreAllocated);
  }

  public void releaseMemory(
      int requireSize, boolean isReleaseFlushMemory, boolean isReleasePreAllocation) {
    shuffleBufferManager.releaseMemory(requireSize, isReleaseFlushMemory, isReleasePreAllocation);
  }

  public byte[] getFinishedBlockIds(
      String appId, Integer shuffleId, Set<Integer> partitions, BlockIdLayout blockIdLayout)
      throws IOException {
    refreshAppId(appId);
    for (int partitionId : partitions) {
      Map.Entry<Range<Integer>, ShuffleBuffer> entry =
          shuffleBufferManager.getShuffleBufferEntry(appId, shuffleId, partitionId);
      if (entry == null) {
        LOG.error(
            "The empty shuffle buffer, this should not happen. appId: {}, shuffleId: {}, partition: {}, layout: {}",
            appId,
            shuffleId,
            partitionId,
            blockIdLayout);
        continue;
      }
      Storage storage =
          storageManager.selectStorage(
              new ShuffleDataReadEvent(
                  appId, shuffleId, partitionId, entry.getKey().lowerEndpoint()));
      // update shuffle's timestamp that was recently read.
      if (storage != null) {
        storage.updateReadMetrics(new StorageReadMetrics(appId, shuffleId));
      }
    }
    ShuffleTaskInfo taskInfo = getShuffleTaskInfo(appId);
    ShuffleBlockIdManager manager = taskInfo.getShuffleBlockIdManager();
    if (manager == null) {
      throw new RssException("appId[" + appId + "] is expired!");
    }
    return manager.getFinishedBlockIds(taskInfo, appId, shuffleId, partitions, blockIdLayout);
  }

  public ShuffleDataResult getInMemoryShuffleData(
      String appId,
      Integer shuffleId,
      Integer partitionId,
      long blockId,
      int readBufferSize,
      Roaring64NavigableMap expectedTaskIds) {
    refreshAppId(appId);
    return shuffleBufferManager.getShuffleData(
        appId, shuffleId, partitionId, blockId, readBufferSize, expectedTaskIds);
  }

  public ShuffleDataResult getShuffleData(
      String appId,
      Integer shuffleId,
      Integer partitionId,
      int partitionNumPerRange,
      int partitionNum,
      String storageType,
      long offset,
      int length,
      int storageId) {
    refreshAppId(appId);

    CreateShuffleReadHandlerRequest request = new CreateShuffleReadHandlerRequest();
    request.setAppId(appId);
    request.setShuffleId(shuffleId);
    request.setPartitionId(partitionId);
    request.setPartitionNumPerRange(partitionNumPerRange);
    request.setPartitionNum(partitionNum);
    request.setStorageType(storageType);
    request.setRssBaseConf(conf);
    int[] range =
        ShuffleStorageUtils.getPartitionRange(partitionId, partitionNumPerRange, partitionNum);
    Storage storage =
        storageManager.selectStorage(
            new ShuffleDataReadEvent(appId, shuffleId, partitionId, range[0], storageId));
    if (storage == null) {
      throw new FileNotFoundException("No such data stored in current storage manager.");
    }

    // only one partition part in one storage
    try {
      return storage.getOrCreateReadHandler(request).getShuffleData(offset, length);
    } catch (FileNotFoundException e) {
      LOG.warn(
          "shuffle file not found {}-{}-{} in {}",
          appId,
          shuffleId,
          partitionId,
          storage.getStoragePath(),
          e);
      throw e;
    }
  }

  public ShuffleIndexResult getShuffleIndex(
      String appId,
      Integer shuffleId,
      Integer partitionId,
      int partitionNumPerRange,
      int partitionNum) {
    refreshAppId(appId);
    String storageType = conf.get(RssBaseConf.RSS_STORAGE_TYPE).name();
    CreateShuffleReadHandlerRequest request = new CreateShuffleReadHandlerRequest();
    request.setAppId(appId);
    request.setShuffleId(shuffleId);
    request.setPartitionId(partitionId);
    request.setPartitionNumPerRange(partitionNumPerRange);
    request.setPartitionNum(partitionNum);
    request.setStorageType(storageType);
    request.setRssBaseConf(conf);
    int[] range =
        ShuffleStorageUtils.getPartitionRange(partitionId, partitionNumPerRange, partitionNum);
    Storage storage =
        storageManager.selectStorageById(
            new ShuffleDataReadEvent(appId, shuffleId, partitionId, range[0]));
    if (storage == null) {
      throw new FileNotFoundException("No such data in current storage manager.");
    }
    ShuffleIndexResult result = storage.getOrCreateReadHandler(request).getShuffleIndex();
    if (result == null) {
      throw new FileNotFoundException("No such data in current storage manager.");
    }
    return result;
  }

  public void checkResourceStatus() {
    try {
      Set<String> appNames = Sets.newHashSet(shuffleTaskInfos.keySet());
      // remove applications which is timeout according to rss.server.app.expired.withoutHeartbeat
      for (String appId : appNames) {
        if (isAppExpired(appId)) {
          LOG.info(
              "Detect expired appId["
                  + appId
                  + "] according "
                  + "to rss.server.app.expired.withoutHeartbeat");
          expiredAppIdQueue.add(new AppPurgeEvent(appId, getUserByAppId(appId)));
        }
      }
      ShuffleServerMetrics.gaugeAppNum.set(shuffleTaskInfos.size());
    } catch (Exception e) {
      LOG.warn("Error happened in checkResourceStatus", e);
    }
  }

  public boolean isAppExpired(String appId) {
    ShuffleTaskInfo shuffleTaskInfo = shuffleTaskInfos.get(appId);
    if (shuffleTaskInfo == null) {
      return true;
    }
    return System.currentTimeMillis() - shuffleTaskInfo.getCurrentTimes() > appExpiredWithoutHB;
  }

  /**
   * Clear up the partial resources of shuffleIds of App.
   *
   * @param appId
   * @param shuffleIds
   */
  public void removeResourcesByShuffleIds(String appId, List<Integer> shuffleIds) {
    removeResourcesByShuffleIds(appId, shuffleIds, false);
  }

  public void removeResourcesByShuffleIds(
      String appId, List<Integer> shuffleIds, boolean isRenameAndDelete) {
    Lock writeLock = getAppWriteLock(appId);
    writeLock.lock();
    try {
      if (CollectionUtils.isEmpty(shuffleIds)) {
        return;
      }

      LOG.info("Start remove resource for appId[{}], shuffleIds[{}]", appId, shuffleIds);
      final long start = System.currentTimeMillis();
      final ShuffleTaskInfo taskInfo = shuffleTaskInfos.get(appId);
      if (taskInfo != null) {
        for (Integer shuffleId : shuffleIds) {
          taskInfo.getCachedBlockIds().remove(shuffleId);
          taskInfo.getCommitCounts().remove(shuffleId);
          taskInfo.getCommitLocks().remove(shuffleId);
        }
        ShuffleBlockIdManager manager = taskInfo.getShuffleBlockIdManager();
        if (manager == null) {
          throw new RssException("appId[" + appId + "] is expired!");
        }
        manager.removeBlockIdByShuffleId(appId, shuffleIds);
      } else {
        shuffleBlockIdManager.removeBlockIdByShuffleId(appId, shuffleIds);
      }
      shuffleBufferManager.removeBufferByShuffleId(appId, shuffleIds);
      shuffleFlushManager.removeResourcesOfShuffleId(appId, shuffleIds);

      String operationMsg =
          String.format("removing storage data for appId:%s, shuffleIds:%s", appId, shuffleIds);
      withTimeoutExecution(
          () -> {
            storageManager.removeResources(
                new ShufflePurgeEvent(appId, getUserByAppId(appId), shuffleIds, isRenameAndDelete));
            return null;
          },
          storageRemoveOperationTimeoutSec,
          operationMsg);
      if (shuffleMergeManager != null) {
        shuffleMergeManager.removeBuffer(appId, shuffleIds);
      }
      LOG.info(
          "Finish remove resource for appId[{}], shuffleIds[{}], cost[{}]",
          appId,
          shuffleIds,
          System.currentTimeMillis() - start);
    } finally {
      writeLock.unlock();
    }
  }

  public void checkLeakShuffleData() {
    LOG.info("Start check leak shuffle data");
    try {
      storageManager.checkAndClearLeakedShuffleData(
          () -> Sets.newHashSet(shuffleTaskInfos.keySet()));
      LOG.info("Finish check leak shuffle data");
    } catch (Exception e) {
      LOG.warn("Error happened in checkLeakShuffleData", e);
    }
  }

  @VisibleForTesting
  public void removeResources(String appId, boolean checkAppExpired) {
    Lock lock = getAppWriteLock(appId);
    lock.lock();
    try {
      LOG.info("Start remove resource for appId[" + appId + "]");
      if (checkAppExpired && !isAppExpired(appId)) {
        LOG.info(
            "It seems that this appId[{}] has registered a new shuffle, just ignore this AppPurgeEvent event.",
            appId);
        return;
      }
      final long start = System.currentTimeMillis();
      ShuffleTaskInfo shuffleTaskInfo = shuffleTaskInfos.remove(appId);
      if (shuffleTaskInfo == null) {
        LOG.info("Resource for appId[" + appId + "] had been removed before.");
        return;
      }

      LOG.info("Dump Removing app summary of {}", appId);
      StringBuilder partitionInfoSummary = new StringBuilder();
      partitionInfoSummary.append("appId: ").append(appId).append("\n");
      for (int shuffleId : shuffleTaskInfo.getShuffleIds()) {
        if (conf.getBoolean(ShuffleServerConf.SERVER_LOG_APP_DETAIL_WHILE_REMOVE_ENABLED)) {
          for (int partitionId : shuffleTaskInfo.getPartitionIds(shuffleId)) {
            long partitionSize = shuffleTaskInfo.getPartitionDataSize(shuffleId, partitionId);
            long partitionBlockCount = shuffleTaskInfo.getBlockNumber(shuffleId, partitionId);
            LOG.info(
                "shufflePartitionInfo(blockCount/size): {}-{}-{}: {}/{}",
                appId,
                shuffleId,
                partitionId,
                partitionBlockCount,
                UnitConverter.formatSize(partitionSize));
          }
        }
      }
      partitionInfoSummary.append("The app task info: ").append(shuffleTaskInfo);
      LOG.info("Removing app summary info: {}", partitionInfoSummary);

      ShuffleBlockIdManager manager = shuffleTaskInfo.getShuffleBlockIdManager();
      if (manager != null) {
        manager.removeBlockIdByAppId(appId);
      }
      shuffleBlockIdManager.removeBlockIdByAppId(appId);
      shuffleBufferManager.removeBuffer(appId);
      shuffleFlushManager.removeResources(appId);

      String operationMsg = String.format("removing storage data for appId:%s", appId);
      withTimeoutExecution(
          () -> {
            storageManager.removeResources(
                new AppPurgeEvent(
                    appId,
                    shuffleTaskInfo.getUser(),
                    new ArrayList<>(shuffleTaskInfo.getShuffleIds()),
                    checkAppExpired));
            return null;
          },
          storageRemoveOperationTimeoutSec,
          operationMsg);
      if (shuffleMergeManager != null) {
        shuffleMergeManager.removeBuffer(appId);
      }
      if (shuffleTaskInfo.hasHugePartition()) {
        ShuffleServerMetrics.gaugeAppWithHugePartitionNum.dec();
        ShuffleServerMetrics.gaugeHugePartitionNum.dec();
      }
      LOG.info(
          "Finish remove resource for appId["
              + appId
              + "] cost "
              + (System.currentTimeMillis() - start)
              + " ms");
    } finally {
      lock.unlock();
    }
  }

  private void withTimeoutExecution(
      Supplier supplier, long timeoutSec, String operationDetailedMsg) {
    CompletableFuture<Void> future =
        CompletableFuture.supplyAsync(supplier, Executors.newSingleThreadExecutor());
    CompletableFuture extended =
        CompletableFutureExtension.orTimeout(future, timeoutSec, TimeUnit.SECONDS);
    try {
      extended.get();
    } catch (Exception e) {
      if (e instanceof ExecutionException) {
        if (e.getCause() instanceof TimeoutException) {
          LOG.error(
              "Errors on finishing operation of [{}] in the {}(sec). This should not happen!",
              operationDetailedMsg,
              timeoutSec);
          return;
        }
        throw new RssException(e);
      }
    }
  }

  public void refreshAppId(String appId) {
    shuffleTaskInfos
        .computeIfAbsent(
            appId,
            x -> {
              ShuffleServerMetrics.counterTotalAppNum.inc();
              return new ShuffleTaskInfo(appId);
            })
        .setCurrentTimes(System.currentTimeMillis());
  }

  // check pre allocated buffer, release the memory if it expired
  private void preAllocatedBufferCheck() {
    try {
      long current = System.currentTimeMillis();
      List<Long> removeIds = Lists.newArrayList();
      for (PreAllocatedBufferInfo info : requireBufferIds.values()) {
        if (current - info.getTimestamp() > preAllocationExpired) {
          removeIds.add(info.getRequireId());
        }
      }
      for (Long requireId : removeIds) {
        PreAllocatedBufferInfo info = requireBufferIds.remove(requireId);
        if (info != null) {
          // move release memory code down to here as the requiredBuffer could be consumed during
          // removing processing.
          shuffleBufferManager.releaseMemory(info.getRequireSize(), false, true);
          LOG.warn(
              "Remove expired preAllocatedBuffer[id={}] that required by app: {}",
              requireId,
              info.getAppId());
          ShuffleServerMetrics.counterPreAllocatedBufferExpired.inc();
        } else {
          LOG.info("PreAllocatedBuffer[id={}] has already be used", requireId);
        }
      }
    } catch (Exception e) {
      LOG.warn("Error happened in preAllocatedBufferCheck", e);
    }
  }

  public int getRequireBufferSize(long requireId) {
    PreAllocatedBufferInfo pabi = requireBufferIds.get(requireId);
    if (pabi == null) {
      return 0;
    }
    return pabi.getRequireSize();
  }

  public String getUserByAppId(String appId) {
    return shuffleTaskInfos.computeIfAbsent(appId, x -> new ShuffleTaskInfo(appId)).getUser();
  }

  @VisibleForTesting
  public Set<String> getAppIds() {
    return shuffleTaskInfos.keySet();
  }

  @VisibleForTesting
  Map<Long, PreAllocatedBufferInfo> getRequireBufferIds() {
    return requireBufferIds;
  }

  public void removeShuffleDataAsync(String appId, int shuffleId) {
    expiredAppIdQueue.add(
        new ShufflePurgeEvent(appId, getUserByAppId(appId), Arrays.asList(shuffleId)));
  }

  public void removeShuffleDataAsync(String appId) {
    expiredAppIdQueue.add(new AppUnregisterPurgeEvent(appId, getUserByAppId(appId)));
  }

  @VisibleForTesting
  public void removeShuffleDataSync(String appId, int shuffleId) {
    removeResourcesByShuffleIds(appId, Arrays.asList(shuffleId));
  }

  public void removeShuffleDataSyncRenameAndDelete(String appId, int shuffleId) {
    removeResourcesByShuffleIds(appId, Arrays.asList(shuffleId), true);
  }

  public ShuffleDataDistributionType getDataDistributionType(String appId) {
    return shuffleTaskInfos.get(appId).getDataDistType();
  }

  @VisibleForTesting
  public ShuffleTaskInfo getShuffleTaskInfo(String appId) {
    return shuffleTaskInfos.get(appId);
  }

  private void triggerFlush() {
    synchronized (this.shuffleBufferManager) {
      this.shuffleBufferManager.flushIfNecessary();
    }
  }

  public Map<String, ShuffleTaskInfo> getShuffleTaskInfos() {
    return shuffleTaskInfos;
  }

  public void stop() {
    topNShuffleDataSizeOfAppCalcTask.stop();
  }

  public void start() {
    clearResourceThread.start();
  }

  // only for tests
  @VisibleForTesting
  protected void setStorageManager(StorageManager storageManager) {
    this.storageManager = storageManager;
  }

  // only for tests
  @VisibleForTesting
  protected void setShuffleFlushManager(ShuffleFlushManager flushManager) {
    this.shuffleFlushManager = flushManager;
  }

  @VisibleForTesting
  public ShuffleBlockIdManager getShuffleBlockIdManager() {
    return shuffleBlockIdManager;
  }
}
