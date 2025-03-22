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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.AuditType;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.server.flush.EventDiscardException;
import org.apache.uniffle.server.flush.EventInvalidException;
import org.apache.uniffle.server.flush.EventRetryException;
import org.apache.uniffle.server.storage.StorageManager;
import org.apache.uniffle.storage.common.LocalStorage;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.handler.api.ShuffleWriteHandlerWrapper;
import org.apache.uniffle.storage.request.CreateShuffleWriteHandlerRequest;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MAX_CONCURRENCY_OF_ONE_PARTITION;
import static org.apache.uniffle.server.ShuffleServerMetrics.COMMITTED_BLOCK_COUNT;

public class ShuffleFlushManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleFlushManager.class);
  private static final Logger AUDIT_LOGGER =
      LoggerFactory.getLogger("SHUFFLE_SERVER_STORAGE_AUDIT_LOG");
  private static final String AUDIT_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
  public static final AtomicLong ATOMIC_EVENT_ID = new AtomicLong(0);
  private final ShuffleServer shuffleServer;
  private final List<String> storageBasePaths;
  private final String storageType;
  private final int storageDataReplica;
  private final ShuffleServerConf shuffleServerConf;
  private final boolean storageTypeWithMemory;
  private Configuration hadoopConf;
  // appId -> shuffleId -> committed shuffle blockIds
  private Map<String, Map<Integer, Roaring64NavigableMap>> committedBlockIds =
      JavaUtils.newConcurrentMap();
  private final int retryMax;

  private final StorageManager storageManager;
  private final long pendingEventTimeoutSec;
  private FlushEventHandler eventHandler;
  private boolean isStorageAuditLogEnabled;

  public ShuffleFlushManager(
      ShuffleServerConf shuffleServerConf,
      ShuffleServer shuffleServer,
      StorageManager storageManager) {
    this.shuffleServer = shuffleServer;
    this.shuffleServerConf = shuffleServerConf;
    this.storageManager = storageManager;
    initHadoopConf();
    retryMax = shuffleServerConf.getInteger(ShuffleServerConf.SERVER_WRITE_RETRY_MAX);
    storageType = shuffleServerConf.get(RssBaseConf.RSS_STORAGE_TYPE).name();
    storageDataReplica = shuffleServerConf.get(RssBaseConf.RSS_STORAGE_DATA_REPLICA);

    storageBasePaths = RssUtils.getConfiguredLocalDirs(shuffleServerConf);
    pendingEventTimeoutSec = shuffleServerConf.getLong(ShuffleServerConf.PENDING_EVENT_TIMEOUT_SEC);
    eventHandler =
        new DefaultFlushEventHandler(
            shuffleServerConf, storageManager, shuffleServer, this::processFlushEvent);
    isStorageAuditLogEnabled =
        this.shuffleServerConf.getBoolean(ShuffleServerConf.SERVER_STORAGE_AUDIT_LOG_ENABLED);

    ShuffleServerMetrics.addLabeledCacheGauge(
        COMMITTED_BLOCK_COUNT,
        () ->
            committedBlockIds.values().stream()
                .flatMap(innerMap -> innerMap.values().stream())
                .mapToLong(bitmap -> bitmap.getLongCardinality())
                .sum(),
        2 * 60 * 1000L /* 2 minutes */);
    this.storageTypeWithMemory = StorageType.withMemory(StorageType.valueOf(storageType));
  }

  public void addToFlushQueue(ShuffleDataFlushEvent event) {
    eventHandler.handle(event);
  }

  /**
   * The method to handle flush event to flush blocks into persistent storage. And we will not
   * change any internal state for event, that means the event is read-only for this processing.
   *
   * <p>Only the blocks are flushed successfully, it can return directly, otherwise it should always
   * throw dedicated exception.
   *
   * @param event
   * @throws Exception
   */
  public void processFlushEvent(ShuffleDataFlushEvent event) throws Exception {
    try {
      ShuffleServerMetrics.gaugeWriteHandler.inc();

      if (!event.isValid()) {
        LOG.warn(
            "AppId {} was removed already, event:{} should be dropped", event.getAppId(), event);
        // we should catch this to avoid cleaning up duplicate.
        throw new EventInvalidException();
      }

      if (reachRetryMax(event)) {
        LOG.error("The event:{} has been reached to max retry times, it will be dropped.", event);
        throw new EventDiscardException();
      }

      Collection<ShufflePartitionedBlock> blocks = event.getShuffleBlocks();
      if (CollectionUtils.isEmpty(blocks)) {
        LOG.info("There is no block to be flushed: {}", event);
        return;
      }

      Storage storage = event.getUnderStorage();
      if (storage == null) {
        LOG.error("Storage selected is null and this should not happen. event: {}", event);
        throw new EventDiscardException();
      }

      if (event.isPended()
          && System.currentTimeMillis() - event.getStartPendingTime()
              > pendingEventTimeoutSec * 1000L) {
        LOG.error(
            "Flush event cannot be flushed for {} sec, the event {} is dropped",
            pendingEventTimeoutSec,
            event);
        throw new EventDiscardException();
      }

      if (!storage.canWrite()) {
        LOG.error(
            "The event: {} is limited to flush due to storage:{} can't write", event, storage);
        throw new EventRetryException();
      }

      String user =
          StringUtils.defaultString(
              shuffleServer.getShuffleTaskManager().getUserByAppId(event.getAppId()),
              StringUtils.EMPTY);
      int maxConcurrencyPerPartitionToWrite = getMaxConcurrencyPerPartitionWrite(event);
      CreateShuffleWriteHandlerRequest request =
          new CreateShuffleWriteHandlerRequest(
              this.shuffleServerConf,
              storageType,
              event.getAppId(),
              event.getShuffleId(),
              event.getStartPartition(),
              event.getEndPartition(),
              storageBasePaths.toArray(new String[storageBasePaths.size()]),
              getShuffleServerId(),
              hadoopConf,
              storageDataReplica,
              user,
              maxConcurrencyPerPartitionToWrite);
      ShuffleWriteHandlerWrapper handlerWrapper;
      try {
        handlerWrapper = storage.getOrCreateWriteHandler(request);
      } catch (Exception e) {
        LOG.warn("Failed to create write handlerWrapper for event: {}", event, e);
        throw new EventRetryException(e);
      }

      long startTime = System.currentTimeMillis();
      boolean writeSuccess = storageManager.write(storage, handlerWrapper.getHandler(), event);
      if (!writeSuccess) {
        throw new EventRetryException();
      }
      long endTime = System.currentTimeMillis();
      ShuffleTaskInfo shuffleTaskInfo =
          shuffleServer.getShuffleTaskManager().getShuffleTaskInfo(event.getAppId());
      if (shuffleTaskInfo == null || !storageTypeWithMemory) {
        // With memory storage type should never need cachedBlockIds,
        // since client do not need call finish shuffle rpc
        // update some metrics for shuffle task
        updateCommittedBlockIds(event.getAppId(), event.getShuffleId(), event.getShuffleBlocks());
      }
      if (isStorageAuditLogEnabled) {
        AUDIT_LOGGER.info(
            String.format(
                "%s|%s|%d|%s|%s|%s|%d|%s|%s|%d",
                AuditType.WRITE.getValue(),
                event.getAppId(),
                event.getShuffleId(),
                event.getStartPartition() + "_" + event.getEndPartition(),
                event.getUnderStorage().getStorageHost(),
                event.getUnderStorage().getStoragePath(),
                event.getDataLength(),
                DateFormatUtils.format(startTime, AUDIT_DATE_PATTERN),
                DateFormatUtils.format(endTime, AUDIT_DATE_PATTERN),
                endTime - startTime));
      }
      if (null != shuffleTaskInfo) {
        String storageHost = event.getUnderStorage().getStorageHost();
        if (LocalStorage.STORAGE_HOST.equals(storageHost)) {
          shuffleTaskInfo.addOnLocalFileDataSize(
              event.getEncodedLength(), handlerWrapper.isNewlyCreated());
        } else {
          shuffleTaskInfo.addOnHadoopDataSize(
              event.getEncodedLength(), handlerWrapper.isNewlyCreated());
        }
      }
    } finally {
      ShuffleServerMetrics.gaugeWriteHandler.dec();
    }
  }

  private boolean reachRetryMax(ShuffleDataFlushEvent event) {
    return event.getRetryTimes() > retryMax;
  }

  private int getMaxConcurrencyPerPartitionWrite(ShuffleDataFlushEvent event) {
    ShuffleTaskInfo taskInfo =
        shuffleServer.getShuffleTaskManager().getShuffleTaskInfo(event.getAppId());
    // For some tests.
    if (taskInfo == null) {
      LOG.warn("Should not happen that shuffle task info of {} is null.", event.getAppId());
      return shuffleServerConf.get(SERVER_MAX_CONCURRENCY_OF_ONE_PARTITION);
    }
    return taskInfo.getMaxConcurrencyPerPartitionToWrite();
  }

  private String getShuffleServerId() {
    return shuffleServerConf.getString(ShuffleServerConf.SHUFFLE_SERVER_ID, "shuffleServerId");
  }

  private void updateCommittedBlockIds(
      String appId, int shuffleId, Collection<ShufflePartitionedBlock> blocks) {
    if (blocks == null || blocks.size() == 0) {
      return;
    }
    committedBlockIds.computeIfAbsent(appId, key -> JavaUtils.newConcurrentMap());
    Map<Integer, Roaring64NavigableMap> shuffleToBlockIds = committedBlockIds.get(appId);
    shuffleToBlockIds.computeIfAbsent(shuffleId, key -> Roaring64NavigableMap.bitmapOf());
    Roaring64NavigableMap bitmap = shuffleToBlockIds.get(shuffleId);
    synchronized (bitmap) {
      for (ShufflePartitionedBlock spb : blocks) {
        bitmap.addLong(spb.getBlockId());
      }
    }
  }

  public Roaring64NavigableMap getCommittedBlockIds(String appId, Integer shuffleId) {
    Map<Integer, Roaring64NavigableMap> shuffleIdToBlockIds = committedBlockIds.get(appId);
    if (shuffleIdToBlockIds == null) {
      LOG.warn("Unexpected value when getCommittedBlockIds for appId[" + appId + "]");
      return Roaring64NavigableMap.bitmapOf();
    }
    Roaring64NavigableMap blockIds = shuffleIdToBlockIds.get(shuffleId);
    if (blockIds == null) {
      LOG.warn(
          "Unexpected value when getCommittedBlockIds for appId["
              + appId
              + "], shuffleId["
              + shuffleId
              + "]");
      return Roaring64NavigableMap.bitmapOf();
    }
    return blockIds;
  }

  public void removeResources(String appId) {
    committedBlockIds.remove(appId);
  }

  protected void initHadoopConf() {
    hadoopConf = new Configuration();
    for (String key : shuffleServerConf.getKeySet()) {
      if (key.startsWith(ShuffleServerConf.PREFIX_HADOOP_CONF)) {
        String value = shuffleServerConf.getString(key, "");
        String hadoopKey = key.substring(ShuffleServerConf.PREFIX_HADOOP_CONF.length() + 1);
        LOG.info("Update hadoop configuration:" + hadoopKey + "=" + value);
        hadoopConf.set(hadoopKey, value);
      }
    }
  }

  public int getEventNumInFlush() {
    return eventHandler.getEventNumInFlush();
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public void removeResourcesOfShuffleId(String appId, Collection<Integer> shuffleIds) {
    Optional.ofNullable(committedBlockIds.get(appId))
        .ifPresent(shuffleIdToBlockIds -> shuffleIds.forEach(shuffleIdToBlockIds::remove));
  }

  public ShuffleDataDistributionType getDataDistributionType(String appId) {
    return shuffleServer.getShuffleTaskManager().getDataDistributionType(appId);
  }

  @VisibleForTesting
  public FlushEventHandler getEventHandler() {
    return eventHandler;
  }
}
