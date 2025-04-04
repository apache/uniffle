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

package org.apache.uniffle.server.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.AuditType;
import org.apache.uniffle.common.ReconfigurableRegistry;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.UnionKey;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.storage.StorageInfo;
import org.apache.uniffle.common.storage.StorageMedia;
import org.apache.uniffle.common.storage.StorageStatus;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.server.Checker;
import org.apache.uniffle.server.LocalStorageChecker;
import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleDataReadEvent;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.server.event.AppPurgeEvent;
import org.apache.uniffle.server.event.PurgeEvent;
import org.apache.uniffle.server.event.ShufflePurgeEvent;
import org.apache.uniffle.storage.common.LocalStorage;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.common.StorageMediaProvider;
import org.apache.uniffle.storage.factory.ShuffleHandlerFactory;
import org.apache.uniffle.storage.handler.api.ShuffleDeleteHandler;
import org.apache.uniffle.storage.request.CreateShuffleDeleteHandlerRequest;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.server.ShuffleServerConf.DISK_CAPACITY_WATERMARK_CHECK_ENABLED;
import static org.apache.uniffle.server.ShuffleServerConf.LOCAL_STORAGE_INITIALIZE_MAX_FAIL_NUMBER;

public class LocalStorageManager extends SingleStorageManager {
  private static final Logger LOG = LoggerFactory.getLogger(LocalStorageManager.class);
  private static final Logger AUDIT_LOGGER =
      LoggerFactory.getLogger("SHUFFLE_SERVER_STORAGE_AUDIT_LOG");
  private static final String UNKNOWN_USER_NAME = "unknown";

  private final List<LocalStorage> localStorages;
  private final List<String> storageBasePaths;
  private final LocalStorageChecker checker;

  private final ConcurrentSkipListMap<String, LocalStorage> sortedPartitionsOfStorageMap;
  private final List<StorageMediaProvider> typeProviders = Lists.newArrayList();

  private boolean isStorageAuditLogEnabled;

  @VisibleForTesting
  public LocalStorageManager(ShuffleServerConf conf) {
    super(conf);
    storageBasePaths = RssUtils.getConfiguredLocalDirs(conf);
    if (CollectionUtils.isEmpty(storageBasePaths)) {
      throw new IllegalArgumentException("Base path dirs must not be empty");
    }
    this.sortedPartitionsOfStorageMap = new ConcurrentSkipListMap<>();
    long capacity = conf.getSizeAsBytes(ShuffleServerConf.DISK_CAPACITY);
    double ratio = conf.getDouble(ShuffleServerConf.DISK_CAPACITY_RATIO);
    double highWaterMarkOfWrite = conf.get(ShuffleServerConf.HIGH_WATER_MARK_OF_WRITE);
    double lowWaterMarkOfWrite = conf.get(ShuffleServerConf.LOW_WATER_MARK_OF_WRITE);
    if (highWaterMarkOfWrite < lowWaterMarkOfWrite) {
      throw new IllegalArgumentException(
          "highWaterMarkOfWrite must be larger than lowWaterMarkOfWrite");
    }

    // We must make sure the order of `storageBasePaths` and `localStorages` is same, or some unit
    // test may be fail
    CountDownLatch countDownLatch = new CountDownLatch(storageBasePaths.size());
    AtomicInteger successCount = new AtomicInteger();
    ServiceLoader<StorageMediaProvider> loader = ServiceLoader.load(StorageMediaProvider.class);
    for (StorageMediaProvider provider : loader) {
      provider.init(conf);
      typeProviders.add(provider);
    }
    ExecutorService executorService = ThreadUtils.getDaemonCachedThreadPool("LocalStorage-check");
    LocalStorage[] localStorageArray = new LocalStorage[storageBasePaths.size()];
    boolean isDiskCapacityWatermarkCheckEnabled = conf.get(DISK_CAPACITY_WATERMARK_CHECK_ENABLED);
    for (int i = 0; i < storageBasePaths.size(); i++) {
      final int idx = i;
      String storagePath = storageBasePaths.get(i);
      executorService.submit(
          () -> {
            try {
              StorageMedia storageType = getStorageTypeForBasePath(storagePath);
              LocalStorage.Builder builder =
                  LocalStorage.newBuilder()
                      .basePath(storagePath)
                      .capacity(capacity)
                      .ratio(ratio)
                      .lowWaterMarkOfWrite(lowWaterMarkOfWrite)
                      .highWaterMarkOfWrite(highWaterMarkOfWrite)
                      .setId(idx)
                      .localStorageMedia(storageType);
              if (isDiskCapacityWatermarkCheckEnabled) {
                builder.enableDiskCapacityWatermarkCheck();
              }
              localStorageArray[idx] = builder.build();
              successCount.incrementAndGet();
            } catch (Exception e) {
              LOG.error("LocalStorage init failed!", e);
            } finally {
              countDownLatch.countDown();
            }
          });
    }

    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      LOG.error("Failed to wait initializing local storage.", e);
    }
    executorService.shutdown();

    int failedCount = storageBasePaths.size() - successCount.get();
    long maxFailedNumber = conf.getLong(LOCAL_STORAGE_INITIALIZE_MAX_FAIL_NUMBER);
    if (failedCount > maxFailedNumber || successCount.get() == 0) {
      throw new RssException(
          String.format(
              "Initialize %s local storage(s) failed, "
                  + "specified local storage paths size: %s, the conf of %s size: %s",
              failedCount,
              localStorageArray.length,
              LOCAL_STORAGE_INITIALIZE_MAX_FAIL_NUMBER.key(),
              maxFailedNumber));
    }
    localStorages =
        Arrays.stream(localStorageArray).filter(Objects::nonNull).collect(Collectors.toList());
    LOG.info(
        "Succeed to initialize storage paths: {}",
        StringUtils.join(
            localStorages.stream().map(LocalStorage::getBasePath).collect(Collectors.toList())));
    this.checker = new LocalStorageChecker(conf, localStorages);
    isStorageAuditLogEnabled =
        conf.getReconfigurableConf(ShuffleServerConf.SERVER_STORAGE_AUDIT_LOG_ENABLED).get();
    ReconfigurableRegistry.register(
        ShuffleServerConf.SERVER_STORAGE_AUDIT_LOG_ENABLED.toString(),
        (rssConf, changedProperties) -> {
          if (changedProperties == null || rssConf == null) {
            return;
          }
          if (changedProperties.contains(
              ShuffleServerConf.SERVER_STORAGE_AUDIT_LOG_ENABLED.key())) {
            isStorageAuditLogEnabled =
                rssConf.getBoolean(ShuffleServerConf.SERVER_STORAGE_AUDIT_LOG_ENABLED);
          }
        });
  }

  private StorageMedia getStorageTypeForBasePath(String basePath) {
    for (StorageMediaProvider provider : this.typeProviders) {
      StorageMedia result = provider.getStorageMediaFor(basePath);
      if (result != StorageMedia.UNKNOWN) {
        return result;
      }
    }
    return StorageMedia.UNKNOWN;
  }

  @Override
  public Storage selectStorage(ShuffleDataFlushEvent event) {
    if (localStorages.size() == 1) {
      if (event.getUnderStorage() == null) {
        event.setUnderStorage(localStorages.get(0));
      }
      return localStorages.get(0);
    }
    String appId = event.getAppId();
    int shuffleId = event.getShuffleId();
    int partitionId = event.getStartPartition();

    LocalStorage storage =
        sortedPartitionsOfStorageMap.get(UnionKey.buildKey(appId, shuffleId, partitionId));
    if (storage != null) {
      if (storage.isCorrupted()) {
        if (storage.containsWriteHandler(appId, shuffleId, partitionId)) {
          LOG.error(
              "LocalStorage: {} is corrupted. Switching another storage for event: {}, some data will be lost",
              storage.getBasePath(),
              event);
        }
      } else {
        if (event.getUnderStorage() == null) {
          event.setUnderStorage(storage);
        }
        return storage;
      }
    }

    List<LocalStorage> candidates =
        localStorages.stream()
            .filter(x -> x.canWrite() && !x.isCorrupted())
            .collect(Collectors.toList());

    if (candidates.size() == 0) {
      return null;
    }
    final LocalStorage selectedStorage =
        candidates.get(
            ShuffleStorageUtils.getStorageIndex(candidates.size(), appId, shuffleId, partitionId));
    return sortedPartitionsOfStorageMap.compute(
        UnionKey.buildKey(appId, shuffleId, partitionId),
        (key, localStorage) -> {
          // If this is the first time to select storage or existing storage is corrupted,
          // we should refresh the cache.
          if (localStorage == null
              || localStorage.isCorrupted()
              || event.getUnderStorage() == null) {
            event.setUnderStorage(selectedStorage);
            return selectedStorage;
          }
          return localStorage;
        });
  }

  @Override
  public Storage selectStorage(ShuffleDataReadEvent event) {
    if (localStorages.size() == 1) {
      return localStorages.get(0);
    }
    String appId = event.getAppId();
    int shuffleId = event.getShuffleId();
    int partitionId = event.getStartPartition();

    return sortedPartitionsOfStorageMap.get(UnionKey.buildKey(appId, shuffleId, partitionId));
  }

  @Override
  public void updateWriteMetrics(ShuffleDataFlushEvent event, long writeTime) {
    super.updateWriteMetrics(event, writeTime);
    ShuffleServerMetrics.counterTotalLocalFileWriteDataSize
        .labels(ShuffleServerMetrics.LOCAL_DISK_PATH_LABEL_ALL)
        .inc(event.getDataLength());
    if (event.getUnderStorage() != null) {
      ShuffleServerMetrics.counterTotalLocalFileWriteDataSize
          .labels(event.getUnderStorage().getStoragePath())
          .inc(event.getDataLength());
    }
  }

  @Override
  public Checker getStorageChecker() {
    return checker;
  }

  @Override
  public void removeResources(PurgeEvent event) {
    String appId = event.getAppId();
    String user = event.getUser();
    List<Integer> shuffleSet =
        Optional.ofNullable(event.getShuffleIds()).orElse(Collections.emptyList());

    // Remove partitions to storage mapping cache
    cleanupStorageSelectionCache(event);

    for (LocalStorage storage : localStorages) {
      if (event instanceof AppPurgeEvent) {
        storage.removeHandlers(appId);
      }
      for (Integer shuffleId : shuffleSet) {
        storage.removeResources(RssUtils.generateShuffleKey(appId, shuffleId));
      }
    }
    // delete shuffle data for application
    ShuffleDeleteHandler deleteHandler =
        ShuffleHandlerFactory.getInstance()
            .createShuffleDeleteHandler(
                new CreateShuffleDeleteHandlerRequest(
                    StorageType.LOCALFILE.name(), new Configuration(), event.isRenameAndDelete()));

    List<String> deletePaths =
        localStorages.stream()
            .filter(x -> !x.isCorrupted())
            .map(x -> x.getBasePath())
            .flatMap(
                path -> {
                  String basicPath = ShuffleStorageUtils.getFullShuffleDataFolder(path, appId);
                  if (event instanceof ShufflePurgeEvent) {
                    List<String> paths = new ArrayList<>();
                    for (int shuffleId : shuffleSet) {
                      paths.add(
                          ShuffleStorageUtils.getFullShuffleDataFolder(
                              basicPath, String.valueOf(shuffleId)));
                    }
                    if (isStorageAuditLogEnabled) {
                      AUDIT_LOGGER.info(
                          String.format(
                              "%s|%s|%s|%s|%s",
                              AuditType.DELETE.getValue(),
                              appId,
                              shuffleSet.stream()
                                  .map(Object::toString)
                                  .collect(Collectors.joining(",")),
                              LocalStorage.STORAGE_HOST,
                              path));
                    }
                    return paths.stream();
                  } else {
                    if (isStorageAuditLogEnabled) {
                      AUDIT_LOGGER.info(
                          String.format(
                              "%s|%s|%s|%s",
                              AuditType.DELETE.getValue(), appId, LocalStorage.STORAGE_HOST, path));
                    }
                    return Stream.of(basicPath);
                  }
                })
            .collect(Collectors.toList());

    boolean isSuccess =
        deleteHandler.delete(deletePaths.toArray(new String[deletePaths.size()]), appId, user);
    if (!isSuccess && event.isRenameAndDelete()) {
      ShuffleServerMetrics.counterLocalRenameAndDeletionFaileTd.inc();
    }
    removeAppStorageInfo(event);
  }

  private void cleanupStorageSelectionCache(PurgeEvent event) {
    Function<String, Boolean> deleteConditionFunc = null;
    String prefixKey = null;
    if (event instanceof AppPurgeEvent) {
      prefixKey = UnionKey.buildKey(event.getAppId(), "");
      deleteConditionFunc =
          partitionUnionKey -> UnionKey.startsWith(partitionUnionKey, event.getAppId());
    } else if (event instanceof ShufflePurgeEvent) {
      int shuffleId = event.getShuffleIds().get(0);
      prefixKey = UnionKey.buildKey(event.getAppId(), shuffleId, "");
      deleteConditionFunc =
          partitionUnionKey -> UnionKey.startsWith(partitionUnionKey, event.getAppId(), shuffleId);
    }
    if (prefixKey == null) {
      throw new RssException("Prefix key is null when handles event: " + event);
    }
    long startTime = System.currentTimeMillis();
    deleteElement(sortedPartitionsOfStorageMap.tailMap(prefixKey), deleteConditionFunc);
    LOG.info(
        "Cleaning the storage selection cache costs: {}(ms) for event: {}",
        System.currentTimeMillis() - startTime,
        event);
  }

  private <K, V> void deleteElement(
      Map<K, V> sortedPartitionsOfStorageMap, Function<K, Boolean> deleteConditionFunc) {
    Iterator<Map.Entry<K, V>> iterator = sortedPartitionsOfStorageMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<K, V> entry = iterator.next();
      if (deleteConditionFunc.apply(entry.getKey())) {
        iterator.remove();
      } else {
        break;
      }
    }
  }

  @Override
  public void registerRemoteStorage(String appId, RemoteStorageInfo remoteStorageInfo) {
    // ignore
  }

  @Override
  public void checkAndClearLeakedShuffleData(Supplier<Collection<String>> appIdsSupplier) {
    Set<String> appIdsOnStorages = new HashSet<>();
    for (LocalStorage localStorage : localStorages) {
      if (!localStorage.isCorrupted()) {
        Set<String> appIdsOnStorage = localStorage.getAppIds();
        appIdsOnStorages.addAll(appIdsOnStorage);
      }
    }

    Collection<String> appIds = appIdsSupplier.get();
    for (String appId : appIdsOnStorages) {
      if (!appIds.contains(appId)) {
        ShuffleDeleteHandler deleteHandler =
            ShuffleHandlerFactory.getInstance()
                .createShuffleDeleteHandler(
                    new CreateShuffleDeleteHandlerRequest(
                        StorageType.LOCALFILE.name(), new Configuration()));
        String[] deletePaths = new String[storageBasePaths.size()];
        for (int i = 0; i < storageBasePaths.size(); i++) {
          deletePaths[i] =
              ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePaths.get(i), appId);
        }
        deleteHandler.delete(deletePaths, appId, UNKNOWN_USER_NAME);
      }
    }
  }

  @Override
  public Map<String, StorageInfo> getStorageInfo() {
    Map<String, StorageInfo> result = Maps.newHashMap();
    for (LocalStorage storage : localStorages) {
      String mountPoint = storage.getMountPoint();
      StorageStatus status = StorageStatus.NORMAL;
      if (storage.isCorrupted()) {
        status = StorageStatus.UNHEALTHY;
      } else if (!storage.canWrite()) {
        status = StorageStatus.OVERUSED;
      }
      StorageMedia media = storage.getStorageMedia();
      if (media == null) {
        media = StorageMedia.UNKNOWN;
      }
      StorageInfo existingMountPoint = result.get(mountPoint);
      long wroteBytes = storage.getServiceUsedBytes();
      // if there is already a storage on the mount point, we should sum up the used bytes.
      if (existingMountPoint != null) {
        wroteBytes += existingMountPoint.getUsedBytes();
      }
      long capacity = storage.getCapacity();
      StorageInfo info = new StorageInfo(mountPoint, media, capacity, wroteBytes, status);
      result.put(mountPoint, info);
    }
    return result;
  }

  public List<LocalStorage> getStorages() {
    return localStorages;
  }

  // Only for test.
  @VisibleForTesting
  public Map<String, LocalStorage> getSortedPartitionsOfStorageMap() {
    return sortedPartitionsOfStorageMap;
  }

  @Override
  public void stop() {
    super.stop();
    ReconfigurableRegistry.unregister(ShuffleServerConf.SERVER_STORAGE_AUDIT_LOG_ENABLED.key());
  }
}
