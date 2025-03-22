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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.AuditType;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.filesystem.HadoopFilesystemProvider;
import org.apache.uniffle.common.storage.StorageInfo;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.server.Checker;
import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleDataReadEvent;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.server.event.AppPurgeEvent;
import org.apache.uniffle.server.event.PurgeEvent;
import org.apache.uniffle.storage.common.HadoopStorage;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.factory.ShuffleHandlerFactory;
import org.apache.uniffle.storage.handler.api.ShuffleDeleteHandler;
import org.apache.uniffle.storage.request.CreateShuffleDeleteHandlerRequest;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;
import org.apache.uniffle.storage.util.StorageType;

public class HadoopStorageManager extends SingleStorageManager {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopStorageManager.class);
  private static final Logger AUDIT_LOGGER =
      LoggerFactory.getLogger("SHUFFLE_SERVER_STORAGE_AUDIT_LOG");

  private final Configuration hadoopConf;
  private final String shuffleServerId;
  private Map<String, HadoopStorage> appIdToStorages = JavaUtils.newConcurrentMap();
  private Map<String, HadoopStorage> pathToStorages = JavaUtils.newConcurrentMap();
  private final boolean isStorageAuditLogEnabled;

  HadoopStorageManager(ShuffleServerConf conf) {
    super(conf);
    hadoopConf = conf.getHadoopConf();
    shuffleServerId = conf.getString(ShuffleServerConf.SHUFFLE_SERVER_ID, "shuffleServerId");
    isStorageAuditLogEnabled = conf.getBoolean(ShuffleServerConf.SERVER_STORAGE_AUDIT_LOG_ENABLED);
  }

  @Override
  public void updateWriteMetrics(ShuffleDataFlushEvent event, long writeTime) {
    super.updateWriteMetrics(event, writeTime);
    Storage storage = event.getUnderStorage();
    if (storage == null) {
      LOG.warn("The storage owned by event: {} is null, this should not happen", event);
      return;
    }
    ShuffleServerMetrics.incHadoopStorageWriteDataSize(
        storage.getStorageHost(), event.getDataLength(), event.isOwnedByHugePartition());
  }

  @Override
  public Storage selectStorage(ShuffleDataFlushEvent event) {
    Storage storage = getStorageByAppId(event.getAppId());
    event.setUnderStorage(storage);
    return storage;
  }

  @Override
  public Storage selectStorage(ShuffleDataReadEvent event) {
    return getStorageByAppId(event.getAppId());
  }

  @Override
  public void removeResources(PurgeEvent event) {
    String appId = event.getAppId();
    HadoopStorage storage = getStorageByAppId(appId);
    if (storage != null) {
      boolean purgeForExpired = false;
      if (event instanceof AppPurgeEvent) {
        storage.removeHandlers(appId);
        appIdToStorages.remove(appId);
        purgeForExpired = ((AppPurgeEvent) event).isAppExpired();
      }
      ShuffleDeleteHandler deleteHandler =
          ShuffleHandlerFactory.getInstance()
              .createShuffleDeleteHandler(
                  new CreateShuffleDeleteHandlerRequest(
                      StorageType.HDFS.name(),
                      storage.getConf(),
                      purgeForExpired ? shuffleServerId : null,
                      event.isRenameAndDelete()));

      String basicPath =
          ShuffleStorageUtils.getFullShuffleDataFolder(storage.getStoragePath(), appId);
      List<String> deletePaths = new ArrayList<>();

      if (event instanceof AppPurgeEvent) {
        deletePaths.add(basicPath);
        if (isStorageAuditLogEnabled) {
          AUDIT_LOGGER.info(
              String.format(
                  "%s|%s|%s|%s",
                  AuditType.DELETE.getValue(),
                  appId,
                  storage.getStorageHost(),
                  storage.getStoragePath()));
        }
      } else {
        for (Integer shuffleId : event.getShuffleIds()) {
          deletePaths.add(
              ShuffleStorageUtils.getFullShuffleDataFolder(basicPath, String.valueOf(shuffleId)));
        }
        if (isStorageAuditLogEnabled) {
          AUDIT_LOGGER.info(
              String.format(
                  "%s|%s|%s|%s|%s",
                  AuditType.DELETE.getValue(),
                  appId,
                  event.getShuffleIds().stream()
                      .map(Object::toString)
                      .collect(Collectors.joining(",")),
                  storage.getStorageHost(),
                  storage.getStoragePath()));
        }
      }
      deleteHandler.delete(deletePaths.toArray(new String[0]), appId, event.getUser());
      removeAppStorageInfo(event);
    } else {
      LOG.warn("Storage gotten is null when removing resources for event: {}", event);
    }
  }

  @Override
  public Checker getStorageChecker() {
    throw new RssException("Not support storage checker");
  }

  @Override
  public void registerRemoteStorage(String appId, RemoteStorageInfo remoteStorageInfo) {
    String remoteStorage = remoteStorageInfo.getPath();
    pathToStorages.computeIfAbsent(
        remoteStorage,
        key -> {
          Map<String, String> remoteStorageConf = remoteStorageInfo.getConfItems();
          Configuration remoteStorageHadoopConf = new Configuration(hadoopConf);
          if (remoteStorageConf != null && remoteStorageConf.size() > 0) {
            for (Map.Entry<String, String> entry : remoteStorageConf.entrySet()) {
              remoteStorageHadoopConf.setStrings(entry.getKey(), entry.getValue());
            }
          }
          HadoopStorage hadoopStorage = new HadoopStorage(remoteStorage, remoteStorageHadoopConf);
          return hadoopStorage;
        });
    appIdToStorages.computeIfAbsent(appId, key -> pathToStorages.get(remoteStorage));
    LOG.info("register remote storage {} successfully for appId {}", remoteStorageInfo, appId);
  }

  @Override
  public void checkAndClearLeakedShuffleData(Supplier<Collection<String>> appIdsSupplier) {}

  @Override
  public Map<String, StorageInfo> getStorageInfo() {
    // todo: report remote storage info
    return Maps.newHashMap();
  }

  public HadoopStorage getStorageByAppId(String appId) {
    if (!appIdToStorages.containsKey(appId)) {
      synchronized (this) {
        FileSystem fs;
        try {
          List<Path> appStoragePath =
              pathToStorages.keySet().stream()
                  .map(basePath -> new Path(basePath + Constants.KEY_SPLIT_CHAR + appId))
                  .collect(Collectors.toList());
          for (Path path : appStoragePath) {
            fs = HadoopFilesystemProvider.getFilesystem(path, hadoopConf);
            if (fs.isDirectory(path)) {
              return new HadoopStorage(path.getParent().toString(), hadoopConf);
            }
          }
        } catch (Exception e) {
          LOG.error("Some error happened when fileSystem got the file status.", e);
        }
        // outside should deal with null situation
        // todo: it's better to have a fake storage for null situation
        return null;
      }
    }
    return appIdToStorages.get(appId);
  }

  @VisibleForTesting
  public Map<String, HadoopStorage> getAppIdToStorages() {
    return appIdToStorages;
  }

  @VisibleForTesting
  public Map<String, HadoopStorage> getPathToStorages() {
    return pathToStorages;
  }
}
