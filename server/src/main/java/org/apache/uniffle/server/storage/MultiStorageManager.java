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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.server.Checker;
import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleDataReadEvent;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.event.PurgeEvent;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.handler.api.ShuffleWriteHandler;
import org.apache.uniffle.storage.request.CreateShuffleWriteHandlerRequest;

public class MultiStorageManager implements StorageManager {

  private static final Logger LOG = LoggerFactory.getLogger(MultiStorageManager.class);

  private final StorageManager warmStorageManager;
  private final StorageManager coldStorageManager;
  private final long flushColdStorageThresholdSize;
  private final long fallBackTimes;
  private AbstractStorageManagerFallbackStrategy storageManagerFallbackStrategy;

  MultiStorageManager(ShuffleServerConf conf) {
    warmStorageManager = new LocalStorageManager(conf);
    coldStorageManager = new HdfsStorageManager(conf);
    fallBackTimes = conf.get(ShuffleServerConf.FALLBACK_MAX_FAIL_TIMES);
    flushColdStorageThresholdSize = conf.getSizeAsBytes(ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE);
    storageManagerFallbackStrategy = new DefaultStorageManagerFallbackStrategy(conf);
  }

  @Override
  public void registerRemoteStorage(String appId, RemoteStorageInfo remoteStorageInfo) {
    coldStorageManager.registerRemoteStorage(appId, remoteStorageInfo);
  }

  @Override
  public Storage selectStorage(ShuffleDataFlushEvent event) {
    return selectStorageManager(event).selectStorage(event);
  }

  @Override
  public Storage selectStorage(ShuffleDataReadEvent event) {
    return warmStorageManager.selectStorage(event);
  }

  @Override
  public void updateWriteMetrics(ShuffleDataFlushEvent event, long writeTime) {
    selectStorageManager(event).updateWriteMetrics(event, writeTime);
  }

  @Override
  public boolean write(Storage storage, ShuffleWriteHandler handler, ShuffleDataFlushEvent event, 
                       CreateShuffleWriteHandlerRequest request) {
    StorageManager storageManager = selectStorageManager(event);
    if (event.getRetryTimes() > 0) {
      try {
        StorageManager newStorageManager = storageManagerFallbackStrategy.tryFallback(
                storageManager, event,  warmStorageManager, coldStorageManager);
        if (newStorageManager != storageManager) {
          storageManager = newStorageManager;
          event.setStorageManager(storageManager);
          storage = storageManager.selectStorage(event);
          handler = storage.getOrCreateWriteHandler(request);
        }
      } catch (IOException ioe) {
        LOG.warn("Create fallback write handler failed ", ioe);
      }
    }
    return storageManager.write(storage, handler, event, request);
  }

  private StorageManager selectStorageManager(ShuffleDataFlushEvent event) {
    if (event.getStorageManager() != null) {
      return event.getStorageManager();
    }
    StorageManager storageManager;
    if (event.getSize() > flushColdStorageThresholdSize) {
      storageManager = coldStorageManager;
    } else {
      storageManager = warmStorageManager;
    }

    if (!storageManager.canWrite(event)) {
      storageManager = storageManagerFallbackStrategy.tryFallback(
          storageManager, event, warmStorageManager, coldStorageManager);
    }
    
    event.setStorageManager(storageManager);
    return storageManager;
  }

  public void start() {
  }

  public void stop() {
  }

  @Override
  public Checker getStorageChecker() {
    return warmStorageManager.getStorageChecker();
  }

  @Override
  public boolean canWrite(ShuffleDataFlushEvent event) {
    return warmStorageManager.canWrite(event) || coldStorageManager.canWrite(event);
  }

  public void removeResources(PurgeEvent event) {
    LOG.info("Start to remove resource of {}", event);
    warmStorageManager.removeResources(event);
    coldStorageManager.removeResources(event);
  }

  public StorageManager getColdStorageManager() {
    return coldStorageManager;
  }
}
