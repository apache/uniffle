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

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.storage.StorageInfo;
import org.apache.uniffle.server.Checker;
import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleDataReadEvent;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.event.PurgeEvent;
import org.apache.uniffle.server.storage.hybrid.StorageManagerSelector;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.handler.api.ShuffleWriteHandler;

public class HybridStorageManager implements StorageManager {

  private static final Logger LOG = LoggerFactory.getLogger(HybridStorageManager.class);

  private final StorageManager warmStorageManager;
  private final StorageManager coldStorageManager;
  private final StorageManagerSelector storageManagerSelector;

  HybridStorageManager(ShuffleServerConf conf) {
    warmStorageManager = LocalStorageManagerFactory.get(conf);
    coldStorageManager = new HadoopStorageManager(conf);

    try {
      AbstractStorageManagerFallbackStrategy storageManagerFallbackStrategy =
          loadFallbackStrategy(conf);
      this.storageManagerSelector =
          loadManagerSelector(
              conf, storageManagerFallbackStrategy, warmStorageManager, coldStorageManager);
    } catch (Exception e) {
      throw new RssException("Errors on loading selector manager.", e);
    }
  }

  private StorageManagerSelector loadManagerSelector(
      ShuffleServerConf conf,
      AbstractStorageManagerFallbackStrategy storageManagerFallbackStrategy,
      StorageManager warmStorageManager,
      StorageManager coldStorageManager)
      throws Exception {
    String name = conf.get(ShuffleServerConf.HYBRID_STORAGE_MANAGER_SELECTOR_CLASS);
    Class<?> klass = Class.forName(name);
    Constructor<?> constructor =
        klass.getConstructor(
            StorageManager.class,
            StorageManager.class,
            AbstractStorageManagerFallbackStrategy.class,
            conf.getClass());
    StorageManagerSelector instance =
        (StorageManagerSelector)
            constructor.newInstance(
                warmStorageManager, coldStorageManager, storageManagerFallbackStrategy, conf);
    return instance;
  }

  public static AbstractStorageManagerFallbackStrategy loadFallbackStrategy(ShuffleServerConf conf)
      throws Exception {
    String name =
        conf.getString(
            ShuffleServerConf.HYBRID_STORAGE_FALLBACK_STRATEGY_CLASS,
            HadoopStorageManagerFallbackStrategy.class.getCanonicalName());
    Class<?> klass = Class.forName(name);
    Constructor<?> constructor;
    AbstractStorageManagerFallbackStrategy instance;
    try {
      constructor = klass.getConstructor(conf.getClass(), Boolean.TYPE);
      instance = (AbstractStorageManagerFallbackStrategy) constructor.newInstance(conf);
    } catch (NoSuchMethodException e) {
      constructor = klass.getConstructor(conf.getClass());
      instance = (AbstractStorageManagerFallbackStrategy) constructor.newInstance(conf);
    }
    return instance;
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
  public Storage selectStorageById(ShuffleDataReadEvent event) {
    return warmStorageManager.selectStorageById(event);
  }

  @Override
  public void updateWriteMetrics(ShuffleDataFlushEvent event, long writeTime) {
    throw new UnsupportedOperationException();
  }

  private StorageManager selectStorageManager(ShuffleDataFlushEvent event) {
    StorageManager storageManager = storageManagerSelector.select(event);
    return storageManager;
  }

  @Override
  public boolean write(Storage storage, ShuffleWriteHandler handler, ShuffleDataFlushEvent event) {
    StorageManager underStorageManager = selectStorageManager(event);
    return underStorageManager.write(storage, handler, event);
  }

  public void start() {}

  public void stop() {}

  @Override
  public Checker getStorageChecker() {
    return warmStorageManager.getStorageChecker();
  }

  @Override
  public boolean canWrite(ShuffleDataFlushEvent event) {
    return warmStorageManager.canWrite(event) || coldStorageManager.canWrite(event);
  }

  @Override
  public void checkAndClearLeakedShuffleData(Supplier<Collection<String>> appIdsSupplier) {
    warmStorageManager.checkAndClearLeakedShuffleData(appIdsSupplier);
  }

  @Override
  public Map<String, StorageInfo> getStorageInfo() {
    Map<String, StorageInfo> localStorageInfo = warmStorageManager.getStorageInfo();
    localStorageInfo.putAll(coldStorageManager.getStorageInfo());
    return localStorageInfo;
  }

  public void removeResources(PurgeEvent event) {
    LOG.info("Start to remove resource of {}", event);
    warmStorageManager.removeResources(event);
    coldStorageManager.removeResources(event);
  }

  public StorageManager getColdStorageManager() {
    return coldStorageManager;
  }

  public StorageManager getWarmStorageManager() {
    return warmStorageManager;
  }
}
