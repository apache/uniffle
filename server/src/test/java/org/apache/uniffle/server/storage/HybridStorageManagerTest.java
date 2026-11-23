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

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.common.HadoopStorage;
import org.apache.uniffle.storage.common.LocalStorage;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class HybridStorageManagerTest {

  /**
   * this tests the fallback strategy when encountering the local storage is invalid. 1. When
   * specifying the fallback max fail time = 0, the event will be discarded 2. When specifying the
   * fallback max fail time < 0, the event will be taken by Hadoop Storage.
   */
  @Test
  public void fallbackTestWhenLocalStorageCorrupted() {
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setLong(ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, 2000L);
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList("test"));
    conf.set(
        ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS, Lists.newArrayList("hdfs://test"));
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L * 1024L * 1024L);
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    conf.setString(
        ShuffleServerConf.HYBRID_STORAGE_MANAGER_SELECTOR_CLASS,
        "org.apache.uniffle.server.storage.hybrid.HugePartitionSensitiveStorageManagerSelector");
    conf.setString(
        ShuffleServerConf.HYBRID_STORAGE_FALLBACK_STRATEGY_CLASS,
        "org.apache.uniffle.server.storage.LocalStorageManagerFallbackStrategy");

    // case1: fallback to hadoop storage when fallback_max_fail_time = -1
    conf.setLong(ShuffleServerConf.FALLBACK_MAX_FAIL_TIMES, -1);
    HybridStorageManager manager = new HybridStorageManager(conf);

    LocalStorageManager localStorageManager = (LocalStorageManager) manager.getWarmStorageManager();
    localStorageManager.getStorages().get(0).markCorrupted();

    String remoteStorage = "hdfs://test";
    String appId = "selectStorageManagerWithSelectorAndFallbackStrategy_appId";
    manager.registerRemoteStorage(appId, new RemoteStorageInfo(remoteStorage));
    List<ShufflePartitionedBlock> blocks =
        Lists.newArrayList(new ShufflePartitionedBlock(100, 1000, 1, 1, 1L, (byte[]) null));
    ShuffleDataFlushEvent event =
        new ShuffleDataFlushEvent(1, appId, 1, 1, 1, 1000, blocks, null, null);
    assertTrue((manager.selectStorage(event) instanceof HadoopStorage));

    // case2: fallback is still valid when fallback_max_fail_time = 0
    conf.setLong(ShuffleServerConf.FALLBACK_MAX_FAIL_TIMES, 0);
    manager = new HybridStorageManager(conf);

    localStorageManager = (LocalStorageManager) manager.getWarmStorageManager();
    localStorageManager.getStorages().get(0).markCorrupted();

    event = new ShuffleDataFlushEvent(1, appId, 1, 1, 1, 1000, blocks, null, null);
    manager.registerRemoteStorage(appId, new RemoteStorageInfo(remoteStorage));
    assertTrue((manager.selectStorage(event) instanceof HadoopStorage));
  }

  @Test
  public void selectStorageManagerTest() {
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setLong(ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, 2000L);
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList("test"));
    conf.set(
        ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS, Lists.newArrayList("hdfs://test"));
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L * 1024L * 1024L);
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    HybridStorageManager manager = new HybridStorageManager(conf);
    String remoteStorage = "hdfs://test";
    String appId = "selectStorageManagerTest_appId";
    manager.registerRemoteStorage(appId, new RemoteStorageInfo(remoteStorage));
    List<ShufflePartitionedBlock> blocks =
        Lists.newArrayList(new ShufflePartitionedBlock(100, 1000, 1, 1, 1L, (byte[]) null));
    ShuffleDataFlushEvent event =
        new ShuffleDataFlushEvent(1, appId, 1, 1, 1, 1000, blocks, null, null);
    assertTrue((manager.selectStorage(event) instanceof LocalStorage));
    event = new ShuffleDataFlushEvent(1, appId, 1, 1, 1, 1000000, blocks, null, null);
    assertTrue((manager.selectStorage(event) instanceof HadoopStorage));
  }

  @Test
  public void testStorageManagerSelectorOfPreferCold() {
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setLong(ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, 10000L);
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList("test"));
    conf.set(
        ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS, Lists.newArrayList("hdfs://test"));
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 10000L);
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    conf.setString(
        ShuffleServerConf.HYBRID_STORAGE_FALLBACK_STRATEGY_CLASS,
        RotateStorageManagerFallbackStrategy.class.getCanonicalName());
    conf.set(
        ShuffleServerConf.HYBRID_STORAGE_MANAGER_SELECTOR_CLASS,
        "org.apache.uniffle.server.storage.hybrid.HugePartitionSensitiveStorageManagerSelector");
    HybridStorageManager manager = new HybridStorageManager(conf);
    String remoteStorage = "hdfs://test";
    String appId = "selectStorageManagerIfCanNotWriteTest_appId";
    manager.registerRemoteStorage(appId, new RemoteStorageInfo(remoteStorage));

    /**
     * case1: only event owned by huge partition will be flushed to cold storage when the {@link
     * org.apache.uniffle.server.storage.hybrid.StorageManagerSelector.ColdStoragePreferredFactor.HUGE_PARTITION}
     * is enabled.
     */
    List<ShufflePartitionedBlock> blocks =
        Lists.newArrayList(new ShufflePartitionedBlock(10001, 1000, 1, 1, 1L, (byte[]) null));
    ShuffleDataFlushEvent event =
        new ShuffleDataFlushEvent(1, appId, 1, 1, 1, 100000, blocks, null, null);
    Storage storage = manager.selectStorage(event);
    assertTrue(storage instanceof LocalStorage);

    ShuffleDataFlushEvent event1 =
        new ShuffleDataFlushEvent(1, appId, 1, 1, 1, 10, blocks, null, null);
    event1.markOwnedByHugePartition();
    storage = manager.selectStorage(event1);
    assertTrue(storage instanceof HadoopStorage);
  }

  @Test
  public void underStorageManagerSelectionTest() {
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setLong(ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, 10000L);
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList("test"));
    conf.set(
        ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS, Lists.newArrayList("hdfs://test"));
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 10000L);
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    conf.setString(
        ShuffleServerConf.HYBRID_STORAGE_FALLBACK_STRATEGY_CLASS,
        RotateStorageManagerFallbackStrategy.class.getCanonicalName());
    HybridStorageManager manager = new HybridStorageManager(conf);
    String remoteStorage = "hdfs://test";
    String appId = "selectStorageManagerIfCanNotWriteTest_appId";
    manager.registerRemoteStorage(appId, new RemoteStorageInfo(remoteStorage));

    /** case1: big event should be written into cold storage directly */
    List<ShufflePartitionedBlock> blocks =
        Lists.newArrayList(new ShufflePartitionedBlock(10001, 1000, 1, 1, 1L, (byte[]) null));
    ShuffleDataFlushEvent hugeEvent =
        new ShuffleDataFlushEvent(1, appId, 1, 1, 1, 10001, blocks, null, null);
    assertTrue(manager.selectStorage(hugeEvent) instanceof HadoopStorage);

    /** case2: fallback when disk can not write */
    blocks = Lists.newArrayList(new ShufflePartitionedBlock(100, 1000, 1, 1, 1L, (byte[]) null));
    ShuffleDataFlushEvent event =
        new ShuffleDataFlushEvent(1, appId, 1, 1, 1, 1000, blocks, null, null);
    Storage storage = manager.selectStorage(event);
    assertTrue((storage instanceof LocalStorage));
    ((LocalStorage) storage).markCorrupted();
    event = new ShuffleDataFlushEvent(1, appId, 1, 1, 1, 1000, blocks, null, null);
    assertTrue((manager.selectStorage(event) instanceof HadoopStorage));
  }
}
