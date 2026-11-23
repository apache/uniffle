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

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.exception.InvalidRequestException;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.server.event.AppPurgeEvent;
import org.apache.uniffle.server.event.ShufflePurgeEvent;
import org.apache.uniffle.storage.common.HadoopStorage;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HadoopStorageManagerTest {

  @BeforeAll
  public static void prepare() {
    ShuffleServerMetrics.register();
  }

  @AfterAll
  public static void clear() {
    ShuffleServerMetrics.clear();
  }

  @Test
  public void testRemoveResources() {
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    conf.set(ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS, Arrays.asList("hdfs://path1"));
    HadoopStorageManager hadoopStorageManager = new HadoopStorageManager(conf);
    final String remoteStoragePath1 = "hdfs://path1";
    String appId = "testRemoveResources_appId";
    hadoopStorageManager.registerRemoteStorage(
        appId, new RemoteStorageInfo(remoteStoragePath1, ImmutableMap.of("k1", "v1", "k2", "v2")));
    Map<String, HadoopStorage> appStorageMap = hadoopStorageManager.getAppIdToStorages();

    // case1
    assertEquals(1, appStorageMap.size());
    ShufflePurgeEvent shufflePurgeEvent = new ShufflePurgeEvent(appId, "", Arrays.asList(1));
    hadoopStorageManager.removeResources(shufflePurgeEvent);
    assertEquals(1, appStorageMap.size());

    // case2
    AppPurgeEvent appPurgeEvent = new AppPurgeEvent(appId, "");
    hadoopStorageManager.removeResources(appPurgeEvent);
    assertEquals(0, appStorageMap.size());
  }

  @Test
  public void testRegisterRemoteStorage() {
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setLong(ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, 2000L);
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList("test"));
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L);
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    conf.set(
        ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS,
        Arrays.asList("hdfs://path1", "hdfs://path2", "hdfs://path3"));
    HadoopStorageManager hadoopStorageManager = new HadoopStorageManager(conf);
    final String remoteStoragePath1 = "hdfs://path1";
    final String remoteStoragePath2 = "hdfs://path2";
    final String remoteStoragePath3 = "hdfs://path3";
    hadoopStorageManager.registerRemoteStorage(
        "app1", new RemoteStorageInfo(remoteStoragePath1, ImmutableMap.of("k1", "v1", "k2", "v2")));
    hadoopStorageManager.registerRemoteStorage(
        "app2", new RemoteStorageInfo(remoteStoragePath2, ImmutableMap.of("k3", "v3")));
    hadoopStorageManager.registerRemoteStorage(
        "app3", new RemoteStorageInfo(remoteStoragePath3, Maps.newHashMap()));
    Map<String, HadoopStorage> appStorageMap = hadoopStorageManager.getAppIdToStorages();
    assertEquals(3, appStorageMap.size());
    assertEquals(Sets.newHashSet("app1", "app2", "app3"), appStorageMap.keySet());
    HadoopStorage hs1 = hadoopStorageManager.getAppIdToStorages().get("app1");
    assertSame(hadoopStorageManager.getPathToStorages().get(remoteStoragePath1), hs1);
    assertEquals("v1", hs1.getConf().get("k1"));
    assertEquals("v2", hs1.getConf().get("k2"));
    assertNull(hs1.getConf().get("k3"));
    HadoopStorage hs2 = hadoopStorageManager.getAppIdToStorages().get("app2");
    assertSame(hadoopStorageManager.getPathToStorages().get(remoteStoragePath2), hs2);
    assertEquals("v3", hs2.getConf().get("k3"));
    assertNull(hs2.getConf().get("k1"));
    assertNull(hs2.getConf().get("k2"));
    HadoopStorage hs3 = hadoopStorageManager.getAppIdToStorages().get("app3");
    assertSame(hadoopStorageManager.getPathToStorages().get(remoteStoragePath3), hs3);
    assertNull(hs3.getConf().get("k1"));
    assertNull(hs3.getConf().get("k2"));
    assertNull(hs3.getConf().get("k3"));
  }

  @Test
  public void testRemoveExpiredResourcesWithTwoReplicas(@TempDir File remoteBasePath)
      throws Exception {
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    String shuffleServerId = "127.0.0.1:19999";
    conf.setString(ShuffleServerConf.SHUFFLE_SERVER_ID, shuffleServerId);
    File remoteStorageDir = new File(remoteBasePath, "path1");
    String remoteStoragePath = remoteStorageDir.toURI().toString();
    conf.set(
        ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS,
        Collections.singletonList(remoteStoragePath));
    HadoopStorageManager hadoopStorageManager = new HadoopStorageManager(conf);
    String appId = "testRemoveExpiredResources";
    hadoopStorageManager.registerRemoteStorage(
        appId, new RemoteStorageInfo(remoteStoragePath, ImmutableMap.of("k1", "v1", "k2", "v2")));

    File appPathFile = new File(remoteStorageDir, appId);
    File partitionDir = new File(appPathFile, "1/1-1/");
    partitionDir.mkdirs();
    // Simulate the case that there are two shuffle servers write data.
    File dataFile = new File(partitionDir, shuffleServerId + "_1.data");
    dataFile.createNewFile();
    File dataFile2 = new File(partitionDir, "shuffleserver2_1.data");
    dataFile2.createNewFile();
    assertTrue(partitionDir.exists());
    Map<String, HadoopStorage> appStorageMap = hadoopStorageManager.getAppIdToStorages();
    // Purged for expired
    assertEquals(1, appStorageMap.size());
    AppPurgeEvent shufflePurgeEvent = new AppPurgeEvent(appId, "", null, true);
    hadoopStorageManager.removeResources(shufflePurgeEvent);
    assertEquals(0, appStorageMap.size());
    // The directory of the partition should have not been deleted, for it was not empty.
    assertTrue(partitionDir.exists());
    assertFalse(dataFile.exists());
    assertTrue(dataFile2.exists());

    // Purged for unregister
    AppPurgeEvent appPurgeEvent = new AppPurgeEvent(appId, "");
    hadoopStorageManager.removeResources(appPurgeEvent);
    assertEquals(0, appStorageMap.size());
    assertFalse(appPathFile.exists());
  }

  @Test
  public void testRemoveExpiredResourcesWithOneReplica(@TempDir File remoteBasePath)
      throws Exception {
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    String shuffleServerId = "127.0.0.1:19999";
    conf.setString(ShuffleServerConf.SHUFFLE_SERVER_ID, shuffleServerId);
    File remoteStorageDir = new File(remoteBasePath, "path1");
    String remoteStoragePath = remoteStorageDir.toURI().toString();
    conf.set(
        ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS,
        Collections.singletonList(remoteStoragePath));
    HadoopStorageManager hadoopStorageManager = new HadoopStorageManager(conf);
    String appId = "testRemoveExpiredResources2";
    hadoopStorageManager.registerRemoteStorage(
        appId, new RemoteStorageInfo(remoteStoragePath, ImmutableMap.of("k1", "v1", "k2", "v2")));

    File appPathFile = new File(remoteStorageDir, appId);
    File partitionDir = new File(appPathFile, "1/1-1/");
    partitionDir.mkdirs();
    // Simulate the case that only one shuffle server writes data.
    File dataFile = new File(partitionDir, shuffleServerId + "_1.data");
    dataFile.createNewFile();
    assertTrue(partitionDir.exists());
    Map<String, HadoopStorage> appStorageMap = hadoopStorageManager.getAppIdToStorages();
    // purged for expired
    assertEquals(1, appStorageMap.size());
    AppPurgeEvent shufflePurgeEvent = new AppPurgeEvent(appId, "", null, true);
    hadoopStorageManager.removeResources(shufflePurgeEvent);
    assertEquals(0, appStorageMap.size());
    // The directory of the application should have been deleted, for it was empty.
    assertFalse(partitionDir.exists());

    // purged for unregister
    AppPurgeEvent appPurgeEvent = new AppPurgeEvent(appId, "");
    hadoopStorageManager.removeResources(appPurgeEvent);
    assertEquals(0, appStorageMap.size());
    assertFalse(appPathFile.exists());
  }

  private HadoopStorageManager createHadoopStorageManager(String... allowedPaths) {
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    if (allowedPaths.length > 0) {
      conf.set(ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS, Arrays.asList(allowedPaths));
    }
    return new HadoopStorageManager(conf);
  }

  @Test
  public void testRemoteStorageIsRejectedWithoutAllowedPaths() {
    // The remote storage comes from a client, so none of it is trusted unless it's configured
    HadoopStorageManager hadoopStorageManager = createHadoopStorageManager();
    for (String remoteStorage : new String[] {"hdfs://path1/rss", "file:///tmp/rss", "/rss"}) {
      assertThrows(
          InvalidRequestException.class,
          () ->
              hadoopStorageManager.registerRemoteStorage(
                  "app1", new RemoteStorageInfo(remoteStorage)));
    }
    assertTrue(hadoopStorageManager.getAppIdToStorages().isEmpty());
    assertTrue(hadoopStorageManager.getPathToStorages().isEmpty());
  }

  @Test
  public void testRegisterRemoteStorageUnderAllowedPaths() {
    // A path without directory part allows the whole cluster, and a trailing slash makes no
    // difference
    HadoopStorageManager hadoopStorageManager =
        createHadoopStorageManager("hdfs://path1", "hdfs://ns2/rss/");
    for (String remoteStorage :
        new String[] {
          "hdfs://path1",
          "hdfs://path1/rss",
          "hdfs://path1/rss/app",
          "hdfs://ns2/rss",
          "hdfs://ns2/rss/app"
        }) {
      hadoopStorageManager.registerRemoteStorage("app1", new RemoteStorageInfo(remoteStorage));
    }
    assertEquals(5, hadoopStorageManager.getPathToStorages().size());
  }

  @Test
  public void testRegisterRemoteStorageOutsideAllowedPaths() {
    HadoopStorageManager hadoopStorageManager =
        createHadoopStorageManager("hdfs://path1", "hdfs://ns2/rss");
    // The scheme and the authority must be the same, and the directory is matched as a whole
    for (String remoteStorage :
        new String[] {
          "hdfs://path2",
          "hdfs://path10/rss",
          "hdfs://path1.evil/rss",
          "hdfs://ns2",
          "hdfs://ns2/rss2",
          "hdfs://ns2/rss-other",
          "hdfs://ns2/RSS",
          "hdfs://other/rss",
          "oss://path1/rss",
          "file:///tmp/rss"
        }) {
      InvalidRequestException e =
          assertThrows(
              InvalidRequestException.class,
              () ->
                  hadoopStorageManager.registerRemoteStorage(
                      "app1", new RemoteStorageInfo(remoteStorage)));
      assertTrue(
          e.getMessage().contains(ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS.key()),
          e.getMessage());
    }
    // A path that is not explicit about its location could point to anywhere
    for (String remoteStorage :
        new String[] {
          null,
          "",
          " ",
          "/rss/app",
          "rss",
          "hdfs:rss",
          "hdfs:/rss",
          "hdfs:///ns2/rss",
          "hdfs://ns2/rss/../other",
          "hdfs://ns2/rss/./app",
          "hdfs://ns2/rss\n"
        }) {
      InvalidRequestException e =
          assertThrows(
              InvalidRequestException.class,
              () ->
                  hadoopStorageManager.registerRemoteStorage(
                      "app1", new RemoteStorageInfo(remoteStorage)));
      assertTrue(e.getMessage().contains("is invalid"), e.getMessage());
    }
    assertTrue(hadoopStorageManager.getPathToStorages().isEmpty());
  }

  @Test
  public void testInvalidAllowedPaths() {
    // A mistake of the configuration fails the creation instead of allowing more than expected
    for (String allowedPath :
        new String[] {"/rss", "hdfs:rss", "hdfs:/rss", "hdfs://ns/rss/../other"}) {
      assertThrows(IllegalArgumentException.class, () -> createHadoopStorageManager(allowedPath));
    }
  }

  @Test
  public void testFileSystemImplementationOfRemoteStorageConfIsIgnored() {
    HadoopStorageManager hadoopStorageManager = createHadoopStorageManager("hdfs://path1");
    hadoopStorageManager.registerRemoteStorage(
        "app1",
        new RemoteStorageInfo(
            "hdfs://path1/rss",
            ImmutableMap.of("k1", "v1", "fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem")));

    HadoopStorage storage = hadoopStorageManager.getAppIdToStorages().get("app1");
    assertEquals("v1", storage.getConf().get("k1"));
    // the implementation stays the one of the server, it can not be replaced by the client
    assertEquals(new Configuration().get("fs.hdfs.impl"), storage.getConf().get("fs.hdfs.impl"));
  }
}
