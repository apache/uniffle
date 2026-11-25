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
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FilterFs;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.exception.InvalidRequestException;
import org.apache.uniffle.common.log.TestLoggerExtension;
import org.apache.uniffle.common.log.TestLoggerParamResolver;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.server.event.AppPurgeEvent;
import org.apache.uniffle.server.event.ShufflePurgeEvent;
import org.apache.uniffle.storage.common.HadoopStorage;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(TestLoggerExtension.class)
@ExtendWith(TestLoggerParamResolver.class)
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
          "hdfs://ns2/rss\n",
          "hdfs://ns2/rss\\other",
          // a local path with an authority reads like the path, but denotes another one
          "file://tmp/rss",
          "file://localhost/tmp/rss"
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
        new String[] {
          "/rss",
          "hdfs:rss",
          "hdfs:/rss",
          "hdfs://ns/rss/../other",
          "file://tmp/rss",
          "hdfs://ns/rss\\other"
        }) {
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
            ImmutableMap.of(
                "k1",
                "v1",
                "fs.hdfs.impl",
                "org.apache.hadoop.fs.LocalFileSystem",
                // every key that selects a class of a file system is refused, not only the
                // one of the file system itself. The value is not the default of any Hadoop
                // version of this build, Hadoop 3.2 defaults it to NullMetadataStore.
                "fs.s3a.metadatastore.impl",
                "org.apache.hadoop.fs.s3a.s3guard.LocalMetadataStore")));

    HadoopStorage storage = hadoopStorageManager.getAppIdToStorages().get("app1");
    assertEquals("v1", storage.getConf().get("k1"));
    // the implementation stays the one of the server, it can not be replaced by the client
    assertEquals(new Configuration().get("fs.hdfs.impl"), storage.getConf().get("fs.hdfs.impl"));
    assertEquals(
        new Configuration().get("fs.s3a.metadatastore.impl"),
        storage.getConf().get("fs.s3a.metadatastore.impl"));
  }

  @Test
  public void testPaddedFileSystemImplementationKeyIsIgnored() {
    // Configuration trims the name of a key when it's set, so a padded key would end up as the
    // key of the implementation if it was matched as it is. String.trim drops every leading and
    // trailing character up to U+0020, including U+0001, which is not a whitespace.
    // FilterFileSystem is not a local file system, which would be refused for the scheme hdfs
    // before the assertions.
    HadoopStorageManager hadoopStorageManager = createHadoopStorageManager("hdfs://path1");
    hadoopStorageManager.registerRemoteStorage(
        "app1",
        new RemoteStorageInfo(
            "hdfs://path1/rss",
            ImmutableMap.of(
                " fs.hdfs.impl",
                FilterFileSystem.class.getName(),
                "fs.hdfs.impl\t",
                FilterFileSystem.class.getName(),
                (char) 1 + "fs.hdfs.impl",
                FilterFileSystem.class.getName(),
                "\nfs.AbstractFileSystem.hdfs.impl",
                FilterFs.class.getName(),
                " k1 ",
                "v1")));

    HadoopStorage storage = hadoopStorageManager.getAppIdToStorages().get("app1");
    // the other keys are still applied, trimmed as Configuration does
    assertEquals("v1", storage.getConf().get("k1"));
    assertAll(
        () ->
            assertEquals(
                new Configuration().get("fs.hdfs.impl"), storage.getConf().get("fs.hdfs.impl")),
        () ->
            assertEquals(
                new Configuration().get("fs.AbstractFileSystem.hdfs.impl"),
                storage.getConf().get("fs.AbstractFileSystem.hdfs.impl")));
  }

  @Test
  public void testBlankAndPaddedAllowedPaths() {
    // a blank entry never denotes a location, and a padded one is the location without the spaces
    HadoopStorageManager hadoopStorageManager =
        createHadoopStorageManager("", " ", " hdfs://path1/rss ");
    hadoopStorageManager.registerRemoteStorage("app1", new RemoteStorageInfo("hdfs://path1/rss"));
    assertEquals(1, hadoopStorageManager.getPathToStorages().size());
    // the skipped entries allow nothing else
    assertThrows(
        InvalidRequestException.class,
        () ->
            hadoopStorageManager.registerRemoteStorage(
                "app2", new RemoteStorageInfo("hdfs://path2/rss")));
  }

  @Test
  public void testLocalFileSystemOfOtherSchemeIsRejected() {
    // An operator can map another scheme to the local file system, and that scheme takes an
    // authority, so a path like raw://tmp/rss reads like another location than the local /rss
    // it denotes. The implementation is registered by hand here, because Hadoop itself does
    // not know such a scheme.
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    conf.setString(
        ShuffleServerConf.PREFIX_HADOOP_CONF + ".fs.raw.impl", RawLocalFileSystem.class.getName());
    conf.set(
        ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS,
        Collections.singletonList("raw://path1/rss"));
    HadoopStorageManager hadoopStorageManager = new HadoopStorageManager(conf);
    InvalidRequestException e =
        assertThrows(
            InvalidRequestException.class,
            () ->
                hadoopStorageManager.registerRemoteStorage(
                    "app1", new RemoteStorageInfo("raw://path1/rss")));
    assertTrue(e.getMessage().contains("local file system"), e.getMessage());
    assertTrue(hadoopStorageManager.getPathToStorages().isEmpty());
  }

  @Test
  public void testRemoteStorageOfClassThatCanNotBeLoaded() {
    // The scheme is known through the conf of the server, but the class behind it is missing,
    // for example because a jar was removed. Hadoop reports that with a RuntimeException.
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    conf.setString(ShuffleServerConf.PREFIX_HADOOP_CONF + ".fs.brokenfs.impl", "org.not.There");
    conf.set(
        ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS,
        Collections.singletonList("brokenfs://path1/rss"));
    HadoopStorageManager hadoopStorageManager = new HadoopStorageManager(conf);
    InvalidRequestException e =
        assertThrows(
            InvalidRequestException.class,
            () ->
                hadoopStorageManager.registerRemoteStorage(
                    "app1", new RemoteStorageInfo("brokenfs://path1/rss")));

    // the request is refused with the reason, and the original error is kept for the operator:
    // it's what tells a missing jar from an unknown scheme
    assertTrue(e.getMessage().contains("brokenfs"), e.getMessage());
    assertTrue(e.getCause() instanceof RuntimeException, String.valueOf(e.getCause()));
    assertTrue(
        e.getCause().getCause() instanceof ClassNotFoundException,
        String.valueOf(e.getCause().getCause()));
    assertTrue(e.getCause().getMessage().contains("org.not.There"), String.valueOf(e.getCause()));
    assertTrue(hadoopStorageManager.getPathToStorages().isEmpty());
  }

  @Test
  public void testUnloadableSchemeIsLoggedWithItsStackOnce(ExtensionContext context) {
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    conf.setString(ShuffleServerConf.PREFIX_HADOOP_CONF + ".fs.brokenfs.impl", "org.not.There");
    conf.set(
        ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS,
        Collections.singletonList("brokenfs://path1"));
    HadoopStorageManager hadoopStorageManager = new HadoopStorageManager(conf);
    TestLoggerExtension testLogger = TestLoggerExtension.getTestLogger(context);
    String withStack =
        "This shuffle server can not load the file system of the scheme \\[brokenfs\\]";
    final String withoutStack =
        "The file system of the scheme \\[brokenfs\\] can not be loaded, the request is refused";

    // the first refusal logs the failure with its stack, which is what tells what's broken
    assertThrows(
        InvalidRequestException.class,
        () ->
            hadoopStorageManager.registerRemoteStorage(
                "app1", new RemoteStorageInfo("brokenfs://path1/rss1")));
    assertEquals(1, testLogger.logCount(withStack));
    assertEquals(1, testLogger.logCountWithThrowable(withStack));
    assertEquals(0, testLogger.logCount(withoutStack));

    // The first attempt threw before anything was stored, so the same path would be checked
    // again as well. Another path is used to make that obvious: the refusal is logged again,
    // but the stack of the failure is not
    assertThrows(
        InvalidRequestException.class,
        () ->
            hadoopStorageManager.registerRemoteStorage(
                "app2", new RemoteStorageInfo("brokenfs://path1/rss2")));
    assertEquals(1, testLogger.logCount(withStack));
    assertEquals(1, testLogger.logCountWithThrowable(withStack));
    assertEquals(1, testLogger.logCount(withoutStack));
    assertEquals(0, testLogger.logCountWithThrowable(withoutStack));
  }

  @Test
  public void testFailureIsLoggedWithItsStackOnlyWhenItTellsMore() {
    HadoopStorageManager hadoopStorageManager = createHadoopStorageManager("hdfs://path1/rss");
    Throwable withReason = new RuntimeException(new ClassNotFoundException("org.not.There"));
    Throwable withoutReason = new IllegalStateException("no reason");

    // the first failure of a scheme is logged with its stack
    assertTrue(hadoopStorageManager.recordLoadFailure("brokenfs", withReason));
    assertTrue(hadoopStorageManager.recordLoadFailure("flakyfs", withoutReason));
    // a later failure is not, once the reason was told
    assertFalse(hadoopStorageManager.recordLoadFailure("brokenfs", withReason));
    assertFalse(hadoopStorageManager.recordLoadFailure("brokenfs", withoutReason));
    assertFalse(hadoopStorageManager.recordLoadFailure("flakyfs", withoutReason));
    // The first failure of flakyfs came without a reason, so a later failure that carries it is
    // logged and takes the place of the first one, after which the reason is not told again
    assertTrue(hadoopStorageManager.recordLoadFailure("flakyfs", withReason));
    assertFalse(hadoopStorageManager.recordLoadFailure("flakyfs", withReason));
    assertFalse(hadoopStorageManager.recordLoadFailure("flakyfs", withoutReason));
  }

  /** A file system whose class fails while it's initialized. */
  public static class BrokenStaticFileSystem extends RawLocalFileSystem {
    static {
      if (Boolean.TRUE) {
        throw new IllegalStateException("broken on purpose");
      }
    }
  }

  @Test
  public void testRemoteStorageOfClassThatFailsToInitialize() {
    // The class is there, but the JVM fails to initialize it and raises an Error, which is not
    // an Exception and which Hadoop doesn't catch
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    conf.setString(
        ShuffleServerConf.PREFIX_HADOOP_CONF + ".fs.brokenstaticfs.impl",
        BrokenStaticFileSystem.class.getName());
    conf.set(
        ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS,
        Collections.singletonList("brokenstaticfs://path1/rss"));
    HadoopStorageManager hadoopStorageManager = new HadoopStorageManager(conf);
    InvalidRequestException e =
        assertThrows(
            InvalidRequestException.class,
            () ->
                hadoopStorageManager.registerRemoteStorage(
                    "app1", new RemoteStorageInfo("brokenstaticfs://path1/rss")));

    assertTrue(e.getMessage().contains("brokenstaticfs"), e.getMessage());
    assertTrue(e.getCause() instanceof LinkageError, String.valueOf(e.getCause()));
    assertTrue(hadoopStorageManager.getPathToStorages().isEmpty());
  }

  @Test
  public void testRemoteStorageOfUnknownFileSystemScheme() {
    HadoopStorageManager hadoopStorageManager = createHadoopStorageManager("myfs://path1/rss");
    // the client brings the implementation of the file system itself, which is not trusted, so
    // the scheme is unknown to the server
    InvalidRequestException e =
        assertThrows(
            InvalidRequestException.class,
            () ->
                hadoopStorageManager.registerRemoteStorage(
                    "app1",
                    new RemoteStorageInfo(
                        "myfs://path1/rss",
                        ImmutableMap.of("fs.myfs.impl", "org.apache.hadoop.fs.LocalFileSystem"))));

    // it's refused at the registration, instead of failing at the first write
    assertTrue(e.getMessage().contains("myfs"), e.getMessage());
    assertTrue(hadoopStorageManager.getPathToStorages().isEmpty());
    assertTrue(hadoopStorageManager.getAppIdToStorages().isEmpty());
    // an unknown scheme is reported by an IOException, which is kept as the cause
    assertTrue(e.getCause() instanceof IOException, String.valueOf(e.getCause()));
  }
}
