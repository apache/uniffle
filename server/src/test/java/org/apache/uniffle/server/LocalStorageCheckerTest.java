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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.common.StorageType;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.storage.common.LocalStorage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class LocalStorageCheckerTest {
  @BeforeAll
  public static void setup() {
    ShuffleServerMetrics.register();
  }

  @AfterAll
  public static void clear() {
    ShuffleServerMetrics.clear();
  }

  private class SlowDiskStorageChecker extends LocalStorageChecker {
    private long hangTimeSec;

    SlowDiskStorageChecker(ShuffleServerConf conf, List<LocalStorage> storages, long hangTimeSec) {
      super(conf, storages);
      this.hangTimeSec = hangTimeSec;

      List<StorageInfo> storageInfoList =
          storages.stream().map(x -> new SlowStorageInfo(x)).collect(Collectors.toList());
      super.storageInfos = storageInfoList;
    }

    private class SlowStorageInfo extends StorageInfo {

      SlowStorageInfo(LocalStorage storage) {
        super(storage);
      }

      @Override
      public boolean checkStorageReadAndWrite() {
        try {
          Thread.sleep(hangTimeSec * 1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        return true;
      }
    }
  }

  @Test
  @Timeout(10)
  public void testCheckingStorageHang(@TempDir File tempDir) {
    String basePath = tempDir.getAbsolutePath();

    ShuffleServerConf conf = new ShuffleServerConf();
    conf.set(RssBaseConf.RSS_STORAGE_BASE_PATH, Arrays.asList(basePath));
    conf.set(RssBaseConf.RSS_STORAGE_TYPE, StorageType.LOCALFILE);
    conf.set(ShuffleServerConf.HEALTH_CHECKER_LOCAL_STORAGE_EXECUTE_TIMEOUT, 2 * 1000L);

    LocalStorage localStorage =
        LocalStorage.newBuilder().basePath(tempDir.getAbsolutePath()).capacity(100000L).build();

    SlowDiskStorageChecker checker =
        new SlowDiskStorageChecker(conf, Arrays.asList(localStorage), 600);
    assertFalse(checker.checkIsHealthy());
    assertEquals(1, ShuffleServerMetrics.gaugeLocalStorageIsTimeout.labels(basePath).get());
    assertEquals(1, ShuffleServerMetrics.gaugeLocalStorageIsHealthy.labels(basePath).get());
  }

  @Test
  public void testCorruptedStorageIsUnhealthy(@TempDir File tempDir) {
    String basePath = tempDir.getAbsolutePath();

    ShuffleServerConf conf = new ShuffleServerConf();
    conf.set(RssBaseConf.RSS_STORAGE_BASE_PATH, Arrays.asList(basePath));
    conf.set(RssBaseConf.RSS_STORAGE_TYPE, StorageType.LOCALFILE);

    LocalStorage localStorage =
        LocalStorage.newBuilder().basePath(basePath).capacity(100000L).build();
    LocalStorageChecker checker = new LocalStorageChecker(conf, Arrays.asList(localStorage));

    checker.checkIsHealthy();
    assertEquals(0, ShuffleServerMetrics.gaugeLocalStorageIsHealthy.labels(basePath).get());

    localStorage.markCorrupted();
    checker.checkIsHealthy();
    assertEquals(1, ShuffleServerMetrics.gaugeLocalStorageIsHealthy.labels(basePath).get());
  }

  @Test
  public void testGetUniffleUsedSpace(@TempDir File tempDir) throws IOException {
    File file1 = createTempFile(tempDir, "file1.txt", 1000);
    File file2 = createTempFile(tempDir, "file2.txt", 2000);
    File subdir1 = createTempSubDirectory(tempDir, "subdir1");
    File file3 = createTempFile(subdir1, "file3.txt", 500);
    File subdir2 = createTempSubDirectory(subdir1, "subdir2");
    File file4 = createTempFile(subdir2, "file4.txt", 1500);

    // Call the method to calculate disk usage
    long calculatedUsage = LocalStorageChecker.getServiceUsedSpace(tempDir);

    // The expected total usage should be the sum of file1 + file2 + file3 + file4
    long expectedUsage = file1.length() + file2.length() + file3.length() + file4.length();

    // Assert that the calculated result matches the expected value
    Assertions.assertEquals(expectedUsage, calculatedUsage);
  }

  private File createTempFile(File directory, String fileName, long fileSize) throws IOException {
    File file = new File(directory, fileName);
    Files.write(file.toPath(), new byte[(int) fileSize]);
    return file;
  }

  private File createTempSubDirectory(File parentDirectory, String directoryName) {
    File subDir = new File(parentDirectory, directoryName);
    subDir.mkdirs();
    return subDir;
  }
}
