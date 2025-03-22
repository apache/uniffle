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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ReconfigurableConfManager;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.future.CompletableFutureExtension;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.storage.common.LocalStorage;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;

import static org.apache.uniffle.common.util.Constants.DEVICE_NO_SPACE_ERROR_MESSAGE;

public class LocalStorageChecker extends Checker {

  private static final Logger LOG = LoggerFactory.getLogger(LocalStorageChecker.class);
  public static final String CHECKER_DIR_NAME = ".check";

  private final double diskMaxUsagePercentage;
  private final double diskRecoveryUsagePercentage;
  private final double minStorageHealthyPercentage;
  protected List<StorageInfo> storageInfos = Lists.newArrayList();
  private boolean isHealthy = true;
  private ExecutorService workers;
  private ReconfigurableConfManager.Reconfigurable<Long> diskCheckerExecutionTimeoutMs;
  private boolean markUnhealthyOnceDirCorrupted = false;

  public LocalStorageChecker(ShuffleServerConf conf, List<LocalStorage> storages) {
    super(conf);
    List<String> basePaths = RssUtils.getConfiguredLocalDirs(conf);
    if (CollectionUtils.isEmpty(basePaths)) {
      throw new IllegalArgumentException("The base path cannot be empty");
    }
    String storageType = conf.get(ShuffleServerConf.RSS_STORAGE_TYPE).name();
    if (!ShuffleStorageUtils.containsLocalFile(storageType)) {
      throw new IllegalArgumentException(
          "Only StorageType contains LOCALFILE support storageChecker");
    }
    for (LocalStorage storage : storages) {
      storageInfos.add(new StorageInfo(storage));
    }

    this.diskMaxUsagePercentage =
        conf.getDouble(ShuffleServerConf.HEALTH_STORAGE_MAX_USAGE_PERCENTAGE);
    this.diskRecoveryUsagePercentage =
        conf.getDouble(ShuffleServerConf.HEALTH_STORAGE_RECOVERY_USAGE_PERCENTAGE);
    this.minStorageHealthyPercentage =
        conf.getDouble(ShuffleServerConf.HEALTH_MIN_STORAGE_PERCENTAGE);

    this.diskCheckerExecutionTimeoutMs =
        conf.getReconfigurableConf(ShuffleServerConf.HEALTH_CHECKER_LOCAL_STORAGE_EXECUTE_TIMEOUT);
    this.workers = Executors.newFixedThreadPool(basePaths.size());

    this.markUnhealthyOnceDirCorrupted =
        conf.get(ShuffleServerConf.SERVER_UNHEALTHY_ONCE_STORAGE_CORRUPTION);
  }

  @Override
  public boolean checkIsHealthy() {
    AtomicInteger num = new AtomicInteger(0);
    AtomicLong totalSpace = new AtomicLong(0L);
    AtomicLong wholeDiskUsedSpace = new AtomicLong(0L);
    AtomicLong serviceUsedSpace = new AtomicLong(0L);
    AtomicInteger corruptedDirs = new AtomicInteger(0);

    Map<StorageInfo, CompletableFuture<Void>> futureMap = new HashMap<>();
    for (StorageInfo storageInfo : storageInfos) {
      CompletableFuture<Void> storageCheckFuture =
          CompletableFuture.supplyAsync(
              () -> {
                if (!storageInfo.checkStorageReadAndWrite()) {
                  storageInfo.markCorrupted();
                  corruptedDirs.incrementAndGet();
                  return null;
                }

                long total = getTotalSpace(storageInfo.storageDir);
                long availableBytes = getFreeSpace(storageInfo.storageDir);

                totalSpace.addAndGet(total);
                wholeDiskUsedSpace.addAndGet(total - availableBytes);
                long usedBytes = getServiceUsedSpace(storageInfo.storageDir);
                serviceUsedSpace.addAndGet(usedBytes);
                storageInfo.updateServiceUsedBytes(usedBytes);
                storageInfo.updateStorageFreeSpace(availableBytes);

                boolean isWritable = storageInfo.canWrite();
                ShuffleServerMetrics.gaugeLocalStorageIsWritable
                    .labels(storageInfo.storage.getBasePath())
                    .set(isWritable ? 0 : 1);
                ShuffleServerMetrics.gaugeLocalStorageIsTimeout
                    .labels(storageInfo.storage.getBasePath())
                    .set(0);

                if (storageInfo.checkIsSpaceEnough(total, availableBytes)) {
                  num.incrementAndGet();
                }
                return null;
              },
              workers);

      futureMap.put(
          storageInfo,
          CompletableFutureExtension.orTimeout(
              storageCheckFuture, diskCheckerExecutionTimeoutMs.get(), TimeUnit.MILLISECONDS));
    }

    for (Map.Entry<StorageInfo, CompletableFuture<Void>> entry : futureMap.entrySet()) {
      StorageInfo storageInfo = entry.getKey();
      CompletableFuture<Void> f = entry.getValue();

      try {
        f.get();
      } catch (Exception e) {
        if (e instanceof ExecutionException) {
          if (e.getCause() instanceof TimeoutException) {
            LOG.warn(
                "Timeout of checking local storage: {}. The current disk's IO load may be very high.",
                storageInfo.storage.getBasePath());
            ShuffleServerMetrics.gaugeLocalStorageIsTimeout
                .labels(storageInfo.storage.getBasePath())
                .set(1);
            continue;
          }
        }

        throw new RssException(e);
      }
    }

    ShuffleServerMetrics.gaugeLocalStorageTotalSpace.set(totalSpace.get());
    ShuffleServerMetrics.gaugeLocalStorageWholeDiskUsedSpace.set(wholeDiskUsedSpace.get());
    ShuffleServerMetrics.gaugeLocalStorageServiceUsedSpace.set(serviceUsedSpace.get());
    ShuffleServerMetrics.gaugeLocalStorageTotalDirsNum.set(storageInfos.size());
    ShuffleServerMetrics.gaugeLocalStorageCorruptedDirsNum.set(corruptedDirs.get());
    ShuffleServerMetrics.gaugeLocalStorageUsedSpaceRatio.set(
        wholeDiskUsedSpace.get() * 1.0 / totalSpace.get());

    if (storageInfos.isEmpty()) {
      if (isHealthy) {
        LOG.info("shuffle server become unhealthy because of empty storage");
      }
      isHealthy = false;
      return false;
    }

    if (markUnhealthyOnceDirCorrupted && corruptedDirs.get() > 0) {
      if (isHealthy) {
        LOG.info(
            "shuffle server become unhealthy because {} corrupted dirs exist", corruptedDirs.get());
      }
      isHealthy = false;
      return false;
    }

    double availablePercentage = num.get() * 100.0 / storageInfos.size();
    if (Double.compare(availablePercentage, minStorageHealthyPercentage) >= 0) {
      if (!isHealthy) {
        LOG.info("shuffle server become healthy");
      }
      isHealthy = true;
    } else {
      if (isHealthy) {
        LOG.info("shuffle server become unhealthy");
      }
      isHealthy = false;
    }
    return isHealthy;
  }

  // Only for testing
  @VisibleForTesting
  long getTotalSpace(File file) {
    return file.getTotalSpace();
  }

  long getFreeSpace(File file) {
    return file.getUsableSpace();
  }

  protected static long getServiceUsedSpace(File storageDir) {
    if (storageDir == null || !storageDir.exists()) {
      return 0;
    }

    if (storageDir.isFile()) {
      return storageDir.length();
    }

    File[] files = storageDir.listFiles();
    if (files == null) {
      return 0;
    }

    long totalUsage = 0;
    for (File file : files) {
      if (file.isFile()) {
        totalUsage += file.length();
      } else {
        totalUsage += getServiceUsedSpace(file);
      }
    }

    return totalUsage;
  }

  // todo: This function will be integrated to MultiStorageManager, currently we only support disk
  // check.
  class StorageInfo {

    private final File storageDir;
    private final LocalStorage storage;
    private boolean isHealthy;

    StorageInfo(LocalStorage storage) {
      this.storageDir = new File(storage.getBasePath());
      this.isHealthy = true;
      this.storage = storage;
    }

    void updateStorageFreeSpace(long availableBytes) {
      storage.updateDiskAvailableBytes(availableBytes);
    }

    void updateServiceUsedBytes(long usedBytes) {
      storage.updateServiceUsedBytes(usedBytes);
    }

    boolean checkIsSpaceEnough(long total, long availableBytes) {
      if (Double.compare(0.0, total) == 0) {
        this.isHealthy = false;
        return false;
      }
      double usagePercent = (total - availableBytes) * 100.0 / total;
      if (isHealthy) {
        if (Double.compare(usagePercent, diskMaxUsagePercentage) >= 0) {
          isHealthy = false;
          LOG.info("storage {} become unhealthy", storageDir.getAbsolutePath());
        }
      } else {
        if (Double.compare(usagePercent, diskRecoveryUsagePercentage) <= 0) {
          isHealthy = true;
          LOG.info("storage {} become healthy", storageDir.getAbsolutePath());
        }
      }
      return isHealthy;
    }

    boolean canWrite() {
      return storage.canWrite();
    }

    boolean checkStorageReadAndWrite() {
      if (storage.isCorrupted()) {
        return false;
      }
      // Use the hidden file to avoid being cleanup
      File checkDir = new File(storageDir, CHECKER_DIR_NAME);
      try {
        if (!checkDir.mkdirs()) {
          return false;
        }
        File writeFile = new File(checkDir, "test");
        if (!writeFile.createNewFile()) {
          return false;
        }
        byte[] data = RandomUtils.nextBytes(1024);
        try (FileOutputStream fos = new FileOutputStream(writeFile)) {
          fos.write(data);
          fos.flush();
          fos.getFD().sync();
        }
        byte[] readData = new byte[1024];
        int readBytes = -1;
        try (FileInputStream fis = new FileInputStream(writeFile)) {
          int hasReadBytes = 0;
          do {
            readBytes = fis.read(readData);
            if (hasReadBytes < 1024) {
              for (int i = 0; i < readBytes; i++) {
                if (data[hasReadBytes + i] != readData[i]) {
                  return false;
                }
              }
            }
            hasReadBytes += readBytes;
          } while (readBytes != -1);
        }
      } catch (Exception e) {
        LOG.error("Storage read and write error. Storage dir: {}", storageDir, e);
        // avoid check bad track failure due to lack of disk space
        return e.getMessage() != null && DEVICE_NO_SPACE_ERROR_MESSAGE.equals(e.getMessage());
      } finally {
        try {
          FileUtils.deleteDirectory(checkDir);
        } catch (IOException ioe) {
          LOG.error("delete directory fail. Storage dir: {}", storageDir, ioe);
          return false;
        }
      }
      return true;
    }

    public void markCorrupted() {
      storage.markCorrupted();
    }
  }
}
