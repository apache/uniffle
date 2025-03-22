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

package org.apache.uniffle.storage.common;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.storage.StorageMedia;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.storage.handler.api.ServerReadHandler;
import org.apache.uniffle.storage.handler.api.ShuffleWriteHandler;
import org.apache.uniffle.storage.handler.impl.LocalFileServerReadHandler;
import org.apache.uniffle.storage.handler.impl.LocalFileWriteHandler;
import org.apache.uniffle.storage.request.CreateShuffleReadHandlerRequest;
import org.apache.uniffle.storage.request.CreateShuffleWriteHandlerRequest;

public class LocalStorage extends AbstractStorage {

  private static final Logger LOG = LoggerFactory.getLogger(LocalStorage.class);
  public static final String STORAGE_HOST = "local";

  private final long diskCapacity;
  private final int id;
  private volatile long diskAvailableBytes;
  private volatile long serviceUsedBytes;
  // for test cases
  private boolean enableDiskCapacityCheck = false;

  private long capacity;
  private final String basePath;
  private final String mountPoint;
  private final double highWaterMarkOfWrite;
  private final double lowWaterMarkOfWrite;
  private final LocalStorageMeta metaData = new LocalStorageMeta();
  private final StorageMedia media;
  private boolean isSpaceEnough = true;
  private volatile boolean isCorrupted = false;

  private LocalStorage(Builder builder) {
    this.basePath = builder.basePath;
    this.highWaterMarkOfWrite = builder.highWaterMarkOfWrite;
    this.lowWaterMarkOfWrite = builder.lowWaterMarkOfWrite;
    this.capacity = builder.capacity;
    this.media = builder.media;
    this.enableDiskCapacityCheck = builder.enableDiskCapacityWatermarkCheck;
    this.id = builder.id;

    File baseFolder = new File(basePath);
    try {
      // similar to mkdir -p, ensure the base folder is a dir
      FileUtils.forceMkdir(baseFolder);
      // clean the directory if it's data left from previous ran.
      FileUtils.cleanDirectory(baseFolder);
      FileStore store = Files.getFileStore(baseFolder.toPath());
      this.mountPoint = store.name();
    } catch (IOException ioe) {
      LOG.warn("Init base directory " + basePath + " fail, the disk should be corrupted", ioe);
      throw new RssException(ioe);
    }
    this.diskCapacity = baseFolder.getTotalSpace();
    this.diskAvailableBytes = baseFolder.getUsableSpace();

    if (capacity < 0L) {
      this.capacity = (long) (diskCapacity * builder.ratio);
      LOG.info(
          "The `rss.server.disk.capacity` is not specified nor negative, the "
              + "ratio(`rss.server.disk.capacity.ratio`:{}) * disk space({}) is used, ",
          builder.ratio,
          diskCapacity);
    } else {
      final long freeSpace = diskAvailableBytes;
      if (freeSpace < capacity) {
        throw new IllegalArgumentException(
            "The Disk of "
                + basePath
                + " Available Capacity "
                + freeSpace
                + " is smaller than configuration");
      }
    }
  }

  @Override
  public String getStoragePath() {
    return basePath;
  }

  @Override
  public String getStorageHost() {
    return STORAGE_HOST;
  }

  @Override
  public void updateWriteMetrics(StorageWriteMetrics metrics) {
    updateWrite(
        RssUtils.generateShuffleKey(metrics.getAppId(), metrics.getShuffleId()),
        metrics.getDataSize(),
        metrics.getPartitions());
  }

  @Override
  public void updateReadMetrics(StorageReadMetrics metrics) {
    String shuffleKey = RssUtils.generateShuffleKey(metrics.getAppId(), metrics.getShuffleId());
    prepareStartRead(shuffleKey);
    updateShuffleLastReadTs(shuffleKey);
  }

  @Override
  ShuffleWriteHandler newWriteHandler(CreateShuffleWriteHandlerRequest request) {
    return new LocalFileWriteHandler(
        request.getRssBaseConf(),
        request.getAppId(),
        request.getShuffleId(),
        request.getStartPartition(),
        request.getEndPartition(),
        basePath,
        request.getFileNamePrefix());
  }

  @Override
  protected ServerReadHandler newReadHandler(CreateShuffleReadHandlerRequest request) {
    return new LocalFileServerReadHandler(
        request.getAppId(),
        request.getShuffleId(),
        request.getPartitionId(),
        request.getPartitionNumPerRange(),
        request.getPartitionNum(),
        basePath,
        id);
  }

  // only for tests.
  @VisibleForTesting
  public void enableDiskCapacityCheck() {
    this.enableDiskCapacityCheck = true;
  }

  public long getDiskCapacity() {
    return diskCapacity;
  }

  @Override
  public boolean canWrite() {
    boolean serviceUsedCapacityCheck;
    boolean diskUsedCapacityCheck;

    if (isSpaceEnough) {
      serviceUsedCapacityCheck =
          (double) (serviceUsedBytes * 100) / capacity < highWaterMarkOfWrite;
      diskUsedCapacityCheck =
          ((double) (diskCapacity - diskAvailableBytes)) * 100 / diskCapacity
              < highWaterMarkOfWrite;
    } else {
      serviceUsedCapacityCheck = (double) (serviceUsedBytes * 100) / capacity < lowWaterMarkOfWrite;
      diskUsedCapacityCheck =
          ((double) (diskCapacity - diskAvailableBytes)) * 100 / diskCapacity < lowWaterMarkOfWrite;
    }
    isSpaceEnough =
        serviceUsedCapacityCheck && (enableDiskCapacityCheck ? diskUsedCapacityCheck : true);
    return isSpaceEnough && !isCorrupted;
  }

  public String getBasePath() {
    return basePath;
  }

  public void createMetadataIfNotExist(String shuffleKey) {
    metaData.createMetadataIfNotExist(shuffleKey);
  }

  public void updateWrite(String shuffleKey, long delta, List<Integer> partitionList) {
    metaData.updateDiskSize(delta);
    metaData.addShufflePartitionList(shuffleKey, partitionList);
    metaData.updateShuffleSize(shuffleKey, delta);
  }

  public void prepareStartRead(String key) {
    metaData.prepareStartRead(key);
  }

  public void updateShuffleLastReadTs(String shuffleKey) {
    metaData.updateShuffleLastReadTs(shuffleKey);
  }

  @VisibleForTesting
  public LocalStorageMeta getMetaData() {
    return metaData;
  }

  public long getCapacity() {
    return capacity;
  }

  public String getMountPoint() {
    return mountPoint;
  }

  public StorageMedia getStorageMedia() {
    return media;
  }

  // This is the only place to remove shuffle metadata, clean and gc thread may remove
  // the shuffle metadata concurrently or serially. Force uploader thread may update the
  // shuffle size so gc thread must acquire write lock before updating disk size, and force
  // uploader thread will not get the lock if the shuffle is removed by gc thread, so
  // add the shuffle key back to the expiredShuffleKeys if get lock but fail to acquire write lock.
  public void removeResources(String shuffleKey) {
    LOG.info("Start to remove resource of {}", shuffleKey);
    try {
      metaData.updateDiskSize(-metaData.getShuffleSize(shuffleKey));
      metaData.removeShuffle(shuffleKey);
      LOG.info(
          "Finish remove resource of {}, disk size is {} and {} shuffle metadata",
          shuffleKey,
          metaData.getDiskSize(),
          metaData.getShuffleMetaSet().size());
    } catch (Exception e) {
      LOG.error("Fail to update disk size", e);
    }
  }

  public boolean isCorrupted() {
    return isCorrupted;
  }

  public void markCorrupted() {
    isCorrupted = true;
  }

  public Set<String> getAppIds() {
    Set<String> appIds = new HashSet<>();
    File baseFolder = new File(basePath);
    File[] files = baseFolder.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory() && !file.isHidden()) {
          appIds.add(file.getName());
        }
      }
    }
    return appIds;
  }

  public void updateDiskAvailableBytes(long bytes) {
    this.diskAvailableBytes = bytes;
  }

  public void updateServiceUsedBytes(long usedBytes) {
    this.serviceUsedBytes = usedBytes;
  }

  public long getServiceUsedBytes() {
    return serviceUsedBytes;
  }

  // Only for test
  @VisibleForTesting
  public void markSpaceFull() {
    isSpaceEnough = false;
  }

  public int getId() {
    return id;
  }

  public static class Builder {
    private long capacity;
    private double ratio;
    private double lowWaterMarkOfWrite;
    private double highWaterMarkOfWrite;
    private String basePath;
    private StorageMedia media;
    private boolean enableDiskCapacityWatermarkCheck;
    private int id;

    private Builder() {}

    public Builder capacity(long capacity) {
      this.capacity = capacity;
      return this;
    }

    public Builder ratio(double ratio) {
      this.ratio = ratio;
      return this;
    }

    public Builder lowWaterMarkOfWrite(double lowWaterMarkOfWrite) {
      this.lowWaterMarkOfWrite = lowWaterMarkOfWrite;
      return this;
    }

    public Builder basePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    public Builder highWaterMarkOfWrite(double highWaterMarkOfWrite) {
      this.highWaterMarkOfWrite = highWaterMarkOfWrite;
      return this;
    }

    public Builder localStorageMedia(StorageMedia media) {
      this.media = media;
      return this;
    }

    public Builder enableDiskCapacityWatermarkCheck() {
      this.enableDiskCapacityWatermarkCheck = true;
      return this;
    }

    public Builder setId(int id) {
      this.id = id;
      return this;
    }

    public LocalStorage build() {
      return new LocalStorage(this);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }
}
