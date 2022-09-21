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

package org.apache.uniffle.coordinator;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.RssException;

import org.apache.uniffle.common.filesystem.HadoopFilesystemProvider;
import org.apache.uniffle.coordinator.LowestIOSampleCostSelectStorageStrategy.RankValue;

/**
 * AppBalanceSelectStorageStrategy will consider the number of apps allocated on each remote path is balanced.
 */
public class AppBalanceSelectStorageStrategy implements SelectStorageStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(AppBalanceSelectStorageStrategy.class);
  /**
   * store remote path -> application count for assignment strategy
   */
  private final Map<String, RankValue> remoteStoragePathRankValue;
  private final Configuration hdfsConf;
  private final int fileSize;
  private final int readAndWriteTimes;
  private boolean remotePathIsHealthy = true;

  public AppBalanceSelectStorageStrategy(Map<String, RankValue> remoteStoragePathRankValue, CoordinatorConf conf) {
    this.remoteStoragePathRankValue = remoteStoragePathRankValue;
    this.hdfsConf = new Configuration();
    fileSize = conf.getInteger(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_SCHEDULE_FILE_SIZE);
    readAndWriteTimes = conf.getInteger(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_SCHEDULE_ACCESS_TIMES);
  }

  @VisibleForTesting
  public List<Map.Entry<String, RankValue>> sortPathByRankValue(
      String path, String test, boolean isHealthy) {
    try {
      FileSystem fs = HadoopFilesystemProvider.getFilesystem(new Path(path), hdfsConf);
      fs.delete(new Path(test),true);
      if (isHealthy) {
        RankValue rankValue = remoteStoragePathRankValue.get(path);
        remoteStoragePathRankValue.put(path, new RankValue(0, rankValue.getAppNum().get()));
      }
    } catch (Exception e) {
      RankValue rankValue = remoteStoragePathRankValue.get(path);
      remoteStoragePathRankValue.put(path, new RankValue(Long.MAX_VALUE, rankValue.getAppNum().get()));
      LOG.error("Failed to sort, we will not use this remote path {}.", path, e);
    }
    return Lists.newCopyOnWriteArrayList(remoteStoragePathRankValue.entrySet()).stream()
        .filter(Objects::nonNull).collect(Collectors.toList());
  }

  @Override
  public List<Map.Entry<String, RankValue>> readAndWrite(String path) {
    if (path.startsWith(ApplicationManager.REMOTE_PATH_SCHEMA.get(0))) {
      setRemotePathIsHealthy(true);
      Path remotePath = new Path(path);
      String rssTest = path + "/rssTest";
      Path testPath = new Path(rssTest);
      try {
        FileSystem fs = HadoopFilesystemProvider.getFilesystem(remotePath, hdfsConf);
        for (int j = 0; j < readAndWriteTimes; j++) {
          byte[] data = RandomUtils.nextBytes(fileSize);
          try (FSDataOutputStream fos = fs.create(testPath)) {
            fos.write(data);
            fos.flush();
          }
          byte[] readData = new byte[fileSize];
          int readBytes;
          try (FSDataInputStream fis = fs.open(testPath)) {
            int hasReadBytes = 0;
            do {
              readBytes = fis.read(readData);
              if (hasReadBytes < fileSize) {
                for (int i = 0; i < readBytes; i++) {
                  if (data[hasReadBytes + i] != readData[i]) {
                    RankValue rankValue = remoteStoragePathRankValue.get(path);
                    remoteStoragePathRankValue.put(path,
                        new RankValue(Long.MAX_VALUE, rankValue.getAppNum().get()));
                    throw new RssException("The content of reading and writing is inconsistent.");
                  }
                }
              }
              hasReadBytes += readBytes;
            } while (readBytes != -1);
          }
        }
      } catch (Exception e) {
        setRemotePathIsHealthy(false);
        LOG.error("Storage read and write error, we will not use this remote path {}.", path, e);
        RankValue rankValue = remoteStoragePathRankValue.get(path);
        remoteStoragePathRankValue.put(path, new RankValue(Long.MAX_VALUE, rankValue.getAppNum().get()));
      } finally {
        return sortPathByRankValue(path, rssTest, remotePathIsHealthy);
      }
    } else {
      return Lists.newCopyOnWriteArrayList(remoteStoragePathRankValue.entrySet());
    }
  }

  public void setRemotePathIsHealthy(boolean remotePathIsHealthy) {
    this.remotePathIsHealthy = remotePathIsHealthy;
  }
}
