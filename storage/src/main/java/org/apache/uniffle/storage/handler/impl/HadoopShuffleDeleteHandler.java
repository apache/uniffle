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

package org.apache.uniffle.storage.handler.impl;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.filesystem.HadoopFilesystemProvider;
import org.apache.uniffle.storage.handler.api.ShuffleDeleteHandler;

public class HadoopShuffleDeleteHandler implements ShuffleDeleteHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopShuffleDeleteHandler.class);
  private final String shuffleServerId;

  private Configuration hadoopConf;

  public HadoopShuffleDeleteHandler(Configuration hadoopConf, String shuffleServerId) {
    this.hadoopConf = hadoopConf;
    this.shuffleServerId = shuffleServerId;
  }

  @Override
  public boolean delete(String[] storageBasePaths, String appId, String user) {
    for (String deletePath : storageBasePaths) {
      final Path path = new Path(deletePath);
      boolean isSuccess = false;
      int times = 0;
      int retryMax = 5;
      long start = System.currentTimeMillis();
      LOG.info(
          "Try delete shuffle data in Hadoop FS for appId[{}] of user[{}] with {}",
          appId,
          user,
          path);
      while (!isSuccess && times < retryMax) {
        try {
          FileSystem fileSystem = HadoopFilesystemProvider.getFilesystem(user, path, hadoopConf);
          delete(fileSystem, path, shuffleServerId);
          isSuccess = true;
        } catch (Exception e) {
          if (e instanceof FileNotFoundException) {
            LOG.info("[{}] doesn't exist, ignore it.", path);
            return false;
          }
          times++;
          LOG.warn(
              "Can't delete shuffle data for appId[" + appId + "] with " + times + " times", e);
          try {
            Thread.sleep(1000);
          } catch (Exception ex) {
            LOG.warn("Exception happened when Thread.sleep", ex);
          }
        }
      }
      if (isSuccess) {
        LOG.info(
            "Delete shuffle data in Hadoop FS for appId["
                + appId
                + "] with "
                + path
                + " successfully in "
                + (System.currentTimeMillis() - start)
                + " ms");
      } else {
        LOG.info(
            "Failed to delete shuffle data in Hadoop FS for appId["
                + appId
                + "] with "
                + path
                + " in "
                + (System.currentTimeMillis() - start)
                + " ms");
      }
    }
    return true;
  }

  private void delete(FileSystem fileSystem, Path path, String filePrefix) throws IOException {
    if (filePrefix == null) {
      fileSystem.delete(path, true);
      return;
    }
    FileStatus[] fileStatuses = fileSystem.listStatus(path);
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        delete(fileSystem, fileStatus.getPath(), filePrefix);
      } else {
        if (fileStatus.getPath().getName().startsWith(filePrefix)) {
          fileSystem.delete(fileStatus.getPath(), true);
        }
      }
    }
    ContentSummary contentSummary = fileSystem.getContentSummary(path);
    if (contentSummary.getFileCount() == 0) {
      fileSystem.delete(path, true);
    }
  }
}
