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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.storage.StorageMedia;

public class DefaultStorageMediaProvider implements StorageMediaProvider {
  private static final Logger logger = LoggerFactory.getLogger(DefaultStorageMediaProvider.class);
  private static final String NUMBERIC_STRING = "0123456789";
  private static final String BLOCK_PATH_FORMAT = "/sys/block/%s/queue/rotational";
  private static final String HDFS = "hdfs";
  private static final List<String> OBJECT_STORE_SCHEMAS =
      Arrays.asList("s3", "oss", "cos", "gcs", "obs", "daos");

  @Override
  public StorageMedia getStorageMediaFor(String baseDir) {
    try {
      URI uri = new URI(baseDir);
      String scheme = uri.getScheme();
      if (HDFS.equals(scheme)) {
        return StorageMedia.HDFS;
      } else if (scheme != null && OBJECT_STORE_SCHEMAS.contains(scheme.toLowerCase())) {
        return StorageMedia.OBJECT_STORE;
      }
    } catch (URISyntaxException e) {
      logger.warn("invalid uri input from " + baseDir + ", with exception:", e);
    }
    // if baseDir starts with HDFS, the hdfs storage type should be reported
    if (SystemUtils.IS_OS_LINUX) {
      // according to https://unix.stackexchange.com/a/65602, we can detect disk types by looking at
      // the
      // `/sys/block/sdx/queue/rotational`.
      try {
        File baseFile = new File(baseDir);
        FileStore store = getFileStore(baseFile.toPath());
        if (store == null) {
          throw new IOException("Can't get FileStore for path:" + baseFile.getAbsolutePath());
        }
        String deviceName = getDeviceName(store.name());
        File blockFile = new File(String.format(BLOCK_PATH_FORMAT, deviceName));
        if (blockFile.exists()) {
          List<String> contents = Files.readAllLines(blockFile.toPath());
          // this should always hold true
          if (contents.size() >= 1) {
            String rotational = contents.get(0);
            if (rotational.equals("0")) {
              return StorageMedia.SSD;
            } else if (rotational.equals("1")) {
              return StorageMedia.HDD;
            }
          }
        }
      } catch (IOException ioe) {
        logger.warn("Get storage type failed with exception", ioe);
      }
    }
    logger.info("Default storage type provider returns HDD by default");
    return StorageMedia.HDD;
  }

  @VisibleForTesting
  FileStore getFileStore(Path path) throws IOException {
    while (!Files.exists(path)) {
      path = path.getParent();
      if (path == null) {
        return null;
      }
    }
    return Files.getFileStore(path);
  }

  @VisibleForTesting
  static String getDeviceName(String mountPoint) {
    // mountPoint would be /dev/sda1, /dev/vda1, /dev/nvme0n1p6, rootfs, etc.
    int separatorIndex = mountPoint.lastIndexOf(File.separator);
    String deviceName = separatorIndex > -1 ? mountPoint.substring(separatorIndex + 1) : mountPoint;
    if (deviceName.startsWith("nvme") && deviceName.contains("p")) {
      return deviceName.substring(0, deviceName.lastIndexOf("p"));
    }
    return StringUtils.stripEnd(deviceName, NUMBERIC_STRING);
  }
}
