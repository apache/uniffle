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

package org.apache.uniffle.client.simulator;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.CodeSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;

public final class Utils {
  public static Path copyToHdfs(Configuration conf, Path srcPath, Path dstPath) throws IOException {
    try (FileSystem fs = FileSystem.get(dstPath.toUri(), conf)) {
      // upload
      fs.copyFromLocalFile(srcPath, dstPath);
      return dstPath;
    }
  }

  public static LocalResource addHdfsToResource(Configuration conf, Path dstPath)
      throws IOException {
    try (FileSystem fs = FileSystem.get(dstPath.toUri(), conf)) {
      FileStatus scFileStatus = fs.getFileStatus(dstPath);
      LocalResource scRsrc =
          LocalResource.newInstance(
              URL.fromURI(dstPath.toUri()),
              LocalResourceType.FILE,
              LocalResourceVisibility.APPLICATION,
              scFileStatus.getLen(),
              scFileStatus.getModificationTime());
      return scRsrc;
    }
  }

  public static Path getHdfsDestPath(Configuration conf, String name) {
    return new Path(conf.get(Constants.KEY_TMP_HDFS_PATH, Constants.TMP_HDFS_PATH_DEFAULT), name);
  }

  public static void writeStringToHdfs(Configuration conf, String content, Path path)
      throws IOException {
    try (FileSystem fs = FileSystem.get(path.toUri(), conf)) {
      // upload
      try (FSDataOutputStream os = fs.create(path)) {
        os.write(content.getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  public static String getCurrentJarPath(Class clazz) throws URISyntaxException {
    CodeSource codeSource = clazz.getProtectionDomain().getCodeSource();
    if (codeSource != null && codeSource.getLocation() != null) {
      return new File(codeSource.getLocation().toURI()).getPath();
    }
    return null;
  }

  public static boolean isBlank(String str) {
    int strLen;
    if (str == null || (strLen = str.length()) == 0) {
      return true;
    }
    for (int i = 0; i < strLen; i++) {
      if ((Character.isWhitespace(str.charAt(i)) == false)) {
        return false;
      }
    }
    return true;
  }
}
