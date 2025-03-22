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

import java.io.File;

import org.apache.commons.lang3.StringUtils;

import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.storage.api.FileWriter;

public class LocalFileWriterFactory {
  public static FileWriter getLocalFileWriter(RssConf conf, File file, int buffer)
      throws Exception {
    String className = conf.get(RssBaseConf.RSS_STORAGE_LOCALFILE_WRITER_CLASS);
    if (StringUtils.isEmpty(className)) {
      throw new IllegalStateException(
          "Configuration error: "
              + RssBaseConf.RSS_STORAGE_LOCALFILE_WRITER_CLASS.toString()
              + " should not set to empty");
    }

    return (FileWriter)
        RssUtils.getConstructor(className, File.class, int.class).newInstance(file, buffer);
  }
}
