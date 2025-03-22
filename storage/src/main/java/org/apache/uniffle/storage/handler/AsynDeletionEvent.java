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

package org.apache.uniffle.storage.handler;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

public class AsynDeletionEvent {
  private static final String TEMPORARYSUFFIX = "_tmp";
  private String appId;
  private String user;
  private String shuffleServerId;
  private Configuration conf;
  /** Records the mapping between the path to be deleted and the path to be renamed. */
  private Map<String, String> needDeletePathAndRenamePath;

  private String storageType;

  public AsynDeletionEvent(
      String appId,
      String user,
      Configuration conf,
      String shuffleServerId,
      List<String> needDeletePath,
      String storageType) {
    this.appId = appId;
    this.user = user;
    this.shuffleServerId = shuffleServerId;
    this.conf = conf;
    this.needDeletePathAndRenamePath =
        needDeletePath.stream()
            .collect(
                Collectors.toMap(Function.identity(), s -> StringUtils.join(s, TEMPORARYSUFFIX)));
    this.storageType = storageType;
  }

  public String getAppId() {
    return appId;
  }

  public String getUser() {
    return user;
  }

  public Configuration getConf() {
    return conf;
  }

  public Map<String, String> getNeedDeletePathAndRenamePath() {
    return needDeletePathAndRenamePath;
  }

  public String[] getNeedDeleteRenamePaths() {
    return needDeletePathAndRenamePath.values().stream().toArray(String[]::new);
  }

  public String getShuffleServerId() {
    return shuffleServerId;
  }

  public String getStorageType() {
    return storageType;
  }
}
