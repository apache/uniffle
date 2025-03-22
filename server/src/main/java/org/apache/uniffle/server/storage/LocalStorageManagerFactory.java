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

import org.apache.commons.lang3.StringUtils;

import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.server.ShuffleServerConf;

public class LocalStorageManagerFactory {
  public static LocalStorageManager get(ShuffleServerConf conf) {
    String className = conf.get(ShuffleServerConf.SERVER_LOCAL_STORAGE_MANAGER_CLASS);
    if (StringUtils.isEmpty(className)) {
      throw new IllegalStateException(
          "Configuration error: "
              + ShuffleServerConf.SERVER_LOCAL_STORAGE_MANAGER_CLASS.toString()
              + " should not set to empty");
    }

    try {
      return (LocalStorageManager)
          RssUtils.getConstructor(className, ShuffleServerConf.class).newInstance(conf);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Configuration error: "
              + ShuffleServerConf.SERVER_LOCAL_STORAGE_MANAGER_CLASS.toString()
              + " is failed to create instance of "
              + className,
          e);
    }
  }
}
