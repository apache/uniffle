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

package org.apache.uniffle.server.block;

import org.apache.commons.lang3.StringUtils;

import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.server.ShuffleServerConf;

public class ShuffleBlockIdManagerFactory {
  public static ShuffleBlockIdManager createShuffleBlockIdManager(ShuffleServerConf conf) {
    String className = conf.get(ShuffleServerConf.SERVER_BLOCK_ID_MANAGER_CLASS);
    return createShuffleBlockIdManager(
        className, ShuffleServerConf.SERVER_BLOCK_ID_MANAGER_CLASS.key());
  }

  public static ShuffleBlockIdManager createShuffleBlockIdManager(
      String className, String configKey) {
    if (StringUtils.isEmpty(className)) {
      throw new IllegalStateException(
          "Configuration error: " + configKey + " should not set to empty");
    }

    try {
      return (ShuffleBlockIdManager) RssUtils.getConstructor(className).newInstance();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Configuration error: " + configKey + " is failed to create instance of " + className, e);
    }
  }
}
