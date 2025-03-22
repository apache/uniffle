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

package org.apache.spark.shuffle;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelegationRssShuffleManagerUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DelegationRssShuffleManagerUtils.class);

  public static String acquireAccessId(SparkConf sparkConf) {
    String accessId = sparkConf.get(RssSparkConfig.RSS_ACCESS_ID.key(), "").trim();
    if (StringUtils.isEmpty(accessId)) {
      String providerKey = sparkConf.get(RssSparkConfig.RSS_ACCESS_ID_PROVIDER_KEY.key(), "");
      if (StringUtils.isNotEmpty(providerKey)) {
        accessId = sparkConf.get(providerKey, "");
        LOG.info("Get access id {} from provider key: {}", accessId, providerKey);
      }
    }
    return accessId;
  }
}
