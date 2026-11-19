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

package org.apache.uniffle.coordinator.access.checker;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.coordinator.AccessManager;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.access.AccessCheckResult;
import org.apache.uniffle.coordinator.access.AccessInfo;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;

/**
 * AccessSupportRssChecker checks whether the extra properties support rss, for example, the
 * serializer is java, rss is not supported.
 */
public class AccessSupportRssChecker extends AbstractAccessChecker {
  private static final Logger LOG = LoggerFactory.getLogger(AccessSupportRssChecker.class);
  private final HashMap<String, String> unsupportedConfigMap;

  public AccessSupportRssChecker(AccessManager accessManager) throws Exception {
    super(accessManager);
    List<String> unsupportedConfigs =
        accessManager.getCoordinatorConf().get(CoordinatorConf.COORDINATOR_UNSUPPORTED_CONFIGS);
    unsupportedConfigMap = new HashMap<>();
    if (unsupportedConfigs != null && !unsupportedConfigs.isEmpty()) {
      for (String keyValue : unsupportedConfigs) {
        String[] pair = keyValue.split(":", 2);
        if (pair.length == 2) {
          unsupportedConfigMap.put(pair[0], pair[1]);
        } else {
          LOG.error("Unsupported config {} has wrong format, skip it.", keyValue);
        }
      }
    }
  }

  @Override
  public AccessCheckResult check(AccessInfo accessInfo) {
    for (Map.Entry<String, String> entry : unsupportedConfigMap.entrySet()) {
      String unsupportedConfKey = entry.getKey();
      String unsupportedConfValue = entry.getValue();
      String actualConfValue = accessInfo.getExtraProperties().get(unsupportedConfKey);
      if (Objects.equals(actualConfValue, unsupportedConfValue)) {
        String msg =
            String.format(
                "Denied by AccessSupportRssChecker, %s is %s, AccessSupportRssChecker does not supported.",
                unsupportedConfKey, actualConfValue);
        LOG.debug(msg);
        CoordinatorMetrics.counterTotalSupportRssDeniedRequest.inc();
        return new AccessCheckResult(false, msg);
      }
    }

    return new AccessCheckResult(true, Constants.COMMON_SUCCESS_MESSAGE);
  }

  @Override
  public void close() throws IOException {}
}
