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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ReconfigurableRegistry;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.coordinator.AccessManager;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.access.AccessCheckResult;
import org.apache.uniffle.coordinator.access.AccessInfo;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;

/**
 * AccessBannedChecker maintain a list of banned id and update it periodically, it checks the banned
 * id in the access request and reject if the id is in the banned list.
 */
public class AccessBannedChecker extends AbstractAccessChecker {
  private static final Logger LOG = LoggerFactory.getLogger(AccessBannedChecker.class);
  private final AccessManager accessManager;
  private String bannedIdProviderKey;
  private Pattern bannedIdProviderPattern;

  public AccessBannedChecker(AccessManager accessManager) throws Exception {
    super(accessManager);
    this.accessManager = accessManager;
    CoordinatorConf conf = accessManager.getCoordinatorConf();
    bannedIdProviderKey = conf.get(CoordinatorConf.COORDINATOR_ACCESS_BANNED_ID_PROVIDER);
    updateBannedIdProviderPattern(conf);

    LOG.info(
        "Construct BannedChecker. BannedIdProviderKey is {}, pattern is {}",
        bannedIdProviderKey,
        bannedIdProviderPattern.pattern());
    ReconfigurableRegistry.register(
        Sets.newHashSet(
            CoordinatorConf.COORDINATOR_ACCESS_BANNED_ID_PROVIDER.key(),
            CoordinatorConf.COORDINATOR_ACCESS_BANNED_ID_PROVIDER_REG_PATTERN.key()),
        (theConf, changedProperties) -> {
          if (changedProperties == null) {
            return;
          }
          if (changedProperties.contains(
              CoordinatorConf.COORDINATOR_ACCESS_BANNED_ID_PROVIDER.key())) {
            this.bannedIdProviderKey =
                conf.get(CoordinatorConf.COORDINATOR_ACCESS_BANNED_ID_PROVIDER);
          }
          if (changedProperties.contains(
              CoordinatorConf.COORDINATOR_ACCESS_BANNED_ID_PROVIDER.key())) {
            updateBannedIdProviderPattern(conf);
          }
        });
  }

  @Override
  public AccessCheckResult check(AccessInfo accessInfo) {
    if (accessInfo.getExtraProperties() != null
        && bannedIdProviderKey != null
        && accessInfo.getExtraProperties().containsKey(bannedIdProviderKey)) {
      String bannedIdPropertyValue = accessInfo.getExtraProperties().get(bannedIdProviderKey);
      Matcher matcher = bannedIdProviderPattern.matcher(bannedIdPropertyValue);
      if (matcher.find()) {
        String bannedId = matcher.group(1);
        if (accessManager.getBannedManager() != null
            && accessManager.getBannedManager().checkBanned(bannedId)) {
          String msg = String.format("Denied by BannedChecker, accessInfo[%s].", accessInfo);
          if (LOG.isDebugEnabled()) {
            LOG.debug("BannedIdPropertyValue is {}, {}", bannedIdPropertyValue, msg);
          }
          CoordinatorMetrics.counterTotalBannedDeniedRequest.inc();
          return new AccessCheckResult(false, msg);
        }
      }
    }

    return new AccessCheckResult(true, Constants.COMMON_SUCCESS_MESSAGE);
  }

  private void updateBannedIdProviderPattern(RssConf conf) {
    String bannedIdProviderRegex =
        conf.get(CoordinatorConf.COORDINATOR_ACCESS_BANNED_ID_PROVIDER_REG_PATTERN);
    bannedIdProviderPattern = Pattern.compile(bannedIdProviderRegex);
  }

  @Override
  public void close() {}
}
