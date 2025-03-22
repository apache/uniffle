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

package org.apache.uniffle.dashboard.web.utils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DashboardUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DashboardUtils.class);

  public static Map<String, String> convertAddressesStrToMap(String coordinatorAddressesStr) {
    Preconditions.checkNotNull(coordinatorAddressesStr, "Coordinator web address is null");
    HashMap<String, String> coordinatorAddressMap = Maps.newHashMap();
    String[] coordinators = coordinatorAddressesStr.split(",");
    for (String coordinator : coordinators) {
      try {
        URL coordinatorURL = new URL(coordinator);
        coordinatorAddressMap.put(coordinatorURL.getHost(), removeBackslash(coordinator));
      } catch (MalformedURLException e) {
        LOG.error("The coordinator address is abnormal.", e);
      }
    }
    LOG.info("Monitored the Coordinator of {}.", String.join(",", coordinatorAddressMap.values()));
    return coordinatorAddressMap;
  }

  /** Remove the trailing backslash. */
  public static String removeBackslash(String addressStr) {
    if (StringUtils.isNotEmpty(addressStr) && addressStr.endsWith("/")) {
      return addressStr.substring(0, addressStr.length() - 1);
    } else {
      return addressStr;
    }
  }
}
