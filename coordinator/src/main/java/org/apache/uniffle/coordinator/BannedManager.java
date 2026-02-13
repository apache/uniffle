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

package org.apache.uniffle.coordinator;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** BannedManager is a manager for ban the abnormal app. */
public class BannedManager {
  private static final Logger LOG = LoggerFactory.getLogger(BannedManager.class);
  // version, bannedIds
  private volatile Pair<String, Set<String>> bannedInfo = Pair.of("0", Collections.emptySet());

  public BannedManager(CoordinatorConf conf) {
    LOG.info("BannedManager initialized successfully.");
  }

  public boolean checkBanned(String id) {
    return bannedInfo.getValue().contains(id);
  }

  public void reloadBannedIdsFromRest(Pair<String, Set<String>> newBannedIds) {
    if (newBannedIds.getKey().equals(bannedInfo.getKey())) {
      LOG.warn("receive bannedIds from rest with the same version: {}", newBannedIds.getKey());
    }
    bannedInfo = newBannedIds;
  }

  public String getVersion() {
    return bannedInfo.getKey();
  }

  public Pair<String, Set<String>> getBannedInfo() {
    return bannedInfo;
  }
}
