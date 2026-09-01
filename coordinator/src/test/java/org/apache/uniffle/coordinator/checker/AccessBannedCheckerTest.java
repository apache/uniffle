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

package org.apache.uniffle.coordinator.checker;

import java.util.Collections;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.coordinator.AccessManager;
import org.apache.uniffle.coordinator.BannedManager;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.access.AccessInfo;
import org.apache.uniffle.coordinator.access.checker.AccessBannedChecker;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AccessBannedCheckerTest {

  @BeforeEach
  public void setUp() {
    CoordinatorMetrics.register();
  }

  @AfterEach
  public void clear() {
    CoordinatorMetrics.clear();
  }

  @Test
  public void test() throws Exception {
    CoordinatorConf conf = new CoordinatorConf();
    conf.set(CoordinatorConf.COORDINATOR_ACCESS_BANNED_ID_PROVIDER, "test.key");
    conf.set(CoordinatorConf.COORDINATOR_ACCESS_BANNED_ID_PROVIDER_REG_PATTERN, "(.*)_.*");
    String checkerClassName = AccessBannedChecker.class.getName();
    conf.setString(CoordinatorConf.COORDINATOR_ACCESS_CHECKERS.key(), checkerClassName);
    AccessManager accessManager = new AccessManager(conf, null, null, new Configuration());
    BannedManager bannedManager = accessManager.getBannedManager();
    bannedManager.reloadBannedIdsFromRest(Pair.of("version1", Sets.newHashSet("2", "9527", "135")));
    AccessBannedChecker checker = (AccessBannedChecker) accessManager.getAccessCheckers().get(0);
    sleep(1200);
    assertFalse(
        checker
            .check(
                new AccessInfo(
                    "DummyAccessId",
                    Sets.newHashSet(),
                    Collections.singletonMap("test.key", "9527_1234"),
                    ""))
            .isSuccess());
    assertFalse(
        checker
            .check(
                new AccessInfo(
                    "DummyAccessId",
                    Sets.newHashSet(),
                    Collections.singletonMap("test.key", "135_1234"),
                    ""))
            .isSuccess());
    assertTrue(
        checker
            .check(
                new AccessInfo(
                    "DummyAccessId",
                    Sets.newHashSet(),
                    Collections.singletonMap("test.key", "2"),
                    ""))
            .isSuccess());
    assertTrue(
        checker
            .check(
                new AccessInfo(
                    "DummyAccessId",
                    Sets.newHashSet(),
                    Collections.singletonMap("test.key", "1"),
                    ""))
            .isSuccess());
    assertTrue(
        checker
            .check(
                new AccessInfo(
                    "DummyAccessId",
                    Sets.newHashSet(),
                    Collections.singletonMap("test.key", "1_2"),
                    ""))
            .isSuccess());

    checker.close();
  }

  @Test
  public void testAbnormal() throws Exception {
    CoordinatorConf conf = new CoordinatorConf();
    String checkerClassName = AccessBannedChecker.class.getName();
    conf.setString(CoordinatorConf.COORDINATOR_ACCESS_CHECKERS.key(), checkerClassName);
    AccessManager accessManager = new AccessManager(conf, null, null, new Configuration());
    AccessBannedChecker checker = (AccessBannedChecker) accessManager.getAccessCheckers().get(0);
    sleep(1200);
    // any access is passed for default config.
    assertTrue(
        checker
            .check(
                new AccessInfo(
                    "DummyAccessId",
                    Sets.newHashSet(),
                    Collections.singletonMap("test.key", "9527_1234"),
                    ""))
            .isSuccess());
    checker.close();
  }
}
