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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.JavaSerialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.coordinator.AccessManager;
import org.apache.uniffle.coordinator.ApplicationManager;
import org.apache.uniffle.coordinator.ClusterManager;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.SimpleClusterManager;
import org.apache.uniffle.coordinator.access.AccessInfo;
import org.apache.uniffle.coordinator.access.checker.AccessSupportRssChecker;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;

import static org.apache.uniffle.coordinator.CoordinatorConf.COORDINATOR_ACCESS_CHECKERS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class AccessSupportRssCheckerTest {

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
    ClusterManager clusterManager = mock(SimpleClusterManager.class);

    CoordinatorConf conf = new CoordinatorConf();
    conf.set(
        COORDINATOR_ACCESS_CHECKERS,
        Collections.singletonList(AccessSupportRssChecker.class.getName()));
    Map<String, String> properties = new HashMap<>();

    /** case1: check success when the serializer config is empty. */
    try (ApplicationManager applicationManager = new ApplicationManager(conf)) {
      AccessManager accessManager =
          new AccessManager(
              conf, clusterManager, applicationManager.getQuotaManager(), new Configuration());
      AccessSupportRssChecker checker =
          (AccessSupportRssChecker) accessManager.getAccessCheckers().get(0);
      AccessInfo accessInfo = new AccessInfo("test", new HashSet<>(), properties, "user");
      assertTrue(checker.check(accessInfo).isSuccess());
    }

    /** case2: check failed when the serializer config is JavaSerialization. */
    properties.put("serializer", JavaSerialization.class.getCanonicalName());
    try (ApplicationManager applicationManager = new ApplicationManager(conf)) {
      AccessManager accessManager =
          new AccessManager(
              conf, clusterManager, applicationManager.getQuotaManager(), new Configuration());
      AccessSupportRssChecker checker =
          (AccessSupportRssChecker) accessManager.getAccessCheckers().get(0);
      AccessInfo accessInfo = new AccessInfo("test", new HashSet<>(), properties, "user");
      assertFalse(checker.check(accessInfo).isSuccess());
    }

    /** case3: check success when the serializer config is other than JavaSerialization. */
    properties.put("serializer", WritableSerialization.class.getCanonicalName());
    try (ApplicationManager applicationManager = new ApplicationManager(conf)) {
      AccessManager accessManager =
          new AccessManager(
              conf, clusterManager, applicationManager.getQuotaManager(), new Configuration());
      AccessSupportRssChecker checker =
          (AccessSupportRssChecker) accessManager.getAccessCheckers().get(0);
      AccessInfo accessInfo = new AccessInfo("test", new HashSet<>(), properties, "user");
      assertTrue(checker.check(accessInfo).isSuccess());
    }
  }
}
