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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** QuotaManager is a manager for resource restriction. */
public class QuotaManagerTest {

  private final String quotaFile =
      Objects.requireNonNull(this.getClass().getClassLoader().getResource(fileName)).getFile();
  private static final String fileName = "quotaFile.properties";
  private static final AtomicInteger uuid = new AtomicInteger();

  @BeforeAll
  public static void setup() {
    CoordinatorMetrics.register();
  }

  @AfterAll
  public static void clear() {
    CoordinatorMetrics.clear();
  }

  @Timeout(value = 10)
  @Test
  public void testDetectUserResource() {
    CoordinatorConf conf = new CoordinatorConf();
    conf.set(CoordinatorConf.COORDINATOR_QUOTA_DEFAULT_PATH, quotaFile);
    try (ApplicationManager applicationManager = new ApplicationManager(conf)) {
      Awaitility.await()
          .timeout(5, TimeUnit.SECONDS)
          .until(() -> applicationManager.getDefaultUserApps().size() > 2);

      Integer user1 = applicationManager.getDefaultUserApps().get("user1");
      Integer user2 = applicationManager.getDefaultUserApps().get("user2");
      Integer user3 = applicationManager.getDefaultUserApps().get("user3");
      assertEquals(user1, 10);
      assertEquals(user2, 20);
      assertEquals(user3, 30);
    }
  }

  @Test
  public void testQuotaManagerWithoutAccessQuotaChecker() throws Exception {
    CoordinatorConf conf = new CoordinatorConf();
    conf.set(CoordinatorConf.COORDINATOR_QUOTA_DEFAULT_PATH, quotaFile);
    conf.set(
        CoordinatorConf.COORDINATOR_ACCESS_CHECKERS,
        Lists.newArrayList(
            "org.apache.uniffle.coordinator.access.checker.AccessClusterLoadChecker"));
    try (ApplicationManager applicationManager = new ApplicationManager(conf)) {
      // it didn't detectUserResource because `org.apache.unifle.coordinator.AccessQuotaChecker` is
      // not configured
      assertNull(applicationManager.getQuotaManager());
    }
  }

  @Test
  public void testCheckQuota() throws Exception {
    CoordinatorConf conf = new CoordinatorConf();
    conf.set(CoordinatorConf.COORDINATOR_QUOTA_DEFAULT_PATH, quotaFile);
    conf.setInteger(CoordinatorConf.COORDINATOR_QUOTA_DEFAULT_APP_NUM, 5);
    try (ApplicationManager applicationManager = new ApplicationManager(conf)) {
      final AtomicInteger uuid = new AtomicInteger();
      Map<String, AppInfo> uuidAndTime = JavaUtils.newConcurrentMap();
      String appId = String.valueOf(uuid.incrementAndGet());
      uuidAndTime.put(appId, AppInfo.createAppInfo(appId, System.currentTimeMillis()));
      appId = String.valueOf(uuid.incrementAndGet());
      uuidAndTime.put(appId, AppInfo.createAppInfo(appId, System.currentTimeMillis()));
      appId = String.valueOf(uuid.incrementAndGet());
      uuidAndTime.put(appId, AppInfo.createAppInfo(appId, System.currentTimeMillis()));
      appId = String.valueOf(uuid.incrementAndGet());
      uuidAndTime.put(appId, AppInfo.createAppInfo(appId, System.currentTimeMillis()));
      final int i1 = uuid.incrementAndGet();
      uuidAndTime.put(
          String.valueOf(i1),
          AppInfo.createAppInfo(String.valueOf(i1), System.currentTimeMillis()));
      Map<String, AppInfo> appAndTime =
          applicationManager
              .getQuotaManager()
              .getCurrentUserAndApp()
              .computeIfAbsent("user4", x -> uuidAndTime);
      // This thread may remove the uuid and put the appId in.
      final Thread registerThread =
          new Thread(
              () ->
                  applicationManager
                      .getQuotaManager()
                      .registerApplicationInfo("application_test_" + i1, appAndTime));
      registerThread.start();
      final boolean icCheck =
          applicationManager.getQuotaManager().checkQuota("user4", String.valueOf(i1));
      registerThread.join();
      assertTrue(icCheck);
      assertEquals(
          applicationManager.getQuotaManager().getCurrentUserAndApp().get("user4").size(), 5);
    }
  }

  @Test
  public void testCheckQuotaMetrics() {
    CoordinatorMetrics.clear();
    CoordinatorMetrics.register();
    CoordinatorConf conf = new CoordinatorConf();
    conf.set(CoordinatorConf.COORDINATOR_QUOTA_DEFAULT_PATH, quotaFile);
    conf.setLong(CoordinatorConf.COORDINATOR_APP_EXPIRED, 1500);
    conf.setInteger(CoordinatorConf.COORDINATOR_QUOTA_DEFAULT_APP_NUM, 2);
    try (ApplicationManager applicationManager = new ApplicationManager(conf)) {
      final AtomicInteger uuid = new AtomicInteger();
      final int i1 = uuid.incrementAndGet();
      final int i2 = uuid.incrementAndGet();
      final int i3 = uuid.incrementAndGet();
      final int i4 = uuid.incrementAndGet();
      Map<String, Long> uuidAndTime = JavaUtils.newConcurrentMap();
      uuidAndTime.put(String.valueOf(i1), System.currentTimeMillis());
      uuidAndTime.put(String.valueOf(i2), System.currentTimeMillis());
      uuidAndTime.put(String.valueOf(i3), System.currentTimeMillis());
      uuidAndTime.put(String.valueOf(i4), System.currentTimeMillis());
      final boolean icCheck =
          applicationManager.getQuotaManager().checkQuota("user4", String.valueOf(i1));
      final boolean icCheck2 =
          applicationManager.getQuotaManager().checkQuota("user4", String.valueOf(i2));
      final boolean icCheck3 =
          applicationManager.getQuotaManager().checkQuota("user4", String.valueOf(i3));
      final boolean icCheck4 =
          applicationManager.getQuotaManager().checkQuota("user3", String.valueOf(i4));
      assertFalse(icCheck);
      assertFalse(icCheck2);
      // The default number of tasks submitted is 2, and the third will be rejected
      assertTrue(icCheck3);
      assertFalse(icCheck4);
      assertEquals(
          applicationManager.getQuotaManager().getCurrentUserAndApp().get("user4").size(), 2);
      assertEquals(CoordinatorMetrics.gaugeRunningAppNumToUser.labels("user4").get(), 2);
      assertEquals(CoordinatorMetrics.gaugeRunningAppNumToUser.labels("user3").get(), 1);
      await()
          .atMost(2, TimeUnit.SECONDS)
          .until(
              () -> {
                applicationManager.statusCheck();
                // If the number of apps corresponding to this user is 0, remove this user
                return CoordinatorMetrics.gaugeRunningAppNumToUser.labels("user4").get() == 0
                    && CoordinatorMetrics.gaugeRunningAppNumToUser.labels("user3").get() == 0;
              });
    }
  }

  @Test
  public void testCheckQuotaWithDefault() {
    CoordinatorConf conf = new CoordinatorConf();
    conf.set(CoordinatorConf.COORDINATOR_QUOTA_DEFAULT_PATH, quotaFile);
    try (ApplicationManager applicationManager = new ApplicationManager(conf)) {
      Awaitility.await()
          .timeout(5, TimeUnit.SECONDS)
          .until(() -> applicationManager.getDefaultUserApps().size() > 2);

      QuotaManager quotaManager = applicationManager.getQuotaManager();
      Map<String, Map<String, AppInfo>> currentUserAndApp = quotaManager.getCurrentUserAndApp();

      currentUserAndApp.computeIfAbsent("user1", x -> mockUUidAppAndTime(30));
      currentUserAndApp.computeIfAbsent("user2", x -> mockUUidAppAndTime(20));
      currentUserAndApp.computeIfAbsent("user3", x -> mockUUidAppAndTime(29));
      currentUserAndApp.computeIfAbsent("disable_quota_user1", x -> mockUUidAppAndTime(100));
      currentUserAndApp.computeIfAbsent("blank_user1", x -> mockUUidAppAndTime(0));

      assertEquals(currentUserAndApp.get("user1").size(), 30);
      assertEquals(currentUserAndApp.get("user2").size(), 20);
      assertEquals(currentUserAndApp.get("user3").size(), 29);
      assertEquals(currentUserAndApp.get("disable_quota_user1").size(), 100);
      assertEquals(currentUserAndApp.get("blank_user1").size(), 0);

      assertTrue(quotaManager.checkQuota("user1", mockUUidAppId()));
      assertTrue(quotaManager.checkQuota("user2", mockUUidAppId()));
      assertFalse(quotaManager.checkQuota("user3", mockUUidAppId()));
      assertTrue(quotaManager.checkQuota("user3", mockUUidAppId()));
      assertFalse(quotaManager.checkQuota("disable_quota_user1", mockUUidAppId()));
      assertTrue(quotaManager.checkQuota("blank_user1", mockUUidAppId()));
    }
  }

  private String mockUUidAppId() {
    return String.valueOf(uuid.incrementAndGet());
  }

  private Map<String, AppInfo> mockUUidAppAndTime(int mockAppNum) {
    Map<String, AppInfo> uuidAndTime = JavaUtils.newConcurrentMap();
    for (int i = 0; i < mockAppNum; i++) {
      String appId = mockUUidAppId();
      long currentTimeMs = System.currentTimeMillis();
      AppInfo appInfo = uuidAndTime.get(appId);
      if (appInfo != null) {
        appInfo.setUpdateTime(currentTimeMs);
      } else {
        appInfo = new AppInfo(appId, currentTimeMs, currentTimeMs, "", "");
        uuidAndTime.put(appId, appInfo);
      }
    }

    return uuidAndTime;
  }
}
