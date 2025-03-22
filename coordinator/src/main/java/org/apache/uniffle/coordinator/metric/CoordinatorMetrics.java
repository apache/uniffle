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

package org.apache.uniffle.coordinator.metric;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.apache.commons.lang3.StringUtils;

import org.apache.uniffle.common.metrics.CommonMetrics;
import org.apache.uniffle.common.metrics.MetricsManager;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.RssUtils;

public class CoordinatorMetrics {
  private static final String ACTIVE_SERVER_NUM = "active_server_num";
  private static final String LOST_SERVER_NUM = "lost_server_num";
  private static final String TOTAL_SERVER_NUM = "total_server_num";
  private static final String RUNNING_APP_NUM = "running_app_num";
  private static final String TOTAL_APP_NUM = "total_app_num";
  private static final String EXCLUDE_SERVER_NUM = "exclude_server_num";
  private static final String UNHEALTHY_SERVER_NUM = "unhealthy_server_num";
  private static final String TOTAL_ACCESS_REQUEST = "total_access_request";
  private static final String TOTAL_CANDIDATES_DENIED_REQUEST = "total_candidates_denied_request";
  private static final String TOTAL_LOAD_DENIED_REQUEST = "total_load_denied_request";
  private static final String TOTAL_QUOTA_DENIED_REQUEST = "total_quota_denied_request";
  public static final String REMOTE_STORAGE_IN_USED_PREFIX = "remote_storage_in_used_";
  public static final String APP_NUM_TO_USER = "app_num";
  public static final String USER_LABEL = "user_name";
  public static Gauge gaugeLostServerNum;
  public static Gauge gaugeActiveServerNum;
  public static Gauge gaugeTotalServerNum;
  public static Gauge gaugeExcludeServerNum;
  public static Gauge gaugeUnhealthyServerNum;
  public static Gauge gaugeRunningAppNum;
  public static Gauge gaugeRunningAppNumToUser;
  public static Counter counterTotalAppNum;
  public static Counter counterTotalAccessRequest;
  public static Counter counterTotalCandidatesDeniedRequest;
  public static Counter counterTotalQuotaDeniedRequest;
  public static Counter counterTotalLoadDeniedRequest;
  public static final Map<String, Gauge> GAUGE_USED_REMOTE_STORAGE = JavaUtils.newConcurrentMap();

  private static MetricsManager metricsManager;
  private static boolean isRegister = false;

  public static synchronized void register(CollectorRegistry collectorRegistry) {
    if (!isRegister) {
      Map<String, String> labels = Maps.newHashMap();
      labels.put(Constants.METRICS_TAG_LABEL_NAME, Constants.COORDINATOR_TAG);
      metricsManager = new MetricsManager(collectorRegistry, labels);
      isRegister = true;
      setUpMetrics();
      CommonMetrics.register(collectorRegistry, Constants.COORDINATOR_TAG);
    }
  }

  @VisibleForTesting
  public static void register() {
    register(CollectorRegistry.defaultRegistry);
  }

  @VisibleForTesting
  public static void clear() {
    isRegister = false;
    GAUGE_USED_REMOTE_STORAGE.clear();
    CollectorRegistry.defaultRegistry.clear();
    CommonMetrics.clear();
  }

  public static CollectorRegistry getCollectorRegistry() {
    return metricsManager.getCollectorRegistry();
  }

  public static void addDynamicGaugeForRemoteStorage(String storageHost) {
    if (!StringUtils.isEmpty(storageHost)) {
      if (!GAUGE_USED_REMOTE_STORAGE.containsKey(storageHost)) {
        String metricName =
            REMOTE_STORAGE_IN_USED_PREFIX + RssUtils.getMetricNameForHostName(storageHost);
        GAUGE_USED_REMOTE_STORAGE.putIfAbsent(storageHost, metricsManager.addGauge(metricName));
      }
    }
  }

  public static void updateDynamicGaugeForRemoteStorage(String storageHost, double value) {
    if (GAUGE_USED_REMOTE_STORAGE.containsKey(storageHost)) {
      GAUGE_USED_REMOTE_STORAGE.get(storageHost).set(value);
    }
  }

  private static void setUpMetrics() {
    gaugeLostServerNum = metricsManager.addGauge(LOST_SERVER_NUM);
    gaugeActiveServerNum = metricsManager.addGauge(ACTIVE_SERVER_NUM);
    gaugeTotalServerNum = metricsManager.addGauge(TOTAL_SERVER_NUM);
    gaugeExcludeServerNum = metricsManager.addGauge(EXCLUDE_SERVER_NUM);
    gaugeUnhealthyServerNum = metricsManager.addGauge(UNHEALTHY_SERVER_NUM);
    gaugeRunningAppNum = metricsManager.addGauge(RUNNING_APP_NUM);
    gaugeRunningAppNumToUser = metricsManager.addGauge(APP_NUM_TO_USER, USER_LABEL);
    counterTotalAppNum = metricsManager.addCounter(TOTAL_APP_NUM);
    counterTotalAccessRequest = metricsManager.addCounter(TOTAL_ACCESS_REQUEST);
    counterTotalCandidatesDeniedRequest =
        metricsManager.addCounter(TOTAL_CANDIDATES_DENIED_REQUEST);
    counterTotalQuotaDeniedRequest = metricsManager.addCounter(TOTAL_QUOTA_DENIED_REQUEST);
    counterTotalLoadDeniedRequest = metricsManager.addCounter(TOTAL_LOAD_DENIED_REQUEST);
  }
}
