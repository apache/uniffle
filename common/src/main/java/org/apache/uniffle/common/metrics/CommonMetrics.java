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

package org.apache.uniffle.common.metrics;

import java.util.Map;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import io.prometheus.client.CollectorRegistry;

import org.apache.uniffle.common.util.Constants;

public class CommonMetrics {
  public static final String JVM_PAUSE_TOTAL_EXTRA_TIME = "JvmPauseMonitorTotalExtraTime";
  public static final String JVM_PAUSE_INFO_TIME_EXCEEDED = "JvmPauseMonitorInfoTimeExceeded";
  public static final String JVM_PAUSE_WARN_TIME_EXCEEDED = "JvmPauseMonitorWarnTimeExceeded";

  private static MetricsManager metricsManager;
  private static boolean isRegister = false;

  @VisibleForTesting
  public static void clear() {
    isRegister = false;
    CollectorRegistry.defaultRegistry.clear();
  }

  public static CollectorRegistry getCollectorRegistry() {
    if (!isRegister) {
      return null;
    }
    return metricsManager.getCollectorRegistry();
  }

  public static <T extends Number> void addLabeledGauge(String name, Supplier<T> supplier) {
    addLabeledCacheGauge(name, supplier, 0);
  }

  public static <T extends Number> void addLabeledCacheGauge(
      String name, Supplier<T> supplier, long updateInterval) {
    if (!isRegister) {
      return;
    }
    metricsManager.addLabeledCacheGauge(name, supplier, updateInterval);
  }

  public static void unregisterSupplierGauge(String name) {
    if (!isRegister) {
      return;
    }
    metricsManager.unregisterSupplierGauge(name);
  }

  public static void register(CollectorRegistry collectorRegistry, String tags) {
    if (!isRegister) {
      Map<String, String> labels = Maps.newHashMap();
      labels.put(Constants.METRICS_TAG_LABEL_NAME, tags);
      metricsManager = new MetricsManager(collectorRegistry, labels);
      isRegister = true;
    }
  }
}
