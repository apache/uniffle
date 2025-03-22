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

import java.util.Arrays;
import java.util.Map;
import java.util.function.Supplier;

import com.google.common.collect.Maps;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;

import org.apache.uniffle.common.util.JavaUtils;

public class MetricsManager {
  private final CollectorRegistry collectorRegistry;
  private final String[] defaultLabelNames;
  private final String[] defaultLabelValues;
  private static final double[] QUANTILES = {0.50, 0.75, 0.90, 0.95, 0.99};
  private static final double QUANTILE_ERROR = 0.01;
  private Map<String, SupplierGauge> supplierGaugeMap;

  public MetricsManager() {
    this(null, Maps.newHashMap());
  }

  public MetricsManager(CollectorRegistry collectorRegistry, Map<String, String> defaultLabels) {
    if (collectorRegistry == null) {
      this.collectorRegistry = CollectorRegistry.defaultRegistry;
    } else {
      this.collectorRegistry = collectorRegistry;
    }
    this.defaultLabelNames = defaultLabels.keySet().toArray(new String[0]);
    this.defaultLabelValues =
        Arrays.stream(defaultLabelNames).map(defaultLabels::get).toArray(String[]::new);
    this.supplierGaugeMap = JavaUtils.newConcurrentMap();
  }

  public CollectorRegistry getCollectorRegistry() {
    return this.collectorRegistry;
  }

  public Counter addCounter(String name, String... labels) {
    return addCounter(name, "Counter " + name, labels);
  }

  public Counter addCounter(String name, String help, String[] labels) {
    return Counter.build().name(name).labelNames(labels).help(help).register(collectorRegistry);
  }

  public Counter.Child addLabeledCounter(String name) {
    Counter c = addCounter(name, this.defaultLabelNames);
    return c.labels(this.defaultLabelValues);
  }

  public Gauge addGauge(String name, String... labels) {
    return addGauge(name, "Gauge " + name, labels);
  }

  public Gauge addGauge(String name, String help, String[] labels) {
    return Gauge.build().name(name).labelNames(labels).help(help).register(collectorRegistry);
  }

  public Gauge.Child addLabeledGauge(String name) {
    Gauge c = addGauge(name, this.defaultLabelNames);
    return c.labels(this.defaultLabelValues);
  }

  public <T extends Number> void addLabeledGauge(String name, Supplier<T> supplier) {
    addLabeledCacheGauge(name, supplier, 0);
  }

  public <T extends Number> void addLabeledCacheGauge(
      String name, Supplier<T> supplier, long updateInterval) {
    supplierGaugeMap.computeIfAbsent(
        name,
        metricName ->
            new SupplierGauge(
                    name,
                    "Gauge " + name,
                    supplier,
                    this.defaultLabelNames,
                    this.defaultLabelValues,
                    updateInterval)
                .register(collectorRegistry));
  }

  public Histogram addHistogram(String name, double[] buckets, String... labels) {
    return addHistogram(name, "Histogram " + name, buckets, labels);
  }

  public Histogram addHistogram(String name, double[] buckets) {
    return addHistogram(name, "Histogram " + name, buckets, defaultLabelNames);
  }

  public Histogram addHistogram(String name, String help, double[] buckets, String[] labels) {
    return Histogram.build()
        .name(name)
        .buckets(buckets)
        .labelNames(labels)
        .help(help)
        .register(collectorRegistry);
  }

  public Summary addSummary(String name) {
    Summary.Builder builder = Summary.build().name(name).help("Summary " + name);
    for (int i = 0; i < QUANTILES.length; i++) {
      builder = builder.quantile(QUANTILES[i], QUANTILE_ERROR);
    }
    return builder.register(collectorRegistry);
  }

  public Summary.Child addLabeledSummary(String name) {
    Summary.Builder builder =
        Summary.build().name(name).labelNames(defaultLabelNames).help("Summary " + name);
    for (int i = 0; i < QUANTILES.length; i++) {
      builder = builder.quantile(QUANTILES[i], QUANTILE_ERROR);
    }
    return builder.register(collectorRegistry).labels(defaultLabelValues);
  }

  public void unregisterAllSupplierGauge() {
    for (SupplierGauge gauge : supplierGaugeMap.values()) {
      collectorRegistry.unregister(gauge);
    }
    supplierGaugeMap.clear();
  }

  public void unregisterSupplierGauge(String name) {
    if (supplierGaugeMap.containsKey(name)) {
      collectorRegistry.unregister(supplierGaugeMap.get(name));
      supplierGaugeMap.remove(name);
    }
  }

  public String[] getDefaultLabelNames() {
    return defaultLabelNames;
  }

  public String[] getDefaultLabelValues() {
    return defaultLabelValues;
  }
}
