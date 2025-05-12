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

package org.apache.uniffle.client.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.request.RssReportShuffleWriteMetricRequest;
import org.apache.uniffle.common.rpc.StatusCode;

/** This class is to track the underlying assigned shuffle servers' data pushing speed. */
public class ShuffleServerPushCostTracker {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleServerPushCostTracker.class);

  // shuffleServerId -> ShuffleServerPushCost Object
  private Map<String, ShuffleServerPushCost> tracking;

  public ShuffleServerPushCostTracker() {
    this.tracking = new ConcurrentHashMap<>();
  }

  public void merge(ShuffleServerPushCostTracker tracker) {
    if (tracker == null) {
      return;
    }
    for (Map.Entry<String, ShuffleServerPushCost> entry : tracker.tracking.entrySet()) {
      String id = entry.getKey();
      ShuffleServerPushCost cost = entry.getValue();
      this.tracking.computeIfAbsent(id, key -> new ShuffleServerPushCost(key)).merge(cost);
    }
  }

  public void recordRequireBufferFailure(String id) {
    ShuffleServerPushCost cost =
        this.tracking.computeIfAbsent(id, key -> new ShuffleServerPushCost(key));
    cost.incRequiredBufferFailure(1);
  }

  public void recordPushFailure(String id, StatusCode failureStatusCode) {
    ShuffleServerPushCost cost =
        this.tracking.computeIfAbsent(id, key -> new ShuffleServerPushCost(key));
    cost.incSentFailure(1, failureStatusCode.name());
  }

  public void record(String id, long sentBytes, long pushDuration) {
    ShuffleServerPushCost cost =
        this.tracking.computeIfAbsent(id, key -> new ShuffleServerPushCost(key));
    cost.incDurationMs(pushDuration);
    cost.incSentBytes(sentBytes);
  }

  public void statistics() {
    List<ShuffleServerPushCost> shuffleServerPushCosts = new ArrayList<>(this.tracking.values());
    if (CollectionUtils.isEmpty(shuffleServerPushCosts)) {
      return;
    }

    Collections.sort(
        shuffleServerPushCosts, Comparator.comparingLong(ShuffleServerPushCost::speed));

    LOGGER.info(
        "Statistics of shuffle server push speed: \n"
            + "-------------------------------------------"
            + "\nMinimum: {} \nP25: {} \nMedian: {} \nP75: {} \nMaximum: {}\n"
            + "-------------------------------------------",
        shuffleServerPushCosts.isEmpty() ? 0 : shuffleServerPushCosts.get(0),
        getPercentile(shuffleServerPushCosts, 25),
        getPercentile(shuffleServerPushCosts, 50),
        getPercentile(shuffleServerPushCosts, 75),
        shuffleServerPushCosts.isEmpty()
            ? 0
            : shuffleServerPushCosts.get(shuffleServerPushCosts.size() - 1));
  }

  private ShuffleServerPushCost getPercentile(
      List<ShuffleServerPushCost> costs, double percentile) {
    if (costs.isEmpty()) {
      return null;
    }
    int index = (int) Math.ceil(percentile / 100.0 * costs.size()) - 1;
    return costs.get(Math.min(Math.max(index, 0), costs.size() - 1));
  }

  public Map<String, RssReportShuffleWriteMetricRequest.TaskShuffleWriteMetric> toMetric() {
    return this.tracking.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                x -> {
                  ShuffleServerPushCost cost = x.getValue();
                  return new RssReportShuffleWriteMetricRequest.TaskShuffleWriteMetric(
                      cost.sentDurationMillis(),
                      cost.sentBytes(),
                      cost.requiredBufferFailureNumber(),
                      cost.pushFailureNumber(),
                      cost.getLastPushFailureReason());
                }));
  }
}
