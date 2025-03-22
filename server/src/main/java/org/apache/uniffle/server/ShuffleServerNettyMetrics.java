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

package org.apache.uniffle.server;

import org.apache.uniffle.common.metrics.NettyMetrics;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleIndexRequest;
import org.apache.uniffle.common.netty.protocol.GetMemoryShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.GetSortedShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.SendShuffleDataRequest;

public class ShuffleServerNettyMetrics extends NettyMetrics {

  private static final String _TRANSPORT_LATENCY = "_transport_latency";
  private static final String _PROCESS_LATENCY = "_process_latency";
  private static final String _TOTAL = "_total";
  private static final String NETTY_SEND_SHUFFLE_DATA_REQUEST = "netty_send_shuffle_data_request";
  private static final String NETTY_GET_SHUFFLE_DATA_REQUEST =
      "netty_get_local_shuffle_data_request";
  private static final String NETTY_GET_SHUFFLE_INDEX_REQUEST =
      "netty_get_local_shuffle_index_request";
  private static final String NETTY_GET_MEMORY_SHUFFLE_DATA_REQUEST =
      "netty_get_memory_shuffle_data_request";
  private static final String NETTY_GET_SORTED_SHUFFLE_DATA_REQUEST =
      "netty_get_sorted_shuffle_data_request";

  public ShuffleServerNettyMetrics(ShuffleServerConf shuffleServerConf, String tags) {
    super(shuffleServerConf, tags);
  }

  @Override
  public void registerMetrics() {
    gaugeMap.putIfAbsent(
        SendShuffleDataRequest.class.getName(),
        metricsManager.addLabeledGauge(NETTY_SEND_SHUFFLE_DATA_REQUEST));
    gaugeMap.putIfAbsent(
        GetLocalShuffleDataRequest.class.getName(),
        metricsManager.addLabeledGauge(NETTY_GET_SHUFFLE_DATA_REQUEST));
    gaugeMap.putIfAbsent(
        GetLocalShuffleIndexRequest.class.getName(),
        metricsManager.addLabeledGauge(NETTY_GET_SHUFFLE_INDEX_REQUEST));
    gaugeMap.putIfAbsent(
        GetMemoryShuffleDataRequest.class.getName(),
        metricsManager.addLabeledGauge(NETTY_GET_MEMORY_SHUFFLE_DATA_REQUEST));
    gaugeMap.putIfAbsent(
        GetSortedShuffleDataRequest.class.getName(),
        metricsManager.addLabeledGauge(NETTY_GET_SORTED_SHUFFLE_DATA_REQUEST));

    counterMap.putIfAbsent(
        SendShuffleDataRequest.class.getName(),
        metricsManager.addLabeledCounter(NETTY_SEND_SHUFFLE_DATA_REQUEST + _TOTAL));
    counterMap.putIfAbsent(
        GetLocalShuffleDataRequest.class.getName(),
        metricsManager.addLabeledCounter(NETTY_GET_SHUFFLE_DATA_REQUEST + _TOTAL));
    counterMap.putIfAbsent(
        GetLocalShuffleIndexRequest.class.getName(),
        metricsManager.addLabeledCounter(NETTY_GET_SHUFFLE_INDEX_REQUEST + _TOTAL));
    counterMap.putIfAbsent(
        GetMemoryShuffleDataRequest.class.getName(),
        metricsManager.addLabeledCounter(NETTY_GET_MEMORY_SHUFFLE_DATA_REQUEST + _TOTAL));
    counterMap.putIfAbsent(
        GetSortedShuffleDataRequest.class.getName(),
        metricsManager.addLabeledCounter(NETTY_GET_SORTED_SHUFFLE_DATA_REQUEST + _TOTAL));

    transportTimeSummaryMap.putIfAbsent(
        SendShuffleDataRequest.class.getName(),
        metricsManager.addLabeledSummary(NETTY_SEND_SHUFFLE_DATA_REQUEST + _TRANSPORT_LATENCY));
    transportTimeSummaryMap.putIfAbsent(
        GetLocalShuffleDataRequest.class.getName(),
        metricsManager.addLabeledSummary(NETTY_GET_SHUFFLE_DATA_REQUEST + _TRANSPORT_LATENCY));
    transportTimeSummaryMap.putIfAbsent(
        GetLocalShuffleIndexRequest.class.getName(),
        metricsManager.addLabeledSummary(NETTY_GET_SHUFFLE_INDEX_REQUEST + _TRANSPORT_LATENCY));
    transportTimeSummaryMap.putIfAbsent(
        GetMemoryShuffleDataRequest.class.getName(),
        metricsManager.addLabeledSummary(
            NETTY_GET_MEMORY_SHUFFLE_DATA_REQUEST + _TRANSPORT_LATENCY));
    transportTimeSummaryMap.putIfAbsent(
        GetSortedShuffleDataRequest.class.getName(),
        metricsManager.addLabeledSummary(
            NETTY_GET_SORTED_SHUFFLE_DATA_REQUEST + _TRANSPORT_LATENCY));

    processTimeSummaryMap.putIfAbsent(
        SendShuffleDataRequest.class.getName(),
        metricsManager.addLabeledSummary(NETTY_SEND_SHUFFLE_DATA_REQUEST + _PROCESS_LATENCY));
    processTimeSummaryMap.putIfAbsent(
        GetLocalShuffleDataRequest.class.getName(),
        metricsManager.addLabeledSummary(NETTY_GET_SHUFFLE_DATA_REQUEST + _PROCESS_LATENCY));
    processTimeSummaryMap.putIfAbsent(
        GetLocalShuffleIndexRequest.class.getName(),
        metricsManager.addLabeledSummary(NETTY_GET_SHUFFLE_INDEX_REQUEST + _PROCESS_LATENCY));
    processTimeSummaryMap.putIfAbsent(
        GetMemoryShuffleDataRequest.class.getName(),
        metricsManager.addLabeledSummary(NETTY_GET_MEMORY_SHUFFLE_DATA_REQUEST + _PROCESS_LATENCY));
    processTimeSummaryMap.putIfAbsent(
        GetSortedShuffleDataRequest.class.getName(),
        metricsManager.addLabeledSummary(NETTY_GET_SORTED_SHUFFLE_DATA_REQUEST + _PROCESS_LATENCY));
  }
}
