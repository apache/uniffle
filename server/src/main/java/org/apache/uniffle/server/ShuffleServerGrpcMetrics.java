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

import org.apache.uniffle.common.metrics.GRPCMetrics;

public class ShuffleServerGrpcMetrics extends GRPCMetrics {

  public static final String REGISTER_SHUFFLE_METHOD = "registerShuffle";
  public static final String SEND_SHUFFLE_DATA_METHOD = "sendShuffleData";
  public static final String COMMIT_SHUFFLE_TASK_METHOD = "commitShuffleTask";
  public static final String FINISH_SHUFFLE_METHOD = "finishShuffle";
  public static final String REQUIRE_BUFFER_METHOD = "requireBuffer";
  public static final String APP_HEARTBEAT_METHOD = "appHeartbeat";
  public static final String REPORT_SHUFFLE_RESULT_METHOD = "reportShuffleResult";
  public static final String GET_SHUFFLE_RESULT_METHOD = "getShuffleResult";
  public static final String GET_SHUFFLE_DATA_METHOD = "getLocalShuffleData";
  public static final String GET_MEMORY_SHUFFLE_DATA_METHOD = "getMemoryShuffleData";
  public static final String GET_SHUFFLE_INDEX_METHOD = "getLocalShuffleIndex";
  public static final String GET_SHUFFLE_RESULT_FOR_MULTI_PART_METHOD =
      "getShuffleResultForMultiPart";
  public static final String GET_SORTED_SHUFFLE_DATA_METHOD = "getSortedShuffleData";
  public static final String REPORT_UNIQUE_BLOCKS_METHOD = "reportUniqueBlocks";

  private static final String GRPC_REGISTERED_SHUFFLE = "grpc_registered_shuffle";
  private static final String GRPC_SEND_SHUFFLE_DATA = "grpc_send_shuffle_data";
  private static final String GRPC_COMMIT_SHUFFLE_TASK = "grpc_commit_shuffle_task";
  private static final String GRPC_FINISH_SHUFFLE = "grpc_finish_shuffle";
  private static final String GRPC_REQUIRE_BUFFER = "grpc_require_buffer";
  private static final String GRPC_APP_HEARTBEAT = "grpc_app_heartbeat";
  private static final String GRPC_REPORT_SHUFFLE_RESULT = "grpc_report_shuffle_result";
  private static final String GRPC_GET_SHUFFLE_RESULT = "grpc_get_shuffle_result";
  private static final String GRPC_GET_SHUFFLE_DATA = "grpc_get_local_shuffle_data";
  private static final String GRPC_GET_MEMORY_SHUFFLE_DATA = "grpc_get_memory_shuffle_data";
  private static final String GRPC_GET_SHUFFLE_INDEX = "grpc_get_local_shuffle_index";
  private static final String GRPC_GET_SORTED_SHUFFLE_DATA = "grpc_get_sorted_shuffle_data";
  private static final String GRPC_START_SORT_MERGE = "grpc_start_sort_merge";

  private static final String GRPC_REGISTERED_SHUFFLE_TOTAL = "grpc_registered_shuffle_total";
  private static final String GRPC_SEND_SHUFFLE_DATA_TOTAL = "grpc_send_shuffle_data_total";
  private static final String GRPC_COMMIT_SHUFFLE_TASK_TOTAL = "grpc_commit_shuffle_task_total";
  private static final String GRPC_FINISH_SHUFFLE_TOTAL = "grpc_finish_shuffle_total";
  private static final String GRPC_REQUIRE_BUFFER_TOTAL = "grpc_require_buffer_total";
  private static final String GRPC_APP_HEARTBEAT_TOTAL = "grpc_app_heartbeat_total";
  private static final String GRPC_REPORT_SHUFFLE_RESULT_TOTAL = "grpc_report_shuffle_result_total";
  private static final String GRPC_GET_SHUFFLE_RESULT_TOTAL = "grpc_get_shuffle_result_total";
  private static final String GRPC_GET_SHUFFLE_DATA_TOTAL = "grpc_get_local_shuffle_data_total";
  private static final String GRPC_GET_MEMORY_SHUFFLE_DATA_TOTAL =
      "grpc_get_memory_shuffle_data_total";
  private static final String GRPC_GET_SHUFFLE_INDEX_TOTAL = "grpc_get_local_shuffle_index_total";
  private static final String GRPC_GET_SHUFFLE_RESULT_FOR_MULTI_PART_TOTAL =
      "grpc_get_shuffle_result_for_multi_part_total";
  private static final String GRPC_GET_SORTED_SHUFFLE_DATA_TOTAL =
      "grpc_get_sorted_shuffle_data_total";
  private static final String GRPC_START_SORT_MERGE_TOTAL = "grpc_start_sort_merge_total";

  private static final String GRPC_SEND_SHUFFLE_DATA_TRANSPORT_LATENCY =
      "grpc_send_shuffle_data_transport_latency";
  private static final String GRPC_GET_SHUFFLE_DATA_TRANSPORT_LATENCY =
      "grpc_get_local_shuffle_data_transport_latency";
  private static final String GRPC_GET_MEMORY_SHUFFLE_DATA_TRANSPORT_LATENCY =
      "grpc_get_memory_shuffle_data_transport_latency";
  private static final String GRPC_GET_SORTED_SHUFFLE_DATA_TRANSPORT_LATENCY =
      "grpc_get_sorted_shuffle_data_transport_latency";

  private static final String GRPC_SEND_SHUFFLE_DATA_PROCESS_LATENCY =
      "grpc_send_shuffle_data_process_latency";
  private static final String GRPC_GET_SHUFFLE_DATA_PROCESS_LATENCY =
      "grpc_get_local_shuffle_data_process_latency";
  private static final String GRPC_GET_MEMORY_SHUFFLE_DATA_PROCESS_LATENCY =
      "grpc_get_memory_shuffle_data_process_latency";
  private static final String GRPC_GET_SHUFFLE_RESULT_FOR_MULTI_PART_PROCESS_LATENCY =
      "grpc_get_shuffle_result_for_multi_part_process_latency";
  private static final String GRPC_REPORT_SHUFFLE_RESULT_PROCESS_LATENCY =
      "grpc_report_shuffle_result_process_latency";
  private static final String GRPC_GET_SORTED_SHUFFLE_DATA_PROCESS_LATENCY =
      "grpc_get_sorted_shuffle_data_process_latency";

  public ShuffleServerGrpcMetrics(ShuffleServerConf shuffleServerConf, String tags) {
    super(shuffleServerConf, tags);
  }

  @Override
  public void registerMetrics() {
    gaugeMap.putIfAbsent(
        REGISTER_SHUFFLE_METHOD, metricsManager.addLabeledGauge(GRPC_REGISTERED_SHUFFLE));
    gaugeMap.putIfAbsent(
        SEND_SHUFFLE_DATA_METHOD, metricsManager.addLabeledGauge(GRPC_SEND_SHUFFLE_DATA));
    gaugeMap.putIfAbsent(
        COMMIT_SHUFFLE_TASK_METHOD, metricsManager.addLabeledGauge(GRPC_COMMIT_SHUFFLE_TASK));
    gaugeMap.putIfAbsent(
        FINISH_SHUFFLE_METHOD, metricsManager.addLabeledGauge(GRPC_FINISH_SHUFFLE));
    gaugeMap.putIfAbsent(
        REQUIRE_BUFFER_METHOD, metricsManager.addLabeledGauge(GRPC_REQUIRE_BUFFER));
    gaugeMap.putIfAbsent(APP_HEARTBEAT_METHOD, metricsManager.addLabeledGauge(GRPC_APP_HEARTBEAT));
    gaugeMap.putIfAbsent(
        REPORT_SHUFFLE_RESULT_METHOD, metricsManager.addLabeledGauge(GRPC_REPORT_SHUFFLE_RESULT));
    gaugeMap.putIfAbsent(
        GET_SHUFFLE_RESULT_METHOD, metricsManager.addLabeledGauge(GRPC_GET_SHUFFLE_RESULT));
    gaugeMap.putIfAbsent(
        GET_SHUFFLE_DATA_METHOD, metricsManager.addLabeledGauge(GRPC_GET_SHUFFLE_DATA));
    gaugeMap.putIfAbsent(
        GET_MEMORY_SHUFFLE_DATA_METHOD,
        metricsManager.addLabeledGauge(GRPC_GET_MEMORY_SHUFFLE_DATA));
    gaugeMap.putIfAbsent(
        GET_SHUFFLE_INDEX_METHOD, metricsManager.addLabeledGauge(GRPC_GET_SHUFFLE_INDEX));
    gaugeMap.putIfAbsent(
        GET_SORTED_SHUFFLE_DATA_METHOD,
        metricsManager.addLabeledGauge(GRPC_GET_SORTED_SHUFFLE_DATA));
    gaugeMap.putIfAbsent(
        REPORT_UNIQUE_BLOCKS_METHOD, metricsManager.addLabeledGauge(GRPC_START_SORT_MERGE));

    counterMap.putIfAbsent(
        REGISTER_SHUFFLE_METHOD, metricsManager.addLabeledCounter(GRPC_REGISTERED_SHUFFLE_TOTAL));
    counterMap.putIfAbsent(
        SEND_SHUFFLE_DATA_METHOD, metricsManager.addLabeledCounter(GRPC_SEND_SHUFFLE_DATA_TOTAL));
    counterMap.putIfAbsent(
        COMMIT_SHUFFLE_TASK_METHOD,
        metricsManager.addLabeledCounter(GRPC_COMMIT_SHUFFLE_TASK_TOTAL));
    counterMap.putIfAbsent(
        FINISH_SHUFFLE_METHOD, metricsManager.addLabeledCounter(GRPC_FINISH_SHUFFLE_TOTAL));
    counterMap.putIfAbsent(
        REQUIRE_BUFFER_METHOD, metricsManager.addLabeledCounter(GRPC_REQUIRE_BUFFER_TOTAL));
    counterMap.putIfAbsent(
        APP_HEARTBEAT_METHOD, metricsManager.addLabeledCounter(GRPC_APP_HEARTBEAT_TOTAL));
    counterMap.putIfAbsent(
        REPORT_SHUFFLE_RESULT_METHOD,
        metricsManager.addLabeledCounter(GRPC_REPORT_SHUFFLE_RESULT_TOTAL));
    counterMap.putIfAbsent(
        GET_SHUFFLE_RESULT_METHOD, metricsManager.addLabeledCounter(GRPC_GET_SHUFFLE_RESULT_TOTAL));
    counterMap.putIfAbsent(
        GET_SHUFFLE_DATA_METHOD, metricsManager.addLabeledCounter(GRPC_GET_SHUFFLE_DATA_TOTAL));
    counterMap.putIfAbsent(
        GET_MEMORY_SHUFFLE_DATA_METHOD,
        metricsManager.addLabeledCounter(GRPC_GET_MEMORY_SHUFFLE_DATA_TOTAL));
    counterMap.putIfAbsent(
        GET_SHUFFLE_INDEX_METHOD, metricsManager.addLabeledCounter(GRPC_GET_SHUFFLE_INDEX_TOTAL));
    counterMap.putIfAbsent(
        GET_SHUFFLE_RESULT_FOR_MULTI_PART_METHOD,
        metricsManager.addLabeledCounter(GRPC_GET_SHUFFLE_RESULT_FOR_MULTI_PART_TOTAL));
    counterMap.putIfAbsent(
        GET_SORTED_SHUFFLE_DATA_METHOD,
        metricsManager.addLabeledCounter(GRPC_GET_SORTED_SHUFFLE_DATA_TOTAL));
    counterMap.putIfAbsent(
        REPORT_UNIQUE_BLOCKS_METHOD, metricsManager.addLabeledCounter(GRPC_START_SORT_MERGE_TOTAL));

    transportTimeSummaryMap.putIfAbsent(
        SEND_SHUFFLE_DATA_METHOD,
        metricsManager.addLabeledSummary(GRPC_SEND_SHUFFLE_DATA_TRANSPORT_LATENCY));
    transportTimeSummaryMap.putIfAbsent(
        GET_SHUFFLE_DATA_METHOD,
        metricsManager.addLabeledSummary(GRPC_GET_SHUFFLE_DATA_TRANSPORT_LATENCY));
    transportTimeSummaryMap.putIfAbsent(
        GET_MEMORY_SHUFFLE_DATA_METHOD,
        metricsManager.addLabeledSummary(GRPC_GET_MEMORY_SHUFFLE_DATA_TRANSPORT_LATENCY));
    transportTimeSummaryMap.putIfAbsent(
        GET_SORTED_SHUFFLE_DATA_METHOD,
        metricsManager.addLabeledSummary(GRPC_GET_SORTED_SHUFFLE_DATA_TRANSPORT_LATENCY));

    processTimeSummaryMap.putIfAbsent(
        SEND_SHUFFLE_DATA_METHOD,
        metricsManager.addLabeledSummary(GRPC_SEND_SHUFFLE_DATA_PROCESS_LATENCY));
    processTimeSummaryMap.putIfAbsent(
        GET_SHUFFLE_DATA_METHOD,
        metricsManager.addLabeledSummary(GRPC_GET_SHUFFLE_DATA_PROCESS_LATENCY));
    processTimeSummaryMap.putIfAbsent(
        GET_MEMORY_SHUFFLE_DATA_METHOD,
        metricsManager.addLabeledSummary(GRPC_GET_MEMORY_SHUFFLE_DATA_PROCESS_LATENCY));
    processTimeSummaryMap.putIfAbsent(
        REPORT_SHUFFLE_RESULT_METHOD,
        metricsManager.addLabeledSummary(GRPC_REPORT_SHUFFLE_RESULT_PROCESS_LATENCY));
    processTimeSummaryMap.putIfAbsent(
        GET_SHUFFLE_RESULT_FOR_MULTI_PART_METHOD,
        metricsManager.addLabeledSummary(GRPC_GET_SHUFFLE_RESULT_FOR_MULTI_PART_PROCESS_LATENCY));
    processTimeSummaryMap.putIfAbsent(
        GET_SORTED_SHUFFLE_DATA_METHOD,
        metricsManager.addLabeledSummary(GRPC_GET_SORTED_SHUFFLE_DATA_PROCESS_LATENCY));
  }
}
