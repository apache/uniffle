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

import java.util.Map;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;
import org.apache.commons.lang3.StringUtils;

import org.apache.uniffle.common.config.ConfigUtils;
import org.apache.uniffle.common.metrics.CommonMetrics;
import org.apache.uniffle.common.metrics.MetricsManager;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.storage.common.LocalStorage;

import static org.apache.uniffle.common.util.Constants.METRICS_APP_LABEL_NAME;

public class ShuffleServerMetrics {

  private static final String TOTAL_RECEIVED_DATA = "total_received_data";
  private static final String TOTAL_WRITE_DATA = "total_write_data";
  private static final String TOTAL_DELETE_DATA = "total_delete_data";
  private static final String TOTAL_FLUSH_FILE_NUM = "total_flush_file_num";
  private static final String TOTAL_DELETE_FILE_NUM = "total_delete_file_num";
  private static final String STORAGE_USED_BYTES = "storage_used_bytes";
  private static final String FLUSH_FILE_NUM = "flush_file_num";
  private static final String TOTAL_WRITE_BLOCK = "total_write_block";
  private static final String WRITE_BLOCK_SIZE = "write_block_size";
  private static final String TOTAL_WRITE_TIME = "total_write_time";
  private static final String TOTAL_WRITE_HANDLER = "total_write_handler";
  private static final String TOTAL_WRITE_EXCEPTION = "total_write_exception";
  private static final String TOTAL_WRITE_SLOW = "total_write_slow";
  private static final String TOTAL_WRITE_NUM = "total_write_num";
  private static final String APP_NUM_WITH_NODE = "app_num_with_node";
  private static final String PARTITION_NUM_WITH_NODE = "partition_num_with_node";
  private static final String EVENT_SIZE_THRESHOLD_LEVEL1 = "event_size_threshold_level1";
  private static final String EVENT_SIZE_THRESHOLD_LEVEL2 = "event_size_threshold_level2";
  private static final String EVENT_SIZE_THRESHOLD_LEVEL3 = "event_size_threshold_level3";
  private static final String EVENT_SIZE_THRESHOLD_LEVEL4 = "event_size_threshold_level4";
  public static final String EVENT_QUEUE_SIZE = "event_queue_size";
  private static final String MERGE_EVENT_QUEUE_SIZE = "merge_event_queue_size";
  private static final String HADOOP_FLUSH_THREAD_POOL_QUEUE_SIZE =
      "hadoop_flush_thread_pool_queue_size";
  private static final String LOCALFILE_FLUSH_THREAD_POOL_QUEUE_SIZE =
      "localfile_flush_thread_pool_queue_size";
  private static final String FALLBACK_FLUSH_THREAD_POOL_QUEUE_SIZE =
      "fallback_flush_thread_pool_queue_size";
  private static final String READ_LOCAL_DATA_FILE_THREAD_NUM = "read_local_data_file_thread_num";
  private static final String READ_LOCAL_INDEX_FILE_THREAD_NUM = "read_local_index_file_thread_num";
  private static final String READ_MEMORY_DATA_THREAD_NUM = "read_memory_data_thread_num";
  private static final String READ_LOCAL_DATA_FILE_BUFFER_SIZE = "read_local_data_file_buffer_size";
  private static final String READ_LOCAL_INDEX_FILE_BUFFER_SIZE =
      "read_local_index_file_buffer_size";
  private static final String READ_MEMORY_DATA_BUFFER_SIZE = "read_memory_data_buffer_size";
  private static final String TOTAL_READ_DATA = "total_read_data";
  private static final String TOTAL_READ_LOCAL_DATA_FILE = "total_read_local_data_file";
  private static final String TOTAL_READ_LOCAL_INDEX_FILE = "total_read_local_index_file";
  private static final String TOTAL_READ_MEMORY_DATA = "total_read_memory_data";
  private static final String TOTAL_READ_TIME = "total_read_time";
  private static final String TOTAL_REQUIRE_READ_MEMORY = "total_require_read_memory_num";
  private static final String TOTAL_REQUIRE_READ_MEMORY_RETRY =
      "total_require_read_memory_retry_num";
  private static final String TOTAL_REQUIRE_READ_MEMORY_FAILED =
      "total_require_read_memory_failed_num";

  private static final String LOCAL_STORAGE_IS_WRITABLE = "local_storage_is_writable";
  private static final String LOCAL_STORAGE_IS_TIMEOUT = "local_storage_is_timeout";
  private static final String LOCAL_STORAGE_TOTAL_DIRS_NUM = "local_storage_total_dirs_num";
  private static final String LOCAL_STORAGE_CORRUPTED_DIRS_NUM = "local_storage_corrupted_dirs_num";
  private static final String LOCAL_STORAGE_TOTAL_SPACE = "local_storage_total_space";
  private static final String LOCAL_STORAGE_WHOLE_DISK_USED_SPACE =
      "local_storage_whole_disk_used_space";
  private static final String LOCAL_STORAGE_SERVICE_USED_SPACE = "local_storage_service_used_space";
  private static final String LOCAL_STORAGE_USED_SPACE_RATIO = "local_storage_used_space_ratio";

  private static final String IS_HEALTHY = "is_healthy";
  private static final String ALLOCATED_BUFFER_SIZE = "allocated_buffer_size";
  private static final String IN_FLUSH_BUFFER_SIZE = "in_flush_buffer_size";
  private static final String USED_BUFFER_SIZE = "used_buffer_size";
  private static final String READ_USED_BUFFER_SIZE = "read_used_buffer_size";
  public static final String USED_DIRECT_MEMORY_SIZE = "used_direct_memory_size";
  public static final String USED_DIRECT_MEMORY_SIZE_BY_NETTY = "used_direct_memory_size_by_netty";
  public static final String USED_DIRECT_MEMORY_SIZE_BY_GRPC_NETTY =
      "used_direct_memory_size_by_grpc_netty";
  private static final String TOTAL_FAILED_WRITTEN_EVENT_NUM = "total_failed_written_event_num";
  private static final String TOTAL_DROPPED_EVENT_NUM = "total_dropped_event_num";
  private static final String TOTAL_HADOOP_WRITE_DATA = "total_hadoop_write_data";
  private static final String TOTAL_HADOOP_WRITE_DATA_FOR_HUGE_PARTITION =
      "total_hadoop_write_data_for_huge_partition";
  private static final String TOTAL_LOCALFILE_WRITE_DATA = "total_localfile_write_data";
  private static final String LOCAL_DISK_PATH_LABEL = "local_disk_path";
  public static final String LOCAL_DISK_PATH_LABEL_ALL = "ALL";
  private static final String TOTAL_REQUIRE_BUFFER_FAILED = "total_require_buffer_failed";
  public static final String TOTAL_REQUIRE_BUFFER_FAILED_FOR_HUGE_PARTITION =
      "total_require_buffer_failed_for_huge_partition";
  private static final String TOTAL_REQUIRE_BUFFER_FAILED_FOR_REGULAR_PARTITION =
      "total_require_buffer_failed_for_regular_partition";
  private static final String STORAGE_TOTAL_WRITE_LOCAL = "storage_total_write_local";
  private static final String STORAGE_RETRY_WRITE_LOCAL = "storage_retry_write_local";
  private static final String STORAGE_FAILED_WRITE_LOCAL = "storage_failed_write_local";
  private static final String STORAGE_SUCCESS_WRITE_LOCAL = "storage_success_write_local";
  private static final String STORAGE_HOST_LABEL = "storage_host";
  public static final String STORAGE_HOST_LABEL_ALL = "ALL";
  public static final String STORAGE_TOTAL_WRITE_REMOTE = "storage_total_write_remote";
  public static final String STORAGE_RETRY_WRITE_REMOTE = "storage_retry_write_remote";
  public static final String STORAGE_FAILED_WRITE_REMOTE = "storage_failed_write_remote";
  public static final String STORAGE_SUCCESS_WRITE_REMOTE = "storage_success_write_remote";

  private static final String TOTAL_APP_NUM = "total_app_num";
  private static final String TOTAL_APP_WITH_HUGE_PARTITION_NUM =
      "total_app_with_huge_partition_num";
  private static final String TOTAL_PARTITION_NUM = "total_partition_num";
  private static final String TOTAL_HUGE_PARTITION_NUM = "total_huge_partition_num";
  private static final String TOTAL_HUGE_PARTITION_EXCEED_HARD_LIMIT_NUM =
      "total_huge_partition_exceed_hard_limit_num";

  private static final String HUGE_PARTITION_NUM = "huge_partition_num";
  private static final String APP_WITH_HUGE_PARTITION_NUM = "app_with_huge_partition_num";

  private static final String LOCAL_FILE_EVENT_FLUSH_NUM = "local_file_event_flush_num";
  private static final String HADOOP_EVENT_FLUSH_NUM = "hadoop_event_flush_num";

  private static final String TOTAL_EXPIRED_PRE_ALLOCATED_BUFFER_NUM =
      "total_expired_preAllocated_buffer_num";
  private static final String TOTAL_APP_NOT_FOUND_NUM = "total_app_not_found_num";

  private static final String TOTAL_REMOVE_RESOURCE_TIME = "total_remove_resource_time";
  private static final String TOTAL_REMOVE_RESOURCE_BY_SHUFFLE_IDS_TIME =
      "total_remove_resource_by_shuffle_ids_time";

  public static final String TOPN_OF_TOTAL_DATA_SIZE_FOR_APP = "topN_of_total_data_size_for_app";
  public static final String TOPN_OF_IN_MEMORY_DATA_SIZE_FOR_APP =
      "topN_of_in_memory_data_size_for_app";
  public static final String TOPN_OF_ON_LOCALFILE_DATA_SIZE_FOR_APP =
      "topN_of_on_localfile_data_size_for_app";
  public static final String TOPN_OF_ON_HADOOP_DATA_SIZE_FOR_APP =
      "topN_of_on_hadoop_data_size_for_app";

  public static final String REQUIRE_BUFFER_COUNT = "require_buffer_count";

  public static final String IN_FLUSH_BLOCK_COUNT_IN_BUFFER_POOL =
      "in_flush_block_count_in_buffer_pool";
  public static final String BLOCK_COUNT_IN_BUFFER_POOL = "block_count_in_buffer_pool";
  public static final String BUFFER_COUNT_IN_BUFFER_POOL = "buffer_count_in_buffer_pool";
  public static final String SHUFFLE_COUNT_IN_BUFFER_POOL = "shuffle_count_in_buffer_pool";

  public static final String COMMITTED_BLOCK_COUNT = "committed_block_count";
  public static final String REPORTED_BLOCK_COUNT = "reported_block_count";
  public static final String CACHED_BLOCK_COUNT = "cached_block_count";

  private static final String TOTAL_LOCAL_RENAME_AND_DELETION_FAILED =
      "total_local_rename_and_deletion_failed";

  public static Counter.Child counterTotalAppNum;
  public static Counter.Child counterTotalAppWithHugePartitionNum;
  public static Counter.Child counterTotalPartitionNum;
  public static Counter.Child counterTotalHugePartitionNum;
  public static Counter.Child counterTotalHugePartitionExceedHardLimitNum;

  public static Counter.Child counterTotalReceivedDataSize;
  public static Counter.Child counterTotalWriteDataSize;
  public static Counter.Child counterTotalDeleteDataSize;
  public static Counter.Child counterTotalFlushFileNum;
  public static Counter.Child counterTotalDeleteFileNum;
  public static Gauge.Child gaugeStorageUsedBytes;
  public static Gauge.Child gaugeFlushFileNum;
  public static Counter.Child counterTotalWriteBlockSize;
  public static Histogram appHistogramWriteBlockSize;
  public static Counter.Child counterTotalWriteTime;
  public static Counter.Child counterWriteException;
  public static Counter.Child counterWriteSlow;
  public static Counter.Child counterWriteTotal;
  public static Counter.Child counterEventSizeThresholdLevel1;
  public static Counter.Child counterEventSizeThresholdLevel2;
  public static Counter.Child counterEventSizeThresholdLevel3;
  public static Counter.Child counterEventSizeThresholdLevel4;
  public static Counter.Child counterTotalReadDataSize;
  public static Counter.Child counterTotalReadLocalDataFileSize;
  public static Counter.Child counterTotalReadLocalIndexFileSize;
  public static Counter.Child counterTotalReadMemoryDataSize;
  public static Counter.Child counterTotalReadTime;
  public static Counter.Child counterTotalFailedWrittenEventNum;
  public static Counter.Child counterTotalDroppedEventNum;
  public static Counter.Child counterTotalRequireBufferFailed;
  public static Counter.Child counterTotalRequireBufferFailedForHugePartition;
  public static Counter.Child counterTotalRequireBufferFailedForRegularPartition;

  public static Counter.Child counterLocalStorageTotalWrite;
  public static Counter.Child counterLocalStorageRetryWrite;
  public static Counter.Child counterLocalStorageFailedWrite;
  public static Counter.Child counterLocalStorageSuccessWrite;
  public static Counter.Child counterTotalRequireReadMemoryNum;
  public static Counter.Child counterTotalRequireReadMemoryRetryNum;
  public static Counter.Child counterTotalRequireReadMemoryFailedNum;

  public static Summary summaryTotalRemoveResourceTime;
  public static Summary summaryTotalRemoveResourceByShuffleIdsTime;

  public static Gauge.Child gaugeHugePartitionNum;
  public static Gauge.Child gaugeAppWithHugePartitionNum;

  public static Gauge gaugeLocalStorageIsWritable;
  public static Gauge gaugeLocalStorageIsTimeout;
  public static Gauge.Child gaugeLocalStorageTotalDirsNum;
  public static Gauge.Child gaugeLocalStorageCorruptedDirsNum;
  public static Gauge.Child gaugeLocalStorageTotalSpace;
  public static Gauge.Child gaugeLocalStorageWholeDiskUsedSpace;
  public static Gauge.Child gaugeLocalStorageServiceUsedSpace;
  public static Gauge.Child gaugeLocalStorageUsedSpaceRatio;

  public static Gauge.Child gaugeIsHealthy;
  public static Gauge.Child gaugeAllocatedBufferSize;
  public static Gauge.Child gaugeInFlushBufferSize;
  public static Gauge.Child gaugeUsedBufferSize;
  public static Gauge.Child gaugeReadBufferUsedSize;
  public static Gauge.Child gaugeWriteHandler;
  public static Gauge.Child gaugeMergeEventQueueSize;
  public static Gauge.Child gaugeHadoopFlushThreadPoolQueueSize;
  public static Gauge.Child gaugeLocalfileFlushThreadPoolQueueSize;
  public static Gauge.Child gaugeFallbackFlushThreadPoolQueueSize;
  public static Gauge.Child gaugeAppNum;
  public static Gauge.Child gaugeTotalPartitionNum;
  public static Gauge.Child gaugeReadLocalDataFileThreadNum;
  public static Gauge.Child gaugeReadLocalIndexFileThreadNum;
  public static Gauge.Child gaugeReadMemoryDataThreadNum;
  public static Gauge.Child gaugeReadLocalDataFileBufferSize;
  public static Gauge.Child gaugeReadLocalIndexFileBufferSize;
  public static Gauge.Child gaugeReadMemoryDataBufferSize;
  public static Counter.Child counterLocalRenameAndDeletionFaileTd;

  public static Gauge gaugeTotalDataSizeUsage;
  public static Gauge gaugeInMemoryDataSizeUsage;
  public static Gauge gaugeOnDiskDataSizeUsage;
  public static Gauge gaugeOnHadoopDataSizeUsage;

  public static Counter counterRemoteStorageTotalWrite;
  public static Counter counterRemoteStorageRetryWrite;
  public static Counter counterRemoteStorageFailedWrite;
  public static Counter counterRemoteStorageSuccessWrite;
  public static Counter counterTotalHadoopWriteDataSize;
  public static Counter counterTotalHadoopWriteDataSizeForHugePartition;
  public static Counter counterTotalLocalFileWriteDataSize;

  private static String tags;
  public static Counter counterLocalFileEventFlush;
  public static Counter counterHadoopEventFlush;
  public static Counter counterPreAllocatedBufferExpired;
  public static Counter counterAppNotFound;

  private static MetricsManager metricsManager;
  private static boolean isRegister = false;

  public static synchronized void register(
      CollectorRegistry collectorRegistry, String tags, ShuffleServerConf serverConf) {
    if (!isRegister) {
      ShuffleServerMetrics.tags = tags;
      Map<String, String> labels = Maps.newHashMap();
      labels.put(Constants.METRICS_TAG_LABEL_NAME, ShuffleServerMetrics.tags);
      metricsManager = new MetricsManager(collectorRegistry, labels);
      isRegister = true;
      setUpMetrics(serverConf);
      CommonMetrics.register(collectorRegistry, tags);
    }
  }

  public static void register(ShuffleServerConf serverConf) {
    register(CollectorRegistry.defaultRegistry, Constants.SHUFFLE_SERVER_VERSION, serverConf);
  }

  @VisibleForTesting
  public static void register() {
    register(
        CollectorRegistry.defaultRegistry,
        Constants.SHUFFLE_SERVER_VERSION,
        new ShuffleServerConf());
  }

  @VisibleForTesting
  public static void clear() {
    isRegister = false;
    CollectorRegistry.defaultRegistry.clear();
    CommonMetrics.clear();
  }

  public static CollectorRegistry getCollectorRegistry() {
    return metricsManager.getCollectorRegistry();
  }

  public static void incStorageRetryCounter(String storageHost) {
    if (LocalStorage.STORAGE_HOST.equals(storageHost)) {
      counterLocalStorageTotalWrite.inc();
      counterLocalStorageRetryWrite.inc();
    } else {
      if (!StringUtils.isEmpty(storageHost)) {
        counterRemoteStorageTotalWrite.labels(tags, storageHost).inc();
        counterRemoteStorageRetryWrite.labels(tags, storageHost).inc();
      }
    }
  }

  public static void incStorageSuccessCounter(String storageHost) {
    if (LocalStorage.STORAGE_HOST.equals(storageHost)) {
      counterLocalStorageTotalWrite.inc();
      counterLocalStorageSuccessWrite.inc();
    } else {
      if (!StringUtils.isEmpty(storageHost)) {
        counterRemoteStorageTotalWrite.labels(tags, storageHost).inc();
        counterRemoteStorageSuccessWrite.labels(tags, storageHost).inc();
      }
    }
  }

  public static void incStorageFailedCounter(String storageHost) {
    if (LocalStorage.STORAGE_HOST.equals(storageHost)) {
      counterLocalStorageTotalWrite.inc();
      counterLocalStorageFailedWrite.inc();
    } else {
      if (!StringUtils.isEmpty(storageHost)) {
        counterRemoteStorageTotalWrite.labels(tags, storageHost).inc();
        counterRemoteStorageFailedWrite.labels(tags, storageHost).inc();
      }
    }
  }

  public static void incHadoopStorageWriteDataSize(
      String storageHost, long size, boolean isOwnedByHugePartition) {
    if (StringUtils.isEmpty(storageHost)) {
      return;
    }
    counterTotalHadoopWriteDataSize.labels(tags, storageHost).inc(size);
    counterTotalHadoopWriteDataSize.labels(tags, STORAGE_HOST_LABEL_ALL).inc(size);
    if (isOwnedByHugePartition) {
      counterTotalHadoopWriteDataSizeForHugePartition.labels(tags, storageHost).inc(size);
      counterTotalHadoopWriteDataSizeForHugePartition
          .labels(tags, STORAGE_HOST_LABEL_ALL)
          .inc(size);
    }
  }

  // only for test cases
  @VisibleForTesting
  public static void incHadoopStorageWriteDataSize(String storageHost, long size) {
    incHadoopStorageWriteDataSize(storageHost, size, false);
  }

  private static void setUpMetrics(ShuffleServerConf serverConf) {
    counterTotalReceivedDataSize = metricsManager.addLabeledCounter(TOTAL_RECEIVED_DATA);
    counterTotalWriteDataSize = metricsManager.addLabeledCounter(TOTAL_WRITE_DATA);
    counterTotalDeleteDataSize = metricsManager.addLabeledCounter(TOTAL_DELETE_DATA);
    counterTotalFlushFileNum = metricsManager.addLabeledCounter(TOTAL_FLUSH_FILE_NUM);
    counterTotalDeleteFileNum = metricsManager.addLabeledCounter(TOTAL_DELETE_FILE_NUM);
    gaugeStorageUsedBytes = metricsManager.addLabeledGauge(STORAGE_USED_BYTES);
    gaugeFlushFileNum = metricsManager.addLabeledGauge(FLUSH_FILE_NUM);
    counterTotalWriteBlockSize = metricsManager.addLabeledCounter(TOTAL_WRITE_BLOCK);
    appHistogramWriteBlockSize =
        metricsManager.addHistogram(
            WRITE_BLOCK_SIZE,
            ConfigUtils.convertBytesStringToDoubleArray(
                serverConf.get(ShuffleServerConf.APP_LEVEL_SHUFFLE_BLOCK_SIZE_METRIC_BUCKETS)),
            METRICS_APP_LABEL_NAME);
    counterTotalWriteTime = metricsManager.addLabeledCounter(TOTAL_WRITE_TIME);
    counterWriteException = metricsManager.addLabeledCounter(TOTAL_WRITE_EXCEPTION);
    counterWriteSlow = metricsManager.addLabeledCounter(TOTAL_WRITE_SLOW);
    counterWriteTotal = metricsManager.addLabeledCounter(TOTAL_WRITE_NUM);
    counterEventSizeThresholdLevel1 = metricsManager.addLabeledCounter(EVENT_SIZE_THRESHOLD_LEVEL1);
    counterEventSizeThresholdLevel2 = metricsManager.addLabeledCounter(EVENT_SIZE_THRESHOLD_LEVEL2);
    counterEventSizeThresholdLevel3 = metricsManager.addLabeledCounter(EVENT_SIZE_THRESHOLD_LEVEL3);
    counterEventSizeThresholdLevel4 = metricsManager.addLabeledCounter(EVENT_SIZE_THRESHOLD_LEVEL4);
    counterTotalReadDataSize = metricsManager.addLabeledCounter(TOTAL_READ_DATA);
    counterTotalReadLocalDataFileSize =
        metricsManager.addLabeledCounter(TOTAL_READ_LOCAL_DATA_FILE);
    counterTotalReadLocalIndexFileSize =
        metricsManager.addLabeledCounter(TOTAL_READ_LOCAL_INDEX_FILE);
    counterTotalReadMemoryDataSize = metricsManager.addLabeledCounter(TOTAL_READ_MEMORY_DATA);
    counterTotalReadTime = metricsManager.addLabeledCounter(TOTAL_READ_TIME);
    counterTotalDroppedEventNum = metricsManager.addLabeledCounter(TOTAL_DROPPED_EVENT_NUM);
    counterTotalFailedWrittenEventNum =
        metricsManager.addLabeledCounter(TOTAL_FAILED_WRITTEN_EVENT_NUM);
    counterTotalHadoopWriteDataSize =
        metricsManager.addCounter(
            TOTAL_HADOOP_WRITE_DATA, Constants.METRICS_TAG_LABEL_NAME, STORAGE_HOST_LABEL);
    counterTotalHadoopWriteDataSizeForHugePartition =
        metricsManager.addCounter(
            TOTAL_HADOOP_WRITE_DATA_FOR_HUGE_PARTITION,
            Constants.METRICS_TAG_LABEL_NAME,
            STORAGE_HOST_LABEL);
    counterTotalLocalFileWriteDataSize =
        metricsManager.addCounter(TOTAL_LOCALFILE_WRITE_DATA, LOCAL_DISK_PATH_LABEL);

    counterTotalRequireBufferFailed = metricsManager.addLabeledCounter(TOTAL_REQUIRE_BUFFER_FAILED);
    counterTotalRequireBufferFailedForRegularPartition =
        metricsManager.addLabeledCounter(TOTAL_REQUIRE_BUFFER_FAILED_FOR_REGULAR_PARTITION);
    counterTotalRequireBufferFailedForHugePartition =
        metricsManager.addLabeledCounter(TOTAL_REQUIRE_BUFFER_FAILED_FOR_HUGE_PARTITION);

    counterLocalStorageTotalWrite = metricsManager.addLabeledCounter(STORAGE_TOTAL_WRITE_LOCAL);
    counterLocalStorageRetryWrite = metricsManager.addLabeledCounter(STORAGE_RETRY_WRITE_LOCAL);
    counterLocalStorageFailedWrite = metricsManager.addLabeledCounter(STORAGE_FAILED_WRITE_LOCAL);
    counterLocalStorageSuccessWrite = metricsManager.addLabeledCounter(STORAGE_SUCCESS_WRITE_LOCAL);
    counterRemoteStorageTotalWrite =
        metricsManager.addCounter(
            STORAGE_TOTAL_WRITE_REMOTE, Constants.METRICS_TAG_LABEL_NAME, STORAGE_HOST_LABEL);
    counterRemoteStorageRetryWrite =
        metricsManager.addCounter(
            STORAGE_RETRY_WRITE_REMOTE, Constants.METRICS_TAG_LABEL_NAME, STORAGE_HOST_LABEL);
    counterRemoteStorageFailedWrite =
        metricsManager.addCounter(
            STORAGE_FAILED_WRITE_REMOTE, Constants.METRICS_TAG_LABEL_NAME, STORAGE_HOST_LABEL);
    counterRemoteStorageSuccessWrite =
        metricsManager.addCounter(
            STORAGE_SUCCESS_WRITE_REMOTE, Constants.METRICS_TAG_LABEL_NAME, STORAGE_HOST_LABEL);
    counterTotalRequireReadMemoryNum = metricsManager.addLabeledCounter(TOTAL_REQUIRE_READ_MEMORY);
    counterTotalRequireReadMemoryRetryNum =
        metricsManager.addLabeledCounter(TOTAL_REQUIRE_READ_MEMORY_RETRY);
    counterTotalRequireReadMemoryFailedNum =
        metricsManager.addLabeledCounter(TOTAL_REQUIRE_READ_MEMORY_FAILED);

    counterTotalAppNum = metricsManager.addLabeledCounter(TOTAL_APP_NUM);
    counterTotalAppWithHugePartitionNum =
        metricsManager.addLabeledCounter(TOTAL_APP_WITH_HUGE_PARTITION_NUM);
    counterTotalPartitionNum = metricsManager.addLabeledCounter(TOTAL_PARTITION_NUM);
    counterTotalHugePartitionNum = metricsManager.addLabeledCounter(TOTAL_HUGE_PARTITION_NUM);
    counterTotalHugePartitionExceedHardLimitNum =
        metricsManager.addLabeledCounter(TOTAL_HUGE_PARTITION_EXCEED_HARD_LIMIT_NUM);
    counterLocalRenameAndDeletionFaileTd =
        metricsManager.addLabeledCounter(TOTAL_LOCAL_RENAME_AND_DELETION_FAILED);

    gaugeLocalStorageIsWritable =
        metricsManager.addGauge(LOCAL_STORAGE_IS_WRITABLE, LOCAL_DISK_PATH_LABEL);
    gaugeLocalStorageIsTimeout =
        metricsManager.addGauge(LOCAL_STORAGE_IS_TIMEOUT, LOCAL_DISK_PATH_LABEL);
    gaugeLocalStorageTotalDirsNum = metricsManager.addLabeledGauge(LOCAL_STORAGE_TOTAL_DIRS_NUM);
    gaugeLocalStorageCorruptedDirsNum =
        metricsManager.addLabeledGauge(LOCAL_STORAGE_CORRUPTED_DIRS_NUM);
    gaugeLocalStorageTotalSpace = metricsManager.addLabeledGauge(LOCAL_STORAGE_TOTAL_SPACE);
    gaugeLocalStorageWholeDiskUsedSpace =
        metricsManager.addLabeledGauge(LOCAL_STORAGE_WHOLE_DISK_USED_SPACE);
    gaugeLocalStorageServiceUsedSpace =
        metricsManager.addLabeledGauge(LOCAL_STORAGE_SERVICE_USED_SPACE);
    gaugeLocalStorageUsedSpaceRatio =
        metricsManager.addLabeledGauge(LOCAL_STORAGE_USED_SPACE_RATIO);

    gaugeIsHealthy = metricsManager.addLabeledGauge(IS_HEALTHY);
    gaugeAllocatedBufferSize = metricsManager.addLabeledGauge(ALLOCATED_BUFFER_SIZE);
    gaugeInFlushBufferSize = metricsManager.addLabeledGauge(IN_FLUSH_BUFFER_SIZE);
    gaugeUsedBufferSize = metricsManager.addLabeledGauge(USED_BUFFER_SIZE);
    gaugeReadBufferUsedSize = metricsManager.addLabeledGauge(READ_USED_BUFFER_SIZE);
    gaugeWriteHandler = metricsManager.addLabeledGauge(TOTAL_WRITE_HANDLER);
    gaugeMergeEventQueueSize = metricsManager.addLabeledGauge(MERGE_EVENT_QUEUE_SIZE);
    gaugeHadoopFlushThreadPoolQueueSize =
        metricsManager.addLabeledGauge(HADOOP_FLUSH_THREAD_POOL_QUEUE_SIZE);
    gaugeLocalfileFlushThreadPoolQueueSize =
        metricsManager.addLabeledGauge(LOCALFILE_FLUSH_THREAD_POOL_QUEUE_SIZE);
    gaugeFallbackFlushThreadPoolQueueSize =
        metricsManager.addLabeledGauge(FALLBACK_FLUSH_THREAD_POOL_QUEUE_SIZE);

    gaugeAppNum = metricsManager.addLabeledGauge(APP_NUM_WITH_NODE);
    gaugeTotalPartitionNum = metricsManager.addLabeledGauge(PARTITION_NUM_WITH_NODE);

    gaugeReadLocalDataFileThreadNum =
        metricsManager.addLabeledGauge(READ_LOCAL_DATA_FILE_THREAD_NUM);
    gaugeReadLocalIndexFileThreadNum =
        metricsManager.addLabeledGauge(READ_LOCAL_INDEX_FILE_THREAD_NUM);
    gaugeReadMemoryDataThreadNum = metricsManager.addLabeledGauge(READ_MEMORY_DATA_THREAD_NUM);
    gaugeReadLocalDataFileBufferSize =
        metricsManager.addLabeledGauge(READ_LOCAL_DATA_FILE_BUFFER_SIZE);
    gaugeReadLocalIndexFileBufferSize =
        metricsManager.addLabeledGauge(READ_LOCAL_INDEX_FILE_BUFFER_SIZE);
    gaugeReadMemoryDataBufferSize = metricsManager.addLabeledGauge(READ_MEMORY_DATA_BUFFER_SIZE);

    gaugeHugePartitionNum = metricsManager.addLabeledGauge(HUGE_PARTITION_NUM);
    gaugeAppWithHugePartitionNum = metricsManager.addLabeledGauge(APP_WITH_HUGE_PARTITION_NUM);

    counterLocalFileEventFlush = metricsManager.addCounter(LOCAL_FILE_EVENT_FLUSH_NUM);
    counterHadoopEventFlush = metricsManager.addCounter(HADOOP_EVENT_FLUSH_NUM);

    counterPreAllocatedBufferExpired =
        metricsManager.addCounter(TOTAL_EXPIRED_PRE_ALLOCATED_BUFFER_NUM);

    counterAppNotFound = metricsManager.addCounter(TOTAL_APP_NOT_FOUND_NUM);

    summaryTotalRemoveResourceTime = metricsManager.addSummary(TOTAL_REMOVE_RESOURCE_TIME);
    summaryTotalRemoveResourceByShuffleIdsTime =
        metricsManager.addSummary(TOTAL_REMOVE_RESOURCE_BY_SHUFFLE_IDS_TIME);

    gaugeTotalDataSizeUsage =
        Gauge.build()
            .name(TOPN_OF_TOTAL_DATA_SIZE_FOR_APP)
            .help("top N of total shuffle data size for app level")
            .labelNames("app_id")
            .register(metricsManager.getCollectorRegistry());

    gaugeInMemoryDataSizeUsage =
        Gauge.build()
            .name(TOPN_OF_IN_MEMORY_DATA_SIZE_FOR_APP)
            .help("top N of in memory shuffle data size for app level")
            .labelNames("app_id")
            .register(metricsManager.getCollectorRegistry());

    gaugeOnDiskDataSizeUsage =
        Gauge.build()
            .name(TOPN_OF_ON_LOCALFILE_DATA_SIZE_FOR_APP)
            .help("top N of on disk shuffle data size for app level")
            .labelNames("app_id")
            .register(metricsManager.getCollectorRegistry());

    gaugeOnHadoopDataSizeUsage =
        Gauge.build()
            .name(TOPN_OF_ON_HADOOP_DATA_SIZE_FOR_APP)
            .help("top N of on hadoop shuffle data size for app level")
            .labelNames("app_id")
            .register(metricsManager.getCollectorRegistry());
  }

  public static <T extends Number> void addLabeledGauge(String name, Supplier<T> supplier) {
    metricsManager.addLabeledGauge(name, supplier);
  }

  public static <T extends Number> void addLabeledCacheGauge(
      String name, Supplier<T> supplier, long updateInterval) {
    if (!isRegister) {
      return;
    }
    metricsManager.addLabeledCacheGauge(name, supplier, updateInterval);
  }

  public static Double getMetricsValue(String metricName) {
    return metricsManager
        .getCollectorRegistry()
        .getSampleValue(
            metricName,
            metricsManager.getDefaultLabelNames(),
            metricsManager.getDefaultLabelValues());
  }
}
