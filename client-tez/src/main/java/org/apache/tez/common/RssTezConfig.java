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

package org.apache.tez.common;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;

import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.config.RssConf;

public class RssTezConfig {

  public static final String TEZ_RSS_CONFIG_PREFIX = "tez.";
  public static final String RSS_CLIENT_HEARTBEAT_THREAD_NUM =
      TEZ_RSS_CONFIG_PREFIX + "rss.client.heartBeat.threadNum";
  public static final int RSS_CLIENT_HEARTBEAT_THREAD_NUM_DEFAULT_VALUE = 4;
  public static final String RSS_CLIENT_TYPE =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_TYPE;
  public static final String RSS_CLIENT_TYPE_DEFAULT_VALUE =
      RssClientConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE;
  public static final String RSS_CLIENT_RETRY_MAX =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_RETRY_MAX;
  public static final int RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE =
      RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE;
  public static final String RSS_CLIENT_RETRY_INTERVAL_MAX =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX;
  public static final long RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE =
      RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE;
  public static final String RSS_COORDINATOR_QUORUM =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_COORDINATOR_QUORUM;
  public static final String RSS_DATA_REPLICA =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA;
  public static final int RSS_DATA_REPLICA_DEFAULT_VALUE =
      RssClientConfig.RSS_DATA_REPLICA_DEFAULT_VALUE;
  public static final String RSS_DATA_REPLICA_WRITE =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA_WRITE;
  public static final int RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE =
      RssClientConfig.RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE;
  public static final String RSS_DATA_REPLICA_READ =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA_READ;
  public static final int RSS_DATA_REPLICA_READ_DEFAULT_VALUE =
      RssClientConfig.RSS_DATA_REPLICA_READ_DEFAULT_VALUE;
  public static final String RSS_DATA_REPLICA_SKIP_ENABLED =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA_SKIP_ENABLED;
  public static final String RSS_DATA_TRANSFER_POOL_SIZE =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_TRANSFER_POOL_SIZE;
  public static final int RSS_DATA_TRANSFER_POOL_SIZE_DEFAULT_VALUE =
      RssClientConfig.RSS_DATA_TRANSFER_POOL_SIZE_DEFAULT_VALUE;
  public static final String RSS_DATA_COMMIT_POOL_SIZE =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_COMMIT_POOL_SIZE;
  public static final int RSS_DATA_COMMIT_POOL_SIZE_DEFAULT_VALUE =
      RssClientConfig.RSS_DATA_COMMIT_POOL_SIZE_DEFAULT_VALUE;

  public static final boolean RSS_DATA_REPLICA_SKIP_ENABLED_DEFAULT_VALUE =
      RssClientConfig.RSS_DATA_REPLICA_SKIP_ENABLED_DEFAULT_VALUE;
  public static final String RSS_HEARTBEAT_INTERVAL =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_HEARTBEAT_INTERVAL;
  public static final long RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE =
      RssClientConfig.RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE;
  public static final String RSS_HEARTBEAT_TIMEOUT =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_HEARTBEAT_TIMEOUT;
  public static final long RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE =
      RssClientConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE;

  // output:
  public static final String RSS_RUNTIME_IO_SORT_MB =
      TEZ_RSS_CONFIG_PREFIX + "rss.runtime.io.sort.mb";
  public static final int RSS_DEFAULT_RUNTIME_IO_SORT_MB = 100;
  public static final String RSS_CLIENT_SORT_MEMORY_USE_THRESHOLD =
      TEZ_RSS_CONFIG_PREFIX + "rss.client.sort.memory.use.threshold";
  public static final double RSS_CLIENT_DEFAULT_SORT_MEMORY_USE_THRESHOLD = 0.9f;
  public static final String RSS_CLIENT_MAX_BUFFER_SIZE =
      TEZ_RSS_CONFIG_PREFIX + "rss.client.max.buffer.size";
  public static final long RSS_CLIENT_DEFAULT_MAX_BUFFER_SIZE = 3 * 1024;
  public static final String RSS_WRITER_BUFFER_SIZE =
      TEZ_RSS_CONFIG_PREFIX + "rss.writer.buffer.size";
  public static final long RSS_DEFAULT_WRITER_BUFFER_SIZE = 1024 * 1024 * 14;
  public static final String RSS_CLIENT_MEMORY_THRESHOLD =
      TEZ_RSS_CONFIG_PREFIX + "rss.client.memory.threshold";
  public static final double RSS_CLIENT_DEFAULT_MEMORY_THRESHOLD = 0.8f;
  public static final String RSS_CLIENT_SEND_THREAD_NUM =
      TEZ_RSS_CONFIG_PREFIX + "rss.client.send.thread.num";
  public static final int RSS_CLIENT_DEFAULT_THREAD_NUM =
      RssClientConfig.RSS_CLIENT_DEFAULT_SEND_NUM;
  public static final String RSS_CLIENT_SEND_THRESHOLD =
      TEZ_RSS_CONFIG_PREFIX + "rss.client.send.threshold";
  public static final double RSS_CLIENT_DEFAULT_SEND_THRESHOLD = 0.2f;
  public static final String RSS_CLIENT_BATCH_TRIGGER_NUM =
      TEZ_RSS_CONFIG_PREFIX + "rss.client.batch.trigger.num";
  public static final int RSS_CLIENT_DEFAULT_BATCH_TRIGGER_NUM = 50;
  public static final String RSS_DEFAULT_STORAGE_TYPE = "MEMORY";
  public static final String RSS_CLIENT_SEND_CHECK_INTERVAL_MS =
      TEZ_RSS_CONFIG_PREFIX + "rss.client.send.check.interval.ms";
  public static final long RSS_CLIENT_DEFAULT_SEND_CHECK_INTERVAL_MS = 500L;
  public static final String RSS_CLIENT_SEND_CHECK_TIMEOUT_MS =
      TEZ_RSS_CONFIG_PREFIX + "rss.client.send.check.timeout.ms";
  public static final long RSS_CLIENT_DEFAULT_SEND_CHECK_TIMEOUT_MS = 60 * 1000 * 10L;
  public static final String RSS_CLIENT_BITMAP_NUM =
      TEZ_RSS_CONFIG_PREFIX + "rss.client.bitmap.num";
  public static final int RSS_CLIENT_DEFAULT_BITMAP_NUM = 1;
  public static final String HIVE_TEZ_LOG_LEVEL = "hive.tez.log.level";
  public static final String DEFAULT_HIVE_TEZ_LOG_LEVEL = "INFO";
  public static final String DEBUG_HIVE_TEZ_LOG_LEVEL = "debug";

  public static final String RSS_STORAGE_TYPE =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_STORAGE_TYPE;
  public static final String RSS_STORAGE_TYPE_DEFAULT_VALUE = "MEMORY_LOCALFILE";

  public static final String RSS_DYNAMIC_CLIENT_CONF_ENABLED =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED;
  public static final boolean RSS_DYNAMIC_CLIENT_CONF_ENABLED_DEFAULT_VALUE =
      RssClientConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED_DEFAULT_VALUE;

  public static final String RSS_CLIENT_ASSIGNMENT_TAGS =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_ASSIGNMENT_TAGS;

  public static final String RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER =
      RssClientConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER;
  public static final int RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER_DEFAULT_VALUE =
      RssClientConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER_DEFAULT_VALUE;

  public static final String RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL;
  public static final long RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL_DEFAULT_VALUE =
      RssClientConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL_DEFAULT_VALUE;
  public static final String RSS_CLIENT_ASSIGNMENT_RETRY_TIMES =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES;
  public static final int RSS_CLIENT_ASSIGNMENT_RETRY_TIMES_DEFAULT_VALUE =
      RssClientConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES_DEFAULT_VALUE;

  public static final String RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED;
  public static final boolean RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED_DEFAULT_VALUE =
      RssClientConfig.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED_DEFAULT_VALUE;

  public static final String RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR;

  public static final double RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR_DEFAULT_VALUE =
      RssClientConfig.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR_DEFAULT_VALUE;

  public static final String RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER;
  public static final int RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER_DEFAULT_VALUE =
      RssClientConfig.RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER_DEFAULT_VALUE;

  public static final String RSS_CLIENT_READ_BUFFER_SIZE =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE;
  public static final String RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE =
      RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE;

  public static final String RSS_PARTITION_NUM_PER_RANGE =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_PARTITION_NUM_PER_RANGE;
  public static final int RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE =
      RssClientConfig.RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE;

  public static final String RSS_REMOTE_STORAGE_PATH =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_REMOTE_STORAGE_PATH;
  public static final String RSS_REMOTE_STORAGE_CONF =
      TEZ_RSS_CONFIG_PREFIX + "rss.remote.storage.conf";

  // Whether enable test mode for the MR Client
  public static final String RSS_TEST_MODE_ENABLE =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_TEST_MODE_ENABLE;

  public static final String RSS_AM_SHUFFLE_MANAGER_ADDRESS =
      TEZ_RSS_CONFIG_PREFIX + "rss.am.shuffle.manager.address";
  public static final String RSS_AM_SHUFFLE_MANAGER_PORT =
      TEZ_RSS_CONFIG_PREFIX + "rss.am.shuffle.manager.port";
  public static final String RSS_AM_SHUFFLE_MANAGER_DEBUG =
      TEZ_RSS_CONFIG_PREFIX + "rss.am.shuffle.manager.debug";
  public static final String RSS_AM_SLOW_START_ENABLE =
      TEZ_RSS_CONFIG_PREFIX + "rss.am.slow.start.enable";
  public static final Boolean RSS_AM_SLOW_START_ENABLE_DEFAULT = false;

  public static final String RSS_REDUCE_INITIAL_MEMORY =
      TEZ_RSS_CONFIG_PREFIX + "rss.reduce.initial.memory";

  public static final String RSS_ACCESS_TIMEOUT_MS =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_ACCESS_TIMEOUT_MS;
  public static final int RSS_ACCESS_TIMEOUT_MS_DEFAULT_VALUE =
      RssClientConfig.RSS_ACCESS_TIMEOUT_MS_DEFAULT_VALUE;

  public static final Set<String> RSS_MANDATORY_CLUSTER_CONF =
      ImmutableSet.of(RSS_STORAGE_TYPE, RSS_REMOTE_STORAGE_PATH);

  public static final String RSS_SHUFFLE_SOURCE_VERTEX_ID =
      TEZ_RSS_CONFIG_PREFIX + "rss.shuffle.source.vertex.id";
  public static final String RSS_SHUFFLE_DESTINATION_VERTEX_ID =
      TEZ_RSS_CONFIG_PREFIX + "rss.shuffle.destination.vertex.id";

  public static final String RSS_AVOID_RECOMPUTE_SUCCEEDED_TASK =
      TEZ_RSS_CONFIG_PREFIX + "rss.avoid.recompute.succeeded.task";
  public static final boolean RSS_AVOID_RECOMPUTE_SUCCEEDED_TASK_DEFAULT = false;

  public static final String RSS_REDUCE_REMOTE_SPILL_ENABLED =
      TEZ_RSS_CONFIG_PREFIX + "rss.reduce.remote.spill.enable";
  public static final boolean RSS_REDUCE_REMOTE_SPILL_ENABLED_DEFAULT = false;
  public static final String RSS_REDUCE_REMOTE_SPILL_REPLICATION =
      TEZ_RSS_CONFIG_PREFIX + "rss.reduce.remote.spill.replication";
  public static final int RSS_REDUCE_REMOTE_SPILL_REPLICATION_DEFAULT = 1;
  public static final String RSS_REDUCE_REMOTE_SPILL_RETRIES =
      TEZ_RSS_CONFIG_PREFIX + "rss.reduce.remote.spill.retries";
  public static final int RSS_REDUCE_REMOTE_SPILL_RETRIES_DEFAULT = 5;
  public static final String RSS_REMOTE_SPILL_STORAGE_PATH =
      TEZ_RSS_CONFIG_PREFIX + "rss.remote.spill.storage.path";
  public static final String RSS_SHUFFLE_MODE = TEZ_RSS_CONFIG_PREFIX + "shuffle.mode";
  public static final String DEFAULT_RSS_SHUFFLE_MODE = "remote";

  public static final String RSS_REMOTE_MERGE_ENABLE =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_REMOTE_MERGE_ENABLE;
  public static final boolean RSS_REMOTE_MERGE_ENABLE_DEFAULT = false;
  public static final String RSS_MERGED_BLOCK_SZIE =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_MERGED_BLOCK_SZIE;
  public static final int RSS_MERGED_BLOCK_SZIE_DEFAULT =
      RssClientConfig.RSS_MERGED_BLOCK_SZIE_DEFAULT;
  public static final String RSS_REMOTE_MERGE_CLASS_LOADER =
      TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_REMOTE_MERGE_CLASS_LOADER;

  public static RssConf toRssConf(Configuration jobConf) {
    RssConf rssConf = new RssConf();
    for (Map.Entry<String, String> entry : jobConf) {
      String key = entry.getKey();
      if (!key.startsWith(TEZ_RSS_CONFIG_PREFIX)) {
        continue;
      }
      key = key.substring(TEZ_RSS_CONFIG_PREFIX.length());
      rssConf.setString(key, entry.getValue());
    }
    return rssConf;
  }
}
