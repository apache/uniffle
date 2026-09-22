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

package org.apache.uniffle.client.util;

import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssClientConf;

public class RssClientConfig {

  public static final String RSS_CLIENT_TYPE = RssClientConf.RSS_CLIENT_TYPE.key();
  public static final String RSS_CLIENT_TYPE_DEFAULT_VALUE =
      RssClientConf.RSS_CLIENT_TYPE.defaultValue().name();
  public static final String RSS_CLIENT_RETRY_MAX = RssClientConf.RSS_CLIENT_RETRY_MAX.key();
  public static final int RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE =
      RssClientConf.RSS_CLIENT_RETRY_MAX.defaultValue();
  public static final String RSS_CLIENT_RETRY_INTERVAL_MAX =
      RssClientConf.RSS_CLIENT_RETRY_INTERVAL_MAX.key();
  public static final long RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE =
      RssClientConf.RSS_CLIENT_RETRY_INTERVAL_MAX.defaultValue();
  public static final String RSS_COORDINATOR_QUORUM = RssBaseConf.RSS_COORDINATOR_QUORUM.key();
  public static final String RSS_DATA_REPLICA = RssClientConf.RSS_DATA_REPLICA.key();
  public static final int RSS_DATA_REPLICA_DEFAULT_VALUE =
      RssClientConf.RSS_DATA_REPLICA.defaultValue();
  public static final String RSS_DATA_REPLICA_WRITE = RssClientConf.RSS_DATA_REPLICA_WRITE.key();
  public static final int RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE =
      RssClientConf.RSS_DATA_REPLICA_WRITE.defaultValue();
  public static final String RSS_DATA_REPLICA_READ = RssClientConf.RSS_DATA_REPLICA_READ.key();
  public static final int RSS_DATA_REPLICA_READ_DEFAULT_VALUE =
      RssClientConf.RSS_DATA_REPLICA_READ.defaultValue();
  public static final String RSS_DATA_REPLICA_SKIP_ENABLED =
      RssClientConf.RSS_DATA_REPLICA_SKIP_ENABLED.key();
  public static final boolean RSS_DATA_REPLICA_SKIP_ENABLED_DEFAULT_VALUE =
      RssClientConf.RSS_DATA_REPLICA_SKIP_ENABLED.defaultValue();
  public static final String RSS_DATA_TRANSFER_POOL_SIZE =
      RssClientConf.RSS_DATA_TRANSFER_POOL_SIZE.key();
  public static final int RSS_DATA_TRANSFER_POOL_SIZE_DEFAULT_VALUE =
      RssClientConf.RSS_DATA_TRANSFER_POOL_SIZE.defaultValue();
  public static final String RSS_DATA_COMMIT_POOL_SIZE =
      RssClientConf.RSS_DATA_COMMIT_POOL_SIZE.key();
  public static final int RSS_DATA_COMMIT_POOL_SIZE_DEFAULT_VALUE =
      RssClientConf.RSS_DATA_COMMIT_POOL_SIZE.defaultValue();
  public static final String RSS_HEARTBEAT_INTERVAL = RssClientConf.RSS_HEARTBEAT_INTERVAL.key();
  public static final long RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE =
      RssClientConf.RSS_HEARTBEAT_INTERVAL.defaultValue();
  public static final String RSS_HEARTBEAT_TIMEOUT = RssClientConf.RSS_HEARTBEAT_TIMEOUT.key();
  public static final String RSS_STORAGE_TYPE = RssBaseConf.RSS_STORAGE_TYPE.key();
  public static final String RSS_CLIENT_SEND_CHECK_INTERVAL_MS =
      RssClientConf.RSS_CLIENT_SEND_CHECK_INTERVAL_MS.key();
  public static final long RSS_CLIENT_SEND_CHECK_INTERVAL_MS_DEFAULT_VALUE =
      RssClientConf.RSS_CLIENT_SEND_CHECK_INTERVAL_MS.defaultValue();
  public static final String RSS_CLIENT_SEND_CHECK_TIMEOUT_MS =
      RssClientConf.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS.key();
  public static final long RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE =
      RssClientConf.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS.defaultValue();
  public static final String RSS_WRITER_BUFFER_SIZE = RssClientConf.RSS_WRITER_BUFFER_SIZE.key();
  public static final String RSS_PARTITION_NUM_PER_RANGE =
      RssClientConf.RSS_PARTITION_NUM_PER_RANGE.key();
  public static final int RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE =
      RssClientConf.RSS_PARTITION_NUM_PER_RANGE.defaultValue();
  public static final String RSS_REMOTE_STORAGE_PATH = RssClientConf.RSS_REMOTE_STORAGE_PATH.key();
  public static final String RSS_INDEX_READ_LIMIT = RssClientConf.RSS_INDEX_READ_LIMIT.key();
  public static final int RSS_INDEX_READ_LIMIT_DEFAULT_VALUE =
      RssClientConf.RSS_INDEX_READ_LIMIT.defaultValue();
  public static final String RSS_CLIENT_SEND_THREAD_NUM = "rss.client.send.thread.num";
  public static final int RSS_CLIENT_DEFAULT_SEND_NUM = 5;
  public static final String RSS_CLIENT_READ_BUFFER_SIZE =
      RssClientConf.RSS_CLIENT_READ_BUFFER_SIZE.key();
  // When the size of read buffer reaches the half of JVM region (i.e., 32m),
  // it will incur humongous allocation, so we set it to 14m.
  public static final String RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE =
      RssClientConf.RSS_CLIENT_READ_BUFFER_SIZE.defaultValue();
  // The tags specified by rss client to determine server assignment.
  public static final String RSS_CLIENT_ASSIGNMENT_TAGS =
      RssClientConf.RSS_CLIENT_ASSIGNMENT_TAGS.key();
  public static final String RSS_TEST_MODE_ENABLE = RssBaseConf.RSS_TEST_MODE_ENABLE.key();

  public static final String RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL =
      RssClientConf.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL.key();
  public static final long RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL_DEFAULT_VALUE =
      RssClientConf.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL.defaultValue();
  public static final String RSS_CLIENT_ASSIGNMENT_RETRY_TIMES =
      RssClientConf.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES.key();
  public static final int RSS_CLIENT_ASSIGNMENT_RETRY_TIMES_DEFAULT_VALUE =
      RssClientConf.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES.defaultValue();

  public static final String RSS_ACCESS_TIMEOUT_MS = RssClientConf.RSS_ACCESS_TIMEOUT_MS.key();
  public static final int RSS_ACCESS_TIMEOUT_MS_DEFAULT_VALUE =
      RssClientConf.RSS_ACCESS_TIMEOUT_MS.defaultValue();
  public static final String RSS_DYNAMIC_CLIENT_CONF_ENABLED =
      RssClientConf.RSS_DYNAMIC_CLIENT_CONF_ENABLED.key();
  public static final boolean RSS_DYNAMIC_CLIENT_CONF_ENABLED_DEFAULT_VALUE =
      RssClientConf.RSS_DYNAMIC_CLIENT_CONF_ENABLED.defaultValue();

  public static final String RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER =
      RssClientConf.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER.key();
  public static final int RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER_DEFAULT_VALUE =
      RssClientConf.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER.defaultValue();

  public static final String RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR =
      RssClientConf.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR.key();
  public static final double RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR_DEFAULT_VALUE =
      RssClientConf.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR.defaultValue();

  public static final String RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED =
      RssClientConf.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED.key();
  public static final boolean RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED_DEFAULT_VALUE =
      RssClientConf.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED.defaultValue();

  public static final String RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER =
      RssClientConf.RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER.key();
  public static final int RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER_DEFAULT_VALUE =
      RssClientConf.RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER.defaultValue();

  public static final String RSS_RESUBMIT_STAGE = "rss.resubmit.stage";

  public static final String RSS_REMOTE_MERGE_ENABLE = "rss.remote.merge.enable";
  public static final String RSS_MERGED_BLOCK_SZIE = "rss.merged.block.size";
  public static final int RSS_MERGED_BLOCK_SZIE_DEFAULT = -1;
  public static final String RSS_REMOTE_MERGE_CLASS_LOADER = "rss.remote.merge.classloader";

  public static final String RSS_CLIENT_COMBINER_ENABLE = "rss.client.combiner.enable";
}
