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

package org.apache.uniffle.common.util;

import org.apache.uniffle.common.ProjectConstants;

public final class Constants {

  private Constants() {}

  /** The version of this Uniffle instance. */
  public static final String VERSION = ProjectConstants.VERSION;

  public static final String REVISION_SHORT =
      ProjectConstants.REVISION.length() > 8
          ? ProjectConstants.REVISION.substring(0, 8)
          : ProjectConstants.REVISION;
  public static final String VERSION_AND_REVISION_SHORT = VERSION + "-" + REVISION_SHORT;

  // the value is used for client/server compatible, eg, online upgrade
  public static final String SHUFFLE_SERVER_VERSION = "ss_v5";
  public static final String METRICS_TAG_LABEL_NAME = "tags";
  public static final String METRICS_APP_LABEL_NAME = "appId";
  public static final String COORDINATOR_TAG = "coordinator";
  public static final String SHUFFLE_DATA_FILE_SUFFIX = ".data";
  public static final String SHUFFLE_INDEX_FILE_SUFFIX = ".index";
  public static final long INVALID_BLOCK_ID = -1L;

  public static final String KEY_SPLIT_CHAR = "/";
  public static final String COMMA_SPLIT_CHAR = ",";
  public static final String EQUAL_SPLIT_CHAR = "=";
  public static final String SEMICOLON_SPLIT_CHAR = ";";
  public static final String COMMON_SUCCESS_MESSAGE = "SUCCESS";
  public static final String SORT_SHUFFLE_MANAGER_NAME =
      "org.apache.spark.shuffle.sort.SortShuffleManager";

  public static final String RSS_CLIENT_CONF_COMMON_PREFIX = "rss.client";
  public static final String CONF_REMOTE_STORAGE_PATH = ".remote.storage.path";
  public static final String RSS_CLIENT_CONF_REMOTE_STORAGE_PATH =
      RSS_CLIENT_CONF_COMMON_PREFIX + CONF_REMOTE_STORAGE_PATH;

  public static final String ACCESS_INFO_REQUIRED_SHUFFLE_NODES_NUM =
      "access_info_required_shuffle_nodes_num";
  public static final String SPARK_DYNAMIC_ENABLED = "spark.dynamicAllocation.enabled";
  public static final String SPARK_MAX_DYNAMIC_EXECUTOR = "spark.dynamicAllocation.maxExecutors";
  public static final String SPARK_MIN_DYNAMIC_EXECUTOR = "spark.dynamicAllocation.minExecutors";
  public static final int SPARK_DYNAMIC_EXECUTOR_DEFAULT_VALUE = 0;
  public static final String SPARK_EXECUTOR_INSTANTS = "spark.executor.instances";
  public static final int SPARK_EXECUTOR_INSTANTS_DEFAULT_VALUE = -1;
  public static final String SPARK_EXECUTOR_CORES = "spark.executor.cores";
  public static final int SPARK_EXECUTOR_CORES_DEFAULT_VALUE = 1;
  public static final String SPARK_TASK_CPUS = "spark.task.cpus";
  public static final int SPARK_TASK_CPUS_DEFAULT_VALUE = 1;
  public static final int SPARK_MAX_DYNAMIC_EXECUTOR_LIMIT = 10000;

  public static final String MR_MAPS = "mapreduce.job.maps";
  public static final String MR_REDUCES = "mapreduce.job.reduces";
  public static final String MR_MAP_LIMIT = "mapreduce.job.running.map.limit";
  public static final String MR_REDUCE_LIMIT = "mapreduce.job.running.reduce.limit";
  public static final int MR_MAP_LIMIT_DEFAULT_VALUE = 0;
  public static final int MR_REDUCE_LIMIT_DEFAULT_VALUE = 0;
  public static final String MR_SLOW_START = "mapreduce.job.reduce.slowstart.completedmaps";
  public static final double MR_SLOW_START_DEFAULT_VALUE = 0.05;

  public static final double MILLION_SECONDS_PER_SECOND = 1E3D;
  public static final String DEVICE_NO_SPACE_ERROR_MESSAGE = "No space left on device";
  public static final String NETTY_STREAM_SERVICE_NAME = "netty.rpc.server";
  public static final String GRPC_SERVICE_NAME = "grpc.server";

  public static final int COMPOSITE_BYTE_BUF_MAX_COMPONENTS = 1024;

  // The `driver.host` is matching spark's `DRIVER_HOST_ADDRESS` configuration, which is
  // `spark.driver.host`.
  // We are accessing this configuration through RssConf, the spark prefix is stripped, hence, this
  // field.
  public static final String DRIVER_HOST = "driver.host";

  public static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";

  public static final String SPARK_RSS_CONFIG_PREFIX = "spark.";
  public static final int[] EMPTY_INT_ARRAY = new int[0];
}
