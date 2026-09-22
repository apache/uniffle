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

package org.apache.spark.shuffle;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.spark.shuffle.RssSparkConfig.toSparkConfKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class RssSparkShuffleUtilsTest {

  private static final String EXPECTED_EXCEPTION_MESSAGE = "Exception should be thrown";

  @Test
  public void testAssignmentTags() {
    SparkConf conf = new SparkConf();

    /**
     * Case 1: don't set the tag implicitly and will return the {@code
     * Constants.SHUFFLE_SERVER_VERSION}
     */
    Set<String> tags = RssSparkShuffleUtils.getAssignmentTags(conf);
    assertEquals(Constants.SHUFFLE_SERVER_VERSION, tags.iterator().next());

    /**
     * Case 2: set the multiple tags implicitly and will return the {@code
     * Constants.SHUFFLE_SERVER_VERSION} and configured tags.
     */
    conf.set(toSparkConfKey(RssClientConf.RSS_CLIENT_ASSIGNMENT_TAGS), " a,b");
    tags = RssSparkShuffleUtils.getAssignmentTags(conf);
    assertEquals(3, tags.size());
    assertTrue(tags.containsAll(Arrays.asList("a", "b", Constants.SHUFFLE_SERVER_VERSION)));

    // Case 3: tags with extra space padding
    conf.set(toSparkConfKey(RssClientConf.RSS_CLIENT_ASSIGNMENT_TAGS), " a,b,c   ");
    tags = RssSparkShuffleUtils.getAssignmentTags(conf);
    assertEquals(4, tags.size());
    assertTrue(tags.containsAll(Arrays.asList("a", "b", "c", Constants.SHUFFLE_SERVER_VERSION)));
  }

  @Test
  public void odfsConfigurationTest() {
    SparkConf conf = new SparkConf();
    Configuration conf1 = RssSparkShuffleUtils.newHadoopConfiguration(conf);
    assertFalse(conf1.getBoolean("dfs.namenode.odfs.enable", false));
    assertEquals("org.apache.hadoop.fs.Hdfs", conf1.get("fs.AbstractFileSystem.hdfs.impl"));

    conf.set(toSparkConfKey(RssSparkConfig.RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE), "true");
    conf1 = RssSparkShuffleUtils.newHadoopConfiguration(conf);
    assertTrue(conf1.getBoolean("dfs.namenode.odfs.enable", false));
    assertEquals("org.apache.hadoop.odfs.HdfsOdfsFilesystem", conf1.get("fs.hdfs.impl"));
    assertEquals("org.apache.hadoop.odfs.HdfsOdfs", conf1.get("fs.AbstractFileSystem.hdfs.impl"));

    conf.set(toSparkConfKey(RssSparkConfig.RSS_OZONE_FS_HDFS_IMPL), "expect_odfs_impl");
    conf.set(
        toSparkConfKey(RssSparkConfig.RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL),
        "expect_odfs_abstract_impl");
    conf1 = RssSparkShuffleUtils.newHadoopConfiguration(conf);
    assertEquals("expect_odfs_impl", conf1.get("fs.hdfs.impl"));
    assertEquals("expect_odfs_abstract_impl", conf1.get("fs.AbstractFileSystem.hdfs.impl"));
  }

  @Test
  public void applyDynamicClientConfTest() {
    final SparkConf conf = new SparkConf();
    Map<String, String> clientConf = Maps.newHashMap();
    String remoteStoragePath = "hdfs://path1";
    String mockKey = "spark.mockKey";
    String mockValue = "v";

    clientConf.put(RssClientConfig.RSS_REMOTE_STORAGE_PATH, remoteStoragePath);
    clientConf.put(RssClientConfig.RSS_CLIENT_TYPE, RssClientConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE);
    clientConf.put(
        RssClientConfig.RSS_CLIENT_RETRY_MAX,
        Integer.toString(RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE));
    clientConf.put(
        RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
        Long.toString(RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE));
    clientConf.put(
        RssClientConfig.RSS_DATA_REPLICA,
        Integer.toString(RssClientConfig.RSS_DATA_REPLICA_DEFAULT_VALUE));
    clientConf.put(
        RssClientConfig.RSS_DATA_REPLICA_WRITE,
        Integer.toString(RssClientConfig.RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE));
    clientConf.put(
        RssClientConfig.RSS_DATA_REPLICA_READ,
        Integer.toString(RssClientConfig.RSS_DATA_REPLICA_READ_DEFAULT_VALUE));
    clientConf.put(
        RssClientConfig.RSS_HEARTBEAT_INTERVAL,
        Long.toString(RssClientConfig.RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE));
    clientConf.put(RssClientConfig.RSS_STORAGE_TYPE, StorageType.MEMORY_LOCALFILE_HDFS.name());
    clientConf.put(
        RssClientConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS,
        Long.toString(RssClientConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS_DEFAULT_VALUE));
    clientConf.put(
        RssClientConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS,
        Long.toString(RssClientConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE));
    clientConf.put(
        RssClientConfig.RSS_PARTITION_NUM_PER_RANGE,
        Integer.toString(RssClientConfig.RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE));
    clientConf.put(
        RssClientConfig.RSS_INDEX_READ_LIMIT,
        Integer.toString(RssClientConfig.RSS_INDEX_READ_LIMIT_DEFAULT_VALUE));
    clientConf.put(
        RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE,
        RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE);
    clientConf.put(mockKey, mockValue);

    RssSparkShuffleUtils.applyDynamicClientConf(conf, clientConf);
    assertEquals(
        remoteStoragePath, conf.get(toSparkConfKey(RssClientConf.RSS_REMOTE_STORAGE_PATH)));
    assertEquals(
        RssClientConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE,
        conf.get(toSparkConfKey(RssClientConf.RSS_CLIENT_TYPE)));
    assertEquals(
        Integer.toString(RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE),
        conf.get(toSparkConfKey(RssClientConf.RSS_CLIENT_RETRY_MAX)));
    assertEquals(
        Long.toString(RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE),
        conf.get(toSparkConfKey(RssClientConf.RSS_CLIENT_RETRY_INTERVAL_MAX)));
    assertEquals(
        Integer.toString(RssClientConfig.RSS_DATA_REPLICA_DEFAULT_VALUE),
        conf.get(toSparkConfKey(RssClientConf.RSS_DATA_REPLICA)));
    assertEquals(
        Integer.toString(RssClientConfig.RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE),
        conf.get(toSparkConfKey(RssClientConf.RSS_DATA_REPLICA_WRITE)));
    assertEquals(
        Integer.toString(RssClientConfig.RSS_DATA_REPLICA_READ_DEFAULT_VALUE),
        conf.get(toSparkConfKey(RssClientConf.RSS_DATA_REPLICA_READ)));
    assertEquals(
        Long.toString(RssClientConfig.RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE),
        conf.get(toSparkConfKey(RssClientConf.RSS_HEARTBEAT_INTERVAL)));
    assertEquals(
        StorageType.MEMORY_LOCALFILE_HDFS.name(),
        conf.get(toSparkConfKey(RssBaseConf.RSS_STORAGE_TYPE)));
    assertEquals(
        Long.toString(RssClientConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS_DEFAULT_VALUE),
        conf.get(toSparkConfKey(RssClientConf.RSS_CLIENT_SEND_CHECK_INTERVAL_MS)));
    assertEquals(
        Long.toString(RssClientConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE),
        conf.get(toSparkConfKey(RssClientConf.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS)));
    assertEquals(
        Integer.toString(RssClientConfig.RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE),
        conf.get(toSparkConfKey(RssClientConf.RSS_PARTITION_NUM_PER_RANGE)));
    assertEquals(
        Integer.toString(RssClientConfig.RSS_INDEX_READ_LIMIT_DEFAULT_VALUE),
        conf.get(toSparkConfKey(RssClientConf.RSS_INDEX_READ_LIMIT)));
    assertEquals(
        RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE,
        conf.get(toSparkConfKey(RssClientConf.RSS_CLIENT_READ_BUFFER_SIZE)));
    assertEquals(mockValue, conf.get(mockKey));

    String remoteStoragePath2 = "hdfs://path2";
    clientConf = Maps.newHashMap();
    clientConf.put(RssClientConfig.RSS_STORAGE_TYPE, StorageType.MEMORY_HDFS.name());
    clientConf.put(toSparkConfKey(RssClientConf.RSS_REMOTE_STORAGE_PATH), remoteStoragePath2);
    clientConf.put(mockKey, "won't be rewrite");
    clientConf.put(RssClientConfig.RSS_CLIENT_RETRY_MAX, "99999");
    RssSparkShuffleUtils.applyDynamicClientConf(conf, clientConf);
    // overwrite
    assertEquals(
        remoteStoragePath2, conf.get(toSparkConfKey(RssClientConf.RSS_REMOTE_STORAGE_PATH)));
    assertEquals(
        StorageType.MEMORY_HDFS.name(), conf.get(toSparkConfKey(RssBaseConf.RSS_STORAGE_TYPE)));
    // won't be overwrite
    assertEquals(mockValue, conf.get(mockKey));
    assertEquals(
        Integer.toString(RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE),
        conf.get(toSparkConfKey(RssClientConf.RSS_CLIENT_RETRY_MAX)));
  }

  @Test
  public void testEstimateTaskConcurrency() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set(Constants.SPARK_DYNAMIC_ENABLED, "true");
    sparkConf.set(Constants.SPARK_MAX_DYNAMIC_EXECUTOR, "200");
    sparkConf.set(Constants.SPARK_MIN_DYNAMIC_EXECUTOR, "100");
    sparkConf.set(Constants.SPARK_EXECUTOR_CORES, "2");
    int taskConcurrency;

    sparkConf.set(
        toSparkConfKey(RssClientConf.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR), "1.0");
    taskConcurrency = RssSparkShuffleUtils.estimateTaskConcurrency(sparkConf);
    assertEquals(400, taskConcurrency);

    sparkConf.set(
        toSparkConfKey(RssClientConf.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR), "0.3");
    taskConcurrency = RssSparkShuffleUtils.estimateTaskConcurrency(sparkConf);
    assertEquals(260, taskConcurrency);

    sparkConf.set(Constants.SPARK_TASK_CPUS, "2");
    sparkConf.set(
        toSparkConfKey(RssClientConf.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR), "0.3");
    taskConcurrency = RssSparkShuffleUtils.estimateTaskConcurrency(sparkConf);
    assertEquals(130, taskConcurrency);

    sparkConf.set(Constants.SPARK_DYNAMIC_ENABLED, "false");
    sparkConf.set(Constants.SPARK_EXECUTOR_INSTANTS, "70");
    sparkConf.set(
        toSparkConfKey(RssClientConf.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR), "1.0");
    taskConcurrency = RssSparkShuffleUtils.estimateTaskConcurrency(sparkConf);
    assertEquals(70, taskConcurrency);
  }

  @Test
  public void testGetRequiredShuffleServerNumber() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set(Constants.SPARK_DYNAMIC_ENABLED, "true");
    sparkConf.set(Constants.SPARK_MAX_DYNAMIC_EXECUTOR, "200");
    sparkConf.set(Constants.SPARK_MIN_DYNAMIC_EXECUTOR, "100");
    sparkConf.set(Constants.SPARK_EXECUTOR_CORES, "4");

    assertEquals(-1, RssSparkShuffleUtils.getRequiredShuffleServerNumber(sparkConf));

    sparkConf.set(toSparkConfKey(RssClientConf.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED), "true");
    assertEquals(10, RssSparkShuffleUtils.getRequiredShuffleServerNumber(sparkConf));

    sparkConf.set(Constants.SPARK_TASK_CPUS, "2");
    assertEquals(5, RssSparkShuffleUtils.getRequiredShuffleServerNumber(sparkConf));

    sparkConf.set(
        toSparkConfKey(RssClientConf.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR), "0.5");
    assertEquals(4, RssSparkShuffleUtils.getRequiredShuffleServerNumber(sparkConf));

    sparkConf.set(toSparkConfKey(RssClientConf.RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER), "100");
    assertEquals(3, RssSparkShuffleUtils.getRequiredShuffleServerNumber(sparkConf));
  }

  @Test
  public void testValidateRssClientConf() {
    SparkConf sparkConf = new SparkConf();
    try {
      RssSparkShuffleUtils.validateRssClientConf(sparkConf);
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must be set by the client or fetched from coordinators"));
    }
    sparkConf.set(toSparkConfKey(RssBaseConf.RSS_STORAGE_TYPE), "MEMORY_LOCALFILE_HDFS");
    RssSparkShuffleUtils.validateRssClientConf(sparkConf);
    sparkConf.set(toSparkConfKey(RssClientConf.RSS_CLIENT_RETRY_MAX), "5");
    sparkConf.set(toSparkConfKey(RssClientConf.RSS_CLIENT_RETRY_INTERVAL_MAX), "1000");
    sparkConf.set(toSparkConfKey(RssClientConf.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS), "4999");
    try {
      RssSparkShuffleUtils.validateRssClientConf(sparkConf);
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("should not bigger than"));
    }
    sparkConf.set(toSparkConfKey(RssClientConf.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS), "5000");
    RssSparkShuffleUtils.validateRssClientConf(sparkConf);
  }
}
