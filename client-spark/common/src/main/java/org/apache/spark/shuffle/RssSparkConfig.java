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

import java.util.Set;

import scala.Tuple2;

import com.google.common.collect.ImmutableSet;
import org.apache.spark.SparkConf;

import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.ConfigOptions;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;

public class RssSparkConfig {
  public static final String SPARK_RSS_CONFIG_PREFIX = "spark.";

  public static final ConfigOption<Integer> RSS_CLIENT_RPC_EXECUTOR_SIZE =
      ConfigOptions.key("rss.client.rpc.executor.size")
          .intType()
          .defaultValue(64)
          .withDescription("Core thread count for the shuffle manager gRPC server on the driver.");

  public static final ConfigOption<Boolean> RSS_CLIENT_INTEGRITY_VALIDATION_ENABLED =
      ConfigOptions.key("rss.client.integrityValidation.enabled")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "Whether or not to enable shuffle data integrity validation mechanism (spark version >= 3.5.0)");

  public static final ConfigOption<Boolean> RSS_DATA_INTEGRATION_VALIDATION_ANALYSIS_ENABLED =
      ConfigOptions.key("rss.client.integrityValidation.failureAnalysisEnabled")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether or not to enable validation failure analysis");

  public static final ConfigOption<Boolean>
      RSS_DATA_INTEGRITY_VALIDATION_BLOCK_NUMBER_CHECK_ENABLED =
          ConfigOptions.key("rss.client.integrityValidation.blockNumberCheckEnabled")
              .booleanType()
              .defaultValue(false)
              .withDescription("Whether or not to enable validation block number check");

  public static final ConfigOption<Codec.Type>
      RSS_CLIENT_INTEGRITY_VALIDATION_STATS_COMPRESSION_TYPE =
          ConfigOptions.key("rss.client.integrityValidation.statsCompressionType")
              .enumType(Codec.Type.class)
              .defaultValue(Codec.Type.ZSTD)
              .withDescription("stats compression type");

  public static final ConfigOption<Boolean>
      RSS_DATA_INTEGRITY_VALIDATION_SERVER_MANAGEMENT_ENABLED =
          ConfigOptions.key("rss.client.integrityValidation.serverManagementEnabled")
              .booleanType()
              .defaultValue(false)
              .withDescription(
                  "Whether or not to enable validation management by shuffle-server rather than client side");

  public static final ConfigOption<Boolean> RSS_READ_SHUFFLE_HANDLE_CACHE_ENABLED =
      ConfigOptions.key("rss.client.read.shuffleHandleCacheEnabled")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether or not to read shuffle handle cache enabled");

  public static final ConfigOption<Boolean> RSS_READ_OVERLAPPING_DECOMPRESSION_ENABLED =
      ConfigOptions.key("rss.client.read.overlappingDecompressionEnable")
          .booleanType()
          .defaultValue(true)
          .withDescription("Whether to overlapping decompress shuffle blocks.");

  public static final ConfigOption<Integer> RSS_READ_OVERLAPPING_DECOMPRESSION_THREADS =
      ConfigOptions.key("rss.client.read.overlappingDecompressionThreads")
          .intType()
          .defaultValue(1)
          .withDescription("Number of threads to use for overlapping decompress shuffle blocks.");

  public static final ConfigOption<Boolean> RSS_WRITE_OVERLAPPING_COMPRESSION_ENABLED =
      ConfigOptions.key("rss.client.write.overlappingCompressionEnable")
          .booleanType()
          .defaultValue(true)
          .withDescription("Whether to overlapping compress shuffle blocks.");

  public static final ConfigOption<Integer> RSS_WRITE_OVERLAPPING_COMPRESSION_THREADS_PER_VCORE =
      ConfigOptions.key("rss.client.write.overlappingCompressionThreadsPerVcore")
          .intType()
          .defaultValue(-1)
          .withDescription(
              "Specifies the ratio between the number of overlapping compression threads and the number of Spark executor vcores. It's disabled by default.");

  public static final ConfigOption<Boolean> RSS_READ_REORDER_MULTI_SERVERS_ENABLED =
      ConfigOptions.key("rss.client.read.reorderMultiServersEnable")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "If multiple replicated or load-balanced shuffle servers are assigned for one partition, "
                  + "this option can be enabled to perform load-balanced reads and avoid hot spots.");

  public static final ConfigOption<Boolean> RSS_RESUBMIT_STAGE_ENABLED =
      ConfigOptions.key("rss.stageRetry.enabled")
          .booleanType()
          .defaultValue(false)
          .withDeprecatedKeys(RssClientConfig.RSS_RESUBMIT_STAGE)
          .withDescription("Whether to enable the resubmit stage for fetch/write failure");

  public static final ConfigOption<Boolean> RSS_RESUBMIT_STAGE_WITH_FETCH_FAILURE_ENABLED =
      ConfigOptions.key("rss.stageRetry.fetchFailureEnabled")
          .booleanType()
          .defaultValue(false)
          .withFallbackKeys(RSS_RESUBMIT_STAGE_ENABLED.key(), RssClientConfig.RSS_RESUBMIT_STAGE)
          .withDescription(
              "If set to true, the stage retry mechanism will be enabled when a fetch failure occurs.");

  public static final ConfigOption<Boolean> RSS_RESUBMIT_STAGE_WITH_WRITE_FAILURE_ENABLED =
      ConfigOptions.key("rss.stageRetry.writeFailureEnabled")
          .booleanType()
          .defaultValue(false)
          .withFallbackKeys(RSS_RESUBMIT_STAGE_ENABLED.key(), RssClientConfig.RSS_RESUBMIT_STAGE)
          .withDescription(
              "If set to true, the stage retry mechanism will be enabled when a write failure occurs.");

  public static final ConfigOption<Boolean> RSS_BLOCK_ID_SELF_MANAGEMENT_ENABLED =
      ConfigOptions.key("rss.blockId.selfManagementEnabled")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "Whether to enable the blockId self management in spark driver side. Default value is false.");

  public static final ConfigOption<Long> RSS_CLIENT_SEND_SIZE_LIMITATION =
      ConfigOptions.key("rss.client.send.size.limit")
          .longType()
          .defaultValue(1024 * 1024 * 16L)
          .withDescription("The max data size sent to shuffle server");

  public static final ConfigOption<Integer> RSS_MEMORY_SPILL_TIMEOUT =
      ConfigOptions.key("rss.client.memory.spill.timeout.sec")
          .intType()
          .defaultValue(1)
          .withDescription(
              "The timeout of spilling data to remote shuffle server, "
                  + "which will be triggered by Spark TaskMemoryManager. Unit is sec, default value is 1");

  public static final ConfigOption<Boolean> RSS_ROW_BASED =
      ConfigOptions.key("rss.row.based")
          .booleanType()
          .defaultValue(true)
          .withDescription("indicates row based shuffle, set false when use in columnar shuffle");

  public static final ConfigOption<Boolean> RSS_MEMORY_SPILL_ENABLED =
      ConfigOptions.key("rss.client.memory.spill.enabled")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "The memory spill switch triggered by Spark TaskMemoryManager, default value is false.");

  public static final ConfigOption<Double> RSS_MEMORY_SPILL_RATIO =
      ConfigOptions.key("rss.client.memory.spill.ratio")
          .doubleType()
          .defaultValue(1.0d)
          .withDescription(
              "The buffer size to spill when spill triggered by config spark.rss.writer.buffer.spill.size");
  public static final ConfigOption<Integer> RSS_PARTITION_REASSIGN_MAX_REASSIGNMENT_SERVER_NUM =
      ConfigOptions.key("rss.client.reassign.maxReassignServerNum")
          .intType()
          .defaultValue(10)
          .withDescription(
              "The max reassign server num for one partition when using partition reassign mechanism.");

  public static final ConfigOption<Integer> RSS_PARTITION_REASSIGN_BLOCK_RETRY_MAX_TIMES =
      ConfigOptions.key("rss.client.reassign.blockRetryMaxTimes")
          .intType()
          .defaultValue(1)
          .withDescription("The block retry max times when partition reassign is enabled.");

  public static final ConfigOption<Boolean>
      RSS_PARTITION_REASSIGN_STALE_ASSIGNMENT_FAST_SWITCH_ENABLED =
          ConfigOptions.key("rss.client.reassign.staleAssignmentFastSwitchEnabled")
              .booleanType()
              .defaultValue(true)
              .withDescription(
                  "Whether to fast-switch the stale shuffle server assignment when pushing shuffle data. It can be enabled when partition reassign mechanism is enabled.");

  public static final ConfigOption<Boolean> RSS_CLIENT_MAP_SIDE_COMBINE_ENABLED =
      ConfigOptions.key("rss.client.mapSideCombine.enabled")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether to enable map side combine of shuffle writer.");

  public static final ConfigOption<String> RSS_WRITER_SERIALIZER_BUFFER_SIZE =
      ConfigOptions.key("rss.writer.serializer.buffer.size").stringType().defaultValue("3k");

  public static final ConfigOption<String> RSS_WRITER_BUFFER_SEGMENT_SIZE =
      ConfigOptions.key("rss.writer.buffer.segment.size").stringType().defaultValue("3k");

  public static final ConfigOption<String> RSS_WRITER_BUFFER_SPILL_SIZE =
      ConfigOptions.key("rss.writer.buffer.spill.size")
          .stringType()
          .defaultValue("128m")
          .withDescription("Buffer size for total partition data");

  public static final ConfigOption<String> RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE =
      ConfigOptions.key("rss.writer.pre.allocated.buffer.size").stringType().defaultValue("16m");

  public static final ConfigOption<Integer> RSS_WRITER_REQUIRE_MEMORY_RETRY_MAX =
      ConfigOptions.key("rss.writer.require.memory.retryMax").intType().defaultValue(1200);

  public static final ConfigOption<Long> RSS_WRITER_REQUIRE_MEMORY_INTERVAL =
      ConfigOptions.key("rss.writer.require.memory.interval").longType().defaultValue(1000L);

  public static final ConfigOption<Boolean> RSS_TEST_FLAG =
      ConfigOptions.key("rss.test").booleanType().defaultValue(false);

  public static final ConfigOption<Integer> RSS_CLIENT_UNREGISTER_THREAD_POOL_SIZE =
      ConfigOptions.key("rss.client.unregister.thread.pool.size").intType().defaultValue(10);

  public static final ConfigOption<Integer> RSS_CLIENT_UNREGISTER_TIMEOUT_SEC =
      ConfigOptions.key("rss.client.unregister.timeout.sec")
          .intType()
          .defaultValue(10)
          .withDescription(
              "Unregister requests are executed concurrently and all requests together "
                  + "have to complete within this timeout.");

  public static final ConfigOption<Integer> RSS_CLIENT_UNREGISTER_REQUEST_TIMEOUT_SEC =
      ConfigOptions.key("rss.client.unregister.request.timeout.sec")
          .intType()
          .defaultValue(10)
          .withDescription(
              "Unregister requests are executed concurrently and individual requests "
                  + "have to complete within this timeout.");

  // When the size of read buffer reaches the half of JVM region (i.e., 32m),
  // it will incur humongous allocation, so we set it to 14m.
  public static final ConfigOption<Integer> RSS_CLIENT_SEND_THREAD_POOL_SIZE =
      ConfigOptions.key("rss.client.send.threadPool.size")
          .intType()
          .defaultValue(10)
          .withDescription("The thread size for send shuffle data to shuffle server");

  public static final ConfigOption<Integer> RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE =
      ConfigOptions.key("rss.client.send.threadPool.keepalive").intType().defaultValue(60);

  public static final ConfigOption<Boolean> RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE =
      ConfigOptions.key("rss.ozone.dfs.namenode.odfs.enable").booleanType().defaultValue(false);

  public static final ConfigOption<String> RSS_OZONE_FS_HDFS_IMPL =
      ConfigOptions.key("rss.ozone.fs.hdfs.impl")
          .stringType()
          .defaultValue("org.apache.hadoop.odfs.HdfsOdfsFilesystem");

  public static final ConfigOption<String> RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL =
      ConfigOptions.key("rss.ozone.fs.AbstractFileSystem.hdfs.impl")
          .stringType()
          .defaultValue("org.apache.hadoop.odfs.HdfsOdfs");

  public static final ConfigOption<Integer> RSS_CLIENT_BITMAP_SPLIT_NUM =
      ConfigOptions.key("rss.client.bitmap.splitNum").intType().defaultValue(1);

  public static final ConfigOption<String> RSS_ACCESS_ID =
      ConfigOptions.key("rss.access.id").stringType().defaultValue("");

  public static final ConfigOption<String> RSS_ACCESS_ID_PROVIDER_KEY =
      ConfigOptions.key("rss.access.id.providerKey").stringType().defaultValue("");

  public static final ConfigOption<Boolean> RSS_ENABLED =
      ConfigOptions.key("rss.enabled").booleanType().defaultValue(false);

  public static final ConfigOption<Long> RSS_CLIENT_ACCESS_RETRY_INTERVAL_MS =
      ConfigOptions.key("rss.client.access.retry.interval.ms")
          .longType()
          .defaultValue(20000L)
          .withDescription("Interval between retries fallback to SortShuffleManager");

  public static final ConfigOption<Integer> RSS_CLIENT_ACCESS_RETRY_TIMES =
      ConfigOptions.key("rss.client.access.retry.times")
          .intType()
          .defaultValue(0)
          .withDescription("Number of retries fallback to SortShuffleManager");

  public static final ConfigOption<Integer> RSS_MAX_PARTITIONS =
      ConfigOptions.key("rss.blockId.maxPartitions")
          .intType()
          .defaultValue(1048576)
          .withDescription(
              "Sets the maximum number of partitions to be supported by block ids. "
                  + "This determines the bits reserved in block ids for the "
                  + "sequence number, the partition id and the task attempt id.");

  // spark2 doesn't have this key defined
  public static final String SPARK_SHUFFLE_COMPRESS_KEY = "spark.shuffle.compress";

  public static final boolean SPARK_SHUFFLE_COMPRESS_DEFAULT = true;

  public static final Set<String> RSS_MANDATORY_CLUSTER_CONF =
      ImmutableSet.of(
          toSparkConfKey(RssBaseConf.RSS_STORAGE_TYPE),
          toSparkConfKey(RssClientConf.RSS_REMOTE_STORAGE_PATH));

  /** Returns the key used by SparkConf and spark-submit. */
  public static String toSparkConfKey(ConfigOption<?> option) {
    return SPARK_RSS_CONFIG_PREFIX + option.key();
  }

  public static RssConf toRssConf(SparkConf sparkConf) {
    RssConf rssConf = new RssConf();
    for (Tuple2<String, String> tuple : sparkConf.getAll()) {
      String key = tuple._1;
      if (!key.startsWith(SPARK_RSS_CONFIG_PREFIX)) {
        continue;
      }
      key = key.substring(SPARK_RSS_CONFIG_PREFIX.length());
      rssConf.setString(key, tuple._2);
    }
    return rssConf;
  }
}
