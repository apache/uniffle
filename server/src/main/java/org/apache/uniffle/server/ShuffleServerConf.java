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

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.ConfigOptions;
import org.apache.uniffle.common.config.ConfigUtils;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.server.block.DefaultShuffleBlockIdManager;
import org.apache.uniffle.server.buffer.ShuffleBufferType;
import org.apache.uniffle.server.storage.LocalStorageManager;

public class ShuffleServerConf extends RssBaseConf {

  public static final String PREFIX_HADOOP_CONF = "rss.server.hadoop";
  public static final String SHUFFLE_SERVER_ID = "rss.server.id";

  public static final ConfigOption<Long> SERVER_BUFFER_CAPACITY =
      ConfigOptions.key("rss.server.buffer.capacity")
          .longType()
          .defaultValue(-1L)
          .withDescription("Max memory of buffer manager for shuffle server");

  public static final ConfigOption<Double> SERVER_BUFFER_CAPACITY_RATIO =
      ConfigOptions.key("rss.server.buffer.capacity.ratio")
          .doubleType()
          .defaultValue(0.6)
          .withDescription(
              "JVM heap size or off-heap size(when enabling Netty) * ratio for the maximum memory of buffer manager for shuffle server, this "
                  + "is only effective when `rss.server.buffer.capacity` is not explicitly set");

  public static final ConfigOption<Long> SERVER_READ_BUFFER_CAPACITY =
      ConfigOptions.key("rss.server.read.buffer.capacity")
          .longType()
          .defaultValue(-1L)
          .withDescription("Max size of buffer for reading data");

  public static final ConfigOption<Double> SERVER_READ_BUFFER_CAPACITY_RATIO =
      ConfigOptions.key("rss.server.read.buffer.capacity.ratio")
          .doubleType()
          .defaultValue(0.2)
          .withDescription(
              "JVM heap size or off-heap size(when enabling Netty) * ratio for read buffer size, this is only effective when "
                  + "`rss.server.reader.buffer.capacity.ratio` is not explicitly set");

  public static final ConfigOption<Long> SERVER_HEARTBEAT_DELAY =
      ConfigOptions.key("rss.server.heartbeat.delay")
          .longType()
          .defaultValue(10 * 1000L)
          .withDescription("rss heartbeat initial delay ms");

  public static final ConfigOption<Integer> SERVER_HEARTBEAT_THREAD_NUM =
      ConfigOptions.key("rss.server.heartbeat.threadNum")
          .intType()
          .defaultValue(2)
          .withDescription("rss heartbeat thread number");

  public static final ConfigOption<Long> SERVER_HEARTBEAT_INTERVAL =
      ConfigOptions.key("rss.server.heartbeat.interval")
          .longType()
          .defaultValue(10 * 1000L)
          .withDescription("Heartbeat interval to Coordinator (ms)");

  public static final ConfigOption<Long> SERVER_NETTY_DIRECT_MEMORY_USAGE_TRACKER_DELAY =
      ConfigOptions.key("rss.server.netty.directMemoryTracker.memoryUsage.initialFetchDelayMs")
          .longType()
          .defaultValue(10 * 1000L)
          .withDescription("Direct memory usage tracker initial delay (ms)");

  public static final ConfigOption<Long> SERVER_NETTY_DIRECT_MEMORY_USAGE_TRACKER_INTERVAL =
      ConfigOptions.key("rss.server.netty.directMemoryTracker.memoryUsage.updateMetricsIntervalMs")
          .longType()
          .defaultValue(10 * 1000L)
          .withDescription("Direct memory usage tracker interval to MetricSystem (ms)");

  public static final ConfigOption<Long> SERVER_NETTY_PENDING_TASKS_NUM_TRACKER_INTERVAL =
      ConfigOptions.key("rss.server.netty.metrics.pendingTaskNumPollingIntervalMs")
          .longType()
          .defaultValue(10 * 1000L)
          .withDescription(
              "How often to collect Netty pending tasks number metrics (in milliseconds)");

  public static final ConfigOption<Integer> SERVER_FLUSH_LOCALFILE_THREAD_POOL_SIZE =
      ConfigOptions.key("rss.server.flush.localfile.threadPool.size")
          .intType()
          .defaultValue(10)
          .withDescription("thread pool for flush data to file");

  public static final ConfigOption<Integer> SERVER_FLUSH_HADOOP_THREAD_POOL_SIZE =
      ConfigOptions.key("rss.server.flush.hadoop.threadPool.size")
          .intType()
          .defaultValue(60)
          .withDescription("thread pool for flush data to hadoop storage");

  public static final ConfigOption<Integer> SERVER_FLUSH_THREAD_POOL_QUEUE_SIZE =
      ConfigOptions.key("rss.server.flush.threadPool.queue.size")
          .intType()
          .defaultValue(Integer.MAX_VALUE)
          .withDescription("size of waiting queue for thread pool");

  public static final ConfigOption<Long> SERVER_FLUSH_THREAD_ALIVE =
      ConfigOptions.key("rss.server.flush.thread.alive")
          .longType()
          .defaultValue(120L)
          .withDescription("thread idle time in pool (s)");

  public static final ConfigOption<Long> SERVER_COMMIT_TIMEOUT =
      ConfigOptions.key("rss.server.commit.timeout")
          .longType()
          .defaultValue(600000L)
          .withDescription("Timeout when commit shuffle data (ms)");

  public static final ConfigOption<Integer> SERVER_WRITE_RETRY_MAX =
      ConfigOptions.key("rss.server.write.retry.max")
          .intType()
          .defaultValue(10)
          .withDescription("Retry times when write fail");

  public static final ConfigOption<Long> SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT =
      ConfigOptions.key("rss.server.app.expired.withoutHeartbeat")
          .longType()
          .defaultValue(60 * 1000L)
          .withDescription(
              "Expired time (ms) for application which has no heartbeat with coordinator");

  public static final ConfigOption<Long> SERVER_PRE_ALLOCATION_EXPIRED =
      ConfigOptions.key("rss.server.preAllocation.expired")
          .longType()
          .defaultValue(20 * 1000L)
          .withDescription("Expired time (ms) for pre allocated buffer");

  public static final ConfigOption<Long> SERVER_COMMIT_CHECK_INTERVAL_MAX =
      ConfigOptions.key("rss.server.commit.check.interval.max.ms")
          .longType()
          .defaultValue(10000L)
          .withDescription("Max interval(ms) for check commit status");

  public static final ConfigOption<Long> SERVER_WRITE_SLOW_THRESHOLD =
      ConfigOptions.key("rss.server.write.slow.threshold")
          .longType()
          .defaultValue(10000L)
          .withDescription("Threshold for write slow defined");

  public static final ConfigOption<Boolean> SERVER_STORAGE_AUDIT_LOG_ENABLED =
      ConfigOptions.key("rss.server.storage.audit.log.enabled")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "When set to true, for auditing purposes, the server will log audit records for every disk write and delete operation. "
                  + "Each file write is logged, while delete operations are specific to application ID/shuffle ID, "
                  + "removing all associated files and recording the deletion of the entire application ID or shuffle ID. "
                  + "For a write operation, it includes the size of the data written, the storage type and the specific disk to which it is written "
                  + "(for instance, in scenarios where multiple local disks are mounted).");
  public static final ConfigOption<Boolean> SERVER_RPC_AUDIT_LOG_ENABLED =
      ConfigOptions.key("rss.server.rpc.audit.log.enabled")
          .booleanType()
          .defaultValue(true)
          .withDescription(
              "When set to true, for auditing purposes, the server will log audit records for every rpc request operation. "
                  + "Each file write is logged.");
  public static final ConfigOption<List<String>> SERVER_RPC_RPC_AUDIT_LOG_EXCLUDE_LIST =
      ConfigOptions.key("rss.server.rpc.audit.log.excludeList")
          .stringType()
          .asList()
          .defaultValues("appHeartbeat")
          .withDescription("Exclude record rpc audit operation list, separated by ','");

  public static final ConfigOption<Long> SERVER_EVENT_SIZE_THRESHOLD_L1 =
      ConfigOptions.key("rss.server.event.size.threshold.l1")
          .longType()
          .defaultValue(200000L)
          .withDescription("Threshold for event size");

  public static final ConfigOption<Long> SERVER_EVENT_SIZE_THRESHOLD_L2 =
      ConfigOptions.key("rss.server.event.size.threshold.l2")
          .longType()
          .defaultValue(1000000L)
          .withDescription("Threshold for event size");

  public static final ConfigOption<Long> SERVER_EVENT_SIZE_THRESHOLD_L3 =
      ConfigOptions.key("rss.server.event.size.threshold.l3")
          .longType()
          .defaultValue(10000000L)
          .withDescription("Threshold for event size");

  public static final ConfigOption<Double> CLEANUP_THRESHOLD =
      ConfigOptions.key("rss.server.cleanup.threshold")
          .doubleType()
          .checkValue(
              ConfigUtils.PERCENTAGE_DOUBLE_VALIDATOR,
              "clean threshold must be between 0.0 and 100.0")
          .defaultValue(10.0)
          .withDescription("Threshold for disk cleanup");

  public static final ConfigOption<Double> HIGH_WATER_MARK_OF_WRITE =
      ConfigOptions.key("rss.server.high.watermark.write")
          .doubleType()
          .checkValue(
              ConfigUtils.PERCENTAGE_DOUBLE_VALIDATOR,
              "high write watermark must be between 0.0 and 100.0")
          .defaultValue(95.0)
          .withDescription("If disk usage is bigger than this value, disk cannot been written");

  public static final ConfigOption<Double> LOW_WATER_MARK_OF_WRITE =
      ConfigOptions.key("rss.server.low.watermark.write")
          .doubleType()
          .checkValue(
              ConfigUtils.PERCENTAGE_DOUBLE_VALIDATOR,
              "low write watermark must be between 0.0 and 100.0")
          .defaultValue(85.0)
          .withDescription("If disk usage is smaller than this value, disk can been written again");

  public static final ConfigOption<Boolean> DISK_CAPACITY_WATERMARK_CHECK_ENABLED =
      ConfigOptions.key("rss.server.disk-capacity.watermark.check.enabled")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "If it is co-located with other services, the high-low watermark check "
                  + "based on the uniffle used is not correct. Due to this, the whole disk capacity "
                  + "watermark check is necessary, which will reuse the current watermark value. "
                  + "It will be disabled by default.");

  public static final ConfigOption<Long> PENDING_EVENT_TIMEOUT_SEC =
      ConfigOptions.key("rss.server.pending.event.timeout.sec")
          .longType()
          .checkValue(ConfigUtils.POSITIVE_LONG_VALIDATOR, "pending event timeout must be positive")
          .defaultValue(600L)
          .withDescription(
              "If disk cannot be written for timeout seconds, the flush data event will fail");

  public static final ConfigOption<Long> DISK_CAPACITY =
      ConfigOptions.key("rss.server.disk.capacity")
          .longType()
          .defaultValue(-1L)
          .withDescription(
              "Disk capacity that shuffle server can use. "
                  + "If it's negative, it will use the default whole space");

  public static final ConfigOption<Double> DISK_CAPACITY_RATIO =
      ConfigOptions.key("rss.server.disk.capacity.ratio")
          .doubleType()
          .defaultValue(0.9)
          .withDescription(
              "The maximum ratio of disk that could be used as shuffle server. This is only effective "
                  + "when `rss.server.disk.capacity` is not explicitly set");

  public static final ConfigOption<Long> SERVER_SHUFFLE_INDEX_SIZE_HINT =
      ConfigOptions.key("rss.server.index.size.hint")
          .longType()
          .defaultValue(2 * 1024L * 1024L)
          .withDescription("The index file size hint");

  public static final ConfigOption<Boolean> SERVER_UNHEALTHY_ONCE_STORAGE_CORRUPTION =
      ConfigOptions.key("rss.server.health.markUnhealthyOnceStorageCorruption")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "Mark server unhealthy once any storage corrupted. Default value is false");

  public static final ConfigOption<Double> HEALTH_STORAGE_MAX_USAGE_PERCENTAGE =
      ConfigOptions.key("rss.server.health.max.storage.usage.percentage")
          .doubleType()
          .checkValue(
              ConfigUtils.PERCENTAGE_DOUBLE_VALIDATOR,
              "The max usage percentage must be between 0.0 and 100.0")
          .defaultValue(90.0)
          .withDescription(
              "The usage percentage of a storage exceed the value, the disk become unavailable");

  public static final ConfigOption<Double> HEALTH_STORAGE_RECOVERY_USAGE_PERCENTAGE =
      ConfigOptions.key("rss.server.health.storage.recovery.usage.percentage")
          .doubleType()
          .checkValue(
              ConfigUtils.PERCENTAGE_DOUBLE_VALIDATOR,
              "The recovery usage percentage must be between 0.0 and 100.0")
          .defaultValue(80.0)
          .withDescription(
              "The usage percentage of an unavailable storage decline the value, the disk"
                  + " will become available");

  public static final ConfigOption<Long> HEALTH_CHECK_INTERVAL =
      ConfigOptions.key("rss.server.health.check.interval.ms")
          .longType()
          .checkValue(
              ConfigUtils.POSITIVE_LONG_VALIDATOR, "The interval for health check must be positive")
          .defaultValue(5000L)
          .withDescription("The interval for health check");

  public static final ConfigOption<Double> HEALTH_MIN_STORAGE_PERCENTAGE =
      ConfigOptions.key("rss.server.health.min.storage.percentage")
          .doubleType()
          .checkValue(
              ConfigUtils.PERCENTAGE_DOUBLE_VALIDATOR,
              "The minimum for healthy storage percentage must be between 0.0 and 100.0")
          .defaultValue(80.0)
          .withDescription(
              "The minimum fraction of storage that must pass the check mark the node as healthy");

  public static final ConfigOption<Boolean> HEALTH_CHECK_ENABLE =
      ConfigOptions.key("rss.server.health.check.enable")
          .booleanType()
          .defaultValue(false)
          .withDescription("The switch for the health check");

  public static final ConfigOption<List<String>> HEALTH_CHECKER_CLASS_NAMES =
      ConfigOptions.key("rss.server.health.checker.class.names")
          .stringType()
          .asList()
          .noDefaultValue()
          .withDescription("The list of the Checker's name");

  public static final ConfigOption<String> HEALTH_CHECKER_SCRIPT_PATH =
      ConfigOptions.key("rss.server.health.checker.script.path")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "The health script file path for HealthScriptChecker, if script file should have execute permission.");

  public static final ConfigOption<Long> HEALTH_CHECKER_SCRIPT_EXECUTE_TIMEOUT =
      ConfigOptions.key("rss.server.health.checker.script.execute.timeout")
          .longType()
          .defaultValue(5000L)
          .withDescription("The health script file execute timeout ms.");

  public static final ConfigOption<Long> HEALTH_CHECKER_LOCAL_STORAGE_EXECUTE_TIMEOUT =
      ConfigOptions.key("rss.server.health.checker.localStorageExecutionTimeoutMS")
          .longType()
          .defaultValue(1000 * 60L)
          .withDescription(
              "The health checker for LocalStorageChecker execution timeout (Unit: ms). Default value is 1min");

  public static final ConfigOption<Double> SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE =
      ConfigOptions.key("rss.server.memory.shuffle.lowWaterMark.percentage")
          .doubleType()
          .checkValue(
              ConfigUtils.PERCENTAGE_DOUBLE_VALIDATOR,
              "The lowWaterMark for memory percentage must be between 0.0 and 100.0")
          .defaultValue(25.0)
          .withDescription("LowWaterMark of memory in percentage style");

  public static final ConfigOption<Double> SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE =
      ConfigOptions.key("rss.server.memory.shuffle.highWaterMark.percentage")
          .doubleType()
          .checkValue(
              ConfigUtils.PERCENTAGE_DOUBLE_VALIDATOR,
              "The highWaterMark for memory percentage must be between 0.0 and 100.0")
          .defaultValue(75.0)
          .withDescription("HighWaterMark of memory in percentage style");

  public static final ConfigOption<Long> FLUSH_COLD_STORAGE_THRESHOLD_SIZE =
      ConfigOptions.key("rss.server.flush.cold.storage.threshold.size")
          .longType()
          .checkValue(
              ConfigUtils.POSITIVE_LONG_VALIDATOR, "flush cold storage threshold must be positive")
          .defaultValue(64L * 1024L * 1024L)
          .withDescription(
              "For hybrid storage, the event size exceed this value, flush data  to cold storage");

  public static final ConfigOption<String> HYBRID_STORAGE_MANAGER_SELECTOR_CLASS =
      ConfigOptions.key("rss.server.hybrid.storage.manager.selector.class")
          .stringType()
          .defaultValue("org.apache.uniffle.server.storage.hybrid.DefaultStorageManagerSelector")
          .withDescription(
              "For hybrid storage, the storage manager selector strategy to support "
                  + "policies of flushing to different storages")
          .withDeprecatedKeys("rss.server.multistorage.manager.selector.class");

  public static final ConfigOption<String> HYBRID_STORAGE_FALLBACK_STRATEGY_CLASS =
      ConfigOptions.key("rss.server.hybrid.storage.fallback.strategy.class")
          .stringType()
          .noDefaultValue()
          .withDescription("For hybrid storage, fallback strategy class")
          .withDeprecatedKeys("rss.server.multistorage.fallback.strategy.class");

  public static final ConfigOption<Long> FALLBACK_MAX_FAIL_TIMES =
      ConfigOptions.key("rss.server.hybrid.storage.fallback.max.fail.times")
          .longType()
          .defaultValue(0L)
          .withDescription("For hybrid storage, fail times exceed the number, will switch storage")
          .withDeprecatedKeys("rss.server.multistorage.fallback.max.fail.times");

  public static final ConfigOption<List<String>> TAGS =
      ConfigOptions.key("rss.server.tags")
          .stringType()
          .asList()
          .noDefaultValue()
          .withDescription("Tags list supported by shuffle server");

  public static final ConfigOption<Long> LOCAL_STORAGE_INITIALIZE_MAX_FAIL_NUMBER =
      ConfigOptions.key("rss.server.localstorage.initialize.max.fail.number")
          .longType()
          .checkValue(
              ConfigUtils.NON_NEGATIVE_LONG_VALIDATOR, " max fail times must be non-negative")
          .defaultValue(0L)
          .withDescription(
              "For localstorage, it will exit when the failed initialized local storage exceed the number");

  public static final ConfigOption<Boolean> SINGLE_BUFFER_FLUSH_ENABLED =
      ConfigOptions.key("rss.server.single.buffer.flush.enabled")
          .booleanType()
          .defaultValue(true)
          .withDescription(
              "Whether single buffer flush when size exceeded rss.server.single.buffer.flush.threshold");

  public static final ConfigOption<Long> SINGLE_BUFFER_FLUSH_SIZE_THRESHOLD =
      ConfigOptions.key("rss.server.single.buffer.flush.threshold")
          .longType()
          .defaultValue(128 * 1024 * 1024L)
          .withDescription("The threshold of single shuffle buffer flush");

  public static final ConfigOption<Integer> SINGLE_BUFFER_FLUSH_BLOCKS_NUM_THRESHOLD =
      ConfigOptions.key("rss.server.single.buffer.flush.blocksNumberThreshold")
          .intType()
          .defaultValue(Integer.MAX_VALUE)
          .withDescription(
              "The blocks number threshold for triggering a flush for a single shuffle buffer. "
                  + "This threshold is mainly used to control jobs with an excessive number of small blocks, "
                  + "allowing these small blocks to be flushed as much as possible, "
                  + "rather than being maintained in the heap and unable to be garbage collected. "
                  + "This can cause severe garbage collection issues on the server side, and may even lead to out-of-heap-memory errors. "
                  + "If the threshold is set too high, it becomes meaningless. It won't be enabled by default.");

  public static final ConfigOption<Long> SERVER_LEAK_SHUFFLE_DATA_CHECK_INTERVAL =
      ConfigOptions.key("rss.server.leak.shuffledata.check.interval")
          .longType()
          .defaultValue(3600 * 1000L)
          .withDescription("the interval of leak shuffle data check");

  public static final ConfigOption<Integer> SERVER_MAX_CONCURRENCY_OF_ONE_PARTITION =
      ConfigOptions.key("rss.server.max.concurrency.of.per-partition.write")
          .intType()
          .defaultValue(30)
          .withDescription(
              "The max concurrency of single partition writer, the data partition file number is "
                  + "equal to this value. Default value is 1.")
          .withDeprecatedKeys("rss.server.max.concurrency.of.single.partition.writer");

  public static final ConfigOption<Integer> CLIENT_MAX_CONCURRENCY_LIMITATION_OF_ONE_PARTITION =
      ConfigOptions.key("rss.server.max.concurrency.limit.of.per-partition.write")
          .intType()
          .defaultValue(Integer.MAX_VALUE)
          .withDescription("The max concurrency limitation of per-partition writing.");

  public static final ConfigOption<Long> SERVER_TRIGGER_FLUSH_CHECK_INTERVAL =
      ConfigOptions.key("rss.server.shuffleBufferManager.trigger.flush.interval")
          .longType()
          .defaultValue(0L)
          .withDescription(
              "The interval of trigger shuffle buffer manager to flush data to persistent storage. If <= 0"
                  + ", then this flush check would be disabled.");

  public static final ConfigOption<ShuffleBufferType> SERVER_SHUFFLE_BUFFER_TYPE =
      ConfigOptions.key("rss.server.shuffleBuffer.type")
          .enumType(ShuffleBufferType.class)
          .defaultValue(ShuffleBufferType.LINKED_LIST)
          .withDescription(
              "The type for shuffle buffers. Setting as LINKED_LIST or SKIP_LIST."
                  + " The default value is LINKED_LIST. SKIP_LIST will help to improve"
                  + " the performance when there are a large number of blocks in memory"
                  + " or when the memory occupied by the blocks is very large."
                  + " The cpu usage of the shuffle server will be reduced."
                  + " But SKIP_LIST doesn't support the slow-start feature of MR.");

  public static final ConfigOption<Long> SERVER_SHUFFLE_FLUSH_THRESHOLD =
      ConfigOptions.key("rss.server.shuffle.flush.threshold")
          .longType()
          .checkValue(
              ConfigUtils.NON_NEGATIVE_LONG_VALIDATOR, "flush threshold must be non negative")
          .defaultValue(0L)
          .withDescription(
              "Threshold when flushing shuffle data to persistent storage, recommend value would be 256K, "
                  + "512K, or even 1M");

  public static final ConfigOption<String> STORAGE_MEDIA_PROVIDER_ENV_KEY =
      ConfigOptions.key("rss.server.storageMediaProvider.from.env.key")
          .stringType()
          .noDefaultValue()
          .withDescription("The env key to get json source of local storage media provider");

  public static final ConfigOption<Long> HUGE_PARTITION_SIZE_THRESHOLD =
      ConfigOptions.key("rss.server.huge-partition.size.threshold")
          .longType()
          .defaultValue(20 * 1024 * 1024 * 1024L)
          .withDescription(
              "Threshold of huge partition size, once exceeding threshold, memory usage limitation and "
                  + "huge partition buffer flushing will be triggered.");

  public static final ConfigOption<Double> HUGE_PARTITION_MEMORY_USAGE_LIMITATION_RATIO =
      ConfigOptions.key("rss.server.huge-partition.memory.limit.ratio")
          .doubleType()
          .defaultValue(0.2)
          .withDescription(
              "The memory usage limit ratio for huge partition, it will only triggered when partition's "
                  + "size exceeds the threshold of '"
                  + HUGE_PARTITION_SIZE_THRESHOLD.key()
                  + "'");

  public static final ConfigOption<Long> HUGE_PARTITION_SIZE_HARD_LIMIT =
      ConfigOptions.key("rss.server.huge-partition.size.hard.limit")
          .longType()
          .defaultValue(Long.MAX_VALUE)
          .withDescription(
              "This option sets the maximum allowable partition size threshold. "
                  + "If the partition size exceeds this threshold, the client will "
                  + "receive an error message and the transmission of shuffle data "
                  + "will be terminated. This helps to significantly improve the "
                  + "stability of the cluster by preventing partitions from becoming too large.");

  public static final ConfigOption<Long> HUGE_PARTITION_SPLIT_LIMIT =
      ConfigOptions.key("rss.server.huge-partition.split.limit")
          .longType()
          .defaultValue(Long.MAX_VALUE)
          .withDescription(
              "This option sets the maximum partition slice size threshold. "
                  + "If the partition size exceeds this threshold, the rss client will "
                  + "receive the need split partition list and resend the failed blocks to "
                  + "new servers through reassign mechanism.");

  public static final ConfigOption<Long> SERVER_DECOMMISSION_CHECK_INTERVAL =
      ConfigOptions.key("rss.server.decommission.check.interval")
          .longType()
          .checkValue(ConfigUtils.POSITIVE_LONG_VALIDATOR, "check interval times must be positive")
          .defaultValue(60 * 1000L)
          .withDescription(
              "The interval(ms) to check if all applications have finish when server is decommissioning");

  public static final ConfigOption<Boolean> SERVER_DECOMMISSION_SHUTDOWN =
      ConfigOptions.key("rss.server.decommission.shutdown")
          .booleanType()
          .defaultValue(true)
          .withDescription("Whether shutdown the server after server is decommissioned");

  public static final ConfigOption<Integer> NETTY_SERVER_PORT =
      ConfigOptions.key("rss.server.netty.port")
          .intType()
          .checkValue(
              ConfigUtils.SERVER_PORT_VALIDATOR,
              "check server port value is 0 or value >= 1024 && value <= 65535")
          .defaultValue(17000)
          .withDescription("Shuffle netty server port");

  public static final ConfigOption<Boolean> NETTY_SERVER_EPOLL_ENABLE =
      ConfigOptions.key("rss.server.netty.epoll.enable")
          .booleanType()
          .defaultValue(false)
          .withDescription("If enable epoll model with netty server");

  public static final ConfigOption<Integer> NETTY_SERVER_ACCEPT_THREAD =
      ConfigOptions.key("rss.server.netty.accept.thread")
          .intType()
          .defaultValue(10)
          .withDescription("Accept thread count in netty");

  public static final ConfigOption<Integer> NETTY_SERVER_WORKER_THREAD =
      ConfigOptions.key("rss.server.netty.worker.thread")
          .intType()
          .defaultValue(0)
          .withDescription(
              "Worker thread count in netty. When set to 0, "
                  + "the default value is dynamically set to twice the number of processor cores, "
                  + "but it will not be less than 100 to ensure the minimum throughput of the service.");

  public static final ConfigOption<Long> SERVER_NETTY_HANDLER_IDLE_TIMEOUT =
      ConfigOptions.key("rss.server.netty.handler.idle.timeout")
          .longType()
          .defaultValue(60000L)
          .withDescription("Idle timeout if there has not data");

  public static final ConfigOption<Integer> NETTY_SERVER_CONNECT_BACKLOG =
      ConfigOptions.key("rss.server.netty.connect.backlog")
          .intType()
          .defaultValue(0)
          .withDescription(
              "For netty server, requested maximum length of the queue of incoming connections. "
                  + "Default 0 for no backlog.");

  public static final ConfigOption<Integer> NETTY_SERVER_CONNECT_TIMEOUT =
      ConfigOptions.key("rss.server.netty.connect.timeout")
          .intType()
          .defaultValue(5000)
          .withDescription("Timeout for connection in netty");

  public static final ConfigOption<Integer> NETTY_SERVER_SEND_BUF =
      ConfigOptions.key("rss.server.netty.send.buf")
          .intType()
          .defaultValue(0)
          .withDescription(
              "the optimal size for send buffer(SO_SNDBUF) "
                  + "should be latency * network_bandwidth. Assuming latency = 1ms,"
                  + "network_bandwidth = 10Gbps, buffer size should be ~ 1.25MB."
                  + "Default is 0, OS will dynamically adjust the buf size.");

  public static final ConfigOption<Integer> NETTY_SERVER_RECEIVE_BUF =
      ConfigOptions.key("rss.server.netty.receive.buf")
          .intType()
          .defaultValue(0)
          .withDescription(
              "the optimal size for receive buffer(SO_RCVBUF) "
                  + "should be latency * network_bandwidth. Assuming latency = 1ms,"
                  + "network_bandwidth = 10Gbps, buffer size should be ~ 1.25MB."
                  + "Default is 0, OS will dynamically adjust the buf size.");

  public static final ConfigOption<Integer> TOP_N_APP_SHUFFLE_DATA_SIZE_NUMBER =
      ConfigOptions.key("rss.server.topN.appShuffleDataSize.number")
          .intType()
          .defaultValue(10)
          .withDescription("number of topN shuffle data size of app level.");

  public static final ConfigOption<Integer> TOP_N_APP_SHUFFLE_DATA_REFRESH_INTERVAL =
      ConfigOptions.key("rss.server.topN.appShuffleDataSize.refreshIntervalMs")
          .intType()
          .defaultValue(1000)
          .withDescription(
              "refresh interval in ms for TopN shuffle data size of app level calc task.");

  public static final ConfigOption<Integer> SUMMARY_METRIC_WAIT_QUEUE_SIZE =
      ConfigOptions.key("rss.server.summary.metric.wait.queue.size")
          .intType()
          .defaultValue(1000)
          .withDescription(
              "size of waiting queue for thread pool that used for calc summary metric.");

  public static final ConfigOption<Integer> SUMMARY_METRIC_THREAD_POOL_CORE_SIZE =
      ConfigOptions.key("rss.server.summary.metric.thread.pool.core.size")
          .intType()
          .defaultValue(2)
          .withDescription("core thread number of thread pool that used for calc summary metric.");

  public static final ConfigOption<Integer> SUMMARY_METRIC_THREAD_POOL_MAX_SIZE =
      ConfigOptions.key("rss.server.summary.metric.thread.pool.max.size")
          .intType()
          .defaultValue(20)
          .withDescription("max thread number of thread pool that used for calc summary metric.");

  public static final ConfigOption<Integer> SUMMARY_METRIC_THREAD_POOL_KEEP_ALIVE_TIME =
      ConfigOptions.key("rss.server.summary.metric.thread.pool.keep.alive.time")
          .intType()
          .defaultValue(60)
          .withDescription(
              "keep alive time of thread pool that used for calc summary metric, in SECONDS.");

  public static final ConfigOption<Boolean> APP_LEVEL_SHUFFLE_BLOCK_SIZE_METRIC_ENABLED =
      ConfigOptions.key("rss.server.metrics.blockSizeStatisticsEnabled")
          .booleanType()
          .defaultValue(false)
          .withDescription("whether or not shuffle block size metric is enabled");

  public static final ConfigOption<String> APP_LEVEL_SHUFFLE_BLOCK_SIZE_METRIC_BUCKETS =
      ConfigOptions.key("rss.server.metrics.blockSizeStatistics.buckets")
          .stringType()
          .defaultValue("32kb,64kb,128kb,256kb,512kb,1mb,2mb,4mb,8mb,16mb")
          .withDescription(
              "A comma-separated block size list, where each value"
                  + " can be suffixed with a memory size unit, such as kb or k, mb or m, etc.");

  public static final ConfigOption<Long> STORAGE_REMOVE_RESOURCE_OPERATION_TIMEOUT_SEC =
      ConfigOptions.key("rss.server.storage.resourceRemoveOperationTimeoutSec")
          .longType()
          .defaultValue(10 * 60L)
          .withDescription("The storage remove resource operation timeout.");

  public static final ConfigOption<Long> STORAGE_FLUSH_OPERATION_TIMEOUT_SEC =
      ConfigOptions.key("rss.server.storage.flushOperationTimeoutSec")
          .longType()
          .defaultValue(-1L)
          .withDescription(
              "The storage flush max timeout second, this will not be activated by default");

  public static final ConfigOption<Boolean> SERVER_MERGE_ENABLE =
      ConfigOptions.key("rss.server.merge.enable")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether to enable remote merge");

  public static final ConfigOption<Integer> SERVER_MERGE_THREAD_POOL_SIZE =
      ConfigOptions.key("rss.server.merge.threadPoolSize")
          .intType()
          .defaultValue(10)
          .withDescription("thread pool for merge");

  public static final ConfigOption<Integer> SERVER_MERGE_THREAD_POOL_QUEUE_SIZE =
      ConfigOptions.key("rss.server.merge.threadPoolQueueSize")
          .intType()
          .defaultValue(Integer.MAX_VALUE)
          .withDescription("size of waiting queue for merge thread pool");

  public static final ConfigOption<Integer> SERVER_MERGE_THREAD_ALIVE_TIME =
      ConfigOptions.key("rss.server.merge.threadAliveTime")
          .intType()
          .defaultValue(120)
          .withDescription("thread idle time in merge thread pool (s)");

  public static final ConfigOption<String> SERVER_MERGE_DEFAULT_MERGED_BLOCK_SIZE =
      ConfigOptions.key("rss.server.merge.defaultMergedBlockSize")
          .stringType()
          .defaultValue("14m")
          .withDescription("The default merged block size.");

  public static final ConfigOption<Long> SERVER_MERGE_CACHE_MERGED_BLOCK_INIT_SLEEP_MS =
      ConfigOptions.key("rss.server.merge.cacheMergedBlockInitSleepMs")
          .longType()
          .defaultValue(100L)
          .withDescription(
              "When caching merged block, the minimum waiting event after failure to require memory");

  public static final ConfigOption<Long> SERVER_MERGE_CACHE_MERGED_BLOCK_MAX_SLEEP_MS =
      ConfigOptions.key("rss.server.merge.cacheMergedBlockMaxSleepMs")
          .longType()
          .defaultValue(2000L)
          .withDescription(
              "When caching merged block, the maximum waiting event after failure to require memory");

  public static final ConfigOption<Integer> SERVER_MERGE_BLOCK_RING_BUFFER_SIZE =
      ConfigOptions.key("rss.server.merge.blockRingBufferSize")
          .intType()
          .defaultValue(2)
          .withDescription("The ring buffer size for read block when merge");

  public static final ConfigOption<String> SERVER_MERGE_CLASS_LOADER_JARS_PATH =
      ConfigOptions.key("rss.server.merge.classLoaderJarsPath")
          .stringType()
          .defaultValue(null)
          .withDescription("The jars path for class loader when merge");
  public static final ConfigOption<Boolean> SERVER_LOG_APP_DETAIL_WHILE_REMOVE_ENABLED =
      ConfigOptions.key("rss.server.log.app.detail.while.remove.enabled")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether to enable app detail log");
  public static final ConfigOption<String> SERVER_BLOCK_ID_MANAGER_CLASS =
      ConfigOptions.key("rss.server.blockIdManagerClass")
          .stringType()
          .defaultValue(DefaultShuffleBlockIdManager.class.getName())
          .withDescription(
              "The block id manager class, the implementation of this interface "
                  + "to manage the shuffle block ids");
  public static final ConfigOption<List<String>> SERVER_DISPLAY_METRICS_LIST =
      ConfigOptions.key("rss.server.displayMetricsList")
          .stringType()
          .asList()
          .defaultValues("app:app_num_with_node", "partition:partition_num_with_node")
          .withDescription(
              "A list of metrics will report to coordinator and dashboard, format in \"displayName:metricsName\", separated by ','");

  public static final ConfigOption<String> SERVER_LOCAL_STORAGE_MANAGER_CLASS =
      ConfigOptions.key("rss.server.localStorageManagerClass")
          .stringType()
          .defaultValue(LocalStorageManager.class.getName())
          .withDescription("The class of local storage manager implementation");

  public static final ConfigOption<Boolean> SERVER_HEARTBEAT_REPORT_ON_UNREGISTER_ENABLED =
      ConfigOptions.key("rss.server.heartbeatReportOnUnregisterEnabled")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether to trigger report while unregister");

  public ShuffleServerConf() {}

  public ShuffleServerConf(String fileName) {
    super();
    boolean ret = loadConfFromFile(fileName);
    if (!ret) {
      throw new IllegalStateException("Fail to load config file " + fileName);
    }
  }

  public Configuration getHadoopConf() {
    Configuration hadoopConf = new Configuration();
    for (String key : getKeySet()) {
      if (key.startsWith(ShuffleServerConf.PREFIX_HADOOP_CONF)) {
        String value = getString(key, "");
        String hadoopKey = key.substring(ShuffleServerConf.PREFIX_HADOOP_CONF.length() + 1);
        hadoopConf.set(hadoopKey, value);
      }
    }
    return hadoopConf;
  }

  public boolean loadConfFromFile(String fileName) {
    return loadConfFromFile(fileName, ConfigUtils.getAllConfigOptions(ShuffleServerConf.class));
  }
}
