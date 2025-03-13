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

package org.apache.uniffle.common.config;

import java.util.List;

import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.PartitionSplitMode;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.netty.IOMode;
import org.apache.uniffle.common.util.BlockIdLayout;

import static org.apache.uniffle.common.compression.Codec.Type.LZ4;

public class RssClientConf {
  /**
   * The prefix key for Hadoop conf. For Spark like that:
   *
   * <p>key: spark.rss.hadoop.fs.defaultFS val: hdfs://rbf-x1
   *
   * <p>The key will be extracted to the hadoop conf: "fs.defaultFS" and inject this into Hadoop
   * storage configuration.
   */
  public static final String HADOOP_CONFIG_KEY_PREFIX = "rss.hadoop.";

  public static final ConfigOption<Codec.Type> COMPRESSION_TYPE =
      ConfigOptions.key("rss.client.io.compression.codec")
          .enumType(Codec.Type.class)
          .defaultValue(LZ4)
          .withDescription(
              "The compression codec is used to compress the shuffle data. "
                  + "Default codec is `LZ4`. Other options are`ZSTD` and `SNAPPY`.");

  public static final ConfigOption<Integer> ZSTD_COMPRESSION_LEVEL =
      ConfigOptions.key("rss.client.io.compression.zstd.level")
          .intType()
          .defaultValue(3)
          .withDescription("The zstd compression level, the default level is 3");

  public static final ConfigOption<Integer> ZSTD_COMPRESSION_WORKER_NUMBER =
      ConfigOptions.key("rss.client.io.compression.zstd.workerNumber")
          .intType()
          .defaultValue(-1)
          .withDescription(
              "Set the parallel compression worker number. This will not enabled by default");

  public static final ConfigOption<ShuffleDataDistributionType> DATA_DISTRIBUTION_TYPE =
      ConfigOptions.key("rss.client.shuffle.data.distribution.type")
          .enumType(ShuffleDataDistributionType.class)
          .defaultValue(ShuffleDataDistributionType.NORMAL)
          .withDescription(
              "The type of partition shuffle data distribution, including normal and local_order. "
                  + "The default value is normal. This config is only valid in Spark3.x");

  public static final ConfigOption<Integer> BLOCKID_SEQUENCE_NO_BITS =
      ConfigOptions.key("rss.client.blockId.sequenceNoBits")
          .intType()
          .defaultValue(BlockIdLayout.DEFAULT.sequenceNoBits)
          .withDescription(
              "Block ids contain three fields: the sequence number, the partition id and "
                  + "the task attempt id. This configures the bits reserved for the sequence "
                  + "number. Each field can at most have 31 bits, while all fields together "
                  + "must sum up to 63 bits.");

  public static final ConfigOption<Integer> BLOCKID_PARTITION_ID_BITS =
      ConfigOptions.key("rss.client.blockId.partitionIdBits")
          .intType()
          .defaultValue(BlockIdLayout.DEFAULT.partitionIdBits)
          .withDescription(
              "Block ids contain three fields: the sequence number, the partition id and "
                  + "the task attempt id. This configures the bits reserved for the partition id. "
                  + "Each field can at most have 31 bits, while all fields together "
                  + "must sum up to 63 bits.");

  public static final ConfigOption<Integer> BLOCKID_TASK_ATTEMPT_ID_BITS =
      ConfigOptions.key("rss.client.blockId.taskAttemptIdBits")
          .intType()
          .defaultValue(BlockIdLayout.DEFAULT.taskAttemptIdBits)
          .withDescription(
              "Block ids contain three fields: the sequence number, the partition id and "
                  + "the task attempt id. This configures the bits reserved for the task attempt id. "
                  + "Each field can at most have 31 bits, while all fields together "
                  + "must sum up to 63 bits.");

  public static final ConfigOption<Integer> MAX_CONCURRENCY_PER_PARTITION_TO_WRITE =
      ConfigOptions.key("rss.client.max.concurrency.of.per-partition.write")
          .intType()
          .defaultValue(0)
          .withDescription(
              "The max concurrency for single partition to write, the value is the max file number "
                  + "for one partition, remote shuffle server should respect this.");

  public static final ConfigOption<Long> RPC_TIMEOUT_MS =
      ConfigOptions.key("rss.client.rpc.timeout.ms")
          .longType()
          .defaultValue(60 * 1000L)
          .withDescription(
              "The timeout value in milliseconds for gRPC and Netty Type RPC Clients, including ShuffleServerClient and ShuffleManagerClient.");

  public static final ConfigOption<Integer> RPC_MAX_ATTEMPTS =
      ConfigOptions.key("rss.client.rpc.maxAttempts")
          .intType()
          .defaultValue(3)
          .withDescription("When we fail to send RPC calls, we will retry for maxAttempts times.");

  public static final ConfigOption<Integer> RPC_NETTY_PAGE_SIZE =
      ConfigOptions.key("rss.client.rpc.netty.pageSize")
          .intType()
          .defaultValue(4096)
          .withDescription(
              "The value of pageSize for PooledByteBufAllocator when using gRPC internal Netty on the client-side. "
                  + "This configuration will only take effect when rss.client.type is set to GRPC_NETTY.");

  public static final ConfigOption<Integer> RPC_NETTY_MAX_ORDER =
      ConfigOptions.key("rss.client.rpc.netty.maxOrder")
          .intType()
          .defaultValue(3)
          .withDescription(
              "The value of maxOrder for PooledByteBufAllocator when using gRPC internal Netty on the client-side. "
                  + "This configuration will only take effect when rss.client.type is set to GRPC_NETTY.");

  public static final ConfigOption<Integer> RPC_NETTY_SMALL_CACHE_SIZE =
      ConfigOptions.key("rss.client.rpc.netty.smallCacheSize")
          .intType()
          .defaultValue(1024)
          .withDescription(
              "The value of smallCacheSize for PooledByteBufAllocator when using gRPC internal Netty on the client-side. "
                  + "This configuration will only take effect when rss.client.type is set to GRPC_NETTY.");

  public static final ConfigOption<Integer> NETTY_IO_CONNECT_TIMEOUT_MS =
      ConfigOptions.key("rss.client.netty.io.connect.timeout.ms")
          .intType()
          .defaultValue(10 * 1000)
          .withDescription("netty connect to server time out mills");

  public static final ConfigOption<IOMode> NETTY_IO_MODE =
      ConfigOptions.key("rss.client.netty.io.mode")
          .enumType(IOMode.class)
          .defaultValue(IOMode.NIO)
          .withDescription("Netty EventLoopGroup backend, available options: NIO, EPOLL.");

  public static final ConfigOption<Integer> NETTY_IO_CONNECTION_TIMEOUT_MS =
      ConfigOptions.key("rss.client.netty.client.connection.timeout.ms")
          .intType()
          .defaultValue(10 * 60 * 1000)
          .withDescription("connection active timeout");

  public static final ConfigOption<Integer> NETTY_CLIENT_THREADS =
      ConfigOptions.key("rss.client.netty.client.threads")
          .intType()
          .defaultValue(0)
          .withDescription("Number of threads used in the client thread pool.");

  public static final ConfigOption<Double> NETTY_CLIENT_THREADS_RATIO =
      ConfigOptions.key("rss.client.netty.client.threads.ratio")
          .doubleType()
          .defaultValue(2.0)
          .withDescription(
              "The number of threads used in the client thread pool will be "
                  + "(`rss.client.netty.client.connections.per.peer` * `rss.client.netty.client.threads.ratio`). "
                  + "This is only effective when `rss.client.netty.client.threads` is not explicitly set");

  public static final ConfigOption<Boolean> NETTY_CLIENT_PREFER_DIRECT_BUFS =
      ConfigOptions.key("rss.client.netty.client.prefer.direct.bufs")
          .booleanType()
          .defaultValue(true)
          .withDescription(
              "If true, we will prefer allocating off-heap byte buffers within Netty.");

  public static final ConfigOption<Boolean> NETTY_CLIENT_POOLED_ALLOCATOR_ENABLED =
      ConfigOptions.key("rss.client.netty.client.pooled.allocator.enabled")
          .booleanType()
          .defaultValue(true)
          .withDescription(
              "If true, we will use PooledByteBufAllocator to allocate byte buffers within Netty, otherwise we'll use UnpooledByteBufAllocator.");

  public static final ConfigOption<Boolean> NETTY_CLIENT_SHARED_ALLOCATOR_ENABLED =
      ConfigOptions.key("rss.client.netty.client.shared.allocator.enabled")
          .booleanType()
          .defaultValue(true)
          .withDescription(
              "A flag indicating whether to share the ByteBuf allocators between the different Netty channels when enabling Netty. "
                  + "If enabled then only three ByteBuf allocators are created: "
                  + "one PooledByteBufAllocator where caching is allowed, one PooledByteBufAllocator where not and one UnpooledByteBufAllocator. "
                  + "When disabled, a new allocator is created for each transport client.");

  public static final ConfigOption<Integer> NETTY_CLIENT_NUM_CONNECTIONS_PER_PEER =
      ConfigOptions.key("rss.client.netty.client.connections.per.peer")
          .intType()
          .defaultValue(2)
          .withDescription("Number of concurrent connections between client and ShuffleServer.");

  public static final ConfigOption<Integer> NETTY_CLIENT_RECEIVE_BUFFER =
      ConfigOptions.key("rss.client.netty.client.receive.buffer")
          .intType()
          .defaultValue(0)
          .withDescription(
              "Receive buffer size (SO_RCVBUF). Note: the optimal size for receive buffer and send buffer "
                  + "should be latency * network_bandwidth. Assuming latency = 1ms, network_bandwidth = 10Gbps "
                  + "buffer size should be ~ 1.25MB.");

  public static final ConfigOption<Integer> NETTY_CLIENT_SEND_BUFFER =
      ConfigOptions.key("rss.client.netty.client.send.buffer")
          .intType()
          .defaultValue(0)
          .withDescription("Send buffer size (SO_SNDBUF).");

  // this is reversed for internal settings, and should never set by user.
  public static final ConfigOption<Integer> SHUFFLE_MANAGER_GRPC_PORT =
      ConfigOptions.key("rss.shuffle.manager.grpc.port")
          .intType()
          .noDefaultValue()
          .withDescription(
              "internal configuration to indicate which port is actually bind for shuffle manager service.");

  public static final ConfigOption<Boolean> OFF_HEAP_MEMORY_ENABLE =
      ConfigOptions.key("rss.client.off.heap.memory.enable")
          .booleanType()
          .defaultValue(false)
          .withDescription("Client can use off heap memory");

  public static final ConfigOption<Integer> RSS_INDEX_READ_LIMIT =
      ConfigOptions.key("rss.index.read.limit").intType().defaultValue(500);

  public static final ConfigOption<String> RSS_STORAGE_TYPE =
      ConfigOptions.key("rss.storage.type")
          .stringType()
          .defaultValue("")
          .withDescription("Supports MEMORY_LOCALFILE, MEMORY_HDFS, MEMORY_LOCALFILE_HDFS");

  public static final ConfigOption<String> RSS_CLIENT_READ_BUFFER_SIZE =
      ConfigOptions.key("rss.client.read.buffer.size")
          .stringType()
          .defaultValue("14m")
          .withDescription("The max data size read from storage");

  public static final ConfigOption<ClientType> RSS_CLIENT_TYPE =
      ConfigOptions.key("rss.client.type")
          .enumType(ClientType.class)
          .defaultValue(ClientType.GRPC)
          .withDescription(
              "Supports GRPC_NETTY, GRPC. The default value is GRPC. But we recommend using GRPC_NETTY to enable Netty on the client side for better stability and performance.");

  public static final ConfigOption<Boolean> RSS_CLIENT_REMOTE_STORAGE_USE_LOCAL_CONF_ENABLED =
      ConfigOptions.key("rss.client.remote.storage.useLocalConfAsDefault")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "This option is only valid when the remote storage path is specified. If ture, "
                  + "the remote storage conf will use the client side hadoop configuration loaded from the classpath.");

  public static final ConfigOption<Boolean> RSS_CLIENT_REASSIGN_ENABLED =
      ConfigOptions.key("rss.client.reassign.enabled")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "Whether to support rss client block send failure retry, default value is false.");

  public static final ConfigOption<PartitionSplitMode> RSS_CLIENT_PARTITION_SPLIT_MODE =
      ConfigOptions.key("rss.client.reassign.partitionSplitMode")
          .enumType(PartitionSplitMode.class)
          .defaultValue(PartitionSplitMode.PIPELINE)
          .withDescription("The partition split mode. default is PIPELINE.");

  public static final ConfigOption<Integer> RSS_CLIENT_PARTITION_SPLIT_LOAD_BALANCE_SERVER_NUMBER =
      ConfigOptions.key("rss.client.reassign.partitionSplitLoadBalanceServerNumber")
          .intType()
          .defaultValue(10)
          .withDescription(
              "The partition split load balance server number. Only valid for load balance split mode.");

  public static final ConfigOption<Integer> RSS_CLIENT_REMOTE_MERGE_FETCH_INIT_SLEEP_MS =
      ConfigOptions.key("rss.client.remote.merge.fetch.initSleepMs")
          .intType()
          .defaultValue(100)
          .withDescription("the init sleep ms for fetch remote merge records");

  public static final ConfigOption<Integer> RSS_CLIENT_REMOTE_MERGE_FETCH_MAX_SLEEP_MS =
      ConfigOptions.key("rss.client.remote.merge.fetch.maxSleepMs")
          .intType()
          .defaultValue(5000)
          .withDescription("the max sleep ms for fetch remote merge records");

  public static final ConfigOption<Integer> RSS_CLIENT_REMOTE_MERGE_READER_MAX_BUFFER =
      ConfigOptions.key("rss.client.remote.merge.reader.maxBuffer")
          .intType()
          .defaultValue(2)
          .withDescription(
              "the max size of buffer in queue for one partition when fetch remote merge records");

  public static final ConfigOption<Integer> RSS_CLIENT_REMOTE_MERGE_READER_MAX_RECORDS_PER_BUFFER =
      ConfigOptions.key("rss.client.remote.merge.reader.maxRecordsPerBuffer")
          .intType()
          .defaultValue(500)
          .withDescription("the max size of records per buffer when fetch remote merge records");

  public static final ConfigOption<List<String>> RSS_CLIENT_EXTRA_JAVA_SYSTEM_PROPERTIES =
      ConfigOptions.key("rss.client.extraJavaSystemProperties")
          .stringType()
          .asList()
          .noDefaultValue()
          .withDescription("the extra java properties could be configured by this option");

  public static final ConfigOption<String> RSS_CLIENT_BLOCK_ID_MANAGER_CLASS =
      ConfigOptions.key("rss.client.blockIdManagerClass")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "The block id manager class of server for this application, "
                  + "the implementation of this interface to manage the shuffle block ids");

  public static final ConfigOption<List<String>> RSS_CLIENT_REPORT_EXCLUDE_PROPERTIES =
      ConfigOptions.key("rss.client.reportExcludeProperties")
          .stringType()
          .asList()
          .defaultValues()
          .withDescription("the report exclude properties could be configured by this option");

  public static final ConfigOption<List<String>> RSS_CLIENT_REPORT_INCLUDE_PROPERTIES =
      ConfigOptions.key("rss.client.reportIncludeProperties")
          .stringType()
          .asList()
          .noDefaultValue()
          .withDescription("the report include properties could be configured by this option");

  public static final ConfigOption<Integer> RSS_CLIENT_GRPC_EVENT_LOOP_THREADS =
      ConfigOptions.key("rss.client.grpc.nettyEventLoopThreads")
          .intType()
          .defaultValue(-1)
          .withDescription("the event loop threads of netty impl for grpc");

  public static final ConfigOption<Boolean> RSS_CLIENT_PREFETCH_ENABLED =
      ConfigOptions.key("rss.client.read.prefetch.enabled")
          .booleanType()
          .defaultValue(false)
          .withDescription("Read prefetch switch that will be disabled by default");

  public static final ConfigOption<Integer> RSS_CLIENT_PREFETCH_CAPACITY =
      ConfigOptions.key("rss.client.read.prefetch.capacity")
          .intType()
          .defaultValue(4)
          .withDescription("Read prefetch capacity");

  public static final ConfigOption<Integer> READ_CLIENT_PREFETCH_TIMEOUT_SEC =
      ConfigOptions.key("rss.client.read.prefetch.timeoutSec")
          .intType()
          .defaultValue(120)
          .withDescription("Read prefetch timeout seconds");
}
