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

package org.apache.uniffle.shuffle.manager;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import scala.Tuple2;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.MapOutputTracker;
import org.apache.spark.MapOutputTrackerMaster;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.SparkException;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.shuffle.RssSparkShuffleUtils;
import org.apache.spark.shuffle.RssStageResubmitManager;
import org.apache.spark.shuffle.ShuffleHandleInfoManager;
import org.apache.spark.shuffle.ShuffleManager;
import org.apache.spark.shuffle.SparkVersionUtils;
import org.apache.spark.shuffle.handle.MutableShuffleHandleInfo;
import org.apache.spark.shuffle.handle.ShuffleHandleInfo;
import org.apache.spark.shuffle.handle.SimpleShuffleHandleInfo;
import org.apache.spark.shuffle.handle.StageAttemptShuffleHandleInfo;
import org.apache.spark.shuffle.writer.AddBlockEvent;
import org.apache.spark.shuffle.writer.DataPusher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.CoordinatorClientFactory;
import org.apache.uniffle.client.factory.ShuffleManagerClientFactory;
import org.apache.uniffle.client.impl.FailedBlockSendTracker;
import org.apache.uniffle.client.impl.grpc.CoordinatorGrpcRetryableClient;
import org.apache.uniffle.client.request.RssFetchClientConfRequest;
import org.apache.uniffle.client.request.RssPartitionToShuffleServerRequest;
import org.apache.uniffle.client.response.RssFetchClientConfResponse;
import org.apache.uniffle.client.response.RssReassignOnBlockSendFailureResponse;
import org.apache.uniffle.client.response.RssReassignOnStageRetryResponse;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ReceivingFailureServer;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.rpc.GrpcServer;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.ExpiringCloseableSupplier;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.RetryUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.shuffle.BlockIdManager;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_BLOCK_ID_SELF_MANAGEMENT_ENABLED;
import static org.apache.spark.shuffle.RssSparkConfig.RSS_PARTITION_REASSIGN_MAX_REASSIGNMENT_SERVER_NUM;
import static org.apache.spark.shuffle.RssSparkConfig.RSS_RESUBMIT_STAGE_WITH_FETCH_FAILURE_ENABLED;
import static org.apache.spark.shuffle.RssSparkConfig.RSS_RESUBMIT_STAGE_WITH_WRITE_FAILURE_ENABLED;
import static org.apache.uniffle.common.config.RssBaseConf.RPC_SERVER_PORT;
import static org.apache.uniffle.common.config.RssClientConf.HADOOP_CONFIG_KEY_PREFIX;
import static org.apache.uniffle.common.config.RssClientConf.MAX_CONCURRENCY_PER_PARTITION_TO_WRITE;
import static org.apache.uniffle.common.config.RssClientConf.RSS_CLIENT_REMOTE_STORAGE_USE_LOCAL_CONF_ENABLED;

public abstract class RssShuffleManagerBase implements RssShuffleManagerInterface, ShuffleManager {
  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleManagerBase.class);
  protected final int dataTransferPoolSize;
  protected final int dataCommitPoolSize;
  protected final int dataReplica;
  protected final int dataReplicaWrite;
  protected final int dataReplicaRead;
  protected final boolean dataReplicaSkipEnabled;
  protected final Map<String, Set<Long>> taskToSuccessBlockIds;
  protected final Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker;
  private Set<String> failedTaskIds = Sets.newConcurrentHashSet();

  private AtomicBoolean isInitialized = new AtomicBoolean(false);
  private Method unregisterAllMapOutputMethod;
  private Method registerShuffleMethod;
  private volatile BlockIdManager blockIdManager;
  protected ShuffleDataDistributionType dataDistributionType;
  private Object blockIdManagerLock = new Object();
  protected AtomicReference<String> id = new AtomicReference<>();
  protected String appId = "";
  protected ShuffleWriteClient shuffleWriteClient;
  protected boolean dynamicConfEnabled;
  protected int maxConcurrencyPerPartitionToWrite;
  protected String clientType;

  protected final SparkConf sparkConf;
  protected final RssConf rssConf;
  protected Map<Integer, Integer> shuffleIdToPartitionNum;
  protected Map<Integer, Integer> shuffleIdToNumMapTasks;
  protected Supplier<ShuffleManagerClient> managerClientSupplier;
  protected boolean rssStageRetryEnabled;
  protected boolean rssStageRetryForWriteFailureEnabled;
  protected boolean rssStageRetryForFetchFailureEnabled;
  /**
   * Mapping between ShuffleId and ShuffleServer list. ShuffleServer list is dynamically allocated.
   * ShuffleServer is not obtained from RssShuffleHandle, but from this mapping.
   */
  protected ShuffleHandleInfoManager shuffleHandleInfoManager;

  protected RssStageResubmitManager rssStageResubmitManager;
  protected int partitionReassignMaxServerNum;
  protected boolean blockIdSelfManagedEnabled;
  protected boolean partitionReassignEnabled;
  protected boolean shuffleManagerRpcServiceEnabled;

  protected boolean heartbeatStarted = false;
  protected final long heartbeatInterval;
  protected final long heartbeatTimeout;
  protected String user;
  protected String uuid;
  protected ScheduledExecutorService heartBeatScheduledExecutorService;
  protected final int maxFailures;
  protected final boolean speculation;
  protected final BlockIdLayout blockIdLayout;
  private ShuffleManagerGrpcService service;
  protected GrpcServer shuffleManagerServer;
  protected DataPusher dataPusher;

  public RssShuffleManagerBase(SparkConf conf, boolean isDriver) {
    LOG.info(
        "Uniffle {} version: {}", this.getClass().getName(), Constants.VERSION_AND_REVISION_SHORT);
    this.sparkConf = conf;
    checkSupported(sparkConf);
    boolean supportsRelocation =
        Optional.ofNullable(SparkEnv.get())
            .map(env -> env.serializer().supportsRelocationOfSerializedObjects())
            .orElse(true);
    if (!supportsRelocation) {
      LOG.warn(
          "RSSShuffleManager requires a serializer which supports relocations of serialized object. Please set "
              + "spark.serializer to org.apache.spark.serializer.KryoSerializer instead");
    }
    this.user = sparkConf.get("spark.rss.quota.user", "user");
    this.uuid = sparkConf.get("spark.rss.quota.uuid", Long.toString(System.currentTimeMillis()));
    this.dynamicConfEnabled = sparkConf.get(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED);

    // fetch client conf and apply them if necessary
    if (isDriver && this.dynamicConfEnabled) {
      fetchAndApplyDynamicConf(sparkConf);
    }
    RssSparkShuffleUtils.validateRssClientConf(sparkConf);

    // convert spark conf to rss conf after fetching dynamic client conf
    this.rssConf = RssSparkConfig.toRssConf(sparkConf);
    RssUtils.setExtraJavaProperties(rssConf);

    // set & check replica config
    this.dataReplica = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA);
    this.dataReplicaWrite = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA_WRITE);
    this.dataReplicaRead = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA_READ);
    this.dataReplicaSkipEnabled = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA_SKIP_ENABLED);
    LOG.info(
        "Check quorum config ["
            + dataReplica
            + ":"
            + dataReplicaWrite
            + ":"
            + dataReplicaRead
            + ":"
            + dataReplicaSkipEnabled
            + "]");
    RssUtils.checkQuorumSetting(dataReplica, dataReplicaWrite, dataReplicaRead);

    this.maxConcurrencyPerPartitionToWrite = rssConf.get(MAX_CONCURRENCY_PER_PARTITION_TO_WRITE);

    this.clientType = sparkConf.get(RssSparkConfig.RSS_CLIENT_TYPE);

    // configure block id layout
    this.maxFailures = sparkConf.getInt("spark.task.maxFailures", 4);
    this.speculation = sparkConf.getBoolean("spark.speculation", false);
    // configureBlockIdLayout requires maxFailures and speculation to be initialized
    configureBlockIdLayout(sparkConf, rssConf);
    this.blockIdLayout = BlockIdLayout.from(rssConf);

    this.dataTransferPoolSize = sparkConf.get(RssSparkConfig.RSS_DATA_TRANSFER_POOL_SIZE);
    this.dataCommitPoolSize = sparkConf.get(RssSparkConfig.RSS_DATA_COMMIT_POOL_SIZE);

    // External shuffle service is not supported when using remote shuffle service
    sparkConf.set("spark.shuffle.service.enabled", "false");
    sparkConf.set("spark.dynamicAllocation.shuffleTracking.enabled", "false");
    sparkConf.set(RssSparkConfig.RSS_ENABLED.key(), "true");
    LOG.info("Disable external shuffle service in RssShuffleManager.");
    sparkConf.set("spark.sql.adaptive.localShuffleReader.enabled", "false");
    LOG.info("Disable local shuffle reader in RssShuffleManager.");
    // If we store shuffle data in distributed filesystem or in a disaggregated
    // shuffle cluster, we don't need shuffle data locality
    sparkConf.set("spark.shuffle.reduceLocality.enabled", "false");
    LOG.info("Disable shuffle data locality in RssShuffleManager.");

    taskToSuccessBlockIds = JavaUtils.newConcurrentMap();
    taskToFailedBlockSendTracker = JavaUtils.newConcurrentMap();
    this.shuffleIdToPartitionNum = JavaUtils.newConcurrentMap();
    this.shuffleIdToNumMapTasks = JavaUtils.newConcurrentMap();

    // stage retry for write/fetch failure
    rssStageRetryForFetchFailureEnabled =
        rssConf.get(RSS_RESUBMIT_STAGE_WITH_FETCH_FAILURE_ENABLED);
    rssStageRetryForWriteFailureEnabled =
        rssConf.get(RSS_RESUBMIT_STAGE_WITH_WRITE_FAILURE_ENABLED);
    if (rssStageRetryForFetchFailureEnabled || rssStageRetryForWriteFailureEnabled) {
      rssStageRetryEnabled = true;
      List<String> logTips = new ArrayList<>();
      if (rssStageRetryForWriteFailureEnabled) {
        logTips.add("write");
      }
      if (rssStageRetryForWriteFailureEnabled) {
        logTips.add("fetch");
      }
      LOG.info(
          "Activate the stage retry mechanism that will resubmit stage on {} failure",
          StringUtils.join(logTips, "/"));
    }

    this.partitionReassignEnabled = rssConf.get(RssClientConf.RSS_CLIENT_REASSIGN_ENABLED);
    // The feature of partition reassign is exclusive with multiple replicas and stage retry.
    if (partitionReassignEnabled) {
      if (dataReplica > 1) {
        throw new RssException(
            "The feature of task partition reassign is incompatible with multiple replicas mechanism.");
      }
      LOG.info("Partition reassign is enabled.");
    }
    this.blockIdSelfManagedEnabled = rssConf.getBoolean(RSS_BLOCK_ID_SELF_MANAGEMENT_ENABLED);
    this.shuffleManagerRpcServiceEnabled =
        partitionReassignEnabled || rssStageRetryEnabled || blockIdSelfManagedEnabled;

    if (isDriver) {
      heartBeatScheduledExecutorService =
          ThreadUtils.getDaemonSingleThreadScheduledExecutor("rss-heartbeat");
      if (shuffleManagerRpcServiceEnabled) {
        LOG.info("stage resubmit is supported and enabled");
        // start shuffle manager server
        rssConf.set(RPC_SERVER_PORT, 0);
        ShuffleManagerServerFactory factory = new ShuffleManagerServerFactory(this, rssConf);
        service = factory.getService();
        shuffleManagerServer = factory.getServer(service);
        try {
          shuffleManagerServer.start();
          // pass this as a spark.rss.shuffle.manager.grpc.port config, so it can be propagated to
          // executor properly.
          sparkConf.set(
              RssSparkConfig.RSS_SHUFFLE_MANAGER_GRPC_PORT, shuffleManagerServer.getPort());
        } catch (Exception e) {
          LOG.error("Failed to start shuffle manager server", e);
          throw new RssException(e);
        }
      }
    }
    if (shuffleManagerRpcServiceEnabled) {
      getOrCreateShuffleManagerClientSupplier();
    }

    // Start heartbeat thread.
    this.heartbeatInterval = sparkConf.get(RssSparkConfig.RSS_HEARTBEAT_INTERVAL);
    this.heartbeatTimeout =
        sparkConf.getLong(RssSparkConfig.RSS_HEARTBEAT_TIMEOUT.key(), heartbeatInterval / 2);
    heartBeatScheduledExecutorService =
        ThreadUtils.getDaemonSingleThreadScheduledExecutor("rss-heartbeat");

    this.shuffleWriteClient = createShuffleWriteClient();
    registerCoordinator();

    LOG.info("Rss data pusher is starting...");
    int poolSize = sparkConf.get(RssSparkConfig.RSS_CLIENT_SEND_THREAD_POOL_SIZE);
    int keepAliveTime = sparkConf.get(RssSparkConfig.RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE);
    this.dataPusher =
        new DataPusher(
            shuffleWriteClient,
            taskToSuccessBlockIds,
            taskToFailedBlockSendTracker,
            failedTaskIds,
            poolSize,
            keepAliveTime);
    this.partitionReassignMaxServerNum =
        rssConf.get(RSS_PARTITION_REASSIGN_MAX_REASSIGNMENT_SERVER_NUM);
    this.shuffleHandleInfoManager = new ShuffleHandleInfoManager();
    this.rssStageResubmitManager = new RssStageResubmitManager();
  }

  @VisibleForTesting
  protected RssShuffleManagerBase(
      SparkConf conf,
      boolean isDriver,
      DataPusher dataPusher,
      Map<String, Set<Long>> taskToSuccessBlockIds,
      Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker) {
    this.sparkConf = conf;
    this.clientType = sparkConf.get(RssSparkConfig.RSS_CLIENT_TYPE);
    this.rssConf = RssSparkConfig.toRssConf(sparkConf);
    this.dataDistributionType = rssConf.get(RssClientConf.DATA_DISTRIBUTION_TYPE);
    this.blockIdLayout = BlockIdLayout.from(rssConf);
    this.maxConcurrencyPerPartitionToWrite = rssConf.get(MAX_CONCURRENCY_PER_PARTITION_TO_WRITE);
    this.maxFailures = sparkConf.getInt("spark.task.maxFailures", 4);
    this.speculation = sparkConf.getBoolean("spark.speculation", false);
    // configureBlockIdLayout requires maxFailures and speculation to be initialized
    configureBlockIdLayout(sparkConf, rssConf);
    this.heartbeatInterval = sparkConf.get(RssSparkConfig.RSS_HEARTBEAT_INTERVAL);
    this.heartbeatTimeout =
        sparkConf.getLong(RssSparkConfig.RSS_HEARTBEAT_TIMEOUT.key(), heartbeatInterval / 2);
    this.dataReplica = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA);
    this.dataReplicaWrite = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA_WRITE);
    this.dataReplicaRead = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA_READ);
    this.dataReplicaSkipEnabled = sparkConf.get(RssSparkConfig.RSS_DATA_REPLICA_SKIP_ENABLED);
    LOG.info(
        "Check quorum config ["
            + dataReplica
            + ":"
            + dataReplicaWrite
            + ":"
            + dataReplicaRead
            + ":"
            + dataReplicaSkipEnabled
            + "]");
    RssUtils.checkQuorumSetting(dataReplica, dataReplicaWrite, dataReplicaRead);

    this.dataTransferPoolSize = sparkConf.get(RssSparkConfig.RSS_DATA_TRANSFER_POOL_SIZE);
    this.dataCommitPoolSize = sparkConf.get(RssSparkConfig.RSS_DATA_COMMIT_POOL_SIZE);
    createShuffleWriteClient();

    this.taskToSuccessBlockIds = taskToSuccessBlockIds;
    this.heartBeatScheduledExecutorService = null;
    this.taskToFailedBlockSendTracker = taskToFailedBlockSendTracker;
    this.dataPusher = dataPusher;
    this.partitionReassignMaxServerNum =
        rssConf.get(RSS_PARTITION_REASSIGN_MAX_REASSIGNMENT_SERVER_NUM);
    this.shuffleHandleInfoManager = new ShuffleHandleInfoManager();
    this.rssStageResubmitManager = new RssStageResubmitManager();
  }

  public BlockIdManager getBlockIdManager() {
    if (blockIdManager == null) {
      synchronized (blockIdManagerLock) {
        if (blockIdManager == null) {
          blockIdManager = new BlockIdManager();
          LOG.info("BlockId manager has been initialized.");
        }
      }
    }
    return blockIdManager;
  }

  @Override
  public boolean unregisterShuffle(int shuffleId) {
    try {
      if (blockIdManager != null) {
        blockIdManager.remove(shuffleId);
      }
      if (SparkEnv.get().executorId().equals("driver")) {
        shuffleWriteClient.unregisterShuffle(getAppId(), shuffleId);
        shuffleIdToPartitionNum.remove(shuffleId);
        shuffleIdToNumMapTasks.remove(shuffleId);
        if (service != null) {
          service.unregisterShuffle(shuffleId);
        }
      }
    } catch (Exception e) {
      LOG.warn("Errors on unregistering from remote shuffle-servers", e);
    }
    return true;
  }

  /**
   * Derives block id layout config from maximum number of allowed partitions. Computes the number
   * of required bits for partition id and task attempt id and reserves remaining bits for sequence
   * number.
   *
   * @param sparkConf Spark config providing max partitions
   * @param rssConf Rss config to amend
   */
  public void configureBlockIdLayout(SparkConf sparkConf, RssConf rssConf) {
    configureBlockIdLayout(sparkConf, rssConf, maxFailures, speculation);
  }

  /**
   * Derives block id layout config from maximum number of allowed partitions. This value can be set
   * in either SparkConf or RssConf via RssSparkConfig.RSS_MAX_PARTITIONS, where SparkConf has
   * precedence.
   *
   * <p>Computes the number of required bits for partition id and task attempt id and reserves
   * remaining bits for sequence number. Adds RssClientConf.BLOCKID_SEQUENCE_NO_BITS,
   * RssClientConf.BLOCKID_PARTITION_ID_BITS, and RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS to the
   * given RssConf and adds them prefixed with "spark." to the given SparkConf.
   *
   * <p>If RssSparkConfig.RSS_MAX_PARTITIONS is not set, given values for
   * RssClientConf.BLOCKID_SEQUENCE_NO_BITS, RssClientConf.BLOCKID_PARTITION_ID_BITS, and
   * RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS are copied
   *
   * <p>Then, BlockIdLayout can consistently be created from both configs:
   *
   * <p>BlockIdLayout.from(rssConf) BlockIdLayout.from(RssSparkConfig.toRssConf(sparkConf))
   *
   * @param sparkConf Spark config providing max partitions
   * @param rssConf Rss config to amend
   * @param maxFailures Spark max failures
   * @param speculation Spark speculative execution
   */
  @VisibleForTesting
  protected static void configureBlockIdLayout(
      SparkConf sparkConf, RssConf rssConf, int maxFailures, boolean speculation) {
    if (sparkConf.contains(RssSparkConfig.RSS_MAX_PARTITIONS.key())) {
      configureBlockIdLayoutFromMaxPartitions(sparkConf, rssConf, maxFailures, speculation);
    } else {
      configureBlockIdLayoutFromLayoutConfig(sparkConf, rssConf, maxFailures, speculation);
    }
  }

  private static void configureBlockIdLayoutFromMaxPartitions(
      SparkConf sparkConf, RssConf rssConf, int maxFailures, boolean speculation) {
    int maxPartitions =
        sparkConf.getInt(
            RssSparkConfig.RSS_MAX_PARTITIONS.key(),
            RssSparkConfig.RSS_MAX_PARTITIONS.defaultValue().get());
    if (maxPartitions <= 1) {
      throw new IllegalArgumentException(
          "Value of "
              + RssSparkConfig.RSS_MAX_PARTITIONS.key()
              + " must be larger than 1: "
              + maxPartitions);
    }

    int attemptIdBits =
        ClientUtils.getNumberOfSignificantBits(
            ClientUtils.getMaxAttemptNo(maxFailures, speculation));
    int partitionIdBits = ClientUtils.getNumberOfSignificantBits(maxPartitions - 1); // [1..31]
    int taskAttemptIdBits = partitionIdBits + attemptIdBits; // [1+attemptIdBits..31+attemptIdBits]
    int sequenceNoBits = 63 - partitionIdBits - taskAttemptIdBits; // [1-attemptIdBits..61]

    if (taskAttemptIdBits > 31) {
      throw new IllegalArgumentException(
          "Cannot support "
              + RssSparkConfig.RSS_MAX_PARTITIONS.key()
              + "="
              + maxPartitions
              + " partitions, "
              + "as this would require to reserve more than 31 bits "
              + "in the block id for task attempt ids. "
              + "With spark.maxFailures="
              + maxFailures
              + " and spark.speculation="
              + (speculation ? "true" : "false")
              + " at most "
              + (1 << (31 - attemptIdBits))
              + " partitions can be supported.");
    }

    // we have to cap the sequence number bits at 31 bits,
    // because BlockIdLayout imposes an upper bound of 31 bits
    // which is fine as this allows for over 2bn sequence ids
    if (sequenceNoBits > 31) {
      // move spare bits (bits over 31) from sequence number to partition id and task attempt id
      int spareBits = sequenceNoBits - 31;

      // make spareBits even, so we add same number of bits to partitionIdBits and taskAttemptIdBits
      spareBits += spareBits % 2;

      // move spare bits over
      partitionIdBits += spareBits / 2;
      taskAttemptIdBits += spareBits / 2;
      maxPartitions = (1 << partitionIdBits);

      // log with original sequenceNoBits
      if (LOG.isInfoEnabled()) {
        LOG.info(
            "Increasing "
                + RssSparkConfig.RSS_MAX_PARTITIONS.key()
                + " to "
                + maxPartitions
                + ", "
                + "otherwise we would have to support 2^"
                + sequenceNoBits
                + " (more than 2^31) sequence numbers.");
      }

      // remove spare bits
      sequenceNoBits -= spareBits;

      // propagate the change value back to SparkConf
      sparkConf.set(RssSparkConfig.RSS_MAX_PARTITIONS.key(), String.valueOf(maxPartitions));
    }

    // set block id layout config in RssConf
    rssConf.set(RssClientConf.BLOCKID_SEQUENCE_NO_BITS, sequenceNoBits);
    rssConf.set(RssClientConf.BLOCKID_PARTITION_ID_BITS, partitionIdBits);
    rssConf.set(RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS, taskAttemptIdBits);

    // materialize these RssConf settings in sparkConf as well
    // so that RssSparkConfig.toRssConf(sparkConf) provides this configuration
    sparkConf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_SEQUENCE_NO_BITS.key(),
        String.valueOf(sequenceNoBits));
    sparkConf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_PARTITION_ID_BITS.key(),
        String.valueOf(partitionIdBits));
    sparkConf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS.key(),
        String.valueOf(taskAttemptIdBits));
  }

  private static void configureBlockIdLayoutFromLayoutConfig(
      SparkConf sparkConf, RssConf rssConf, int maxFailures, boolean speculation) {
    String sparkPrefix = RssSparkConfig.SPARK_RSS_CONFIG_PREFIX;
    String sparkSeqNoBitsKey = sparkPrefix + RssClientConf.BLOCKID_SEQUENCE_NO_BITS.key();
    String sparkPartIdBitsKey = sparkPrefix + RssClientConf.BLOCKID_PARTITION_ID_BITS.key();
    String sparkTaskIdBitsKey = sparkPrefix + RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS.key();

    // if one bit field is configured, all three must be given
    List<String> sparkKeys =
        Arrays.asList(sparkSeqNoBitsKey, sparkPartIdBitsKey, sparkTaskIdBitsKey);
    if (sparkKeys.stream().anyMatch(sparkConf::contains)
        && !sparkKeys.stream().allMatch(sparkConf::contains)) {
      String allKeys = sparkKeys.stream().collect(Collectors.joining(", "));
      String existingKeys =
          Arrays.stream(sparkConf.getAll())
              .map(t -> t._1)
              .filter(sparkKeys.stream().collect(Collectors.toSet())::contains)
              .collect(Collectors.joining(", "));
      throw new IllegalArgumentException(
          "All block id bit config keys must be provided ("
              + allKeys
              + "), not just a sub-set: "
              + existingKeys);
    }

    // if one bit field is configured, all three must be given
    List<ConfigOption<Integer>> rssKeys =
        Arrays.asList(
            RssClientConf.BLOCKID_SEQUENCE_NO_BITS,
            RssClientConf.BLOCKID_PARTITION_ID_BITS,
            RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS);
    if (rssKeys.stream().anyMatch(rssConf::contains)
        && !rssKeys.stream().allMatch(rssConf::contains)) {
      String allKeys = rssKeys.stream().map(ConfigOption::key).collect(Collectors.joining(", "));
      String existingKeys =
          rssConf.getKeySet().stream()
              .filter(rssKeys.stream().map(ConfigOption::key).collect(Collectors.toSet())::contains)
              .collect(Collectors.joining(", "));
      throw new IllegalArgumentException(
          "All block id bit config keys must be provided ("
              + allKeys
              + "), not just a sub-set: "
              + existingKeys);
    }

    if (sparkKeys.stream().allMatch(sparkConf::contains)) {
      rssConf.set(RssClientConf.BLOCKID_SEQUENCE_NO_BITS, sparkConf.getInt(sparkSeqNoBitsKey, 0));
      rssConf.set(RssClientConf.BLOCKID_PARTITION_ID_BITS, sparkConf.getInt(sparkPartIdBitsKey, 0));
      rssConf.set(
          RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS, sparkConf.getInt(sparkTaskIdBitsKey, 0));
    } else if (rssKeys.stream().allMatch(rssConf::contains)) {
      sparkConf.set(sparkSeqNoBitsKey, rssConf.getValue(RssClientConf.BLOCKID_SEQUENCE_NO_BITS));
      sparkConf.set(sparkPartIdBitsKey, rssConf.getValue(RssClientConf.BLOCKID_PARTITION_ID_BITS));
      sparkConf.set(
          sparkTaskIdBitsKey, rssConf.getValue(RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS));
    } else {
      // use default max partitions
      sparkConf.set(
          RssSparkConfig.RSS_MAX_PARTITIONS.key(),
          RssSparkConfig.RSS_MAX_PARTITIONS.defaultValueString());
      configureBlockIdLayoutFromMaxPartitions(sparkConf, rssConf, maxFailures, speculation);
    }
  }

  /** See static overload of this method. */
  public long getTaskAttemptIdForBlockId(int mapIndex, int attemptNo) {
    return getTaskAttemptIdForBlockId(
        mapIndex, attemptNo, maxFailures, speculation, blockIdLayout.taskAttemptIdBits);
  }

  /**
   * Provides a task attempt id to be used in the block id, that is unique for a shuffle stage.
   *
   * <p>We are not using context.taskAttemptId() here as this is a monotonically increasing number
   * that is unique across the entire Spark app which can reach very large numbers, which can
   * practically reach LONG.MAX_VALUE. That would overflow the bits in the block id.
   *
   * <p>Here we use the map index or task id, appended by the attempt number per task. The map index
   * is limited by the number of partitions of a stage. The attempt number per task is limited /
   * configured by spark.task.maxFailures (default: 4).
   *
   * @return a task attempt id unique for a shuffle stage
   */
  protected static long getTaskAttemptIdForBlockId(
      int mapIndex, int attemptNo, int maxFailures, boolean speculation, int maxTaskAttemptIdBits) {
    int maxAttemptNo = ClientUtils.getMaxAttemptNo(maxFailures, speculation);
    int attemptBits = ClientUtils.getNumberOfSignificantBits(maxAttemptNo);

    if (attemptNo > maxAttemptNo) {
      // this should never happen, if it does, our assumptions are wrong,
      // and we risk overflowing the attempt number bits
      throw new RssException(
          "Observing attempt number "
              + attemptNo
              + " while maxFailures is set to "
              + maxFailures
              + (speculation ? " with speculation enabled" : "")
              + ".");
    }

    int mapIndexBits = ClientUtils.getNumberOfSignificantBits(mapIndex);
    if (mapIndexBits + attemptBits > maxTaskAttemptIdBits) {
      throw new RssException(
          "Observing mapIndex["
              + mapIndex
              + "] that would produce a taskAttemptId with "
              + (mapIndexBits + attemptBits)
              + " bits which is larger than the allowed "
              + maxTaskAttemptIdBits
              + " bits (maxFailures["
              + maxFailures
              + "], speculation["
              + speculation
              + "]). Please consider providing more bits for taskAttemptIds.");
    }

    return (long) mapIndex << attemptBits | attemptNo;
  }

  protected static void fetchAndApplyDynamicConf(SparkConf sparkConf) {
    String clientType = sparkConf.get(RssSparkConfig.RSS_CLIENT_TYPE);
    String coordinators = sparkConf.get(RssSparkConfig.RSS_COORDINATOR_QUORUM.key());
    long retryIntervalMs = sparkConf.get(RssSparkConfig.RSS_CLIENT_RETRY_INTERVAL_MAX);
    int retryTimes = sparkConf.get(RssSparkConfig.RSS_CLIENT_RETRY_MAX);
    int heartbeatThread = sparkConf.get(RssSparkConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM);
    CoordinatorClientFactory coordinatorClientFactory = CoordinatorClientFactory.getInstance();
    CoordinatorGrpcRetryableClient coordinatorClient =
        coordinatorClientFactory.createCoordinatorClient(
            ClientType.valueOf(clientType),
            coordinators,
            retryIntervalMs,
            retryTimes,
            heartbeatThread);

    int timeoutMs =
        sparkConf.getInt(
            RssSparkConfig.RSS_ACCESS_TIMEOUT_MS.key(),
            RssSparkConfig.RSS_ACCESS_TIMEOUT_MS.defaultValue().get());
    String user;
    try {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (Exception e) {
      throw new RssException("Errors on getting current user.", e);
    }
    RssFetchClientConfRequest request =
        new RssFetchClientConfRequest(timeoutMs, user, Collections.emptyMap());
    RssFetchClientConfResponse response = coordinatorClient.fetchClientConf(request);
    if (response.getStatusCode() == StatusCode.SUCCESS) {
      RssSparkShuffleUtils.applyDynamicClientConf(sparkConf, response.getClientConf());
    }
    coordinatorClient.close();
  }

  @Override
  public void unregisterAllMapOutput(int shuffleId) throws SparkException {
    if (!RssSparkShuffleUtils.isStageResubmitSupported()) {
      return;
    }
    MapOutputTrackerMaster tracker = getMapOutputTrackerMaster();
    if (isInitialized.compareAndSet(false, true)) {
      unregisterAllMapOutputMethod = getUnregisterAllMapOutputMethod(tracker);
      registerShuffleMethod = getRegisterShuffleMethod(tracker);
    }
    if (unregisterAllMapOutputMethod != null) {
      try {
        unregisterAllMapOutputMethod.invoke(tracker, shuffleId);
      } catch (InvocationTargetException | IllegalAccessException e) {
        throw new RssException("Invoke unregisterAllMapOutput method failed", e);
      }
    } else {
      int numMaps = getNumMaps(shuffleId);
      int numReduces = getPartitionNum(shuffleId);
      defaultUnregisterAllMapOutput(tracker, registerShuffleMethod, shuffleId, numMaps, numReduces);
    }
  }

  private static void defaultUnregisterAllMapOutput(
      MapOutputTrackerMaster tracker,
      Method registerShuffle,
      int shuffleId,
      int numMaps,
      int numReduces)
      throws SparkException {
    if (tracker != null && registerShuffle != null) {
      tracker.unregisterShuffle(shuffleId);
      // re-register this shuffle id into map output tracker
      try {
        if (SparkVersionUtils.MAJOR_VERSION > 3
            || (SparkVersionUtils.isSpark3() && SparkVersionUtils.MINOR_VERSION >= 2)) {
          registerShuffle.invoke(tracker, shuffleId, numMaps, numReduces);
        } else {
          registerShuffle.invoke(tracker, shuffleId, numMaps);
        }
      } catch (InvocationTargetException | IllegalAccessException e) {
        throw new RssException("Invoke registerShuffle method failed", e);
      }
      tracker.incrementEpoch();
    } else {
      throw new SparkException(
          "default unregisterAllMapOutput should only be called on the driver side");
    }
  }

  private static Method getUnregisterAllMapOutputMethod(MapOutputTrackerMaster tracker) {
    if (tracker != null) {
      Class<? extends MapOutputTrackerMaster> klass = tracker.getClass();
      Method m = null;
      try {
        if (SparkVersionUtils.isSpark2() && SparkVersionUtils.MINOR_VERSION <= 3) {
          // for spark version less than 2.3, there's no unregisterAllMapOutput support
          LOG.warn("Spark version <= 2.3, fallback to default method");
        } else if (SparkVersionUtils.isSpark2()) {
          // this method is added in Spark 2.4+
          m = klass.getDeclaredMethod("unregisterAllMapOutput", int.class);
        } else if (SparkVersionUtils.isSpark3() && SparkVersionUtils.MINOR_VERSION <= 1) {
          // spark 3.1 will have unregisterAllMapOutput method
          m = klass.getDeclaredMethod("unregisterAllMapOutput", int.class);
        } else if (SparkVersionUtils.isSpark3()) {
          m = klass.getDeclaredMethod("unregisterAllMapAndMergeOutput", int.class);
        } else {
          LOG.warn(
              "Unknown spark version({}), fallback to default method",
              SparkVersionUtils.SPARK_VERSION);
        }
      } catch (NoSuchMethodException e) {
        LOG.warn(
            "Got no such method error when get unregisterAllMapOutput method for spark version({})",
            SparkVersionUtils.SPARK_VERSION);
      }
      return m;
    } else {
      return null;
    }
  }

  private static Method getRegisterShuffleMethod(MapOutputTrackerMaster tracker) {
    if (tracker != null) {
      Class<? extends MapOutputTrackerMaster> klass = tracker.getClass();
      Method m = null;
      try {
        if (SparkVersionUtils.MAJOR_VERSION > 3
            || (SparkVersionUtils.isSpark3() && SparkVersionUtils.MINOR_VERSION >= 2)) {
          // for spark >= 3.2, the register shuffle method is changed to signature:
          //   registerShuffle(shuffleId, numMapTasks, numReduceTasks);
          m = klass.getDeclaredMethod("registerShuffle", int.class, int.class, int.class);
        } else {
          m = klass.getDeclaredMethod("registerShuffle", int.class, int.class);
        }
      } catch (NoSuchMethodException e) {
        LOG.warn(
            "Got no such method error when get registerShuffle method for spark version({})",
            SparkVersionUtils.SPARK_VERSION);
      }
      return m;
    } else {
      return null;
    }
  }

  private static MapOutputTrackerMaster getMapOutputTrackerMaster() {
    MapOutputTracker tracker =
        Optional.ofNullable(SparkEnv.get()).map(SparkEnv::mapOutputTracker).orElse(null);
    return tracker instanceof MapOutputTrackerMaster ? (MapOutputTrackerMaster) tracker : null;
  }

  private static Map<String, String> parseRemoteStorageConf(Configuration conf) {
    Map<String, String> confItems = Maps.newHashMap();
    for (Map.Entry<String, String> entry : conf) {
      confItems.put(entry.getKey(), entry.getValue());
    }
    return confItems;
  }

  protected static RemoteStorageInfo getDefaultRemoteStorageInfo(SparkConf sparkConf) {
    Map<String, String> confItems = Maps.newHashMap();
    RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);
    if (rssConf.getBoolean(RSS_CLIENT_REMOTE_STORAGE_USE_LOCAL_CONF_ENABLED)) {
      confItems = parseRemoteStorageConf(new Configuration(true));
    }

    for (String key : rssConf.getKeySet()) {
      if (key.startsWith(HADOOP_CONFIG_KEY_PREFIX)) {
        String val = rssConf.getString(key, null);
        if (val != null) {
          String extractedKey = key.replaceFirst(HADOOP_CONFIG_KEY_PREFIX, "");
          confItems.put(extractedKey, val);
        }
      }
    }

    return new RemoteStorageInfo(
        sparkConf.get(RssSparkConfig.RSS_REMOTE_STORAGE_PATH.key(), ""), confItems);
  }

  public ShuffleHandleInfo getShuffleHandleInfo(
      int stageAttemptId,
      int stageAttemptNumber,
      RssShuffleHandle<?, ?, ?> rssHandle,
      boolean isWritePhase) {
    int shuffleId = rssHandle.getShuffleId();
    if (shuffleManagerRpcServiceEnabled && rssStageRetryForWriteFailureEnabled) {
      // In Stage Retry mode, Get the ShuffleServer list from the Driver based on the shuffleId.
      return getRemoteShuffleHandleInfoWithStageRetry(
          stageAttemptId, stageAttemptNumber, shuffleId, isWritePhase);
    } else if (shuffleManagerRpcServiceEnabled && partitionReassignEnabled) {
      // In partition block Retry mode, Get the ShuffleServer list from the Driver based on the
      // shuffleId.
      return getRemoteShuffleHandleInfoWithBlockRetry(
          stageAttemptId, stageAttemptNumber, shuffleId, isWritePhase);
    } else {
      return new SimpleShuffleHandleInfo(
          shuffleId, rssHandle.getPartitionToServers(), rssHandle.getRemoteStorage());
    }
  }

  /**
   * In Stage Retry mode, obtain the Shuffle Server list from the Driver based on shuffleId.
   *
   * @param shuffleId shuffleId
   * @return ShuffleHandleInfo
   */
  protected synchronized StageAttemptShuffleHandleInfo getRemoteShuffleHandleInfoWithStageRetry(
      int stageAttemptId, int stageAttemptNumber, int shuffleId, boolean isWritePhase) {
    RssPartitionToShuffleServerRequest rssPartitionToShuffleServerRequest =
        new RssPartitionToShuffleServerRequest(
            stageAttemptId, stageAttemptNumber, shuffleId, isWritePhase);
    RssReassignOnStageRetryResponse rpcPartitionToShufflerServer =
        getOrCreateShuffleManagerClientSupplier()
            .get()
            .getPartitionToShufflerServerWithStageRetry(rssPartitionToShuffleServerRequest);
    StageAttemptShuffleHandleInfo shuffleHandleInfo =
        StageAttemptShuffleHandleInfo.fromProto(
            rpcPartitionToShufflerServer.getShuffleHandleInfoProto());
    return shuffleHandleInfo;
  }

  /**
   * In Block Retry mode, obtain the Shuffle Server list from the Driver based on shuffleId.
   *
   * @param shuffleId shuffleId
   * @return ShuffleHandleInfo
   */
  protected synchronized MutableShuffleHandleInfo getRemoteShuffleHandleInfoWithBlockRetry(
      int stageAttemptId, int stageAttemptNumber, int shuffleId, boolean isWritePhase) {
    RssPartitionToShuffleServerRequest rssPartitionToShuffleServerRequest =
        new RssPartitionToShuffleServerRequest(
            stageAttemptId, stageAttemptNumber, shuffleId, isWritePhase);
    RssReassignOnBlockSendFailureResponse rpcPartitionToShufflerServer =
        getOrCreateShuffleManagerClientSupplier()
            .get()
            .getPartitionToShufflerServerWithBlockRetry(rssPartitionToShuffleServerRequest);
    MutableShuffleHandleInfo shuffleHandleInfo =
        MutableShuffleHandleInfo.fromProto(rpcPartitionToShufflerServer.getHandle());
    return shuffleHandleInfo;
  }

  protected synchronized Supplier<ShuffleManagerClient> getOrCreateShuffleManagerClientSupplier() {
    if (managerClientSupplier == null) {
      RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);
      String driver = rssConf.getString("driver.host", "");
      int port = rssConf.get(RssClientConf.SHUFFLE_MANAGER_GRPC_PORT);
      long rpcTimeout = rssConf.getLong(RssClientConf.RPC_TIMEOUT_MS);
      this.managerClientSupplier =
          ExpiringCloseableSupplier.of(
              () ->
                  ShuffleManagerClientFactory.getInstance()
                      .createShuffleManagerClient(ClientType.GRPC, driver, port, rpcTimeout));
    }
    return managerClientSupplier;
  }

  @Override
  public ShuffleHandleInfo getShuffleHandleInfoByShuffleId(int shuffleId) {
    return shuffleHandleInfoManager.get(shuffleId);
  }

  /**
   * @return the maximum number of fetch failures per shuffle partition before that shuffle stage
   *     should be recomputed
   */
  @Override
  public int getMaxFetchFailures() {
    final String TASK_MAX_FAILURE = "spark.task.maxFailures";
    return Math.max(0, sparkConf.getInt(TASK_MAX_FAILURE, 4) - 1);
  }

  /**
   * Add the shuffleServer that failed to write to the failure list
   *
   * @param shuffleServerId
   */
  @Override
  public void addFailuresShuffleServerInfos(String shuffleServerId) {
    rssStageResubmitManager.recordFailuresShuffleServer(shuffleServerId);
  }

  /**
   * Reassign the ShuffleServer list for ShuffleId
   *
   * @param shuffleId
   */
  @Override
  public boolean reassignOnStageResubmit(
      int shuffleId, int stageAttemptId, int stageAttemptNumber) {
    int requiredShuffleServerNumber =
        RssSparkShuffleUtils.getRequiredShuffleServerNumber(sparkConf);
    int estimateTaskConcurrency = RssSparkShuffleUtils.estimateTaskConcurrency(sparkConf);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers =
        requestShuffleAssignment(
            shuffleId,
            getPartitionNum(shuffleId),
            1,
            requiredShuffleServerNumber,
            estimateTaskConcurrency,
            rssStageResubmitManager.getServerIdBlackList(),
            stageAttemptId,
            stageAttemptNumber,
            false);
    MutableShuffleHandleInfo shuffleHandleInfo =
        new MutableShuffleHandleInfo(shuffleId, partitionToServers, getRemoteStorageInfo());
    StageAttemptShuffleHandleInfo stageAttemptShuffleHandleInfo =
        (StageAttemptShuffleHandleInfo) shuffleHandleInfoManager.get(shuffleId);
    stageAttemptShuffleHandleInfo.replaceCurrentShuffleHandleInfo(shuffleHandleInfo);
    LOG.info(
        "The stage retry has been triggered successfully for the shuffleId: {}, attemptNumber: {}",
        shuffleId,
        stageAttemptNumber);
    return true;
  }

  /** this is only valid on driver side that exposed to being invoked by grpc server */
  @Override
  public MutableShuffleHandleInfo reassignOnBlockSendFailure(
      int stageId,
      int stageAttemptNumber,
      int shuffleId,
      Map<Integer, List<ReceivingFailureServer>> partitionToFailureServers,
      boolean partitionSplit) {
    long startTime = System.currentTimeMillis();
    ShuffleHandleInfo handleInfo = shuffleHandleInfoManager.get(shuffleId);
    MutableShuffleHandleInfo internalHandle = null;
    if (handleInfo instanceof MutableShuffleHandleInfo) {
      internalHandle = (MutableShuffleHandleInfo) handleInfo;
    } else if (handleInfo instanceof StageAttemptShuffleHandleInfo) {
      internalHandle =
          (MutableShuffleHandleInfo) ((StageAttemptShuffleHandleInfo) handleInfo).getCurrent();
    }
    if (internalHandle == null) {
      throw new RssException(
          "An unexpected error occurred: internalHandle is null, which should not happen");
    }
    synchronized (internalHandle) {
      // If the reassignment servers for one partition exceeds the max reassign server num,
      // it should fast fail.
      if (!partitionSplit) {
        // Do not check the partition reassign server num for partition split case
        internalHandle.checkPartitionReassignServerNum(
            partitionToFailureServers.keySet(), partitionReassignMaxServerNum);
      }

      Map<ShuffleServerInfo, List<PartitionRange>> newServerToPartitions = new HashMap<>();
      // receivingFailureServer -> partitionId -> replacementServerIds. For logging
      Map<String, Map<Integer, Set<String>>> reassignResult = new HashMap<>();

      for (Map.Entry<Integer, List<ReceivingFailureServer>> entry :
          partitionToFailureServers.entrySet()) {
        int partitionId = entry.getKey();
        for (ReceivingFailureServer receivingFailureServer : entry.getValue()) {
          StatusCode code = receivingFailureServer.getStatusCode();
          String serverId = receivingFailureServer.getServerId();

          boolean serverHasReplaced = false;

          Set<ShuffleServerInfo> updatedReassignServers;
          if (!partitionSplit) {
            Set<ShuffleServerInfo> replacements = internalHandle.getReplacements(serverId);
            if (CollectionUtils.isEmpty(replacements)) {
              replacements =
                  requestReassignServer(
                      stageId,
                      stageAttemptNumber,
                      shuffleId,
                      internalHandle,
                      partitionId,
                      serverId);
            } else {
              serverHasReplaced = true;
            }
            updatedReassignServers =
                internalHandle.updateAssignment(partitionId, serverId, replacements);
          } else {
            Set<ShuffleServerInfo> replacements =
                internalHandle.getReplacementsForPartition(partitionId, serverId);
            if (CollectionUtils.isEmpty(replacements)) {
              replacements =
                  requestReassignServer(
                      stageId,
                      stageAttemptNumber,
                      shuffleId,
                      internalHandle,
                      partitionId,
                      serverId);
            } else {
              serverHasReplaced = true;
            }
            updatedReassignServers =
                internalHandle.updateAssignmentOnPartitionSplit(
                    partitionId, serverId, replacements);
          }

          if (!updatedReassignServers.isEmpty()) {
            reassignResult
                .computeIfAbsent(serverId, x -> new HashMap<>())
                .computeIfAbsent(partitionId, x -> new HashSet<>())
                .addAll(
                    updatedReassignServers.stream()
                        .map(x -> x.getId())
                        .collect(Collectors.toSet()));

            if (serverHasReplaced) {
              for (ShuffleServerInfo serverInfo : updatedReassignServers) {
                newServerToPartitions
                    .computeIfAbsent(serverInfo, x -> new ArrayList<>())
                    .add(new PartitionRange(partitionId, partitionId));
              }
            }
          }
        }
      }
      if (!newServerToPartitions.isEmpty()) {
        LOG.info(
            "Register the new partition->servers assignment on reassign. {}",
            newServerToPartitions);
        registerShuffleServers(
            getAppId(), shuffleId, newServerToPartitions, getRemoteStorageInfo());
      }

      LOG.info(
          "Finished reassignOnBlockSendFailure request and cost {}(ms). Reassign result: {}",
          System.currentTimeMillis() - startTime,
          reassignResult);

      return internalHandle;
    }
  }

  private Set<ShuffleServerInfo> requestReassignServer(
      int stageId,
      int stageAttemptNumber,
      int shuffleId,
      MutableShuffleHandleInfo internalHandle,
      int partitionId,
      String serverId) {
    Set<ShuffleServerInfo> replacements;
    final int requiredServerNum = 1;
    Set<String> excludedServers = new HashSet<>(internalHandle.listExcludedServers());
    // Exclude the servers that has already been replaced for partition split case.
    excludedServers.addAll(internalHandle.listExcludedServersForPartition(partitionId));
    excludedServers.add(serverId);
    replacements =
        reassignServerForTask(
            stageId,
            stageAttemptNumber,
            shuffleId,
            Sets.newHashSet(partitionId),
            excludedServers,
            requiredServerNum,
            true);
    return replacements;
  }

  @Override
  public void stop() {
    if (managerClientSupplier != null
        && managerClientSupplier instanceof ExpiringCloseableSupplier) {
      ((ExpiringCloseableSupplier<ShuffleManagerClient>) managerClientSupplier).close();
    }
    if (heartBeatScheduledExecutorService != null) {
      heartBeatScheduledExecutorService.shutdownNow();
    }
    if (shuffleWriteClient != null) {
      // Unregister shuffle before closing shuffle write client.
      shuffleWriteClient.unregisterShuffle(getAppId());
      shuffleWriteClient.close();
    }
    if (dataPusher != null) {
      try {
        dataPusher.close();
      } catch (IOException e) {
        LOG.warn("Errors on closing data pusher", e);
      }
    }

    if (shuffleManagerServer != null) {
      try {
        shuffleManagerServer.stop();
      } catch (InterruptedException e) {
        // ignore
        LOG.info("shuffle manager server is interrupted during stop");
      }
    }
  }

  /** @return the unique spark id for rss shuffle */
  @Override
  public String getAppId() {
    return id.get();
  }

  @Override
  public int getPartitionNum(int shuffleId) {
    return shuffleIdToPartitionNum.getOrDefault(shuffleId, 0);
  }

  /**
   * @param shuffleId the shuffle id to query
   * @return the num of map tasks for current shuffle with shuffle id.
   */
  @Override
  public int getNumMaps(int shuffleId) {
    return shuffleIdToNumMapTasks.getOrDefault(shuffleId, 0);
  }

  @VisibleForTesting
  public void addSuccessBlockIds(String taskId, Set<Long> blockIds) {
    if (taskToSuccessBlockIds.get(taskId) == null) {
      taskToSuccessBlockIds.put(taskId, Sets.newHashSet());
    }
    taskToSuccessBlockIds.get(taskId).addAll(blockIds);
  }

  @VisibleForTesting
  public void addFailedBlockSendTracker(
      String taskId, FailedBlockSendTracker failedBlockSendTracker) {
    taskToFailedBlockSendTracker.putIfAbsent(taskId, failedBlockSendTracker);
  }

  /** Create the shuffleWriteClient. */
  protected abstract ShuffleWriteClient createShuffleWriteClient();

  /**
   * Check whether the configuration is supported.
   *
   * @param sparkConf the sparkConf
   */
  protected void checkSupported(SparkConf sparkConf) {}

  /**
   * Creating the shuffleAssignmentInfo from the servers and partitionIds
   *
   * @param servers
   * @param partitionIds
   * @return
   */
  private ShuffleAssignmentsInfo createShuffleAssignmentsInfo(
      Set<ShuffleServerInfo> servers, Set<Integer> partitionIds) {
    Map<Integer, List<ShuffleServerInfo>> newPartitionToServers = new HashMap<>();
    List<PartitionRange> partitionRanges = new ArrayList<>();
    for (Integer partitionId : partitionIds) {
      newPartitionToServers.put(partitionId, new ArrayList<>(servers));
      partitionRanges.add(new PartitionRange(partitionId, partitionId));
    }
    Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges = new HashMap<>();
    for (ShuffleServerInfo server : servers) {
      serverToPartitionRanges.put(server, partitionRanges);
    }
    return new ShuffleAssignmentsInfo(newPartitionToServers, serverToPartitionRanges);
  }

  /** Request the new shuffle-servers to replace faulty server. */
  private Set<ShuffleServerInfo> reassignServerForTask(
      int stageId,
      int stageAttemptNumber,
      int shuffleId,
      Set<Integer> partitionIds,
      Set<String> excludedServers,
      int requiredServerNum,
      boolean reassign) {
    AtomicReference<Set<ShuffleServerInfo>> replacementsRef =
        new AtomicReference<>(new HashSet<>());
    requestShuffleAssignment(
        shuffleId,
        requiredServerNum,
        1,
        requiredServerNum,
        1,
        excludedServers,
        shuffleAssignmentsInfo -> {
          if (shuffleAssignmentsInfo == null) {
            return null;
          }
          Set<ShuffleServerInfo> replacements =
              shuffleAssignmentsInfo.getPartitionToServers().values().stream()
                  .flatMap(x -> x.stream())
                  .collect(Collectors.toSet());
          replacementsRef.set(replacements);
          return createShuffleAssignmentsInfo(replacements, partitionIds);
        },
        stageId,
        stageAttemptNumber,
        reassign);
    return replacementsRef.get();
  }

  private Map<Integer, List<ShuffleServerInfo>> requestShuffleAssignment(
      int shuffleId,
      int partitionNum,
      int partitionNumPerRange,
      int assignmentShuffleServerNumber,
      int estimateTaskConcurrency,
      Set<String> faultyServerIds,
      Function<ShuffleAssignmentsInfo, ShuffleAssignmentsInfo> reassignmentHandler,
      int stageId,
      int stageAttemptNumber,
      boolean reassign) {
    Set<String> assignmentTags = RssSparkShuffleUtils.getAssignmentTags(sparkConf);
    ClientUtils.validateClientType(clientType);
    assignmentTags.add(clientType);
    long retryInterval = sparkConf.get(RssSparkConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL);
    int retryTimes = sparkConf.get(RssSparkConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES);
    faultyServerIds.addAll(rssStageResubmitManager.getServerIdBlackList());
    try {
      ShuffleAssignmentsInfo response =
          shuffleWriteClient.getShuffleAssignments(
              getAppId(),
              shuffleId,
              partitionNum,
              partitionNumPerRange,
              assignmentTags,
              assignmentShuffleServerNumber,
              estimateTaskConcurrency,
              faultyServerIds,
              stageId,
              stageAttemptNumber,
              reassign,
              retryInterval,
              retryTimes);
      LOG.info("Finished reassign");
      if (reassignmentHandler != null) {
        response = reassignmentHandler.apply(response);
      }
      registerShuffleServers(
          getAppId(), shuffleId, response.getServerToPartitionRanges(), getRemoteStorageInfo());
      return response.getPartitionToServers();
    } catch (Throwable throwable) {
      throw new RssException("registerShuffle failed!", throwable);
    }
  }

  protected Map<Integer, List<ShuffleServerInfo>> requestShuffleAssignment(
      int shuffleId,
      int partitionNum,
      int partitionNumPerRange,
      int assignmentShuffleServerNumber,
      int estimateTaskConcurrency,
      Set<String> faultyServerIds,
      int stageId,
      int stageAttemptNumber,
      boolean reassign) {
    Set<String> assignmentTags = RssSparkShuffleUtils.getAssignmentTags(sparkConf);
    ClientUtils.validateClientType(clientType);
    assignmentTags.add(clientType);

    long retryInterval = sparkConf.get(RssSparkConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL);
    int retryTimes = sparkConf.get(RssSparkConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES);
    faultyServerIds.addAll(rssStageResubmitManager.getServerIdBlackList());
    try {
      return RetryUtils.retry(
          () -> {
            // retry zero times in shuffleWriteClient.getShuffleAssignments, let
            // getShuffleAssignments and registerShuffleServers in one retry func
            ShuffleAssignmentsInfo response =
                shuffleWriteClient.getShuffleAssignments(
                    appId,
                    shuffleId,
                    partitionNum,
                    partitionNumPerRange,
                    assignmentTags,
                    assignmentShuffleServerNumber,
                    estimateTaskConcurrency,
                    faultyServerIds,
                    stageId,
                    stageAttemptNumber,
                    reassign,
                    0,
                    0);
            registerShuffleServers(
                appId,
                shuffleId,
                response.getServerToPartitionRanges(),
                getRemoteStorageInfo(),
                stageAttemptNumber);
            return response.getPartitionToServers();
          },
          retryInterval,
          retryTimes);
    } catch (Throwable throwable) {
      throw new RssException("getShuffleAssignments or registerShuffle failed!", throwable);
    }
  }

  protected Map<Integer, List<ShuffleServerInfo>> requestShuffleAssignment(
      int shuffleId,
      int partitionNum,
      int partitionNumPerRange,
      int assignmentShuffleServerNumber,
      int estimateTaskConcurrency,
      Set<String> faultyServerIds,
      int stageAttemptNumber) {
    return requestShuffleAssignment(
        shuffleId,
        partitionNum,
        partitionNumPerRange,
        assignmentShuffleServerNumber,
        estimateTaskConcurrency,
        faultyServerIds,
        -1,
        stageAttemptNumber,
        false);
  }

  protected void registerShuffleServers(
      String appId,
      int shuffleId,
      Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges,
      RemoteStorageInfo remoteStorage,
      int stageAttemptNumber) {
    if (serverToPartitionRanges == null || serverToPartitionRanges.isEmpty()) {
      return;
    }
    LOG.info("Start to register shuffleId {}", shuffleId);
    long start = System.currentTimeMillis();
    Map<String, String> sparkConfMap = sparkConfToMap(getSparkConf());
    serverToPartitionRanges.entrySet().stream()
        .forEach(
            entry -> {
              shuffleWriteClient.registerShuffle(
                  entry.getKey(),
                  appId,
                  shuffleId,
                  entry.getValue(),
                  remoteStorage,
                  ShuffleDataDistributionType.NORMAL,
                  maxConcurrencyPerPartitionToWrite,
                  stageAttemptNumber,
                  null,
                  sparkConfMap);
            });
    LOG.info(
        "Finish register shuffleId {} with {} ms", shuffleId, (System.currentTimeMillis() - start));
  }

  @VisibleForTesting
  protected void registerShuffleServers(
      String appId,
      int shuffleId,
      Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges,
      RemoteStorageInfo remoteStorage) {
    if (serverToPartitionRanges == null || serverToPartitionRanges.isEmpty()) {
      return;
    }
    LOG.info("Start to register shuffleId[{}]", shuffleId);
    long start = System.currentTimeMillis();
    Map<String, String> sparkConfMap = sparkConfToMap(getSparkConf());
    Set<Map.Entry<ShuffleServerInfo, List<PartitionRange>>> entries =
        serverToPartitionRanges.entrySet();
    entries.stream()
        .forEach(
            entry -> {
              shuffleWriteClient.registerShuffle(
                  entry.getKey(),
                  appId,
                  shuffleId,
                  entry.getValue(),
                  remoteStorage,
                  dataDistributionType,
                  maxConcurrencyPerPartitionToWrite,
                  sparkConfMap);
            });
    LOG.info(
        "Finish register shuffleId[{}] with {} ms",
        shuffleId,
        (System.currentTimeMillis() - start));
  }

  protected RemoteStorageInfo getRemoteStorageInfo() {
    String storageType = sparkConf.get(RssSparkConfig.RSS_STORAGE_TYPE.key());
    RemoteStorageInfo defaultRemoteStorage =
        new RemoteStorageInfo(sparkConf.get(RssSparkConfig.RSS_REMOTE_STORAGE_PATH.key(), ""));
    return ClientUtils.fetchRemoteStorage(
        appId, defaultRemoteStorage, dynamicConfEnabled, storageType, shuffleWriteClient);
  }

  public boolean isRssStageRetryEnabled() {
    return rssStageRetryEnabled;
  }

  public boolean isRssStageRetryForWriteFailureEnabled() {
    return rssStageRetryForWriteFailureEnabled;
  }

  public boolean isRssStageRetryForFetchFailureEnabled() {
    return rssStageRetryForFetchFailureEnabled;
  }

  @VisibleForTesting
  public SparkConf getSparkConf() {
    return sparkConf;
  }

  public Map<String, String> sparkConfToMap(SparkConf sparkConf) {
    Map<String, String> map = new HashMap<>();

    for (Tuple2<String, String> tuple : sparkConf.getAll()) {
      String key = tuple._1;
      map.put(key, tuple._2);
    }

    return map;
  }

  public ShuffleWriteClient getShuffleWriteClient() {
    return shuffleWriteClient;
  }

  protected synchronized void startHeartbeat() {
    shuffleWriteClient.registerApplicationInfo(getAppId(), heartbeatTimeout, user);
    if (!sparkConf.getBoolean(RssSparkConfig.RSS_TEST_FLAG.key(), false) && !heartbeatStarted) {
      heartBeatScheduledExecutorService.scheduleAtFixedRate(
          () -> {
            try {
              String appId = getAppId();
              shuffleWriteClient.sendAppHeartbeat(appId, heartbeatTimeout);
              LOG.info("Finish send heartbeat to coordinator and servers");
            } catch (Exception e) {
              LOG.warn("Fail to send heartbeat to coordinator and servers", e);
            }
          },
          heartbeatInterval / 2,
          heartbeatInterval,
          TimeUnit.MILLISECONDS);
      heartbeatStarted = true;
    }
  }

  public void clearTaskMeta(String taskId) {
    taskToSuccessBlockIds.remove(taskId);
    taskToFailedBlockSendTracker.remove(taskId);
  }

  @VisibleForTesting
  protected void registerCoordinator() {
    String coordinators = sparkConf.get(RssSparkConfig.RSS_COORDINATOR_QUORUM.key());
    LOG.info("Start Registering coordinators {}", coordinators);
    shuffleWriteClient.registerCoordinators(
        coordinators,
        this.sparkConf.get(RssSparkConfig.RSS_CLIENT_RETRY_INTERVAL_MAX),
        this.sparkConf.get(RssSparkConfig.RSS_CLIENT_RETRY_MAX));
  }

  public Set<Long> getFailedBlockIds(String taskId) {
    FailedBlockSendTracker blockIdsFailedSendTracker = getBlockIdsFailedSendTracker(taskId);
    if (blockIdsFailedSendTracker == null) {
      return Collections.emptySet();
    }
    return blockIdsFailedSendTracker.getFailedBlockIds();
  }

  public Set<Long> getSuccessBlockIds(String taskId) {
    Set<Long> result = taskToSuccessBlockIds.get(taskId);
    if (result == null) {
      result = Collections.emptySet();
    }
    return result;
  }

  public FailedBlockSendTracker getBlockIdsFailedSendTracker(String taskId) {
    return taskToFailedBlockSendTracker.get(taskId);
  }

  public boolean markFailedTask(String taskId) {
    LOG.info("Mark the task: {} failed.", taskId);
    failedTaskIds.add(taskId);
    return true;
  }

  public boolean isValidTask(String taskId) {
    return !failedTaskIds.contains(taskId);
  }

  @VisibleForTesting
  public void setDataPusher(DataPusher dataPusher) {
    this.dataPusher = dataPusher;
  }

  public DataPusher getDataPusher() {
    return dataPusher;
  }

  @VisibleForTesting
  public Map<String, Set<Long>> getTaskToSuccessBlockIds() {
    return taskToSuccessBlockIds;
  }

  @VisibleForTesting
  public Map<String, FailedBlockSendTracker> getTaskToFailedBlockSendTracker() {
    return taskToFailedBlockSendTracker;
  }

  public CompletableFuture<Long> sendData(AddBlockEvent event) {
    if (dataPusher != null && event != null) {
      return dataPusher.send(event);
    }
    return new CompletableFuture<>();
  }
}
