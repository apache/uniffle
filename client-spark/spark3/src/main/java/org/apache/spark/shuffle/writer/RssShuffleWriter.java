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

package org.apache.spark.shuffle.writer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import scala.Function1;
import scala.Option;
import scala.Product2;
import scala.Tuple2;
import scala.collection.Iterator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.shuffle.FetchFailedException;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.RssShuffleManager;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.shuffle.RssSparkShuffleUtils;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.handle.MutableShuffleHandleInfo;
import org.apache.spark.shuffle.handle.ShuffleHandleInfo;
import org.apache.spark.storage.BlockManagerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.impl.FailedBlockSendTracker;
import org.apache.uniffle.client.impl.TrackingBlockStatus;
import org.apache.uniffle.client.request.RssReassignOnBlockSendFailureRequest;
import org.apache.uniffle.client.request.RssReportShuffleWriteFailureRequest;
import org.apache.uniffle.client.response.RssReassignOnBlockSendFailureResponse;
import org.apache.uniffle.client.response.RssReportShuffleWriteFailureResponse;
import org.apache.uniffle.common.ReceivingFailureServer;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.exception.RssSendFailedException;
import org.apache.uniffle.common.exception.RssWaitFailedException;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_CLIENT_MAP_SIDE_COMBINE_ENABLED;
import static org.apache.spark.shuffle.RssSparkConfig.RSS_PARTITION_REASSIGN_BLOCK_RETRY_MAX_TIMES;
import static org.apache.spark.shuffle.RssSparkConfig.RSS_REMOTE_MERGE_ENABLE;
import static org.apache.spark.shuffle.RssSparkConfig.RSS_RESUBMIT_STAGE_WITH_WRITE_FAILURE_ENABLED;

public class RssShuffleWriter<K, V, C> extends ShuffleWriter<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleWriter.class);
  private static final String DUMMY_HOST = "dummy_host";
  private static final int DUMMY_PORT = 99999;
  public static final String DEFAULT_ERROR_MESSAGE = "Default Error Message";

  private final String appId;
  private final int shuffleId;
  private final ShuffleHandleInfo shuffleHandleInfo;
  private WriteBufferManager bufferManager;
  private String taskId;
  private final int numMaps;
  private final ShuffleDependency<K, V, C> shuffleDependency;
  private final Partitioner partitioner;
  private final RssShuffleManager shuffleManager;
  private final boolean shouldPartition;
  private final long sendCheckTimeout;
  private final long sendCheckInterval;
  private final int bitmapSplitNum;
  // server -> partitionId -> blockIds
  private Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds;
  private final ShuffleWriteClient shuffleWriteClient;
  private final Set<ShuffleServerInfo> shuffleServersForData;
  private final long[] partitionLengths;
  // Gluten needs this variable
  protected final boolean isMemoryShuffleEnabled;
  private final Function<String, Boolean> taskFailureCallback;
  private final Set<Long> blockIds = Sets.newConcurrentHashSet();
  private TaskContext taskContext;
  private SparkConf sparkConf;
  private boolean blockFailSentRetryEnabled;
  private int blockFailSentRetryMaxTimes = 1;

  /** used by columnar rss shuffle writer implementation */
  protected final long taskAttemptId;

  protected final ShuffleWriteMetrics shuffleWriteMetrics;

  private final BlockingQueue<Object> finishEventQueue = new LinkedBlockingQueue<>();

  // Will be updated when the reassignment is triggered.
  private TaskAttemptAssignment taskAttemptAssignment;

  private static final Set<StatusCode> STATUS_CODE_WITHOUT_BLOCK_RESEND =
      Sets.newHashSet(StatusCode.NO_REGISTER);

  private final Supplier<ShuffleManagerClient> managerClientSupplier;
  private boolean enableWriteFailureRetry;
  private Set<ShuffleServerInfo> recordReportFailedShuffleservers;

  // Only for tests
  @VisibleForTesting
  public RssShuffleWriter(
      String appId,
      int shuffleId,
      String taskId,
      long taskAttemptId,
      WriteBufferManager bufferManager,
      ShuffleWriteMetrics shuffleWriteMetrics,
      RssShuffleManager shuffleManager,
      SparkConf sparkConf,
      ShuffleWriteClient shuffleWriteClient,
      Supplier<ShuffleManagerClient> managerClientSupplier,
      RssShuffleHandle<K, V, C> rssHandle,
      ShuffleHandleInfo shuffleHandleInfo,
      TaskContext context) {
    this(
        appId,
        shuffleId,
        taskId,
        taskAttemptId,
        shuffleWriteMetrics,
        shuffleManager,
        sparkConf,
        shuffleWriteClient,
        managerClientSupplier,
        rssHandle,
        (tid) -> true,
        shuffleHandleInfo,
        context);
    this.bufferManager = bufferManager;
    this.taskAttemptAssignment = new TaskAttemptAssignment(taskAttemptId, shuffleHandleInfo);
  }

  private RssShuffleWriter(
      String appId,
      int shuffleId,
      String taskId,
      long taskAttemptId,
      ShuffleWriteMetrics shuffleWriteMetrics,
      RssShuffleManager shuffleManager,
      SparkConf sparkConf,
      ShuffleWriteClient shuffleWriteClient,
      Supplier<ShuffleManagerClient> managerClientSupplier,
      RssShuffleHandle<K, V, C> rssHandle,
      Function<String, Boolean> taskFailureCallback,
      ShuffleHandleInfo shuffleHandleInfo,
      TaskContext context) {
    LOG.info(
        "RssShuffle start write taskAttemptId[{}] data with RssHandle[appId {}, shuffleId {}].",
        taskAttemptId,
        rssHandle.getAppId(),
        rssHandle.getShuffleId());
    this.shuffleManager = shuffleManager;
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.taskId = taskId;
    this.taskAttemptId = taskAttemptId;
    this.numMaps = rssHandle.getNumMaps();
    this.shuffleWriteMetrics = shuffleWriteMetrics;
    this.shuffleDependency = rssHandle.getDependency();
    this.partitioner = shuffleDependency.partitioner();
    this.shouldPartition = partitioner.numPartitions() > 1;
    this.sendCheckTimeout = sparkConf.get(RssSparkConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS);
    this.sendCheckInterval = sparkConf.get(RssSparkConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS);
    this.bitmapSplitNum = sparkConf.get(RssSparkConfig.RSS_CLIENT_BITMAP_SPLIT_NUM);
    this.serverToPartitionToBlockIds = Maps.newHashMap();
    this.shuffleWriteClient = shuffleWriteClient;
    this.shuffleServersForData = shuffleHandleInfo.getServers();
    this.partitionLengths = new long[partitioner.numPartitions()];
    Arrays.fill(partitionLengths, 0);
    this.isMemoryShuffleEnabled =
        isMemoryShuffleEnabled(sparkConf.get(RssSparkConfig.RSS_STORAGE_TYPE.key()));
    this.taskFailureCallback = taskFailureCallback;
    this.shuffleHandleInfo = shuffleHandleInfo;
    this.taskContext = context;
    this.sparkConf = sparkConf;
    this.managerClientSupplier = managerClientSupplier;
    this.blockFailSentRetryEnabled =
        sparkConf.getBoolean(
            RssSparkConfig.SPARK_RSS_CONFIG_PREFIX
                + RssClientConf.RSS_CLIENT_REASSIGN_ENABLED.key(),
            RssClientConf.RSS_CLIENT_REASSIGN_ENABLED.defaultValue());
    this.blockFailSentRetryMaxTimes =
        RssSparkConfig.toRssConf(sparkConf).get(RSS_PARTITION_REASSIGN_BLOCK_RETRY_MAX_TIMES);
    this.enableWriteFailureRetry =
        RssSparkConfig.toRssConf(sparkConf).get(RSS_RESUBMIT_STAGE_WITH_WRITE_FAILURE_ENABLED);
    this.recordReportFailedShuffleservers = Sets.newConcurrentHashSet();
  }

  public RssShuffleWriter(
      String appId,
      int shuffleId,
      String taskId,
      long taskAttemptId,
      ShuffleWriteMetrics shuffleWriteMetrics,
      RssShuffleManager shuffleManager,
      SparkConf sparkConf,
      ShuffleWriteClient shuffleWriteClient,
      Supplier<ShuffleManagerClient> managerClientSupplier,
      RssShuffleHandle<K, V, C> rssHandle,
      Function<String, Boolean> taskFailureCallback,
      TaskContext context) {
    this(
        appId,
        shuffleId,
        taskId,
        taskAttemptId,
        shuffleWriteMetrics,
        shuffleManager,
        sparkConf,
        shuffleWriteClient,
        managerClientSupplier,
        rssHandle,
        taskFailureCallback,
        shuffleManager.getShuffleHandleInfo(
            context.stageId(), context.stageAttemptNumber(), rssHandle, true),
        context);
    this.taskAttemptAssignment = new TaskAttemptAssignment(taskAttemptId, shuffleHandleInfo);
    BufferManagerOptions bufferOptions = new BufferManagerOptions(sparkConf);
    if (sparkConf.get(RSS_REMOTE_MERGE_ENABLE)) {
      final WriteBufferManager bufferManager =
          new RMWriteBufferManager(
              shuffleId,
              taskId,
              taskAttemptId,
              bufferOptions,
              rssHandle.getDependency().serializer(),
              context.taskMemoryManager(),
              shuffleWriteMetrics,
              RssSparkConfig.toRssConf(sparkConf),
              this::processShuffleBlockInfos,
              this::getPartitionAssignedServers,
              context.stageAttemptNumber(),
              shuffleDependency.keyOrdering().getOrElse(() -> null));
      this.bufferManager = bufferManager;
    } else {
      final WriteBufferManager bufferManager =
          new WriteBufferManager(
              shuffleId,
              taskId,
              taskAttemptId,
              bufferOptions,
              rssHandle.getDependency().serializer(),
              context.taskMemoryManager(),
              shuffleWriteMetrics,
              RssSparkConfig.toRssConf(sparkConf),
              this::processShuffleBlockInfos,
              this::getPartitionAssignedServers,
              context.stageAttemptNumber());
      this.bufferManager = bufferManager;
    }
  }

  @VisibleForTesting
  protected List<ShuffleServerInfo> getPartitionAssignedServers(int partitionId) {
    return this.taskAttemptAssignment.retrieve(partitionId);
  }

  private boolean isMemoryShuffleEnabled(String storageType) {
    return StorageType.withMemory(StorageType.valueOf(storageType));
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) {
    try {
      writeImpl(records);
    } catch (Exception e) {
      taskFailureCallback.apply(taskId);
      if (enableWriteFailureRetry) {
        throwFetchFailedIfNecessary(e, Sets.newConcurrentHashSet());
      } else {
        throw e;
      }
    }
  }

  // Gluten needs this method.
  protected void writeImpl(Iterator<Product2<K, V>> records) {
    List<ShuffleBlockInfo> shuffleBlockInfos;
    boolean isCombine = shuffleDependency.mapSideCombine();

    Iterator<? extends Product2<K, ?>> iterator = records;
    if (isCombine) {
      if (RssSparkConfig.toRssConf(sparkConf).get(RSS_CLIENT_MAP_SIDE_COMBINE_ENABLED)) {
        iterator = shuffleDependency.aggregator().get().combineValuesByKey(records, taskContext);
      } else {
        Function1<V, C> combiner = shuffleDependency.aggregator().get().createCombiner();
        iterator =
            records.map(
                (Function1<Product2<K, V>, Product2<K, C>>)
                    x -> new Tuple2<>(x._1(), combiner.apply(x._2())));
      }
    }
    long recordCount = 0;
    while (iterator.hasNext()) {
      recordCount++;
      checkDataIfAnyFailure();
      Product2<K, ?> record = iterator.next();
      K key = record._1();
      int partition = getPartition(key);
      shuffleBlockInfos = bufferManager.addRecord(partition, record._1(), record._2());
      if (shuffleBlockInfos != null && !shuffleBlockInfos.isEmpty()) {
        processShuffleBlockInfos(shuffleBlockInfos);
      }
    }
    final long start = System.currentTimeMillis();
    shuffleBlockInfos = bufferManager.clear(1.0);
    if (shuffleBlockInfos != null && !shuffleBlockInfos.isEmpty()) {
      processShuffleBlockInfos(shuffleBlockInfos);
    }
    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    long checkStartTs = System.currentTimeMillis();
    checkAllBufferSpilled();
    checkSentRecordCount(recordCount);
    checkBlockSendResult(new HashSet<>(blockIds));
    checkSentBlockCount();
    long commitStartTs = System.currentTimeMillis();
    long checkDuration = commitStartTs - checkStartTs;
    if (!isMemoryShuffleEnabled) {
      sendCommit();
    }
    long writeDurationMs = bufferManager.getWriteTime() + (System.currentTimeMillis() - start);
    shuffleWriteMetrics.incWriteTime(TimeUnit.MILLISECONDS.toNanos(writeDurationMs));
    LOG.info(
        "Finish write shuffle for appId["
            + appId
            + "], shuffleId["
            + shuffleId
            + "], taskId["
            + taskId
            + "] with write "
            + writeDurationMs
            + " ms, include checkSendResult["
            + checkDuration
            + "], commit["
            + (System.currentTimeMillis() - commitStartTs)
            + "], "
            + bufferManager.getManagerCostInfo());
  }

  private void checkAllBufferSpilled() {
    if (bufferManager.getBuffers().size() > 0) {
      throw new RssSendFailedException(
          "Potential data loss due to existing remaining data buffers that are not flushed. This should not happen.");
    }
  }

  private void checkSentRecordCount(long recordCount) {
    if (recordCount != bufferManager.getRecordCount()) {
      String errorMsg =
          "Potential record loss may have occurred while preparing to send blocks for task["
              + taskId
              + "]";
      throw new RssSendFailedException(errorMsg);
    }
  }

  private void checkSentBlockCount() {
    long expected = blockIds.size();
    long bufferManagerTracked = bufferManager.getBlockCount();

    if (serverToPartitionToBlockIds == null) {
      throw new RssException("serverToPartitionToBlockIds should not be null");
    }

    // to filter the multiple replica's duplicate blockIds
    Set<Long> blockIds = new HashSet<>();
    for (Map<Integer, Set<Long>> partitionBlockIds : serverToPartitionToBlockIds.values()) {
      partitionBlockIds.values().forEach(x -> blockIds.addAll(x));
    }
    long serverTracked = blockIds.size();
    if (expected != serverTracked || expected != bufferManagerTracked) {
      throw new RssSendFailedException(
          "Potential block loss may occur for task["
              + taskId
              + "]. BlockId number expected: "
              + expected
              + ", serverTracked: "
              + serverTracked
              + ", bufferManagerTracked: "
              + bufferManagerTracked);
    }
  }

  // only push-based shuffle use this interface, but rss won't be used when push-based shuffle is
  // enabled.
  public long[] getPartitionLengths() {
    return new long[0];
  }

  @VisibleForTesting
  protected List<CompletableFuture<Long>> processShuffleBlockInfos(
      List<ShuffleBlockInfo> shuffleBlockInfoList) {
    if (shuffleBlockInfoList != null && !shuffleBlockInfoList.isEmpty()) {
      shuffleBlockInfoList.forEach(
          sbi -> {
            long blockId = sbi.getBlockId();
            // add blockId to set, check if it is sent later
            blockIds.add(blockId);
            // update [partition, blockIds], it will be sent to shuffle server
            int partitionId = sbi.getPartitionId();
            sbi.getShuffleServerInfos()
                .forEach(
                    shuffleServerInfo -> {
                      Map<Integer, Set<Long>> pToBlockIds =
                          serverToPartitionToBlockIds.computeIfAbsent(
                              shuffleServerInfo, k -> Maps.newHashMap());
                      pToBlockIds.computeIfAbsent(partitionId, v -> Sets.newHashSet()).add(blockId);
                    });
            partitionLengths[partitionId] += sbi.getLength();
          });
      return postBlockEvent(shuffleBlockInfoList);
    }
    return Collections.emptyList();
  }

  protected List<CompletableFuture<Long>> postBlockEvent(
      List<ShuffleBlockInfo> shuffleBlockInfoList) {
    List<CompletableFuture<Long>> futures = new ArrayList<>();
    for (AddBlockEvent event : bufferManager.buildBlockEvents(shuffleBlockInfoList)) {
      if (blockFailSentRetryEnabled) {
        // do nothing if failed.
        for (ShuffleBlockInfo block : event.getShuffleDataInfoList()) {
          block.withCompletionCallback(
              (completionBlock, isSuccessful) -> {
                if (isSuccessful) {
                  bufferManager.releaseBlockResource(completionBlock);
                }
              });
        }
      }
      event.addCallback(
          () -> {
            boolean ret = finishEventQueue.add(new Object());
            if (!ret) {
              LOG.error("Add event " + event + " to finishEventQueue fail");
            }
          });
      futures.add(shuffleManager.sendData(event));
    }
    return futures;
  }

  // Gluten needs this method
  protected void internalCheckBlockSendResult() {
    this.checkBlockSendResult(this.blockIds);
  }

  @VisibleForTesting
  protected void checkBlockSendResult(Set<Long> blockIds) {
    boolean interrupted = false;

    try {
      long remainingMs = sendCheckTimeout;
      long end = System.currentTimeMillis() + remainingMs;

      while (true) {
        try {
          finishEventQueue.clear();
          checkDataIfAnyFailure();
          Set<Long> successBlockIds = shuffleManager.getSuccessBlockIds(taskId);
          blockIds.removeAll(successBlockIds);
          if (blockIds.isEmpty()) {
            break;
          }
          if (finishEventQueue.isEmpty()) {
            remainingMs = Math.max(end - System.currentTimeMillis(), 0);
            Object event = finishEventQueue.poll(remainingMs, TimeUnit.MILLISECONDS);
            if (event == null) {
              break;
            }
          }
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
      if (!blockIds.isEmpty()) {
        String errorMsg =
            "Timeout: Task["
                + taskId
                + "] failed because "
                + blockIds.size()
                + " blocks can't be sent to shuffle server in "
                + sendCheckTimeout
                + " ms.";
        LOG.error(errorMsg);
        throw new RssWaitFailedException(errorMsg);
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void checkDataIfAnyFailure() {
    if (blockFailSentRetryEnabled) {
      collectFailedBlocksToResend();
    } else {
      String errorMsg = getFirstBlockFailure();
      if (errorMsg != null) {
        throw new RssSendFailedException("Fail to send the block. Error: " + errorMsg);
      }
    }
  }

  private String getFirstBlockFailure() {
    Set<Long> failedBlockIds = shuffleManager.getFailedBlockIds(taskId);
    if (!failedBlockIds.isEmpty()) {
      List<TrackingBlockStatus> trackingBlockStatues =
          shuffleManager
              .getBlockIdsFailedSendTracker(taskId)
              .getFailedBlockStatus(failedBlockIds.iterator().next());
      String errorMsg = DEFAULT_ERROR_MESSAGE;
      if (CollectionUtils.isNotEmpty(trackingBlockStatues)) {
        errorMsg = trackingBlockStatues.get(0).getStatusCode().name();
      }
      LOG.error(
          "Errors on sending blocks for task[{}]. {} blocks can't be sent to remote servers: {}",
          taskId,
          failedBlockIds.size(),
          shuffleManager.getBlockIdsFailedSendTracker(taskId).getFaultyShuffleServers());
      return errorMsg;
    }
    return null;
  }

  private void collectFailedBlocksToResend() {
    if (!blockFailSentRetryEnabled) {
      return;
    }

    FailedBlockSendTracker failedTracker = shuffleManager.getBlockIdsFailedSendTracker(taskId);
    if (failedTracker == null) {
      return;
    }

    reassignOnPartitionNeedSplit(failedTracker);

    Set<Long> failedBlockIds = failedTracker.getFailedBlockIds();
    if (CollectionUtils.isEmpty(failedBlockIds)) {
      return;
    }

    boolean isFastFail = false;
    Set<TrackingBlockStatus> resendCandidates = new HashSet<>();
    // to check whether the blocks resent exceed the max resend count.
    for (Long blockId : failedBlockIds) {
      List<TrackingBlockStatus> failedBlockStatus = failedTracker.getFailedBlockStatus(blockId);
      synchronized (failedBlockStatus) {
        int retryIndex =
            failedBlockStatus.stream()
                .map(x -> x.getShuffleBlockInfo().getRetryCnt())
                .max(Comparator.comparing(Integer::valueOf))
                .get();
        if (retryIndex >= blockFailSentRetryMaxTimes) {
          LOG.error(
              "Partial blocks for taskId: [{}] retry exceeding the max retry times: [{}]. Fast fail! faulty server list: {}",
              taskId,
              blockFailSentRetryMaxTimes,
              failedBlockStatus.stream()
                  .map(x -> x.getShuffleServerInfo())
                  .collect(Collectors.toSet()));
          isFastFail = true;
          break;
        }

        for (TrackingBlockStatus status : failedBlockStatus) {
          StatusCode code = status.getStatusCode();
          if (STATUS_CODE_WITHOUT_BLOCK_RESEND.contains(code)) {
            LOG.error(
                "Partial blocks for taskId: [{}] failed on the illegal status code: [{}] without resend on server: {}",
                taskId,
                code,
                status.getShuffleServerInfo());
            isFastFail = true;
            break;
          }
        }

        // todo: if setting multi replica and another replica is succeed to send, no need to resend
        resendCandidates.addAll(failedBlockStatus);
      }
    }

    if (isFastFail) {
      // release data and allocated memory
      for (Long blockId : failedBlockIds) {
        List<TrackingBlockStatus> failedBlockStatus = failedTracker.getFailedBlockStatus(blockId);
        if (CollectionUtils.isNotEmpty(failedBlockStatus)) {
          TrackingBlockStatus blockStatus = failedBlockStatus.get(0);
          blockStatus.getShuffleBlockInfo().executeCompletionCallback(true);
        }
      }

      throw new RssSendFailedException(
          "Errors on resending the blocks data to the remote shuffle-server.");
    }

    reassignAndResendBlocks(resendCandidates);
  }

  private void reassignOnPartitionNeedSplit(FailedBlockSendTracker failedTracker) {
    Map<Integer, List<ReceivingFailureServer>> failurePartitionToServers = new HashMap<>();

    failedTracker
        .removeAllTrackedPartitions()
        .forEach(
            partitionStatus ->
                failurePartitionToServers
                    .computeIfAbsent(partitionStatus.getPartitionId(), x -> new ArrayList<>())
                    .add(
                        new ReceivingFailureServer(
                            partitionStatus.getShuffleServerInfo().getId(), StatusCode.SUCCESS)));
    if (!failurePartitionToServers.isEmpty()) {
      doReassignOnBlockSendFailure(failurePartitionToServers, true);
    }
  }

  private void doReassignOnBlockSendFailure(
      Map<Integer, List<ReceivingFailureServer>> failurePartitionToServers,
      boolean partitionSplit) {
    LOG.info(
        "Initiate reassignOnBlockSendFailure. failure partition servers: {}",
        failurePartitionToServers);
    String executorId = SparkEnv.get().executorId();
    long taskAttemptId = taskContext.taskAttemptId();
    int stageId = taskContext.stageId();
    int stageAttemptNum = taskContext.stageAttemptNumber();
    try {
      RssReassignOnBlockSendFailureRequest request =
          new RssReassignOnBlockSendFailureRequest(
              shuffleId,
              failurePartitionToServers,
              executorId,
              taskAttemptId,
              stageId,
              stageAttemptNum,
              partitionSplit);
      RssReassignOnBlockSendFailureResponse response =
          managerClientSupplier.get().reassignOnBlockSendFailure(request);
      if (response.getStatusCode() != StatusCode.SUCCESS) {
        String msg =
            String.format(
                "Reassign failed. statusCode: %s, msg: %s",
                response.getStatusCode(), response.getMessage());
        throw new RssException(msg);
      }
      MutableShuffleHandleInfo handle = MutableShuffleHandleInfo.fromProto(response.getHandle());
      taskAttemptAssignment.update(handle);
      LOG.info(
          "Success to reassign. The latest available assignment is {}",
          handle.getAvailablePartitionServersForWriter());
    } catch (Exception e) {
      throw new RssException(
          "Errors on reassign on block send failure. failure partition->servers : "
              + failurePartitionToServers,
          e);
    }
  }

  private void reassignAndResendBlocks(Set<TrackingBlockStatus> blocks) {
    List<ShuffleBlockInfo> resendCandidates = Lists.newArrayList();
    Map<Integer, List<TrackingBlockStatus>> partitionedFailedBlocks =
        blocks.stream()
            .collect(Collectors.groupingBy(d -> d.getShuffleBlockInfo().getPartitionId()));

    Map<Integer, List<ReceivingFailureServer>> failurePartitionToServers = new HashMap<>();
    for (Map.Entry<Integer, List<TrackingBlockStatus>> entry : partitionedFailedBlocks.entrySet()) {
      int partitionId = entry.getKey();
      List<TrackingBlockStatus> partitionBlocks = entry.getValue();
      Map<ShuffleServerInfo, TrackingBlockStatus> serverBlocks =
          partitionBlocks.stream()
              .collect(Collectors.groupingBy(d -> d.getShuffleServerInfo()))
              .entrySet()
              .stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey, x -> x.getValue().stream().findFirst().get()));
      for (Map.Entry<ShuffleServerInfo, TrackingBlockStatus> blockStatusEntry :
          serverBlocks.entrySet()) {
        String serverId = blockStatusEntry.getKey().getId();
        // avoid duplicate reassign for the same failure server.
        // todo: getting the replacement should support multi replica.
        List<ShuffleServerInfo> servers = getPartitionAssignedServers(partitionId);
        // Gets the first replica for this partition for now.
        // It can not work if we want to use multiple replicas.
        ShuffleServerInfo replacement = servers.get(0);
        String latestServerId = replacement.getId();
        if (!serverId.equals(latestServerId)) {
          continue;
        }
        StatusCode code = blockStatusEntry.getValue().getStatusCode();
        failurePartitionToServers
            .computeIfAbsent(partitionId, x -> new ArrayList<>())
            .add(new ReceivingFailureServer(serverId, code));
      }
    }

    if (!failurePartitionToServers.isEmpty()) {
      doReassignOnBlockSendFailure(failurePartitionToServers, false);
    }

    for (TrackingBlockStatus blockStatus : blocks) {
      ShuffleBlockInfo block = blockStatus.getShuffleBlockInfo();
      // todo: getting the replacement should support multi replica.
      List<ShuffleServerInfo> servers = getPartitionAssignedServers(block.getPartitionId());
      // Gets the first replica for this partition for now.
      // It can not work if we want to use multiple replicas.
      ShuffleServerInfo replacement = servers.get(0);
      if (blockStatus.getShuffleServerInfo().getId().equals(replacement.getId())) {
        throw new RssException(
            "No available replacement server for: " + blockStatus.getShuffleServerInfo().getId());
      }
      // clear the previous retry state of block
      clearFailedBlockState(block);
      final ShuffleBlockInfo newBlock = block;
      newBlock.incrRetryCnt();
      newBlock.reassignShuffleServers(Arrays.asList(replacement));
      resendCandidates.add(newBlock);
    }

    processShuffleBlockInfos(resendCandidates);
    LOG.info(
        "Failed blocks have been resent to data pusher queue since reassignment has been finished successfully");
  }

  private void clearFailedBlockState(ShuffleBlockInfo block) {
    shuffleManager.getBlockIdsFailedSendTracker(taskId).remove(block.getBlockId());
    block.getShuffleServerInfos().stream()
        .forEach(
            s ->
                serverToPartitionToBlockIds
                    .get(s)
                    .get(block.getPartitionId())
                    .remove(block.getBlockId()));
    partitionLengths[block.getPartitionId()] -= block.getLength();
    blockIds.remove(block.getBlockId());
  }

  @VisibleForTesting
  protected void sendCommit() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Boolean> future =
        executor.submit(
            () -> shuffleWriteClient.sendCommit(shuffleServersForData, appId, shuffleId, numMaps));
    int maxWait = 5000;
    int currentWait = 200;
    long start = System.currentTimeMillis();
    while (!future.isDone()) {
      LOG.info(
          "Wait commit to shuffle server for task["
              + taskAttemptId
              + "] cost "
              + (System.currentTimeMillis() - start)
              + " ms");
      Uninterruptibles.sleepUninterruptibly(currentWait, TimeUnit.MILLISECONDS);
      currentWait = Math.min(currentWait * 2, maxWait);
    }
    try {
      if (!future.get()) {
        throw new RssException("Failed to commit task to shuffle server");
      }
    } catch (InterruptedException ie) {
      LOG.warn("Ignore the InterruptedException which should be caused by internal killed");
    } catch (Exception e) {
      throw new RssException("Exception happened when get commit status", e);
    } finally {
      executor.shutdown();
    }
  }

  @VisibleForTesting
  protected <T> int getPartition(T key) {
    int result = 0;
    if (shouldPartition) {
      result = partitioner.getPartition(key);
    }
    return result;
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      if (success) {
        long start = System.currentTimeMillis();
        shuffleWriteClient.reportShuffleResult(
            serverToPartitionToBlockIds,
            appId,
            shuffleId,
            taskAttemptId,
            bitmapSplitNum,
            recordReportFailedShuffleservers,
            enableWriteFailureRetry);
        long reportDuration = System.currentTimeMillis() - start;
        LOG.info(
            "Reported all shuffle result for shuffleId[{}] task[{}] with bitmapNum[{}] cost {} ms",
            shuffleId,
            taskAttemptId,
            bitmapSplitNum,
            reportDuration);
        shuffleWriteMetrics.incWriteTime(TimeUnit.MILLISECONDS.toNanos(reportDuration));
        // todo: we can replace the dummy host and port with the real shuffle server which we prefer
        // to read
        final BlockManagerId blockManagerId =
            BlockManagerId.apply(
                appId + "_" + taskId,
                DUMMY_HOST,
                DUMMY_PORT,
                Option.apply(Long.toString(taskAttemptId)));
        MapStatus mapStatus = MapStatus.apply(blockManagerId, partitionLengths, taskAttemptId);
        return Option.apply(mapStatus);
      } else {
        return Option.empty();
      }
    } catch (Exception e) {
      // If an exception is thrown during the reporting process, it should be judged as a failure
      // and Stage retry should be triggered.
      if (enableWriteFailureRetry) {
        throw throwFetchFailedIfNecessary(e, recordReportFailedShuffleservers);
      } else {
        throw e;
      }
    } finally {
      if (blockFailSentRetryEnabled) {
        if (success) {
          if (CollectionUtils.isNotEmpty(shuffleManager.getFailedBlockIds(taskId))) {
            LOG.error(
                "Errors on stopping writer due to the remaining failed blockIds. This should not happen.");
            return Option.empty();
          }
        } else {
          shuffleManager.getBlockIdsFailedSendTracker(taskId).clearAndReleaseBlockResources();
        }
      }
      // free all memory & metadata, or memory leak happen in executor
      if (bufferManager != null) {
        bufferManager.freeAllMemory();
      }
      if (shuffleManager != null) {
        shuffleManager.clearTaskMeta(taskId);
      }
    }
  }

  @VisibleForTesting
  Map<Integer, Set<Long>> getPartitionToBlockIds() {
    return serverToPartitionToBlockIds.values().stream()
        .flatMap(s -> s.entrySet().stream())
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (existingSet, newSet) -> {
                  Set<Long> mergedSet = new HashSet<>(existingSet);
                  mergedSet.addAll(newSet);
                  return mergedSet;
                }));
  }

  @VisibleForTesting
  public WriteBufferManager getBufferManager() {
    return bufferManager;
  }

  private RssException throwFetchFailedIfNecessary(
      Exception e, Set<ShuffleServerInfo> reportFailuredServers) {
    // The shuffleServer is registered only when a Block fails to be sent
    if (e instanceof RssSendFailedException) {
      FailedBlockSendTracker blockIdsFailedSendTracker =
          shuffleManager.getBlockIdsFailedSendTracker(taskId);
      List<ShuffleServerInfo> shuffleServerInfos =
          Lists.newArrayList(blockIdsFailedSendTracker.getFaultyShuffleServers());
      shuffleServerInfos.addAll(reportFailuredServers);
      RssReportShuffleWriteFailureRequest req =
          new RssReportShuffleWriteFailureRequest(
              appId,
              shuffleId,
              taskContext.stageId(),
              taskContext.stageAttemptNumber(),
              shuffleServerInfos,
              e.getMessage());
      RssReportShuffleWriteFailureResponse response =
          managerClientSupplier.get().reportShuffleWriteFailure(req);
      if (response.getReSubmitWholeStage()) {
        LOG.warn(response.getMessage());
        FetchFailedException ffe =
            RssSparkShuffleUtils.createFetchFailedException(
                shuffleId, -1, taskContext.stageAttemptNumber(), e);
        throw new RssException(ffe);
      }
    }
    throw new RssException(e);
  }

  @VisibleForTesting
  protected void enableBlockFailSentRetry() {
    this.blockFailSentRetryEnabled = true;
  }

  @VisibleForTesting
  protected void setBlockFailSentRetryMaxTimes(int blockFailSentRetryMaxTimes) {
    this.blockFailSentRetryMaxTimes = blockFailSentRetryMaxTimes;
  }

  @VisibleForTesting
  protected void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  @VisibleForTesting
  protected Map<ShuffleServerInfo, Map<Integer, Set<Long>>> getServerToPartitionToBlockIds() {
    return serverToPartitionToBlockIds;
  }

  @VisibleForTesting
  protected RssShuffleManager getShuffleManager() {
    return shuffleManager;
  }

  public TaskAttemptAssignment getTaskAttemptAssignment() {
    return taskAttemptAssignment;
  }
}
