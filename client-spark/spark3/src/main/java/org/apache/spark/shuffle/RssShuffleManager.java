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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Iterator;
import scala.collection.Seq;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.MapOutputTracker;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.rdd.DeterministicLevel;
import org.apache.spark.shuffle.events.ShuffleAssignmentInfoEvent;
import org.apache.spark.shuffle.handle.MutableShuffleHandleInfo;
import org.apache.spark.shuffle.handle.ShuffleHandleInfo;
import org.apache.spark.shuffle.handle.SimpleShuffleHandleInfo;
import org.apache.spark.shuffle.handle.StageAttemptShuffleHandleInfo;
import org.apache.spark.shuffle.reader.RssShuffleReader;
import org.apache.spark.shuffle.writer.DataPusher;
import org.apache.spark.shuffle.writer.RssShuffleWriter;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.client.api.ShuffleResult;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.impl.FailedBlockSendTracker;
import org.apache.uniffle.client.impl.MergedPartitionStats;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.exception.RssFetchFailedException;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.shuffle.RssShuffleClientFactory;
import org.apache.uniffle.shuffle.ShuffleWriteTaskStats;
import org.apache.uniffle.shuffle.manager.RssShuffleManagerBase;

public class RssShuffleManager extends RssShuffleManagerBase {
  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleManager.class);

  public RssShuffleManager(SparkConf conf, boolean isDriver) {
    super(conf, isDriver);
    this.dataDistributionType = getDataDistributionType(sparkConf);
    if (isIntegrityValidationEnabled(rssConf)) {
      LOG.info("shuffle integrity validation has been enabled.");
    }
  }

  // For testing only
  @VisibleForTesting
  RssShuffleManager(
      SparkConf conf,
      boolean isDriver,
      DataPusher dataPusher,
      Map<String, Set<Long>> taskToSuccessBlockIds,
      Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker) {
    super(conf, isDriver, dataPusher, taskToSuccessBlockIds, taskToFailedBlockSendTracker);
  }

  // This method is called in Spark driver side,
  // and Spark driver will make some decision according to coordinator,
  // e.g. determining what RSS servers to use.
  // Then Spark driver will return a ShuffleHandle and
  // pass that ShuffleHandle to executors (getWriter/getReader)
  @Override
  public <K, V, C> ShuffleHandle registerShuffle(
      int shuffleId, ShuffleDependency<K, V, C> dependency) {

    // fail fast if number of partitions is not supported by block id layout
    if (dependency.partitioner().numPartitions() > blockIdLayout.maxNumPartitions) {
      throw new RssException(
          "Cannot register shuffle with "
              + dependency.partitioner().numPartitions()
              + " partitions because the configured block id layout supports at most "
              + blockIdLayout.maxNumPartitions
              + " partitions.");
    }

    // Spark have three kinds of serializer:
    // org.apache.spark.serializer.JavaSerializer
    // org.apache.spark.sql.execution.UnsafeRowSerializer
    // org.apache.spark.serializer.KryoSerializer,
    // Only org.apache.spark.serializer.JavaSerializer don't support RelocationOfSerializedObjects.
    // So when we find the parameters to use org.apache.spark.serializer.JavaSerializer, We should
    // throw an exception
    if (!SparkEnv.get().serializer().supportsRelocationOfSerializedObjects()) {
      throw new IllegalArgumentException(
          "Can't use serialized shuffle for shuffleId: "
              + shuffleId
              + ", because the"
              + " serializer: "
              + SparkEnv.get().serializer().getClass().getName()
              + " does not support object "
              + "relocation.");
    }

    if (id.get() == null) {
      id.compareAndSet(null, SparkEnv.get().conf().getAppId() + "_" + uuid);
      appId = id.get();
      if (dataPusher != null) {
        dataPusher.setRssAppId(id.get());
      }
    }
    LOG.info("Generate application id used in rss: " + id.get());
    // If stage retry is enabled, the Deterministic status of the ShuffleId needs to be recorded.
    if (rssStageRetryEnabled) {
      shuffleIdMappingManager.recordShuffleIdDeterminate(
          shuffleId,
          dependency.rdd().getOutputDeterministicLevel() != DeterministicLevel.INDETERMINATE());
    }

    if (dependency.partitioner().numPartitions() == 0) {
      shuffleIdToPartitionNum.putIfAbsent(shuffleId, 0);
      shuffleIdToNumMapTasks.computeIfAbsent(
          shuffleId, key -> dependency.rdd().partitions().length);
      LOG.info(
          "RegisterShuffle with ShuffleId[{}], partitionNum is 0, "
              + "return the empty RssShuffleHandle directly",
          shuffleId);
      Broadcast<SimpleShuffleHandleInfo> hdlInfoBd =
          RssSparkShuffleUtils.broadcastShuffleHdlInfo(
              RssSparkShuffleUtils.getActiveSparkContext(),
              shuffleId,
              Collections.emptyMap(),
              RemoteStorageInfo.EMPTY_REMOTE_STORAGE);
      return new RssShuffleHandle<>(
          shuffleId, id.get(), dependency.rdd().getNumPartitions(), dependency, hdlInfoBd);
    }

    String storageType = sparkConf.get(RssSparkConfig.RSS_STORAGE_TYPE.key());
    RemoteStorageInfo defaultRemoteStorage = getDefaultRemoteStorageInfo(sparkConf);
    RemoteStorageInfo remoteStorage =
        ClientUtils.fetchRemoteStorage(
            id.get(), defaultRemoteStorage, dynamicConfEnabled, storageType, shuffleWriteClient);

    Set<String> assignmentTags = RssSparkShuffleUtils.getAssignmentTags(sparkConf);
    ClientUtils.validateClientType(clientType);
    assignmentTags.add(clientType);

    int requiredShuffleServerNumber =
        RssSparkShuffleUtils.getRequiredShuffleServerNumber(sparkConf);
    int estimateTaskConcurrency = RssSparkShuffleUtils.estimateTaskConcurrency(sparkConf);

    Map<Integer, List<ShuffleServerInfo>> partitionToServers =
        requestShuffleAssignment(
            shuffleId,
            dependency.partitioner().numPartitions(),
            1,
            requiredShuffleServerNumber,
            estimateTaskConcurrency,
            rssStageResubmitManager.getServerIdBlackList());
    startHeartbeat();
    shuffleIdToPartitionNum.computeIfAbsent(
        shuffleId, key -> dependency.partitioner().numPartitions());
    shuffleIdToNumMapTasks.computeIfAbsent(shuffleId, key -> dependency.rdd().partitions().length);
    if (shuffleManagerRpcServiceEnabled && rssStageRetryForWriteFailureEnabled) {
      ShuffleHandleInfo shuffleHandleInfo =
          new MutableShuffleHandleInfo(
              shuffleId, partitionToServers, remoteStorage, partitionSplitMode);
      StageAttemptShuffleHandleInfo handleInfo =
          new StageAttemptShuffleHandleInfo(shuffleId, remoteStorage, shuffleHandleInfo);
      shuffleHandleInfoManager.register(shuffleId, handleInfo);
    } else if (shuffleManagerRpcServiceEnabled && partitionReassignEnabled) {
      ShuffleHandleInfo shuffleHandleInfo =
          new MutableShuffleHandleInfo(
              shuffleId, partitionToServers, remoteStorage, partitionSplitMode);
      shuffleHandleInfoManager.register(shuffleId, shuffleHandleInfo);
    }
    Broadcast<SimpleShuffleHandleInfo> hdlInfoBd =
        RssSparkShuffleUtils.broadcastShuffleHdlInfo(
            RssSparkShuffleUtils.getActiveSparkContext(),
            shuffleId,
            partitionToServers,
            remoteStorage);
    LOG.info(
        "RegisterShuffle with ShuffleId[{}], uniffleShuffleId[{}], partitionNum[{}], shuffleServerForResult: {}",
        shuffleId,
        shuffleId,
        partitionToServers.size(),
        partitionToServers);

    // Post assignment event
    RssSparkShuffleUtils.getActiveSparkContext()
        .listenerBus()
        .post(
            new ShuffleAssignmentInfoEvent(
                shuffleId,
                new ArrayList<>(
                    partitionToServers.values().stream()
                        .flatMap(x -> x.stream())
                        .map(x -> x.getId())
                        .collect(Collectors.toSet()))));

    return new RssShuffleHandle<>(
        shuffleId, id.get(), dependency.rdd().getNumPartitions(), dependency, hdlInfoBd);
  }

  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(
      ShuffleHandle handle, long mapId, TaskContext context, ShuffleWriteMetricsReporter metrics) {
    if (!(handle instanceof RssShuffleHandle)) {
      throw new RssException("Unexpected ShuffleHandle:" + handle.getClass().getName());
    }
    RssShuffleHandle<K, V, ?> rssHandle = (RssShuffleHandle<K, V, ?>) handle;
    setPusherAppId(rssHandle);
    int shuffleId = rssHandle.getShuffleId();
    ShuffleWriteMetrics writeMetrics;
    if (metrics != null) {
      writeMetrics = new WriteMetrics(metrics);
    } else {
      writeMetrics = context.taskMetrics().shuffleWriteMetrics();
    }
    String taskId = "" + context.taskAttemptId() + "_" + context.attemptNumber();
    return new RssShuffleWriter<>(
        rssHandle.getAppId(),
        shuffleId,
        taskId,
        getTaskAttemptIdForBlockId(context.partitionId(), context.attemptNumber()),
        writeMetrics,
        this,
        sparkConf,
        shuffleWriteClient,
        managerClientSupplier,
        rssHandle,
        this::markFailedTask,
        context);
  }

  public void setPusherAppId(RssShuffleHandle rssShuffleHandle) {
    // todo: this implement is tricky, we should refactor it
    if (id.get() == null) {
      id.compareAndSet(null, rssShuffleHandle.getAppId());
      if (dataPusher != null) {
        dataPusher.setRssAppId(id.get());
      }
    }
  }

  @Override
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    return getReader(handle, 0, Integer.MAX_VALUE, startPartition, endPartition, context, metrics);
  }

  // The interface is used for compatibility with spark 3.0.1
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    long start = System.currentTimeMillis();
    Pair<Roaring64NavigableMap, Long> info =
        getExpectedTasksAndRecordsForReader(
            handle.shuffleId(), startPartition, endPartition, startMapIndex, endMapIndex);
    Roaring64NavigableMap taskIdBitmap = info.getLeft();
    long expectedRecordsRead = info.getRight();
    return getReaderImpl(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics,
        taskIdBitmap,
        expectedRecordsRead,
        System.currentTimeMillis() - start);
  }

  // The interface is used for compatibility with spark 3.0.1
  public <K, C> ShuffleReader<K, C> getReaderForRange(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    long start = System.currentTimeMillis();
    Roaring64NavigableMap taskIdBitmap =
        getExpectedTasksByRange(
            handle.shuffleId(), startPartition, endPartition, startMapIndex, endMapIndex);
    LOG.info(
        "Get taskId cost "
            + (System.currentTimeMillis() - start)
            + " ms, and request expected blockIds from "
            + taskIdBitmap.getLongCardinality()
            + " tasks for shuffleId["
            + handle.shuffleId()
            + "], partitionId["
            + startPartition
            + ", "
            + endPartition
            + "]");
    return getReaderImpl(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics,
        taskIdBitmap,
        -1,
        System.currentTimeMillis() - start);
  }

  public <K, C> ShuffleReader<K, C> getReaderImpl(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics,
      Roaring64NavigableMap taskIdBitmap,
      long expectedRecordsRead,
      long taskIdRetrievedMillis) {
    if (!(handle instanceof RssShuffleHandle)) {
      throw new RssException("Unexpected ShuffleHandle:" + handle.getClass().getName());
    }
    RssShuffleHandle<K, ?, C> rssShuffleHandle = (RssShuffleHandle<K, ?, C>) handle;
    final int partitionNum = rssShuffleHandle.getDependency().partitioner().numPartitions();
    int shuffleId = rssShuffleHandle.getShuffleId();

    ShuffleHandleInfo shuffleHandleInfo;
    Supplier<ShuffleHandleInfo> func =
        () ->
            getShuffleHandleInfo(
                context.stageId(), context.stageAttemptNumber(), rssShuffleHandle, false);
    if (readShuffleHandleCacheEnabled) {
      shuffleHandleInfo = super.getOrFetchShuffleHandle(shuffleId, func);
    } else {
      shuffleHandleInfo = func.get();
    }

    Map<ShuffleServerInfo, Set<Integer>> serverToPartitions =
        getPartitionDataServers(shuffleHandleInfo, startPartition, endPartition);
    long start = System.currentTimeMillis();
    ShuffleResult shuffleResult =
        getShuffleResultForMultiPart(
            clientType,
            serverToPartitions,
            rssShuffleHandle.getAppId(),
            shuffleId,
            context.stageAttemptNumber(),
            shuffleHandleInfo.createPartitionReplicaTracking());
    Roaring64NavigableMap blockIdBitmap = shuffleResult.getBlockIds();

    if (isIntegrityValidationServerManagementEnabled(rssConf)) {
      MergedPartitionStats mergedPartitionStats = shuffleResult.getMergedPartitionStats();
      if (mergedPartitionStats != null) {
        long records = mergedPartitionStats.getExpectedRecordNumberByTaskIds(taskIdBitmap);
        if (records > 0) {
          expectedRecordsRead = records;
        }
      }
    }

    LOG.info(
        "Retrieved {} upstream task ids in {} ms and {} block IDs from {} shuffle-servers in {} ms for shuffleId[{}], partitionId[{},{}]",
        taskIdBitmap.getLongCardinality(),
        taskIdRetrievedMillis,
        blockIdBitmap.getLongCardinality(),
        serverToPartitions.size(),
        System.currentTimeMillis() - start,
        handle.shuffleId(),
        startPartition,
        endPartition);

    ShuffleReadMetrics readMetrics;
    if (metrics != null) {
      readMetrics = new ReadMetrics(metrics);
    } else {
      readMetrics = context.taskMetrics().shuffleReadMetrics();
    }

    final RemoteStorageInfo shuffleRemoteStorageInfo = rssShuffleHandle.getRemoteStorage();
    LOG.debug("Shuffle reader using remote storage {}", shuffleRemoteStorageInfo);
    final String shuffleRemoteStoragePath = shuffleRemoteStorageInfo.getPath();
    Configuration readerHadoopConf =
        RssSparkShuffleUtils.getRemoteStorageHadoopConf(sparkConf, shuffleRemoteStorageInfo);

    return new RssShuffleReader<K, C>(
        startPartition,
        endPartition,
        startMapIndex,
        endMapIndex,
        context,
        rssShuffleHandle,
        shuffleRemoteStoragePath,
        readerHadoopConf,
        partitionNum,
        RssUtils.generatePartitionToBitmap(
            blockIdBitmap, startPartition, endPartition, blockIdLayout),
        taskIdBitmap,
        readMetrics,
        managerClientSupplier,
        RssSparkConfig.toRssConf(sparkConf),
        dataDistributionType,
        shuffleHandleInfo.getAllPartitionServersForReader(),
        expectedRecordsRead);
  }

  private Map<ShuffleServerInfo, Set<Integer>> getPartitionDataServers(
      ShuffleHandleInfo shuffleHandleInfo, int startPartition, int endPartition) {
    Map<Integer, List<ShuffleServerInfo>> allPartitionToServers =
        shuffleHandleInfo.getAllPartitionServersForReader();
    Map<Integer, List<ShuffleServerInfo>> requirePartitionToServers =
        allPartitionToServers.entrySet().stream()
            .filter(x -> x.getKey() >= startPartition && x.getKey() < endPartition)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Map<ShuffleServerInfo, Set<Integer>> serverToPartitions =
        RssUtils.generateServerToPartitions(requirePartitionToServers);
    return serverToPartitions;
  }

  public static boolean isIntegrityValidationEnabled(RssConf rssConf) {
    assert rssConf != null;
    // disable integrity validation when the multi replicas is enabled.
    if (rssConf.getInteger(
            RssClientConfig.RSS_DATA_REPLICA, RssClientConfig.RSS_DATA_REPLICA_DEFAULT_VALUE)
        > 1) {
      return false;
    }
    // only enable integrity validation when the spark version >= 3.5.0
    if (!Spark3VersionUtils.isSparkVersionAtLeast("3.5.0")) {
      return false;
    }
    return rssConf.get(RssSparkConfig.RSS_CLIENT_INTEGRITY_VALIDATION_ENABLED);
  }

  public static boolean isIntegrityValidationServerManagementEnabled(RssConf rssConf) {
    if (!isIntegrityValidationEnabled(rssConf)) {
      return false;
    }
    return rssConf.get(RssSparkConfig.RSS_DATA_INTEGRITY_VALIDATION_SERVER_MANAGEMENT_ENABLED);
  }

  public static boolean isIntegrityValidationClientManagementEnabled(RssConf rssConf) {
    if (!isIntegrityValidationEnabled(rssConf)) {
      return false;
    }
    return !isIntegrityValidationServerManagementEnabled(rssConf);
  }

  public static boolean isIntegrationValidationFailureAnalysisEnabled(RssConf rssConf) {
    // todo: enable the validation failure analysis when the server management is enabled
    if (isIntegrityValidationServerManagementEnabled(rssConf)) {
      return false;
    }
    return rssConf.get(RssSparkConfig.RSS_DATA_INTEGRATION_VALIDATION_ANALYSIS_ENABLED);
  }

  @SuppressFBWarnings("REC_CATCH_EXCEPTION")
  private static Iterator<BlockManagerId> getUpstreamBlockManagerIdsForShuffleReader(
      int shuffleId, int startPartition, int endPartition, int startMapIndex, int endMapIndex) {
    Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>> mapStatusIter = null;
    try {
      // Since Spark 3.1 refactors the interface of getMapSizesByExecutorId,
      // we use reflection and catch for the compatibility with 3.0 & 3.1 & 3.2
      if (Spark3VersionUtils.MAJOR_VERSION > 3
          || Spark3VersionUtils.MINOR_VERSION > 2
          || Spark3VersionUtils.MINOR_VERSION == 2 && !Spark3VersionUtils.isSpark320()
          || Spark3VersionUtils.MINOR_VERSION == 1) {
        // use Spark 3.1's API
        mapStatusIter =
            (Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>>)
                SparkEnv.get()
                    .mapOutputTracker()
                    .getClass()
                    .getDeclaredMethod(
                        "getMapSizesByExecutorId",
                        int.class,
                        int.class,
                        int.class,
                        int.class,
                        int.class)
                    .invoke(
                        SparkEnv.get().mapOutputTracker(),
                        shuffleId,
                        startMapIndex,
                        endMapIndex,
                        startPartition,
                        endPartition);
      } else if (Spark3VersionUtils.isSpark320()) {
        // use Spark 3.2.0's API
        // Each Spark release will be versioned: [MAJOR].[FEATURE].[MAINTENANCE].
        // Usually we only need to adapt [MAJOR].[FEATURE] . Unfortunately,
        // some interfaces were removed wrongly in Spark 3.2.0. And they were added by Spark
        // 3.2.1.
        // So we need to adapt Spark 3.2.0 here
        mapStatusIter =
            (Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>>)
                MapOutputTracker.class
                    .getDeclaredMethod(
                        "getMapSizesByExecutorId",
                        int.class,
                        int.class,
                        int.class,
                        int.class,
                        int.class)
                    .invoke(
                        SparkEnv.get().mapOutputTracker(),
                        shuffleId,
                        startMapIndex,
                        endMapIndex,
                        startPartition,
                        endPartition);
      } else {
        // use Spark 3.0's API
        mapStatusIter =
            (Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>>)
                SparkEnv.get()
                    .mapOutputTracker()
                    .getClass()
                    .getDeclaredMethod("getMapSizesByExecutorId", int.class, int.class, int.class)
                    .invoke(
                        SparkEnv.get().mapOutputTracker(), shuffleId, startPartition, endPartition);
      }
    } catch (Exception e) {
      throw new RssException(e);
    }
    final Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>> immutableIter =
        mapStatusIter;
    Iterator<BlockManagerId> iter =
        new Iterator<BlockManagerId>() {
          @Override
          public boolean hasNext() {
            return immutableIter.hasNext();
          }

          @Override
          public BlockManagerId next() {
            return immutableIter.next()._1();
          }
        };
    return iter;
  }

  public static Map<Long, ShuffleWriteTaskStats> getUpstreamWriteTaskStats(
      RssConf rssConf,
      int shuffleId,
      int startPartition,
      int endPartition,
      int startMapIndex,
      int endMapIndex) {
    if (isIntegrityValidationServerManagementEnabled(rssConf)) {
      return Collections.emptyMap();
    }
    Iterator<BlockManagerId> iter =
        getUpstreamBlockManagerIdsForShuffleReader(
            shuffleId, startPartition, endPartition, startMapIndex, endMapIndex);
    Map<Long, ShuffleWriteTaskStats> upstreamStats = new HashMap<>();
    while (iter.hasNext()) {
      BlockManagerId blockManagerId = iter.next();
      ShuffleWriteTaskStats shuffleWriteTaskStats =
          ShuffleWriteTaskStats.decode(rssConf, blockManagerId.topologyInfo().get());
      upstreamStats.put(shuffleWriteTaskStats.getTaskAttemptId(), shuffleWriteTaskStats);
    }
    return upstreamStats;
  }

  private Pair<Roaring64NavigableMap, Long> getExpectedTasksAndRecordsForReader(
      int shuffleId, int startPartition, int endPartition, int startMapIndex, int endMapIndex) {
    Iterator<BlockManagerId> iter =
        getUpstreamBlockManagerIdsForShuffleReader(
            shuffleId, startPartition, endPartition, startMapIndex, endMapIndex);
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf();
    long expectedRecordsRead = 0;
    while (iter.hasNext()) {
      BlockManagerId blockManagerId = iter.next();
      if (!blockManagerId.topologyInfo().isDefined()) {
        throw new RssException("Can't get expected taskAttemptId");
      }

      String raw = blockManagerId.topologyInfo().get();
      if (isIntegrityValidationClientManagementEnabled(rssConf)) {
        ShuffleWriteTaskStats shuffleWriteTaskStats = ShuffleWriteTaskStats.decode(rssConf, raw);
        taskIdBitmap.add(shuffleWriteTaskStats.getTaskAttemptId());
        for (int i = startPartition; i < endPartition; i++) {
          expectedRecordsRead += shuffleWriteTaskStats.getRecordsWritten(i);
        }
      } else {
        taskIdBitmap.add(Long.parseLong(raw));
      }
    }
    return Pair.of(taskIdBitmap, expectedRecordsRead);
  }

  // This API is only used by Spark3.0 and removed since 3.1,
  // so we extract it from getExpectedTasksByExecutorId.
  private Roaring64NavigableMap getExpectedTasksByRange(
      int shuffleId, int startPartition, int endPartition, int startMapIndex, int endMapIndex) {
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf();
    Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>> mapStatusIter = null;
    try {
      mapStatusIter =
          (Iterator<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>>)
              SparkEnv.get()
                  .mapOutputTracker()
                  .getClass()
                  .getDeclaredMethod(
                      "getMapSizesByRange", int.class, int.class, int.class, int.class, int.class)
                  .invoke(
                      SparkEnv.get().mapOutputTracker(),
                      shuffleId,
                      startMapIndex,
                      endMapIndex,
                      startPartition,
                      endPartition);
    } catch (Exception e) {
      throw new RssException(e);
    }
    while (mapStatusIter.hasNext()) {
      Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>> tuple2 = mapStatusIter.next();
      if (!tuple2._1().topologyInfo().isDefined()) {
        throw new RssException("Can't get expected taskAttemptId");
      }
      taskIdBitmap.add(Long.parseLong(tuple2._1().topologyInfo().get()));
    }
    return taskIdBitmap;
  }

  @Override
  public ShuffleBlockResolver shuffleBlockResolver() {
    throw new RssException("RssShuffleManager.shuffleBlockResolver is not implemented");
  }

  @Override
  protected ShuffleWriteClient createShuffleWriteClient() {
    int unregisterThreadPoolSize =
        sparkConf.get(RssSparkConfig.RSS_CLIENT_UNREGISTER_THREAD_POOL_SIZE);
    int unregisterTimeoutSec = sparkConf.get(RssSparkConfig.RSS_CLIENT_UNREGISTER_TIMEOUT_SEC);
    int unregisterRequestTimeoutSec =
        sparkConf.get(RssSparkConfig.RSS_CLIENT_UNREGISTER_REQUEST_TIMEOUT_SEC);
    long retryIntervalMax = sparkConf.get(RssSparkConfig.RSS_CLIENT_RETRY_INTERVAL_MAX);
    int heartBeatThreadNum = sparkConf.get(RssSparkConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM);

    final int retryMax = sparkConf.get(RssSparkConfig.RSS_CLIENT_RETRY_MAX);
    return RssShuffleClientFactory.getInstance()
        .createShuffleWriteClient(
            RssShuffleClientFactory.newWriteBuilder()
                .blockIdSelfManagedEnabled(blockIdSelfManagedEnabled)
                .managerClientSupplier(managerClientSupplier)
                .clientType(clientType)
                .retryMax(retryMax)
                .retryIntervalMax(retryIntervalMax)
                .heartBeatThreadNum(heartBeatThreadNum)
                .replica(dataReplica)
                .replicaWrite(dataReplicaWrite)
                .replicaRead(dataReplicaRead)
                .replicaSkipEnabled(dataReplicaSkipEnabled)
                .dataTransferPoolSize(dataTransferPoolSize)
                .dataCommitPoolSize(dataCommitPoolSize)
                .unregisterThreadPoolSize(unregisterThreadPoolSize)
                .unregisterTimeSec(unregisterTimeoutSec)
                .unregisterRequestTimeSec(unregisterRequestTimeoutSec)
                .rssConf(rssConf));
  }

  @VisibleForTesting
  protected static ShuffleDataDistributionType getDataDistributionType(SparkConf sparkConf) {
    RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);
    if ((boolean) sparkConf.get(SQLConf.ADAPTIVE_EXECUTION_ENABLED())
        && !rssConf.containsKey(RssClientConf.DATA_DISTRIBUTION_TYPE.key())) {
      return ShuffleDataDistributionType.LOCAL_ORDER;
    }

    return rssConf.get(RssClientConf.DATA_DISTRIBUTION_TYPE);
  }

  static class ReadMetrics extends ShuffleReadMetrics {
    private ShuffleReadMetricsReporter reporter;

    ReadMetrics(ShuffleReadMetricsReporter reporter) {
      this.reporter = reporter;
    }

    @Override
    public void incRemoteBytesRead(long v) {
      reporter.incRemoteBytesRead(v);
    }

    @Override
    public void incFetchWaitTime(long v) {
      reporter.incFetchWaitTime(v);
    }

    @Override
    public void incRecordsRead(long v) {
      reporter.incRecordsRead(v);
    }
  }

  public static class WriteMetrics extends ShuffleWriteMetrics {
    private ShuffleWriteMetricsReporter reporter;

    public WriteMetrics(ShuffleWriteMetricsReporter reporter) {
      this.reporter = reporter;
    }

    @Override
    public void incBytesWritten(long v) {
      reporter.incBytesWritten(v);
    }

    @Override
    public void incRecordsWritten(long v) {
      reporter.incRecordsWritten(v);
    }

    @Override
    public void incWriteTime(long v) {
      reporter.incWriteTime(v);
    }
  }

  @VisibleForTesting
  public void setAppId(String appId) {
    this.id = new AtomicReference<>(appId);
  }

  private ShuffleResult getShuffleResultForMultiPart(
      String clientType,
      Map<ShuffleServerInfo, Set<Integer>> serverToPartitions,
      String appId,
      int shuffleId,
      int stageAttemptId,
      PartitionDataReplicaRequirementTracking replicaRequirementTracking) {
    Set<Integer> failedPartitions = Sets.newHashSet();
    try {
      return shuffleWriteClient.getShuffleResultForMultiPartV2(
          clientType,
          serverToPartitions,
          appId,
          shuffleId,
          failedPartitions,
          replicaRequirementTracking);
    } catch (RssFetchFailedException e) {
      throw RssSparkShuffleUtils.reportRssFetchFailedException(
          managerClientSupplier, e, sparkConf, appId, shuffleId, stageAttemptId, failedPartitions);
    }
  }
}
