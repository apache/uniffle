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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.rdd.DeterministicLevel;
import org.apache.spark.shuffle.handle.ShuffleHandleInfo;
import org.apache.spark.shuffle.handle.SimpleShuffleHandleInfo;
import org.apache.spark.shuffle.reader.RssShuffleReader;
import org.apache.spark.shuffle.writer.RssShuffleWriter;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.exception.RssFetchFailedException;
import org.apache.uniffle.shuffle.RssShuffleClientFactory;
import org.apache.uniffle.shuffle.manager.RssShuffleManagerBase;

public class RssShuffleManager extends RssShuffleManagerBase {
  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleManager.class);

  public RssShuffleManager(SparkConf sparkConf, boolean isDriver) {
    super(sparkConf, isDriver);
  }

  // This method is called in Spark driver side,
  // and Spark driver will make some decision according to coordinator,
  // e.g. determining what RSS servers to use.
  // Then Spark driver will return a ShuffleHandle and
  // pass that ShuffleHandle to executors (getWriter/getReader).
  @Override
  public <K, V, C> ShuffleHandle registerShuffle(
      int shuffleId, int numMaps, ShuffleDependency<K, V, C> dependency) {

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

    // If yarn enable retry ApplicationMaster, appId will be not unique and shuffle data will be
    // incorrect,
    // appId + uuid can avoid such problem,
    // can't get appId in construct because SparkEnv is not created yet,
    // appId will be initialized only once in this method which
    // will be called many times depend on how many shuffle stage
    if ("".equals(appId)) {
      appId = SparkEnv.get().conf().getAppId() + "_" + uuid;
      dataPusher.setRssAppId(appId);
      LOG.info("Generate application id used in rss: " + appId);
    }

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
          shuffleId, appId, dependency.rdd().getNumPartitions(), dependency, hdlInfoBd);
    }

    // get all register info according to coordinator's response
    Set<String> assignmentTags = RssSparkShuffleUtils.getAssignmentTags(sparkConf);
    ClientUtils.validateClientType(clientType);
    assignmentTags.add(clientType);

    int requiredShuffleServerNumber =
        RssSparkShuffleUtils.getRequiredShuffleServerNumber(sparkConf);
    int estimateTaskConcurrency = RssSparkShuffleUtils.estimateTaskConcurrency(sparkConf);

    Map<Integer, List<ShuffleServerInfo>> partitionToServers = null;
    if (!rssStageRetryEnabled) {
      // The ShuffleServer will be allocated here only when the Spark phase retry is not enabled;
      // otherwise, the ShuffleServer will be allocated and registered during the Shuffle write
      // phase.
      partitionToServers =
          requestShuffleAssignment(
              shuffleId,
              dependency.partitioner().numPartitions(),
              1,
              requiredShuffleServerNumber,
              estimateTaskConcurrency,
              rssStageResubmitManager.getServerIdBlackList());
    }
    startHeartbeat();

    shuffleIdToPartitionNum.computeIfAbsent(
        shuffleId, key -> dependency.partitioner().numPartitions());
    shuffleIdToNumMapTasks.computeIfAbsent(shuffleId, key -> dependency.rdd().partitions().length);
    String storageType = sparkConf.get(RssSparkConfig.RSS_STORAGE_TYPE.key());
    RemoteStorageInfo defaultRemoteStorage = getDefaultRemoteStorageInfo(sparkConf);
    RemoteStorageInfo remoteStorage =
        ClientUtils.fetchRemoteStorage(
            appId, defaultRemoteStorage, dynamicConfEnabled, storageType, shuffleWriteClient);

    Broadcast<SimpleShuffleHandleInfo> hdlInfoBd =
        RssSparkShuffleUtils.broadcastShuffleHdlInfo(
            RssSparkShuffleUtils.getActiveSparkContext(),
            shuffleId,
            partitionToServers,
            remoteStorage);
    if (partitionToServers != null) {
      LOG.info(
          "RegisterShuffle with ShuffleId[{}], partitionNum[{}], server:{}",
          shuffleId,
          partitionToServers.size(),
          partitionToServers);
    }
    return new RssShuffleHandle(shuffleId, appId, numMaps, dependency, hdlInfoBd);
  }

  // This method is called in Spark executor,
  // getting information from Spark driver via the ShuffleHandle.
  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(
      ShuffleHandle handle, int mapId, TaskContext context) {
    if (handle instanceof RssShuffleHandle) {
      RssShuffleHandle<K, V, ?> rssHandle = (RssShuffleHandle<K, V, ?>) handle;
      appId = rssHandle.getAppId();
      dataPusher.setRssAppId(appId);

      int shuffleId = rssHandle.getShuffleId();
      int uniffleShuffleId;
      if (rssStageRetryEnabled) {
        uniffleShuffleId = getUniffleShuffleId(shuffleId, context, true);
      } else {
        uniffleShuffleId = shuffleId;
      }

      String taskId = "" + context.taskAttemptId() + "_" + context.attemptNumber();
      ShuffleWriteMetrics writeMetrics = context.taskMetrics().shuffleWriteMetrics();
      return new RssShuffleWriter<>(
          rssHandle.getAppId(),
          shuffleId,
          uniffleShuffleId,
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
    } else {
      throw new RssException("Unexpected ShuffleHandle:" + handle.getClass().getName());
    }
  }

  // This method is called in Spark executor,
  // getting information from Spark driver via the ShuffleHandle.
  @Override
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle, int startPartition, int endPartition, TaskContext context) {
    if (handle instanceof RssShuffleHandle) {
      RssShuffleHandle<K, C, ?> rssShuffleHandle = (RssShuffleHandle<K, C, ?>) handle;
      final int partitionNumPerRange = sparkConf.get(RssSparkConfig.RSS_PARTITION_NUM_PER_RANGE);
      final int partitionNum = rssShuffleHandle.getDependency().partitioner().numPartitions();
      int shuffleId = rssShuffleHandle.getShuffleId();
      long start = System.currentTimeMillis();
      Roaring64NavigableMap taskIdBitmap =
          getExpectedTasks(shuffleId, startPartition, endPartition);
      LOG.info(
          "Get taskId cost {} ms, and request expected blockIds from {} tasks for shuffleId[{}], partitionId[{}]",
          (System.currentTimeMillis() - start),
          taskIdBitmap.getLongCardinality(),
          shuffleId,
          startPartition);
      start = System.currentTimeMillis();
      int uniffleShuffleId;
      if (rssStageRetryEnabled) {
        uniffleShuffleId = getUniffleShuffleId(shuffleId, context, false);
      } else {
        uniffleShuffleId = shuffleId;
      }
      ShuffleHandleInfo shuffleHandleInfo =
          getShuffleHandleInfo(uniffleShuffleId, rssShuffleHandle);
      Map<Integer, List<ShuffleServerInfo>> partitionToServers =
          shuffleHandleInfo.getAllPartitionServersForReader();
      Roaring64NavigableMap blockIdBitmap =
          getShuffleResult(
              clientType,
              Sets.newHashSet(partitionToServers.get(startPartition)),
              rssShuffleHandle.getAppId(),
              shuffleId,
              uniffleShuffleId,
              startPartition,
              context.stageId(),
              context.stageAttemptNumber());
      LOG.info(
          "Get shuffle blockId cost {} ms, and get {} blockIds for shuffleId[{}], uniffleShuffleId[{}],partitionId[{}]",
          (System.currentTimeMillis() - start),
          blockIdBitmap.getLongCardinality(),
          shuffleId,
          uniffleShuffleId,
          startPartition);

      final RemoteStorageInfo shuffleRemoteStorageInfo = rssShuffleHandle.getRemoteStorage();
      LOG.info("Shuffle reader using remote storage {}", shuffleRemoteStorageInfo);
      final String shuffleRemoteStoragePath = shuffleRemoteStorageInfo.getPath();
      Configuration readerHadoopConf =
          RssSparkShuffleUtils.getRemoteStorageHadoopConf(sparkConf, shuffleRemoteStorageInfo);

      return new RssShuffleReader<K, C>(
          uniffleShuffleId,
          startPartition,
          endPartition,
          context,
          rssShuffleHandle,
          shuffleRemoteStoragePath,
          readerHadoopConf,
          partitionNumPerRange,
          partitionNum,
          blockIdBitmap,
          taskIdBitmap,
          RssSparkConfig.toRssConf(sparkConf),
          partitionToServers,
          managerClientSupplier);
    } else {
      throw new RssException("Unexpected ShuffleHandle:" + handle.getClass().getName());
    }
  }

  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startPartition,
      int endPartition,
      TaskContext context,
      int startMapId,
      int endMapId) {
    return null;
  }

  @Override
  public ShuffleBlockResolver shuffleBlockResolver() {
    throw new RssException("RssShuffleManager.shuffleBlockResolver is not implemented");
  }

  // when speculation enable, duplicate data will be sent and reported to shuffle server,
  // get the actual tasks and filter the duplicate data caused by speculation task
  private Roaring64NavigableMap getExpectedTasks(
      int shuffleId, int startPartition, int endPartition) {
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf();
    // In 2.3, getMapSizesByExecutorId returns Seq, while it returns Iterator in 2.4,
    // so we use toIterator() to support Spark 2.3 & 2.4
    Iterator<Tuple2<BlockManagerId, Seq<Tuple2<BlockId, Object>>>> mapStatusIter =
        SparkEnv.get()
            .mapOutputTracker()
            .getMapSizesByExecutorId(shuffleId, startPartition, endPartition)
            .toIterator();
    while (mapStatusIter.hasNext()) {
      Tuple2<BlockManagerId, Seq<Tuple2<BlockId, Object>>> tuple2 = mapStatusIter.next();
      Option<String> topologyInfo = tuple2._1().topologyInfo();
      if (topologyInfo.isDefined()) {
        taskIdBitmap.addLong(Long.parseLong(tuple2._1().topologyInfo().get()));
      } else {
        throw new RssException("Can't get expected taskAttemptId");
      }
    }
    LOG.info("Got result from MapStatus for expected tasks " + taskIdBitmap.getLongCardinality());
    return taskIdBitmap;
  }

  @VisibleForTesting
  public void setAppId(String appId) {
    this.appId = appId;
  }

  /** @return the unique spark id for rss shuffle */
  @Override
  public String getAppId() {
    return appId;
  }

  private Roaring64NavigableMap getShuffleResult(
      String clientType,
      Set<ShuffleServerInfo> shuffleServerInfoSet,
      String appId,
      int shuffleId,
      int uniffleShuffleId,
      int partitionId,
      int stageAttemptId,
      int stageAttemptNumber) {
    try {
      return shuffleWriteClient.getShuffleResult(
          clientType, shuffleServerInfoSet, appId, uniffleShuffleId, partitionId);
    } catch (RssFetchFailedException e) {
      throw RssSparkShuffleUtils.reportRssFetchFailedException(
          managerClientSupplier,
          e,
          sparkConf,
          appId,
          shuffleId,
          uniffleShuffleId,
          stageAttemptId,
          stageAttemptNumber,
          Sets.newHashSet(partitionId));
    }
  }

  private ShuffleServerInfo assignShuffleServer(int shuffleId, String faultyShuffleServerId) {
    Set<String> faultyServerIds = Sets.newHashSet(faultyShuffleServerId);
    faultyServerIds.addAll(rssStageResubmitManager.getServerIdBlackList());
    Map<Integer, List<ShuffleServerInfo>> partitionToServers =
        requestShuffleAssignment(shuffleId, 1, 1, 1, 1, faultyServerIds);
    if (partitionToServers.get(0) != null && partitionToServers.get(0).size() == 1) {
      return partitionToServers.get(0).get(0);
    }
    return null;
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

  @Override
  protected void checkSupported(SparkConf sparkConf) {
    if (sparkConf.getBoolean("spark.sql.adaptive.enabled", false)) {
      throw new IllegalArgumentException(
          "Spark2 doesn't support AQE, spark.sql.adaptive.enabled should be false.");
    }
  }
}
