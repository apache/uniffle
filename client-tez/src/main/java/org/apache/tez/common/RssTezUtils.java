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

package org.apache.tez.common;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.input.ConcatenatedMergedKeyValueInput;
import org.apache.tez.runtime.library.input.ConcatenatedMergedKeyValuesInput;
import org.apache.tez.runtime.library.input.OrderedGroupedInputLegacy;
import org.apache.tez.runtime.library.input.OrderedGroupedKVInput;
import org.apache.tez.runtime.library.input.OrderedGroupedMergedKVInput;
import org.apache.tez.runtime.library.input.RMRssOrderedGroupedKVInput;
import org.apache.tez.runtime.library.input.RssConcatenatedMergedKeyValueInput;
import org.apache.tez.runtime.library.input.RssConcatenatedMergedKeyValuesInput;
import org.apache.tez.runtime.library.input.RssOrderedGroupedInputLegacy;
import org.apache.tez.runtime.library.input.RssOrderedGroupedKVInput;
import org.apache.tez.runtime.library.input.RssOrderedGroupedMergedKVInput;
import org.apache.tez.runtime.library.input.RssUnorderedKVInput;
import org.apache.tez.runtime.library.input.UnorderedKVInput;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;
import org.apache.tez.runtime.library.output.RssOrderedPartitionedKVOutput;
import org.apache.tez.runtime.library.output.RssUnorderedKVOutput;
import org.apache.tez.runtime.library.output.RssUnorderedPartitionedKVOutput;
import org.apache.tez.runtime.library.output.UnorderedKVOutput;
import org.apache.tez.runtime.library.output.UnorderedPartitionedKVOutput;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.Constants;

public class RssTezUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RssTezUtils.class);
  private static final BlockIdLayout LAYOUT = BlockIdLayout.DEFAULT;

  public static final String HOST_NAME = "hostname";

  public static final String UNDERLINE_DELIMITER = "_";
  // constant to compute shuffle id
  private static final int VERTEX_ID_MAPPING_MAX_ID = 500;
  private static final String VERTEX_ID_MAPPING_MAP = "Map";
  private static final String VERTEX_ID_MAPPING_REDUCER = "Reducer";
  private static final int VERTEX_ID_MAPPING_MAGIC = 600;
  private static final int SHUFFLE_ID_MAGIC = 1000;

  private RssTezUtils() {}

  public static ShuffleWriteClient createShuffleClient(Configuration conf) {
    int heartBeatThreadNum =
        conf.getInt(
            RssTezConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM,
            RssTezConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM_DEFAULT_VALUE);
    int retryMax =
        conf.getInt(
            RssTezConfig.RSS_CLIENT_RETRY_MAX, RssTezConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE);
    long retryIntervalMax =
        conf.getLong(
            RssTezConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
            RssTezConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE);
    String clientType =
        conf.get(RssTezConfig.RSS_CLIENT_TYPE, RssTezConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE);
    int replicaWrite =
        conf.getInt(
            RssTezConfig.RSS_DATA_REPLICA_WRITE, RssTezConfig.RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE);
    int replicaRead =
        conf.getInt(
            RssTezConfig.RSS_DATA_REPLICA_READ, RssTezConfig.RSS_DATA_REPLICA_READ_DEFAULT_VALUE);
    int replica =
        conf.getInt(RssTezConfig.RSS_DATA_REPLICA, RssTezConfig.RSS_DATA_REPLICA_DEFAULT_VALUE);
    boolean replicaSkipEnabled =
        conf.getBoolean(
            RssTezConfig.RSS_DATA_REPLICA_SKIP_ENABLED,
            RssTezConfig.RSS_DATA_REPLICA_SKIP_ENABLED_DEFAULT_VALUE);
    int dataTransferPoolSize =
        conf.getInt(
            RssTezConfig.RSS_DATA_TRANSFER_POOL_SIZE,
            RssTezConfig.RSS_DATA_TRANSFER_POOL_SIZE_DEFAULT_VALUE);
    int dataCommitPoolSize =
        conf.getInt(
            RssTezConfig.RSS_DATA_COMMIT_POOL_SIZE,
            RssTezConfig.RSS_DATA_COMMIT_POOL_SIZE_DEFAULT_VALUE);
    ShuffleWriteClient client =
        ShuffleClientFactory.getInstance()
            .createShuffleWriteClient(
                ShuffleClientFactory.newWriteBuilder()
                    .clientType(clientType)
                    .retryMax(retryMax)
                    .retryIntervalMax(retryIntervalMax)
                    .heartBeatThreadNum(heartBeatThreadNum)
                    .replica(replica)
                    .replicaWrite(replicaWrite)
                    .replicaRead(replicaRead)
                    .replicaSkipEnabled(replicaSkipEnabled)
                    .dataTransferPoolSize(dataTransferPoolSize)
                    .dataCommitPoolSize(dataCommitPoolSize)
                    .rssConf(RssTezConfig.toRssConf(conf)));
    return client;
  }

  public static long getInitialMemoryRequirement(Configuration conf, long maxAvailableTaskMemory) {
    long initialMemRequestMb =
        conf.getLong(
            RssTezConfig.RSS_RUNTIME_IO_SORT_MB, RssTezConfig.RSS_DEFAULT_RUNTIME_IO_SORT_MB);
    LOG.info("InitialMemRequestMb is {}", initialMemRequestMb);
    LOG.info("MaxAvailableTaskMemory is {}", maxAvailableTaskMemory);
    long reqBytes = initialMemRequestMb << 20;
    Preconditions.checkArgument(
        initialMemRequestMb > 0 && reqBytes < maxAvailableTaskMemory,
        RssTezConfig.RSS_RUNTIME_IO_SORT_MB
            + initialMemRequestMb
            + " should be "
            + "larger than 0 and should be less than the available task memory (MB):"
            + (maxAvailableTaskMemory >> 20));
    LOG.info(
        "Requested BufferSize ("
            + TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB
            + ") : "
            + initialMemRequestMb);
    return reqBytes;
  }

  public static String uniqueIdentifierToAttemptId(String uniqueIdentifier) {
    if (uniqueIdentifier == null) {
      throw new RssException("uniqueIdentifier should not be null");
    }
    String[] ids = uniqueIdentifier.split("_");
    return StringUtils.join(ids, "_", 0, 7);
  }

  public static long getBlockId(int partitionId, long taskAttemptId, int nextSeqNo) {
    return LAYOUT.getBlockId(nextSeqNo, partitionId, taskAttemptId);
  }

  public static int getTaskAttemptId(long blockId) {
    return LAYOUT.getTaskAttemptId(blockId);
  }

  public static int estimateTaskConcurrency(Configuration jobConf, int mapNum, int reduceNum) {
    double dynamicFactor =
        jobConf.getDouble(
            RssTezConfig.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR,
            RssTezConfig.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR_DEFAULT_VALUE);
    double slowStart =
        jobConf.getDouble(Constants.MR_SLOW_START, Constants.MR_SLOW_START_DEFAULT_VALUE);
    int mapLimit = jobConf.getInt(Constants.MR_MAP_LIMIT, Constants.MR_MAP_LIMIT_DEFAULT_VALUE);
    int reduceLimit =
        jobConf.getInt(Constants.MR_REDUCE_LIMIT, Constants.MR_REDUCE_LIMIT_DEFAULT_VALUE);

    int estimateMapNum = mapLimit > 0 ? Math.min(mapNum, mapLimit) : mapNum;
    int estimateReduceNum = reduceLimit > 0 ? Math.min(reduceNum, reduceLimit) : reduceNum;
    if (slowStart == 1) {
      return (int) (Math.max(estimateMapNum, estimateReduceNum) * dynamicFactor);
    } else {
      return (int) (((1 - slowStart) * estimateMapNum + estimateReduceNum) * dynamicFactor);
    }
  }

  public static int getRequiredShuffleServerNumber(
      Configuration jobConf, int mapNum, int reduceNum) {
    int requiredShuffleServerNumber =
        jobConf.getInt(
            RssTezConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER,
            RssTezConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER_DEFAULT_VALUE);
    boolean enabledEstimateServer =
        jobConf.getBoolean(
            RssTezConfig.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED,
            RssTezConfig.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED_DEFAULT_VALUE);
    if (!enabledEstimateServer || requiredShuffleServerNumber > 0) {
      return requiredShuffleServerNumber;
    }
    int taskConcurrency = estimateTaskConcurrency(jobConf, mapNum, reduceNum);
    int taskConcurrencyPerServer =
        jobConf.getInt(
            RssTezConfig.RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER,
            RssTezConfig.RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER_DEFAULT_VALUE);
    return (int) Math.ceil(taskConcurrency * 1.0 / taskConcurrencyPerServer);
  }

  /**
   * @param tezDagID Get from tez InputContext, represent dag id.
   * @param upVertexId Up stream vertex id of the task.
   * @param downVertexId The vertex id of task.
   * @return The shuffle id.
   */
  public static int computeShuffleId(int tezDagID, int upVertexId, int downVertexId) {
    int shuffleId =
        tezDagID * (SHUFFLE_ID_MAGIC * SHUFFLE_ID_MAGIC)
            + upVertexId * SHUFFLE_ID_MAGIC
            + downVertexId;
    LOG.info(
        "Compute Shuffle Id:{}, up vertex id:{}, down vertex id:{}",
        shuffleId,
        upVertexId,
        downVertexId);
    return shuffleId;
  }

  public static int parseDagId(int shuffleId) {
    Preconditions.checkArgument(shuffleId > 0, "shuffleId should be positive.");
    int dagId = shuffleId / (SHUFFLE_ID_MAGIC * SHUFFLE_ID_MAGIC);
    if (dagId == 0) {
      throw new RssException("Illegal shuffleId: " + shuffleId);
    }
    return dagId;
  }

  /**
   * @param vertexName: vertex name, like "Map 1" or "Reducer 2"
   * @return Map vertex name of String type to int type. Split vertex name, get vertex type and
   *     vertex id number, if it's map vertex, then return vertex id number, else if it's reducer
   *     vertex, then add VERTEX_ID_MAPPING_MAGIC and vertex id number finally return it.
   */
  private static int mapVertexId(String vertexName) {
    String[] ss = vertexName.split("\\s+");
    if (Integer.parseInt(ss[1]) > VERTEX_ID_MAPPING_MAX_ID) {
      throw new RssException("Too large vertex name to id mapping, vertexName:" + vertexName);
    }
    if (VERTEX_ID_MAPPING_MAP.equals(ss[0])) {
      return Integer.parseInt(ss[1]);
    } else if (VERTEX_ID_MAPPING_REDUCER.equals(ss[0])) {
      return VERTEX_ID_MAPPING_MAGIC + Integer.parseInt(ss[1]);
    } else {
      throw new RssException("Wrong vertex name to id mapping, vertexName:" + vertexName);
    }
  }

  public static int createRssTaskAttemptId(TezTaskAttemptID taskAttemptId, int maxAttemptNo) {
    int attemptBits = ClientUtils.getNumberOfSignificantBits(maxAttemptNo);

    int attemptId = taskAttemptId.getId();
    if (attemptId > maxAttemptNo || attemptId < 0) {
      throw new RssException(
          "TaskAttempt " + taskAttemptId + " attemptId " + attemptId + " exceed");
    }
    int taskId = taskAttemptId.getTaskID().getId();

    int mapIndexBits = ClientUtils.getNumberOfSignificantBits(taskId);
    if (mapIndexBits + attemptBits > LAYOUT.taskAttemptIdBits) {
      throw new RssException(
          "Observing taskId["
              + taskId
              + "] that would produce a taskAttemptId with "
              + (mapIndexBits + attemptBits)
              + " bits which is larger than the allowed "
              + LAYOUT.taskAttemptIdBits
              + "]). Please consider providing more bits for taskAttemptIds.");
    }

    int id = (taskId << attemptBits) | attemptId;
    LOG.info("createRssTaskAttemptId taskAttemptId:{}, id is {}, .", taskAttemptId, id);
    return id;
  }

  public static int createRssTaskAttemptId(TezTaskAttemptID taskAttemptId, Configuration conf) {
    int maxAttemptNo = getMaxAttemptNo(conf);
    return createRssTaskAttemptId(taskAttemptId, maxAttemptNo);
  }

  public static int getMaxAttemptNo(Configuration conf) {
    int maxFailures =
        conf.getInt(
            TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS,
            TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS_DEFAULT);
    boolean speculation =
        conf.getBoolean(
            TezConfiguration.TEZ_AM_SPECULATION_ENABLED,
            TezConfiguration.TEZ_AM_SPECULATION_ENABLED_DEFAULT);
    return ClientUtils.getMaxAttemptNo(maxFailures, speculation);
  }

  public static Roaring64NavigableMap fetchAllRssTaskIds(
      Set<InputAttemptIdentifier> successMapTaskAttempts,
      int totalMapsCount,
      int appAttemptId,
      int maxAttemptNo) {
    String errMsg = "TaskAttemptIDs are inconsistent with map tasks";
    Roaring64NavigableMap rssTaskIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap mapTaskIdBitmap = Roaring64NavigableMap.bitmapOf();
    LOG.info("FetchAllRssTaskIds successMapTaskAttempts size:{}", successMapTaskAttempts.size());
    LOG.info("FetchAllRssTaskIds totalMapsCount:{}, appAttemptId:{}", totalMapsCount, appAttemptId);

    for (InputAttemptIdentifier inputAttemptIdentifier : successMapTaskAttempts) {
      String pathComponent = inputAttemptIdentifier.getPathComponent();
      TezTaskAttemptID mapTaskAttemptId = IdUtils.convertTezTaskAttemptID(pathComponent);
      long rssTaskId = RssTezUtils.createRssTaskAttemptId(mapTaskAttemptId, maxAttemptNo);
      long mapTaskId = mapTaskAttemptId.getTaskID().getId();

      LOG.info(
          "FetchAllRssTaskIds, pathComponent: {}, mapTaskId:{}, rssTaskId:{}, is contains:{}",
          pathComponent,
          mapTaskId,
          rssTaskId,
          mapTaskIdBitmap.contains(mapTaskId));
      if (!mapTaskIdBitmap.contains(mapTaskId)) {
        rssTaskIdBitmap.addLong(rssTaskId);
        mapTaskIdBitmap.addLong(mapTaskId);
        if (mapTaskId
            >= totalMapsCount) { // up-stream map task index should < total task number(including
          // failed task)
          LOG.warn(
              inputAttemptIdentifier
                  + " has overflowed mapIndex, pathComponent: "
                  + pathComponent
                  + ",totalMapsCount: "
                  + totalMapsCount);
        }
      } else {
        LOG.warn(inputAttemptIdentifier + " is redundant on index: " + mapTaskId);
      }
    }
    // each map should have only one success attempt
    if (mapTaskIdBitmap.getLongCardinality() != rssTaskIdBitmap.getLongCardinality()) {
      throw new IllegalStateException(errMsg);
    }
    return rssTaskIdBitmap;
  }

  public static int taskIdStrToTaskId(String taskIdStr) {
    try {
      int pos1 = taskIdStr.indexOf(UNDERLINE_DELIMITER);
      int pos2 = taskIdStr.indexOf(UNDERLINE_DELIMITER, pos1 + 1);
      int pos3 = taskIdStr.indexOf(UNDERLINE_DELIMITER, pos2 + 1);
      int pos4 = taskIdStr.indexOf(UNDERLINE_DELIMITER, pos3 + 1);
      int pos5 = taskIdStr.indexOf(UNDERLINE_DELIMITER, pos4 + 1);
      int pos6 = taskIdStr.indexOf(UNDERLINE_DELIMITER, pos5 + 1);
      return Integer.parseInt(taskIdStr.substring(pos5 + 1, pos6));
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("Failed to get VertexId, taskId:{}.", taskIdStr, e);
      throw e;
    }
  }

  // multiHostInfo is like:
  // 172.19.193.247:19999+1_4_7, 172.19.193.55:19999+2_5, 172.19.193.152:19999+0_3_6
  private static void parseRssWorkerFromHostInfo(
      Map<Integer, Set<ShuffleServerInfo>> rssWorker, String multiHostInfo) {
    for (String hostInfo : multiHostInfo.split(",")) {
      // LOG.info("ParseRssWorker, hostInfo:{}", hostInfo);
      String[] info = hostInfo.split("\\+");
      ShuffleServerInfo serverInfo =
          new ShuffleServerInfo(info[0].split(":")[0], Integer.parseInt(info[0].split(":")[1]));

      String[] partitions = info[1].split("_");

      if (partitions.length <= 0) {
        throw new RssException("The length of partitions should not be less than 0");
      }
      for (String partitionId : partitions) {
        rssWorker.computeIfAbsent(Integer.parseInt(partitionId), k -> new HashSet<>());
        rssWorker.get(Integer.parseInt(partitionId)).add(serverInfo);
      }
    }
  }

  // hostnameInfo is like:
  // 172.19.193.247:19999+1_4_7, 172.19.193.55:19999+2_5,172.19.193.152:19999+0_3_6
  public static void parseRssWorker(
      Map<Integer, Set<ShuffleServerInfo>> rssWorker, int shuffleId, String hostnameInfo) {
    LOG.info("ParseRssWorker, hostnameInfo length:{}", hostnameInfo.length());
    for (String toVertex : hostnameInfo.split(";")) {
      // toVertex is like:
      // 1001602=172.19.193.247:19999+1_4_7,172.19.193.55:19999+2_5,172.19.193.152:19999+0_3_6
      String[] splits = toVertex.split("=");
      if (splits.length == 2 && String.valueOf(shuffleId).equals(splits[0])) {
        String workerStr = splits[1];
        parseRssWorkerFromHostInfo(rssWorker, workerStr);
      }
    }
  }

  public static String replaceRssOutputClassName(String className) {
    if (className.equals(OrderedPartitionedKVOutput.class.getName())) {
      LOG.info(
          "Output class name will transient from {} to {}",
          className,
          RssOrderedPartitionedKVOutput.class.getName());
      return RssOrderedPartitionedKVOutput.class.getName();
    } else if (className.equals(UnorderedKVOutput.class.getName())) {
      LOG.info(
          "Output class name will transient from {} to {}",
          className,
          RssUnorderedKVOutput.class.getName());
      return RssUnorderedKVOutput.class.getName();
    } else if (className.equals(UnorderedPartitionedKVOutput.class.getName())) {
      LOG.info(
          "Output class name will transient from {} to {}",
          className,
          RssUnorderedPartitionedKVOutput.class.getName());
      return RssUnorderedPartitionedKVOutput.class.getName();
    } else {
      LOG.info("Unexpected kv output class name {}.", className);
      return className;
    }
  }

  public static String replaceRssInputClassName(String className, boolean isRemoteMergeEnable) {
    if (className.equals(OrderedGroupedKVInput.class.getName())) {
      String orderedInputClasName =
          isRemoteMergeEnable
              ? RMRssOrderedGroupedKVInput.class.getName()
              : RssOrderedGroupedKVInput.class.getName();
      LOG.info("Input class name will transient from {} to {}", className, orderedInputClasName);
      return orderedInputClasName;
    } else if (className.equals(OrderedGroupedMergedKVInput.class.getName())) {
      LOG.info(
          "Input class name will transient from {} to {}",
          className,
          RssOrderedGroupedMergedKVInput.class.getName());
      return RssOrderedGroupedMergedKVInput.class.getName();
    } else if (className.equals(OrderedGroupedInputLegacy.class.getName())) {
      LOG.info(
          "Input class name will transient from {} to {}",
          className,
          RssOrderedGroupedInputLegacy.class.getName());
      return RssOrderedGroupedInputLegacy.class.getName();
    } else if (className.equals(UnorderedKVInput.class.getName())) {
      LOG.info(
          "Input class name will transient from {} to {}",
          className,
          RssUnorderedKVInput.class.getName());
      return RssUnorderedKVInput.class.getName();
    } else if (className.equals(ConcatenatedMergedKeyValueInput.class.getName())) {
      LOG.info(
          "Input class name will transient from {} to {}",
          className,
          RssConcatenatedMergedKeyValueInput.class.getName());
      return RssConcatenatedMergedKeyValueInput.class.getName();
    } else if (className.equals(ConcatenatedMergedKeyValuesInput.class.getName())) {
      LOG.info(
          "Input class name will transient from {} to {}",
          className,
          RssConcatenatedMergedKeyValuesInput.class.getName());
      return RssConcatenatedMergedKeyValuesInput.class.getName();
    } else {
      LOG.info("Unexpected kv input class name {}.", className);
      return className;
    }
  }

  public static void applyDynamicClientConf(Configuration conf, Map<String, String> confItems) {
    if (conf == null) {
      LOG.warn("Tez conf is null");
      return;
    }

    if (confItems == null || confItems.isEmpty()) {
      LOG.warn("Empty conf items");
      return;
    }

    for (Map.Entry<String, String> kv : confItems.entrySet()) {
      String tezConfKey = kv.getKey();
      if (!tezConfKey.startsWith(RssTezConfig.TEZ_RSS_CONFIG_PREFIX)) {
        tezConfKey = RssTezConfig.TEZ_RSS_CONFIG_PREFIX + tezConfKey;
      }
      String tezConfVal = kv.getValue();
      if (StringUtils.isEmpty(conf.get(tezConfKey, ""))
          || RssTezConfig.RSS_MANDATORY_CLUSTER_CONF.contains(tezConfKey)) {
        LOG.warn("Use conf dynamic conf {} = {}", tezConfKey, tezConfVal);
        conf.set(tezConfKey, tezConfVal);
      }
    }
  }

  public static Configuration filterRssConf(Configuration extraConf) {
    Configuration conf = new Configuration(false);
    for (Map.Entry<String, String> entry : extraConf) {
      String key = entry.getKey();
      if (key.startsWith(RssTezConfig.TEZ_RSS_CONFIG_PREFIX)) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }
    return conf;
  }
}
