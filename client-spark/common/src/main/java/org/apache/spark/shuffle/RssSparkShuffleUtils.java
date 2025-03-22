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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import scala.Option;
import scala.reflect.ClassTag;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.shuffle.handle.SimpleShuffleHandleInfo;
import org.apache.spark.storage.BlockManagerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.factory.CoordinatorClientFactory;
import org.apache.uniffle.client.impl.grpc.CoordinatorGrpcRetryableClient;
import org.apache.uniffle.client.request.RssReportShuffleFetchFailureRequest;
import org.apache.uniffle.client.response.RssReportShuffleFetchFailureResponse;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.exception.RssFetchFailedException;
import org.apache.uniffle.common.util.Constants;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_RESUBMIT_STAGE_WITH_FETCH_FAILURE_ENABLED;

public class RssSparkShuffleUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RssSparkShuffleUtils.class);

  public static final ClassTag<SimpleShuffleHandleInfo> DEFAULT_SHUFFLE_HANDLER_INFO_CLASS_TAG =
      scala.reflect.ClassTag$.MODULE$.apply(SimpleShuffleHandleInfo.class);
  public static final ClassTag<byte[]> BYTE_ARRAY_CLASS_TAG =
      scala.reflect.ClassTag$.MODULE$.apply(byte[].class);

  public static Configuration newHadoopConfiguration(SparkConf sparkConf) {
    SparkHadoopUtil util = new SparkHadoopUtil();
    Configuration conf = util.newConfiguration(sparkConf);

    boolean useOdfs = sparkConf.get(RssSparkConfig.RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE);
    if (useOdfs) {
      final int OZONE_PREFIX_LEN = "spark.rss.ozone.".length();
      conf.setBoolean(
          RssSparkConfig.RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE.key().substring(OZONE_PREFIX_LEN),
          useOdfs);
      conf.set(
          RssSparkConfig.RSS_OZONE_FS_HDFS_IMPL.key().substring(OZONE_PREFIX_LEN),
          sparkConf.get(RssSparkConfig.RSS_OZONE_FS_HDFS_IMPL));
      conf.set(
          RssSparkConfig.RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL
              .key()
              .substring(OZONE_PREFIX_LEN),
          sparkConf.get(RssSparkConfig.RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL));
    }

    return conf;
  }

  public static ShuffleManager loadShuffleManager(String name, SparkConf conf, boolean isDriver)
      throws Exception {
    Class<?> klass = Class.forName(name);
    Constructor<?> constructor;
    ShuffleManager instance;
    try {
      constructor = klass.getConstructor(conf.getClass(), Boolean.TYPE);
      instance = (ShuffleManager) constructor.newInstance(conf, isDriver);
    } catch (NoSuchMethodException e) {
      constructor = klass.getConstructor(conf.getClass());
      instance = (ShuffleManager) constructor.newInstance(conf);
    }
    return instance;
  }

  public static CoordinatorGrpcRetryableClient createCoordinatorClientsWithoutHeartbeat(
      SparkConf sparkConf) {
    String clientType = sparkConf.get(RssSparkConfig.RSS_CLIENT_TYPE);
    String coordinators = getCoordinatorQuorumStr(sparkConf);
    long retryIntervalMs = sparkConf.get(RssSparkConfig.RSS_CLIENT_RETRY_INTERVAL_MAX);
    int retryTimes = sparkConf.get(RssSparkConfig.RSS_CLIENT_RETRY_MAX);
    CoordinatorClientFactory coordinatorClientFactory = CoordinatorClientFactory.getInstance();
    return coordinatorClientFactory.createCoordinatorClientWithoutHeartbeat(
        ClientType.valueOf(clientType), coordinators, retryIntervalMs, retryTimes);
  }

  public static CoordinatorGrpcRetryableClient createCoordinatorClientsForAccessCluster(
      SparkConf sparkConf) {
    String clientType = sparkConf.get(RssSparkConfig.RSS_CLIENT_TYPE);
    String coordinators = getCoordinatorQuorumStr(sparkConf);
    long retryIntervalMs = sparkConf.get(RssSparkConfig.RSS_CLIENT_ACCESS_RETRY_INTERVAL_MS);
    int retryTimes = sparkConf.get(RssSparkConfig.RSS_CLIENT_ACCESS_RETRY_TIMES);
    CoordinatorClientFactory coordinatorClientFactory = CoordinatorClientFactory.getInstance();
    return coordinatorClientFactory.createCoordinatorClientWithoutHeartbeat(
        ClientType.valueOf(clientType), coordinators, retryIntervalMs, retryTimes);
  }

  public static String getCoordinatorQuorumStr(SparkConf sparkConf) {
    return sparkConf.get(RssSparkConfig.RSS_COORDINATOR_QUORUM);
  }

  public static void applyDynamicClientConf(SparkConf sparkConf, Map<String, String> confItems) {
    if (sparkConf == null) {
      LOG.warn("Spark conf is null");
      return;
    }

    if (confItems == null || confItems.isEmpty()) {
      LOG.warn("Empty conf items");
      return;
    }

    for (Map.Entry<String, String> kv : confItems.entrySet()) {
      String sparkConfKey = kv.getKey();
      if (!sparkConfKey.startsWith(RssSparkConfig.SPARK_RSS_CONFIG_PREFIX)) {
        sparkConfKey = RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + sparkConfKey;
      }
      String confVal = kv.getValue();
      boolean isMandatory = RssSparkConfig.RSS_MANDATORY_CLUSTER_CONF.contains(sparkConfKey);
      if (!sparkConf.contains(sparkConfKey) || isMandatory) {
        if (sparkConf.contains(sparkConfKey) && isMandatory) {
          LOG.warn("Override with mandatory dynamic conf {} = {}", sparkConfKey, confVal);
        } else {
          LOG.info("Use dynamic conf {} = {}", sparkConfKey, confVal);
        }
        sparkConf.set(sparkConfKey, confVal);
      }
    }
  }

  public static void validateRssClientConf(SparkConf sparkConf) {
    String msgFormat = "%s must be set by the client or fetched from coordinators.";
    if (!sparkConf.contains(RssSparkConfig.RSS_STORAGE_TYPE.key())) {
      String msg = String.format(msgFormat, "Storage type");
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    String storageType = sparkConf.get(RssSparkConfig.RSS_STORAGE_TYPE.key());
    boolean testMode = sparkConf.getBoolean(RssSparkConfig.RSS_TEST_MODE_ENABLE.key(), false);
    ClientUtils.validateTestModeConf(testMode, storageType);
    int retryMax = sparkConf.get(RssSparkConfig.RSS_CLIENT_RETRY_MAX);
    long retryIntervalMax = sparkConf.get(RssSparkConfig.RSS_CLIENT_RETRY_INTERVAL_MAX);
    long sendCheckTimeout = sparkConf.get(RssSparkConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS);
    if (retryIntervalMax * retryMax > sendCheckTimeout) {
      throw new IllegalArgumentException(
          String.format(
              "%s(%s) * %s(%s) should not bigger than %s(%s)",
              RssSparkConfig.RSS_CLIENT_RETRY_MAX.key(),
              retryMax,
              RssSparkConfig.RSS_CLIENT_RETRY_INTERVAL_MAX.key(),
              retryIntervalMax,
              RssSparkConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS.key(),
              sendCheckTimeout));
    }
  }

  public static Configuration getRemoteStorageHadoopConf(
      SparkConf sparkConf, RemoteStorageInfo remoteStorageInfo) {
    Configuration readerHadoopConf = RssSparkShuffleUtils.newHadoopConfiguration(sparkConf);
    final Map<String, String> shuffleRemoteStorageConf = remoteStorageInfo.getConfItems();
    if (shuffleRemoteStorageConf != null && !shuffleRemoteStorageConf.isEmpty()) {
      for (Map.Entry<String, String> entry : shuffleRemoteStorageConf.entrySet()) {
        readerHadoopConf.set(entry.getKey(), entry.getValue());
      }
    }
    return readerHadoopConf;
  }

  public static Set<String> getAssignmentTags(SparkConf sparkConf) {
    Set<String> assignmentTags = new HashSet<>();
    String rawTags = sparkConf.get(RssSparkConfig.RSS_CLIENT_ASSIGNMENT_TAGS.key(), "");
    if (StringUtils.isNotEmpty(rawTags)) {
      rawTags = rawTags.trim();
      assignmentTags.addAll(Arrays.asList(rawTags.split(",")));
    }
    assignmentTags.add(Constants.SHUFFLE_SERVER_VERSION);
    return assignmentTags;
  }

  public static int estimateTaskConcurrency(SparkConf sparkConf) {
    int taskConcurrency;
    double dynamicAllocationFactor =
        sparkConf.get(RssSparkConfig.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR);
    if (dynamicAllocationFactor > 1 || dynamicAllocationFactor < 0) {
      throw new RssException("dynamicAllocationFactor is not valid: " + dynamicAllocationFactor);
    }
    int executorCores =
        sparkConf.getInt(
            Constants.SPARK_EXECUTOR_CORES, Constants.SPARK_EXECUTOR_CORES_DEFAULT_VALUE);
    int taskCpus =
        sparkConf.getInt(Constants.SPARK_TASK_CPUS, Constants.SPARK_TASK_CPUS_DEFAULT_VALUE);
    int taskConcurrencyPerExecutor = Math.floorDiv(executorCores, taskCpus);
    if (!sparkConf.getBoolean(Constants.SPARK_DYNAMIC_ENABLED, false)) {
      int executorInstances =
          sparkConf.getInt(
              Constants.SPARK_EXECUTOR_INSTANTS, Constants.SPARK_EXECUTOR_INSTANTS_DEFAULT_VALUE);
      taskConcurrency = executorInstances > 0 ? executorInstances * taskConcurrencyPerExecutor : 0;
    } else {
      // Default is infinity
      int maxExecutors =
          Math.min(
              sparkConf.getInt(
                  Constants.SPARK_MAX_DYNAMIC_EXECUTOR,
                  Constants.SPARK_DYNAMIC_EXECUTOR_DEFAULT_VALUE),
              Constants.SPARK_MAX_DYNAMIC_EXECUTOR_LIMIT);
      int minExecutors =
          sparkConf.getInt(
              Constants.SPARK_MIN_DYNAMIC_EXECUTOR, Constants.SPARK_DYNAMIC_EXECUTOR_DEFAULT_VALUE);
      taskConcurrency =
          (int) ((maxExecutors - minExecutors) * dynamicAllocationFactor + minExecutors)
              * taskConcurrencyPerExecutor;
    }
    return taskConcurrency;
  }

  public static int getRequiredShuffleServerNumber(SparkConf sparkConf) {
    boolean enabledEstimateServer =
        sparkConf.get(RssSparkConfig.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED);
    int requiredShuffleServerNumber =
        sparkConf.get(RssSparkConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER);
    if (!enabledEstimateServer || requiredShuffleServerNumber > 0) {
      return requiredShuffleServerNumber;
    }
    int estimateTaskConcurrency = RssSparkShuffleUtils.estimateTaskConcurrency(sparkConf);
    int taskConcurrencyPerServer =
        sparkConf.get(RssSparkConfig.RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER);
    return (int) Math.ceil(estimateTaskConcurrency * 1.0 / taskConcurrencyPerServer);
  }

  /**
   * Get current active {@link SparkContext}. It should be called inside Driver since we don't mean
   * to create any new {@link SparkContext} here.
   *
   * <p>Note: We could use "SparkContext.getActive()" instead of "SparkContext.getOrCreate()" if the
   * "getActive" method is not declared as package private in Scala.
   *
   * @return Active SparkContext created by Driver.
   */
  public static SparkContext getActiveSparkContext() {
    return SparkContext.getOrCreate();
  }

  /**
   * create broadcast variable of {@link SimpleShuffleHandleInfo}
   *
   * @param sc expose for easy unit-test
   * @param shuffleId
   * @param partitionToServers
   * @param storageInfo
   * @return Broadcast variable registered for auto cleanup
   */
  public static Broadcast<SimpleShuffleHandleInfo> broadcastShuffleHdlInfo(
      SparkContext sc,
      int shuffleId,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      RemoteStorageInfo storageInfo) {
    SimpleShuffleHandleInfo handleInfo =
        new SimpleShuffleHandleInfo(shuffleId, partitionToServers, storageInfo);
    return sc.broadcast(handleInfo, DEFAULT_SHUFFLE_HANDLER_INFO_CLASS_TAG);
  }

  private static <T> T instantiateFetchFailedException(
      BlockManagerId dummy, int shuffleId, int mapIndex, int reduceId, Throwable cause) {
    String className = FetchFailedException.class.getName();
    T instance;
    Class<?> klass;
    try {
      klass = Class.forName(className);
    } catch (ClassNotFoundException e) {
      // ever happens;
      throw new RssException(e);
    }
    try {
      instance =
          (T)
              klass
                  .getConstructor(
                      dummy.getClass(),
                      Integer.TYPE,
                      Long.TYPE,
                      Integer.TYPE,
                      Integer.TYPE,
                      Throwable.class)
                  .newInstance(dummy, shuffleId, (long) mapIndex, mapIndex, reduceId, cause);
    } catch (NoSuchMethodException
        | IllegalAccessException
        | IllegalArgumentException
        | InstantiationException
        | InvocationTargetException
            e) { // anything goes wrong, fallback to the another constructor.
      try {
        instance =
            (T)
                klass
                    .getConstructor(
                        dummy.getClass(), Integer.TYPE, Integer.TYPE, Integer.TYPE, Throwable.class)
                    .newInstance(dummy, shuffleId, mapIndex, reduceId, cause);
      } catch (Exception ae) {
        LOG.error("Fail to new instance.", ae);
        throw new RssException(ae);
      }
    }
    return instance;
  }

  public static FetchFailedException createFetchFailedException(
      int shuffleId, int mapIndex, int reduceId, Throwable cause) {
    final String dummyHost = "dummy_host";
    final int dummyPort = 9999;
    BlockManagerId dummy = BlockManagerId.apply("exec-dummy", dummyHost, dummyPort, Option.empty());
    // if no cause
    cause = cause == null ? new Throwable("No cause") : cause;
    return instantiateFetchFailedException(dummy, shuffleId, mapIndex, reduceId, cause);
  }

  public static boolean isStageResubmitSupported() {
    // Stage re-computation requires the ShuffleMapTask to throw a FetchFailedException, which would
    // be produced by the
    // shuffle data reader iterator. However, the shuffle reader iterator interface is defined in
    // Scala, which doesn't
    // have checked exceptions. This makes it hard to throw a FetchFailedException on the Java side.
    // Fortunately, starting from Spark 2.3 (or maybe even Spark 2.2), it is possible to create a
    // FetchFailedException
    // and wrap it into a runtime exception. Spark will consider this exception as a
    // FetchFailedException.
    // Therefore, the stage re-computation feature is only enabled for Spark versions larger than or
    // equal to 2.3.
    return SparkVersionUtils.isSpark3()
        || (SparkVersionUtils.isSpark2() && SparkVersionUtils.MINOR_VERSION >= 3);
  }

  public static RssException reportRssFetchFailedException(
      Supplier<ShuffleManagerClient> managerClientSupplier,
      RssFetchFailedException rssFetchFailedException,
      SparkConf sparkConf,
      String appId,
      int shuffleId,
      int stageAttemptId,
      Set<Integer> failedPartitions) {
    RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);
    if (rssConf.getBoolean(RSS_RESUBMIT_STAGE_WITH_FETCH_FAILURE_ENABLED)
        && RssSparkShuffleUtils.isStageResubmitSupported()) {
      for (int partitionId : failedPartitions) {
        RssReportShuffleFetchFailureRequest req =
            new RssReportShuffleFetchFailureRequest(
                appId,
                shuffleId,
                stageAttemptId,
                partitionId,
                rssFetchFailedException.getMessage());
        RssReportShuffleFetchFailureResponse response =
            managerClientSupplier.get().reportShuffleFetchFailure(req);
        if (response.getReSubmitWholeStage()) {
          // since we are going to roll out the whole stage, mapIndex shouldn't matter, hence -1
          // is provided.
          FetchFailedException ffe =
              RssSparkShuffleUtils.createFetchFailedException(
                  shuffleId, -1, partitionId, rssFetchFailedException);
          return new RssException(ffe);
        }
      }
    }
    return rssFetchFailedException;
  }
}
