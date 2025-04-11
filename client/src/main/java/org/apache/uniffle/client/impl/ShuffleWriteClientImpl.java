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

package org.apache.uniffle.client.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.CoordinatorClientFactory;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.factory.ShuffleServerClientFactory;
import org.apache.uniffle.client.impl.grpc.CoordinatorGrpcRetryableClient;
import org.apache.uniffle.client.request.RssAppHeartBeatRequest;
import org.apache.uniffle.client.request.RssApplicationInfoRequest;
import org.apache.uniffle.client.request.RssFetchClientConfRequest;
import org.apache.uniffle.client.request.RssFetchRemoteStorageRequest;
import org.apache.uniffle.client.request.RssFinishShuffleRequest;
import org.apache.uniffle.client.request.RssGetShuffleAssignmentsRequest;
import org.apache.uniffle.client.request.RssGetShuffleResultForMultiPartRequest;
import org.apache.uniffle.client.request.RssGetShuffleResultRequest;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssReportShuffleResultRequest;
import org.apache.uniffle.client.request.RssSendCommitRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.client.request.RssStartSortMergeRequest;
import org.apache.uniffle.client.request.RssUnregisterShuffleByAppIdRequest;
import org.apache.uniffle.client.request.RssUnregisterShuffleRequest;
import org.apache.uniffle.client.response.ClientResponse;
import org.apache.uniffle.client.response.RssAppHeartBeatResponse;
import org.apache.uniffle.client.response.RssFinishShuffleResponse;
import org.apache.uniffle.client.response.RssGetShuffleAssignmentsResponse;
import org.apache.uniffle.client.response.RssGetShuffleResultResponse;
import org.apache.uniffle.client.response.RssRegisterShuffleResponse;
import org.apache.uniffle.client.response.RssReportShuffleResultResponse;
import org.apache.uniffle.client.response.RssSendCommitResponse;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.client.response.RssStartSortMergeResponse;
import org.apache.uniffle.client.response.RssUnregisterShuffleByAppIdResponse;
import org.apache.uniffle.client.response.RssUnregisterShuffleResponse;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.exception.RssFetchFailedException;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.ThreadUtils;

public class ShuffleWriteClientImpl implements ShuffleWriteClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleWriteClientImpl.class);

  private String clientType;
  private int retryMax;
  private long retryIntervalMax;
  private CoordinatorGrpcRetryableClient coordinatorClients;
  // appId -> shuffleId -> servers
  private Map<String, Map<Integer, Set<ShuffleServerInfo>>> shuffleServerInfoMap =
      JavaUtils.newConcurrentMap();
  private CoordinatorClientFactory coordinatorClientFactory;
  private int heartBeatThreadNum;
  private ExecutorService heartBeatExecutorService;
  private int replica;
  private int replicaWrite;
  private int replicaRead;
  private boolean replicaSkipEnabled;
  private int dataCommitPoolSize = -1;
  private final ExecutorService dataTransferPool;
  private final int unregisterThreadPoolSize;
  private final int unregisterTimeSec;
  private final int unregisterRequestTimeSec;
  private Set<ShuffleServerInfo> defectiveServers;
  private RssConf rssConf;
  private BlockIdLayout blockIdLayout;

  public ShuffleWriteClientImpl(ShuffleClientFactory.WriteClientBuilder builder) {
    // set default value
    if (builder.getRssConf() == null) {
      builder.rssConf(new RssConf());
    }
    if (builder.getUnregisterThreadPoolSize() == 0) {
      builder.unregisterThreadPoolSize(10);
    }
    if (builder.getUnregisterTimeSec() == 0) {
      builder.unregisterTimeSec(10);
    }
    if (builder.getUnregisterRequestTimeSec() == 0) {
      builder.unregisterRequestTimeSec(10);
    }
    this.clientType = builder.getClientType();
    this.retryMax = builder.getRetryMax();
    this.retryIntervalMax = builder.getRetryIntervalMax();
    this.coordinatorClientFactory = CoordinatorClientFactory.getInstance();
    this.heartBeatThreadNum = builder.getHeartBeatThreadNum();
    this.heartBeatExecutorService =
        ThreadUtils.getDaemonFixedThreadPool(heartBeatThreadNum, "client-heartbeat");
    this.replica = builder.getReplica();
    this.replicaWrite = builder.getReplicaWrite();
    this.replicaRead = builder.getReplicaRead();
    this.replicaSkipEnabled = builder.isReplicaSkipEnabled();
    this.dataTransferPool =
        ThreadUtils.getDaemonFixedThreadPool(
            builder.getDataTransferPoolSize(), "client-data-transfer");
    this.dataCommitPoolSize = builder.getDataCommitPoolSize();
    this.unregisterThreadPoolSize = builder.getUnregisterThreadPoolSize();
    this.unregisterTimeSec = builder.getUnregisterTimeSec();
    this.unregisterRequestTimeSec = builder.getUnregisterRequestTimeSec();
    if (replica > 1) {
      defectiveServers = Sets.newConcurrentHashSet();
    }
    this.rssConf = builder.getRssConf();
    this.blockIdLayout = BlockIdLayout.from(rssConf);
  }

  private boolean sendShuffleDataAsync(
      String appId,
      int stageAttemptNumber,
      Map<ShuffleServerInfo, Map<Integer, Map<Integer, List<ShuffleBlockInfo>>>> serverToBlocks,
      Map<ShuffleServerInfo, List<Long>> serverToBlockIds,
      Map<Long, AtomicInteger> blockIdsSendSuccessTracker,
      FailedBlockSendTracker failedBlockSendTracker,
      boolean allowFastFail,
      Supplier<Boolean> needCancelRequest) {

    if (serverToBlockIds == null) {
      return true;
    }

    // If one or more servers is failed, the sending is not totally successful.
    List<CompletableFuture<Boolean>> futures = new ArrayList<>();
    for (Map.Entry<ShuffleServerInfo, Map<Integer, Map<Integer, List<ShuffleBlockInfo>>>> entry :
        serverToBlocks.entrySet()) {
      CompletableFuture<Boolean> future =
          CompletableFuture.supplyAsync(
                  () -> {
                    if (needCancelRequest.get()) {
                      LOG.info("The upstream task has been failed. Abort this data send.");
                      return true;
                    }
                    ShuffleServerInfo ssi = entry.getKey();
                    try {
                      Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks =
                          entry.getValue();
                      // todo: compact unnecessary blocks that reach replicaWrite
                      RssSendShuffleDataRequest request =
                          new RssSendShuffleDataRequest(
                              appId,
                              stageAttemptNumber,
                              retryMax,
                              retryIntervalMax,
                              shuffleIdToBlocks);
                      long s = System.currentTimeMillis();
                      RssSendShuffleDataResponse response =
                          getShuffleServerClient(ssi).sendShuffleData(request);

                      String logMsg =
                          String.format(
                              "ShuffleWriteClientImpl sendShuffleData with %s blocks to %s cost: %s(ms)",
                              serverToBlockIds.get(ssi).size(),
                              ssi.getId(),
                              System.currentTimeMillis() - s);

                      if (response.getStatusCode() == StatusCode.SUCCESS) {
                        // mark a replica of block that has been sent
                        serverToBlockIds
                            .get(ssi)
                            .forEach(
                                blockId ->
                                    blockIdsSendSuccessTracker.get(blockId).incrementAndGet());
                        if (defectiveServers != null) {
                          defectiveServers.remove(ssi);
                        }
                        if (LOG.isDebugEnabled()) {
                          LOG.debug("{} successfully.", logMsg);
                        }
                      } else {
                        recordFailedBlocks(
                            failedBlockSendTracker, serverToBlocks, ssi, response.getStatusCode());
                        if (defectiveServers != null) {
                          defectiveServers.add(ssi);
                        }
                        LOG.warn(
                            "{}, it failed wth statusCode[{}]", logMsg, response.getStatusCode());
                        return false;
                      }
                    } catch (Exception e) {
                      recordFailedBlocks(
                          failedBlockSendTracker, serverToBlocks, ssi, StatusCode.INTERNAL_ERROR);
                      if (defectiveServers != null) {
                        defectiveServers.add(ssi);
                      }
                      LOG.warn(
                          "Send: "
                              + serverToBlockIds.get(ssi).size()
                              + " blocks to ["
                              + ssi.getId()
                              + "] failed.",
                          e);
                      return false;
                    }
                    return true;
                  },
                  dataTransferPool)
              .exceptionally(
                  ex -> {
                    LOG.error("Unexpected exceptions occurred while sending shuffle data", ex);
                    return false;
                  });
      futures.add(future);
    }

    boolean result = ClientUtils.waitUntilDoneOrFail(futures, allowFastFail);
    if (!result) {
      LOG.error(
          "Some shuffle data can't be sent to shuffle-server, is fast fail: {}, cancelled task size: {}",
          allowFastFail,
          futures.size());
    }
    return result;
  }

  void recordFailedBlocks(
      FailedBlockSendTracker blockIdsSendFailTracker,
      Map<ShuffleServerInfo, Map<Integer, Map<Integer, List<ShuffleBlockInfo>>>> serverToBlocks,
      ShuffleServerInfo shuffleServerInfo,
      StatusCode statusCode) {
    serverToBlocks.getOrDefault(shuffleServerInfo, Collections.emptyMap()).values().stream()
        .flatMap(innerMap -> innerMap.values().stream())
        .flatMap(List::stream)
        .forEach(block -> blockIdsSendFailTracker.add(block, shuffleServerInfo, statusCode));
  }

  void genServerToBlocks(
      ShuffleBlockInfo sbi,
      List<ShuffleServerInfo> serverList,
      int replicaNum,
      Collection<ShuffleServerInfo> excludeServers,
      Map<ShuffleServerInfo, Map<Integer, Map<Integer, List<ShuffleBlockInfo>>>> serverToBlocks,
      Map<ShuffleServerInfo, List<Long>> serverToBlockIds,
      boolean excludeDefectiveServers) {
    if (replicaNum <= 0) {
      return;
    }

    Stream<ShuffleServerInfo> servers;
    if (excludeDefectiveServers && CollectionUtils.isNotEmpty(defectiveServers)) {
      servers =
          Stream.concat(
              serverList.stream().filter(x -> !defectiveServers.contains(x)),
              serverList.stream().filter(defectiveServers::contains));
    } else {
      servers = serverList.stream();
    }
    if (excludeServers != null) {
      servers = servers.filter(x -> !excludeServers.contains(x));
    }

    Stream<ShuffleServerInfo> selected = servers.limit(replicaNum);
    if (excludeServers != null) {
      selected = selected.peek(excludeServers::add);
    }
    selected.forEach(
        ssi -> {
          serverToBlockIds.computeIfAbsent(ssi, id -> Lists.newArrayList()).add(sbi.getBlockId());
          serverToBlocks
              .computeIfAbsent(ssi, id -> Maps.newHashMap())
              .computeIfAbsent(sbi.getShuffleId(), id -> Maps.newHashMap())
              .computeIfAbsent(sbi.getPartitionId(), id -> Lists.newArrayList())
              .add(sbi);
        });
  }

  @Override
  public SendShuffleDataResult sendShuffleData(
      String appId,
      List<ShuffleBlockInfo> shuffleBlockInfoList,
      Supplier<Boolean> needCancelRequest) {
    return sendShuffleData(appId, 0, shuffleBlockInfoList, needCancelRequest);
  }

  /** The batch of sending belongs to the same task */
  @Override
  public SendShuffleDataResult sendShuffleData(
      String appId,
      int stageAttemptNumber,
      List<ShuffleBlockInfo> shuffleBlockInfoList,
      Supplier<Boolean> needCancelRequest) {

    // shuffleServer -> shuffleId -> partitionId -> blocks
    Map<ShuffleServerInfo, Map<Integer, Map<Integer, List<ShuffleBlockInfo>>>>
        primaryServerToBlocks = Maps.newHashMap();
    Map<ShuffleServerInfo, Map<Integer, Map<Integer, List<ShuffleBlockInfo>>>>
        secondaryServerToBlocks = Maps.newHashMap();
    Map<ShuffleServerInfo, List<Long>> primaryServerToBlockIds = Maps.newHashMap();
    Map<ShuffleServerInfo, List<Long>> secondaryServerToBlockIds = Maps.newHashMap();

    // send shuffle block to shuffle server
    // for all ShuffleBlockInfo, create the data structure as shuffleServer -> shuffleId ->
    // partitionId -> blocks
    // it will be helpful to send rpc request to shuffleServer

    // In order to reduce the data to send in quorum protocol,
    // we split these blocks into two rounds: primary and secondary.
    // The primary round contains [0, replicaWrite) replicas,
    // which is minimum number when there is no sending server failures.
    // The secondary round contains [replicaWrite, replica) replicas,
    // which is minimum number when there is at most *replicaWrite - replica* sending server
    // failures.
    for (ShuffleBlockInfo sbi : shuffleBlockInfoList) {
      List<ShuffleServerInfo> allServers = sbi.getShuffleServerInfos();
      if (replicaSkipEnabled) {
        Set<ShuffleServerInfo> excludeServers = Sets.newHashSet();
        genServerToBlocks(
            sbi,
            allServers,
            replicaWrite,
            excludeServers,
            primaryServerToBlocks,
            primaryServerToBlockIds,
            true);
        genServerToBlocks(
            sbi,
            allServers,
            replica - replicaWrite,
            excludeServers,
            secondaryServerToBlocks,
            secondaryServerToBlockIds,
            false);
      } else {
        // When replicaSkip is disabled, we send data to all replicas within one round.
        genServerToBlocks(
            sbi,
            allServers,
            allServers.size(),
            null,
            primaryServerToBlocks,
            primaryServerToBlockIds,
            false);
      }
    }
    /** Records the ShuffleServer that successfully or failed to send blocks */
    // we assume that most of the blocks can be sent successfully
    // so initialize the map at first without concurrency insurance
    // AtomicInteger is enough to reflect value changes in other threads
    Map<Long, AtomicInteger> blockIdsSendSuccessTracker = Maps.newHashMap();
    primaryServerToBlockIds
        .values()
        .forEach(
            blockList ->
                blockList.forEach(
                    block ->
                        blockIdsSendSuccessTracker.computeIfAbsent(
                            block, id -> new AtomicInteger(0))));
    secondaryServerToBlockIds
        .values()
        .forEach(
            blockList ->
                blockList.forEach(
                    block ->
                        blockIdsSendSuccessTracker.computeIfAbsent(
                            block, id -> new AtomicInteger(0))));
    FailedBlockSendTracker blockIdsSendFailTracker = new FailedBlockSendTracker();

    // sent the primary round of blocks.
    boolean isAllSuccess =
        sendShuffleDataAsync(
            appId,
            stageAttemptNumber,
            primaryServerToBlocks,
            primaryServerToBlockIds,
            blockIdsSendSuccessTracker,
            blockIdsSendFailTracker,
            secondaryServerToBlocks.isEmpty(),
            needCancelRequest);

    // The secondary round of blocks is sent only when the primary group issues failed sending.
    // This should be infrequent.
    // Even though the secondary round may send blocks more than replicaWrite replicas,
    // we do not apply complicated skipping logic, because server crash is rare in production
    // environment.
    if (!isAllSuccess && !secondaryServerToBlocks.isEmpty() && !needCancelRequest.get()) {
      LOG.info("The sending of primary round is failed partially, so start the secondary round");
      sendShuffleDataAsync(
          appId,
          stageAttemptNumber,
          secondaryServerToBlocks,
          secondaryServerToBlockIds,
          blockIdsSendSuccessTracker,
          blockIdsSendFailTracker,
          true,
          needCancelRequest);
    }

    Set<Long> blockIdsSendSuccessSet = Sets.newHashSet();
    blockIdsSendSuccessTracker
        .entrySet()
        .forEach(
            successBlockId -> {
              if (successBlockId.getValue().get() >= replicaWrite) {
                blockIdsSendSuccessSet.add(successBlockId.getKey());
                // If the replicaWrite to be sent is reached,
                // no matter whether the block fails to be sent or not,
                // the block is considered to have been sent successfully and is removed from the
                // failed block tracker
                blockIdsSendFailTracker.remove(successBlockId.getKey());
              }
            });
    return new SendShuffleDataResult(blockIdsSendSuccessSet, blockIdsSendFailTracker);
  }

  /**
   * This method will wait until all shuffle data have been flushed to durable storage in assigned
   * shuffle servers.
   *
   * @param shuffleServerInfoSet
   * @param appId
   * @param shuffleId
   * @param numMaps
   * @return
   */
  @Override
  public boolean sendCommit(
      Set<ShuffleServerInfo> shuffleServerInfoSet, String appId, int shuffleId, int numMaps) {
    ForkJoinPool forkJoinPool =
        new ForkJoinPool(
            dataCommitPoolSize == -1 ? shuffleServerInfoSet.size() : dataCommitPoolSize);
    AtomicInteger successfulCommit = new AtomicInteger(0);
    try {
      forkJoinPool
          .submit(
              () -> {
                shuffleServerInfoSet
                    .parallelStream()
                    .forEach(
                        ssi -> {
                          RssSendCommitRequest request = new RssSendCommitRequest(appId, shuffleId);
                          String errorMsg =
                              "Failed to commit shuffle data to "
                                  + ssi
                                  + " for shuffleId["
                                  + shuffleId
                                  + "]";
                          long startTime = System.currentTimeMillis();
                          try {
                            RssSendCommitResponse response =
                                getShuffleServerClient(ssi).sendCommit(request);
                            if (response.getStatusCode() == StatusCode.SUCCESS) {
                              int commitCount = response.getCommitCount();
                              LOG.info(
                                  "Successfully sendCommit for appId["
                                      + appId
                                      + "], shuffleId["
                                      + shuffleId
                                      + "] to ShuffleServer["
                                      + ssi.getId()
                                      + "], cost "
                                      + (System.currentTimeMillis() - startTime)
                                      + " ms, got committed maps["
                                      + commitCount
                                      + "], map number of stage is "
                                      + numMaps);
                              if (commitCount >= numMaps) {
                                RssFinishShuffleResponse rfsResponse =
                                    getShuffleServerClient(ssi)
                                        .finishShuffle(
                                            new RssFinishShuffleRequest(appId, shuffleId));
                                if (rfsResponse.getStatusCode() != StatusCode.SUCCESS) {
                                  String msg =
                                      "Failed to finish shuffle to "
                                          + ssi
                                          + " for shuffleId["
                                          + shuffleId
                                          + "] with statusCode "
                                          + rfsResponse.getStatusCode();
                                  LOG.error(msg);
                                  throw new Exception(msg);
                                } else {
                                  LOG.info(
                                      "Successfully finish shuffle to "
                                          + ssi
                                          + " for shuffleId["
                                          + shuffleId
                                          + "]");
                                }
                              }
                            } else {
                              String msg =
                                  errorMsg + " with statusCode " + response.getStatusCode();
                              LOG.error(msg);
                              throw new Exception(msg);
                            }
                            successfulCommit.incrementAndGet();
                          } catch (Exception e) {
                            LOG.error(errorMsg, e);
                          }
                        });
              })
          .join();
    } finally {
      forkJoinPool.shutdownNow();
    }

    // check if every commit/finish call is successful
    return successfulCommit.get() == shuffleServerInfoSet.size();
  }

  @Override
  public void registerShuffle(
      ShuffleServerInfo shuffleServerInfo,
      String appId,
      int shuffleId,
      List<PartitionRange> partitionRanges,
      RemoteStorageInfo remoteStorage,
      ShuffleDataDistributionType dataDistributionType,
      int maxConcurrencyPerPartitionToWrite,
      int stageAttemptNumber,
      String keyClassName,
      String valueClassName,
      String comparatorClassName,
      int mergedBlockSize,
      String mergeClassLoader) {
    String user = null;
    try {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (Exception e) {
      LOG.error("Error on getting user from ugi.", e);
    }

    RssRegisterShuffleRequest request =
        new RssRegisterShuffleRequest(
            appId,
            shuffleId,
            partitionRanges,
            remoteStorage,
            user,
            dataDistributionType,
            maxConcurrencyPerPartitionToWrite,
            stageAttemptNumber,
            keyClassName,
            valueClassName,
            comparatorClassName,
            mergedBlockSize,
            mergeClassLoader);
    RssRegisterShuffleResponse response =
        getShuffleServerClient(shuffleServerInfo).registerShuffle(request);

    String msg =
        "Error happened when registerShuffle with appId["
            + appId
            + "], shuffleId["
            + shuffleId
            + "], "
            + shuffleServerInfo;
    throwExceptionIfNecessary(response, msg);
    addShuffleServer(appId, shuffleId, shuffleServerInfo);
  }

  @Override
  public void registerCoordinators(String coordinators, long retryIntervalMs, int retryTimes) {
    coordinatorClients =
        coordinatorClientFactory.createCoordinatorClient(
            ClientType.valueOf(this.clientType),
            coordinators,
            retryIntervalMs,
            retryTimes,
            this.heartBeatThreadNum);
  }

  @Override
  public Map<String, String> fetchClientConf(int timeoutMs) {
    if (coordinatorClients == null) {
      return Maps.newHashMap();
    }
    try {
      return coordinatorClients
          .fetchClientConf(new RssFetchClientConfRequest(timeoutMs))
          .getClientConf();
    } catch (RssException e) {
      return Maps.newHashMap();
    }
  }

  @Override
  public RemoteStorageInfo fetchRemoteStorage(String appId) {
    if (coordinatorClients == null) {
      return new RemoteStorageInfo("");
    }
    try {
      return coordinatorClients.fetchRemoteStorage(new RssFetchRemoteStorageRequest(appId));
    } catch (RssException e) {
      return new RemoteStorageInfo("");
    }
  }

  @Override
  public ShuffleAssignmentsInfo getShuffleAssignments(
      String appId,
      int shuffleId,
      int partitionNum,
      int partitionNumPerRange,
      Set<String> requiredTags,
      int assignmentShuffleServerNumber,
      int estimateTaskConcurrency,
      Set<String> faultyServerIds,
      int stageId,
      int stageAttemptNumber,
      boolean reassign,
      long retryIntervalMs,
      int retryTimes) {
    RssGetShuffleAssignmentsRequest request =
        new RssGetShuffleAssignmentsRequest(
            appId,
            shuffleId,
            partitionNum,
            partitionNumPerRange,
            replica,
            requiredTags,
            assignmentShuffleServerNumber,
            estimateTaskConcurrency,
            faultyServerIds,
            stageId,
            stageAttemptNumber,
            reassign);

    RssGetShuffleAssignmentsResponse response =
        new RssGetShuffleAssignmentsResponse(StatusCode.INTERNAL_ERROR);
    try {
      if (coordinatorClients != null) {
        response = coordinatorClients.getShuffleAssignments(request, retryIntervalMs, retryTimes);
      }
    } catch (RssException e) {
      String msg =
          "Error happened when getShuffleAssignments with appId["
              + appId
              + "], shuffleId["
              + shuffleId
              + "], numMaps["
              + partitionNum
              + "], partitionNumPerRange["
              + partitionNumPerRange
              + "] to coordinator. "
              + "Error message: "
              + response.getMessage();
      LOG.error(msg);
      throw new RssException(msg);
    }

    return new ShuffleAssignmentsInfo(
        response.getPartitionToServers(), response.getServerToPartitionRanges());
  }

  @Override
  public void reportShuffleResult(
      Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds,
      String appId,
      int shuffleId,
      long taskAttemptId,
      int bitmapNum) {
    // record blockId count for quora check,but this is not a good realization.
    Map<Long, Integer> blockReportTracker = createBlockReportTracker(serverToPartitionToBlockIds);
    for (Map.Entry<ShuffleServerInfo, Map<Integer, Set<Long>>> entry :
        serverToPartitionToBlockIds.entrySet()) {
      Map<Integer, Set<Long>> requestBlockIds = entry.getValue();
      if (requestBlockIds.isEmpty()) {
        continue;
      }
      RssReportShuffleResultRequest request =
          new RssReportShuffleResultRequest(
              appId,
              shuffleId,
              taskAttemptId,
              requestBlockIds.entrySet().stream()
                  .collect(Collectors.toMap(Map.Entry::getKey, e -> new ArrayList<>(e.getValue()))),
              bitmapNum);
      ShuffleServerInfo ssi = entry.getKey();
      try {
        RssReportShuffleResultResponse response =
            getShuffleServerClient(ssi).reportShuffleResult(request);
        if (response.getStatusCode() == StatusCode.SUCCESS) {
          LOG.info(
              "Report shuffle result to "
                  + ssi
                  + " for appId["
                  + appId
                  + "], shuffleId["
                  + shuffleId
                  + "] successfully");
        } else {
          LOG.warn(
              "Report shuffle result to "
                  + ssi
                  + " for appId["
                  + appId
                  + "], shuffleId["
                  + shuffleId
                  + "] failed with "
                  + response.getStatusCode());
          recordFailedBlockIds(blockReportTracker, requestBlockIds);
        }
      } catch (Exception e) {
        LOG.warn(
            "Report shuffle result is failed to "
                + ssi
                + " for appId["
                + appId
                + "], shuffleId["
                + shuffleId
                + "]");
        recordFailedBlockIds(blockReportTracker, requestBlockIds);
      }
    }
    if (blockReportTracker.values().stream().anyMatch(cnt -> cnt < replicaWrite)) {
      throw new RssException(
          "Quorum check of report shuffle result is failed for appId["
              + appId
              + "], shuffleId["
              + shuffleId
              + "]");
    }
  }

  private void recordFailedBlockIds(
      Map<Long, Integer> blockReportTracker, Map<Integer, Set<Long>> requestBlockIds) {
    requestBlockIds.values().stream()
        .flatMap(Set::stream)
        .forEach(blockId -> blockReportTracker.merge(blockId, -1, Integer::sum));
  }

  private Map<Long, Integer> createBlockReportTracker(
      Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds) {
    Map<Long, Integer> blockIdCount = new HashMap<>();
    for (Map<Integer, Set<Long>> partitionToBlockIds : serverToPartitionToBlockIds.values()) {
      for (Set<Long> blockIds : partitionToBlockIds.values()) {
        for (Long blockId : blockIds) {
          blockIdCount.put(blockId, blockIdCount.getOrDefault(blockId, 0) + 1);
        }
      }
    }
    return blockIdCount;
  }

  @Override
  public Roaring64NavigableMap getShuffleResult(
      String clientType,
      Set<ShuffleServerInfo> shuffleServerInfoSet,
      String appId,
      int shuffleId,
      int partitionId) {
    RssGetShuffleResultRequest request =
        new RssGetShuffleResultRequest(appId, shuffleId, partitionId, blockIdLayout);
    boolean isSuccessful = false;
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    int successCnt = 0;
    for (ShuffleServerInfo ssi : shuffleServerInfoSet) {
      try {
        RssGetShuffleResultResponse response =
            getShuffleServerClient(ssi).getShuffleResult(request);
        if (response.getStatusCode() == StatusCode.SUCCESS) {
          // merge into blockIds from multiple servers.
          Roaring64NavigableMap blockIdBitmapOfServer = response.getBlockIdBitmap();
          blockIdBitmap.or(blockIdBitmapOfServer);
          successCnt++;
          if (successCnt >= replicaRead) {
            isSuccessful = true;
            break;
          }
        }
      } catch (Exception e) {
        LOG.warn(
            "Get shuffle result is failed from "
                + ssi
                + " for appId["
                + appId
                + "], shuffleId["
                + shuffleId
                + "]");
      }
    }
    if (!isSuccessful) {
      throw new RssFetchFailedException(
          "Get shuffle result is failed for appId[" + appId + "], shuffleId[" + shuffleId + "]");
    }
    return blockIdBitmap;
  }

  @Override
  public Roaring64NavigableMap getShuffleResultForMultiPart(
      String clientType,
      Map<ShuffleServerInfo, Set<Integer>> serverToPartitions,
      String appId,
      int shuffleId,
      Set<Integer> failedPartitions,
      PartitionDataReplicaRequirementTracking replicaRequirementTracking) {
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Set<Integer> allRequestedPartitionIds = new HashSet<>();
    for (Map.Entry<ShuffleServerInfo, Set<Integer>> entry : serverToPartitions.entrySet()) {
      ShuffleServerInfo shuffleServerInfo = entry.getKey();
      Set<Integer> requestPartitions = Sets.newHashSet();
      for (Integer partitionId : entry.getValue()) {
        if (!replicaRequirementTracking.isSatisfied(partitionId, replicaRead)) {
          requestPartitions.add(partitionId);
        }
      }
      allRequestedPartitionIds.addAll(requestPartitions);
      RssGetShuffleResultForMultiPartRequest request =
          new RssGetShuffleResultForMultiPartRequest(
              appId, shuffleId, requestPartitions, blockIdLayout);
      try {
        RssGetShuffleResultResponse response =
            getShuffleServerClient(shuffleServerInfo).getShuffleResultForMultiPart(request);
        if (response.getStatusCode() == StatusCode.SUCCESS) {
          // merge into blockIds from multiple servers.
          Roaring64NavigableMap blockIdBitmapOfServer = response.getBlockIdBitmap();
          blockIdBitmap.or(blockIdBitmapOfServer);
          for (Integer partitionId : requestPartitions) {
            replicaRequirementTracking.markPartitionOfServerSuccessful(
                partitionId, shuffleServerInfo);
          }
        }
      } catch (Exception e) {
        failedPartitions.addAll(requestPartitions);
        LOG.warn(
            "Get shuffle result is failed from "
                + shuffleServerInfo
                + " for appId["
                + appId
                + "], shuffleId["
                + shuffleId
                + "], requestPartitions"
                + requestPartitions,
            e);
      }
    }
    boolean isSuccessful =
        allRequestedPartitionIds.stream()
            .allMatch(x -> replicaRequirementTracking.isSatisfied(x, replicaRead));
    if (!isSuccessful) {
      LOG.error("Failed to meet replica requirement: {}", replicaRequirementTracking);
      throw new RssFetchFailedException(
          "Get shuffle result is failed for appId[" + appId + "], shuffleId[" + shuffleId + "]");
    }
    return blockIdBitmap;
  }

  @Override
  public void registerApplicationInfo(String appId, long timeoutMs, String user) {
    RssApplicationInfoRequest request = new RssApplicationInfoRequest(appId, timeoutMs, user);
    if (coordinatorClients != null) {
      coordinatorClients.registerApplicationInfo(request, timeoutMs);
    }
  }

  @Override
  public void sendAppHeartbeat(String appId, long timeoutMs) {
    RssAppHeartBeatRequest request = new RssAppHeartBeatRequest(appId, timeoutMs);
    Set<ShuffleServerInfo> allShuffleServers = getAllShuffleServers(appId);

    ThreadUtils.executeTasks(
        heartBeatExecutorService,
        allShuffleServers,
        shuffleServerInfo -> {
          try {
            ShuffleServerClient client =
                ShuffleServerClientFactory.getInstance()
                    .getShuffleServerClient(clientType, shuffleServerInfo, rssConf);
            RssAppHeartBeatResponse response = client.sendHeartBeat(request);
            if (response.getStatusCode() != StatusCode.SUCCESS) {
              LOG.warn("Failed to send heartbeat to " + shuffleServerInfo);
            }
          } catch (Exception e) {
            LOG.warn("Error happened when send heartbeat to " + shuffleServerInfo, e);
          }
          return null;
        },
        timeoutMs,
        "send heartbeat to shuffle server");
    if (coordinatorClients != null) {
      coordinatorClients.sendAppHeartBeat(request, timeoutMs);
    }
  }

  @Override
  public void close() {
    heartBeatExecutorService.shutdownNow();
    if (coordinatorClients != null) {
      coordinatorClients.close();
    }
    dataTransferPool.shutdownNow();
  }

  @Override
  public void unregisterShuffle(String appId, int shuffleId) {
    int unregisterTimeMs = unregisterTimeSec * 1000;
    RssUnregisterShuffleRequest request =
        new RssUnregisterShuffleRequest(appId, shuffleId, unregisterRequestTimeSec);

    Map<Integer, Set<ShuffleServerInfo>> appServerMap = shuffleServerInfoMap.get(appId);
    if (appServerMap == null) {
      return;
    }
    Set<ShuffleServerInfo> shuffleServerInfos = appServerMap.get(shuffleId);
    if (shuffleServerInfos == null) {
      return;
    }
    LOG.info(
        "Unregistering shuffleId[{}] from {} shuffle servers with individual timeout[{}s] and overall timeout[{}s]",
        shuffleId,
        shuffleServerInfos.size(),
        unregisterRequestTimeSec,
        unregisterTimeSec);

    ExecutorService executorService = null;
    try {
      int concurrency = Math.min(unregisterThreadPoolSize, shuffleServerInfos.size());
      executorService = ThreadUtils.getDaemonFixedThreadPool(concurrency, "unregister-shuffle");

      ThreadUtils.executeTasks(
          executorService,
          shuffleServerInfos,
          shuffleServerInfo -> {
            try {
              ShuffleServerClient client =
                  ShuffleServerClientFactory.getInstance()
                      .getShuffleServerClient(clientType, shuffleServerInfo, rssConf);
              RssUnregisterShuffleResponse response = client.unregisterShuffle(request);
              if (response.getStatusCode() == StatusCode.SUCCESS) {
                LOG.info("Successfully unregistered shuffle from {}", shuffleServerInfo);
              } else {
                LOG.warn("Failed to unregister shuffle from {}", shuffleServerInfo);
              }
            } catch (Exception e) {
              // this request observed the unregisterRequestTimeSec timeout
              if (e instanceof StatusRuntimeException
                  && ((StatusRuntimeException) e).getStatus().getCode()
                      == Status.DEADLINE_EXCEEDED.getCode()) {
                LOG.warn(
                    "Timeout occurred while unregistering from {}. The request timeout is {}s: {}",
                    shuffleServerInfo,
                    unregisterRequestTimeSec,
                    ((StatusRuntimeException) e).getStatus().getDescription());
              } else {
                LOG.warn("Error while unregistering from {}", shuffleServerInfo, e);
              }
            }
            return null;
          },
          unregisterTimeMs,
          "unregister shuffle server",
          String.format(
              "Please consider increasing the thread pool size (%s) or the overall timeout (%ss) "
                  + "if you still think the request timeout (%ss) is sensible.",
              unregisterThreadPoolSize, unregisterTimeSec, unregisterRequestTimeSec));

    } finally {
      if (executorService != null) {
        executorService.shutdownNow();
      }
      removeShuffleServer(appId, shuffleId);
    }
  }

  @Override
  public void unregisterShuffle(String appId) {
    int unregisterTimeMs = unregisterTimeSec * 1000;
    RssUnregisterShuffleByAppIdRequest request =
        new RssUnregisterShuffleByAppIdRequest(appId, unregisterRequestTimeSec);

    if (appId == null) {
      return;
    }
    Map<Integer, Set<ShuffleServerInfo>> appServerMap = shuffleServerInfoMap.get(appId);
    if (appServerMap == null) {
      return;
    }
    Set<ShuffleServerInfo> shuffleServerInfos = getAllShuffleServers(appId);
    LOG.info(
        "Unregistering shuffles of appId[{}] from {} shuffle servers with individual timeout[{}s] and overall timeout[{}s]",
        appId,
        shuffleServerInfos.size(),
        unregisterRequestTimeSec,
        unregisterTimeSec);

    ExecutorService executorService = null;
    try {
      int concurrency = Math.min(unregisterThreadPoolSize, shuffleServerInfos.size());
      executorService = ThreadUtils.getDaemonFixedThreadPool(concurrency, "unregister-shuffle");

      ThreadUtils.executeTasks(
          executorService,
          shuffleServerInfos,
          shuffleServerInfo -> {
            try {
              ShuffleServerClient client =
                  ShuffleServerClientFactory.getInstance()
                      .getShuffleServerClient(clientType, shuffleServerInfo, rssConf);
              RssUnregisterShuffleByAppIdResponse response =
                  client.unregisterShuffleByAppId(request);
              if (response.getStatusCode() == StatusCode.SUCCESS) {
                LOG.info("Successfully unregistered shuffle from {}", shuffleServerInfo);
              } else {
                LOG.warn("Failed to unregister shuffle from {}", shuffleServerInfo);
              }
            } catch (Exception e) {
              // this request observed the unregisterRequestTimeSec timeout
              if (e instanceof StatusRuntimeException
                  && ((StatusRuntimeException) e).getStatus().getCode()
                      == Status.DEADLINE_EXCEEDED.getCode()) {
                LOG.warn(
                    "Timeout occurred while unregistering from {}. The request timeout is {}s: {}",
                    shuffleServerInfo,
                    unregisterRequestTimeSec,
                    ((StatusRuntimeException) e).getStatus().getDescription());
              } else {
                LOG.warn("Error while unregistering from {}", shuffleServerInfo, e);
              }
            }
            return null;
          },
          unregisterTimeMs,
          "unregister shuffle server",
          String.format(
              "Please consider increasing the thread pool size (%s) or the overall timeout (%ss) "
                  + "if you still think the request timeout (%ss) is sensible.",
              unregisterThreadPoolSize, unregisterTimeSec, unregisterRequestTimeSec));

    } finally {
      if (executorService != null) {
        executorService.shutdownNow();
      }
      shuffleServerInfoMap.remove(appId);
      ShuffleServerClientFactory.getInstance()
          .cleanUselessShuffleServerClients(getAllShuffleServers());
    }
  }

  @Override
  public void startSortMerge(
      Set<ShuffleServerInfo> serverInfos,
      String appId,
      int shuffleId,
      int partitionId,
      Roaring64NavigableMap expectedBlockIds) {
    RssStartSortMergeRequest request =
        new RssStartSortMergeRequest(appId, shuffleId, partitionId, expectedBlockIds);
    boolean atLeastOneSucceeful = false;
    for (ShuffleServerInfo ssi : serverInfos) {
      RssStartSortMergeResponse response = getShuffleServerClient(ssi).startSortMerge(request);
      if (response.getStatusCode() == StatusCode.SUCCESS) {
        atLeastOneSucceeful = true;
        LOG.info(
            "Report unique blocks to "
                + ssi
                + " for appId["
                + appId
                + "], shuffleId["
                + shuffleId
                + "], partitionIds["
                + partitionId
                + "] successfully");
      } else {
        LOG.warn(
            "Report unique blocks to "
                + ssi
                + " for appId["
                + appId
                + "], shuffleId["
                + shuffleId
                + "], partitionIds["
                + partitionId
                + "] failed with "
                + response.getStatusCode());
      }
    }
    if (!atLeastOneSucceeful) {
      throw new RssFetchFailedException(
          "Report Unique Blocks failed for appId["
              + appId
              + "], shuffleId["
              + shuffleId
              + "], partitionIds["
              + partitionId
              + "]");
    }
  }

  private void throwExceptionIfNecessary(ClientResponse response, String errorMsg) {
    if (response != null && response.getStatusCode() != StatusCode.SUCCESS) {
      LOG.error(errorMsg);
      throw new RssException(errorMsg);
    }
  }

  Set<ShuffleServerInfo> getAllShuffleServers(String appId) {
    Map<Integer, Set<ShuffleServerInfo>> appServerMap = shuffleServerInfoMap.get(appId);
    if (appServerMap == null) {
      return Collections.emptySet();
    }
    Set<ShuffleServerInfo> serverInfos = Sets.newHashSet();
    appServerMap.values().forEach(serverInfos::addAll);
    return serverInfos;
  }

  Set<ShuffleServerInfo> getAllShuffleServers() {
    Set<ShuffleServerInfo> serverInfos = Sets.newHashSet();
    shuffleServerInfoMap
        .values()
        .forEach(appServerMap -> appServerMap.values().forEach(serverInfos::addAll));
    return serverInfos;
  }

  @VisibleForTesting
  public ShuffleServerClient getShuffleServerClient(ShuffleServerInfo shuffleServerInfo) {
    return ShuffleServerClientFactory.getInstance()
        .getShuffleServerClient(clientType, shuffleServerInfo, rssConf);
  }

  @VisibleForTesting
  Set<ShuffleServerInfo> getDefectiveServers() {
    return defectiveServers;
  }

  void addShuffleServer(String appId, int shuffleId, ShuffleServerInfo serverInfo) {
    Map<Integer, Set<ShuffleServerInfo>> appServerMap = shuffleServerInfoMap.get(appId);
    if (appServerMap == null) {
      appServerMap = JavaUtils.newConcurrentMap();
      shuffleServerInfoMap.put(appId, appServerMap);
    }
    Set<ShuffleServerInfo> shuffleServerInfos = appServerMap.get(shuffleId);
    if (shuffleServerInfos == null) {
      shuffleServerInfos = Sets.newConcurrentHashSet();
      appServerMap.put(shuffleId, shuffleServerInfos);
    }
    shuffleServerInfos.add(serverInfo);
  }

  @VisibleForTesting
  void removeShuffleServer(String appId, int shuffleId) {
    Map<Integer, Set<ShuffleServerInfo>> appServerMap = shuffleServerInfoMap.get(appId);
    if (appServerMap != null) {
      Set<ShuffleServerInfo> removed = appServerMap.remove(shuffleId);
      if (removed != null) {
        ShuffleServerClientFactory.getInstance()
            .cleanUselessShuffleServerClients(getAllShuffleServers());
      }
    }
  }
}
