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

package org.apache.uniffle.client.impl.grpc;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.StatusRuntimeException;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.request.RetryableRequest;
import org.apache.uniffle.client.request.RssAppHeartBeatRequest;
import org.apache.uniffle.client.request.RssFinishShuffleRequest;
import org.apache.uniffle.client.request.RssGetInMemoryShuffleDataRequest;
import org.apache.uniffle.client.request.RssGetShuffleDataRequest;
import org.apache.uniffle.client.request.RssGetShuffleIndexRequest;
import org.apache.uniffle.client.request.RssGetShuffleResultForMultiPartRequest;
import org.apache.uniffle.client.request.RssGetShuffleResultRequest;
import org.apache.uniffle.client.request.RssGetSortedShuffleDataRequest;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssReportShuffleResultRequest;
import org.apache.uniffle.client.request.RssSendCommitRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.client.request.RssStartSortMergeRequest;
import org.apache.uniffle.client.request.RssUnregisterShuffleByAppIdRequest;
import org.apache.uniffle.client.request.RssUnregisterShuffleRequest;
import org.apache.uniffle.client.response.RssAppHeartBeatResponse;
import org.apache.uniffle.client.response.RssFinishShuffleResponse;
import org.apache.uniffle.client.response.RssGetInMemoryShuffleDataResponse;
import org.apache.uniffle.client.response.RssGetShuffleDataResponse;
import org.apache.uniffle.client.response.RssGetShuffleIndexResponse;
import org.apache.uniffle.client.response.RssGetShuffleResultResponse;
import org.apache.uniffle.client.response.RssGetSortedShuffleDataResponse;
import org.apache.uniffle.client.response.RssRegisterShuffleResponse;
import org.apache.uniffle.client.response.RssReportShuffleResultResponse;
import org.apache.uniffle.client.response.RssSendCommitResponse;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.client.response.RssStartSortMergeResponse;
import org.apache.uniffle.client.response.RssUnregisterShuffleByAppIdResponse;
import org.apache.uniffle.client.response.RssUnregisterShuffleResponse;
import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.NotRetryException;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.exception.RssFetchFailedException;
import org.apache.uniffle.common.netty.buffer.NettyManagedBuffer;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.OutputUtils;
import org.apache.uniffle.common.util.RetryUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.proto.RssProtos.AppHeartBeatRequest;
import org.apache.uniffle.proto.RssProtos.AppHeartBeatResponse;
import org.apache.uniffle.proto.RssProtos.FinishShuffleRequest;
import org.apache.uniffle.proto.RssProtos.FinishShuffleResponse;
import org.apache.uniffle.proto.RssProtos.GetLocalShuffleDataRequest;
import org.apache.uniffle.proto.RssProtos.GetLocalShuffleDataResponse;
import org.apache.uniffle.proto.RssProtos.GetLocalShuffleIndexRequest;
import org.apache.uniffle.proto.RssProtos.GetLocalShuffleIndexResponse;
import org.apache.uniffle.proto.RssProtos.GetMemoryShuffleDataRequest;
import org.apache.uniffle.proto.RssProtos.GetMemoryShuffleDataResponse;
import org.apache.uniffle.proto.RssProtos.GetShuffleResultForMultiPartRequest;
import org.apache.uniffle.proto.RssProtos.GetShuffleResultForMultiPartResponse;
import org.apache.uniffle.proto.RssProtos.GetShuffleResultRequest;
import org.apache.uniffle.proto.RssProtos.GetShuffleResultResponse;
import org.apache.uniffle.proto.RssProtos.MergeContext;
import org.apache.uniffle.proto.RssProtos.PartitionToBlockIds;
import org.apache.uniffle.proto.RssProtos.RemoteStorage;
import org.apache.uniffle.proto.RssProtos.RemoteStorageConfItem;
import org.apache.uniffle.proto.RssProtos.ReportShuffleResultRequest;
import org.apache.uniffle.proto.RssProtos.ReportShuffleResultResponse;
import org.apache.uniffle.proto.RssProtos.RequireBufferRequest;
import org.apache.uniffle.proto.RssProtos.RequireBufferResponse;
import org.apache.uniffle.proto.RssProtos.SendShuffleDataRequest;
import org.apache.uniffle.proto.RssProtos.SendShuffleDataResponse;
import org.apache.uniffle.proto.RssProtos.ShuffleBlock;
import org.apache.uniffle.proto.RssProtos.ShuffleCommitRequest;
import org.apache.uniffle.proto.RssProtos.ShuffleCommitResponse;
import org.apache.uniffle.proto.RssProtos.ShuffleData;
import org.apache.uniffle.proto.RssProtos.ShuffleDataBlockSegment;
import org.apache.uniffle.proto.RssProtos.ShufflePartitionRange;
import org.apache.uniffle.proto.RssProtos.ShuffleRegisterRequest;
import org.apache.uniffle.proto.RssProtos.ShuffleRegisterResponse;
import org.apache.uniffle.proto.ShuffleServerGrpc;
import org.apache.uniffle.proto.ShuffleServerGrpc.ShuffleServerBlockingStub;

import static org.apache.uniffle.common.config.RssClientConf.RSS_CLIENT_GRPC_EVENT_LOOP_THREADS;
import static org.apache.uniffle.proto.RssProtos.StatusCode.NO_BUFFER;

public class ShuffleServerGrpcClient extends GrpcClient implements ShuffleServerClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServerGrpcClient.class);
  protected static final long FAILED_REQUIRE_ID = -1;
  protected long rpcTimeout;
  private ShuffleServerBlockingStub blockingStub;
  /**
   * A single instance of the Random class is created as a member variable to be reused throughout
   * `ShuffleServerGrpcClient`. This approach has the following benefits: 1. Performance
   * optimization: It avoids the overhead of creating and destroying objects frequently, reducing
   * memory allocation and garbage collection costs. 2. Randomness: Reusing the same Random object
   * helps maintain the randomness of the generated numbers. If multiple Random objects are created
   * in a short period of time, their seeds may be the same or very close, leading to less random
   * numbers.
   */
  protected Random random = new Random();

  protected static final int BACK_OFF_BASE = 2000;
  static final List<StatusCode> NOT_RETRY_STATUS_CODES =
      Lists.newArrayList(
          StatusCode.NO_REGISTER,
          StatusCode.APP_NOT_FOUND,
          StatusCode.INTERNAL_NOT_RETRY_ERROR,
          StatusCode.EXCEED_HUGE_PARTITION_HARD_LIMIT);

  @VisibleForTesting
  public ShuffleServerGrpcClient(String host, int port) {
    this(
        host,
        port,
        RssClientConf.RPC_MAX_ATTEMPTS.defaultValue(),
        RssClientConf.RPC_TIMEOUT_MS.defaultValue(),
        true,
        0,
        0,
        0,
        -1);
  }

  public ShuffleServerGrpcClient(RssConf rssConf, String host, int port) {
    this(
        host,
        port,
        rssConf == null
            ? RssClientConf.RPC_MAX_ATTEMPTS.defaultValue()
            : rssConf.getInteger(RssClientConf.RPC_MAX_ATTEMPTS),
        rssConf == null
            ? RssClientConf.RPC_TIMEOUT_MS.defaultValue()
            : rssConf.getLong(RssClientConf.RPC_TIMEOUT_MS),
        true,
        0,
        0,
        0,
        rssConf == null ? -1 : rssConf.get(RSS_CLIENT_GRPC_EVENT_LOOP_THREADS));
  }

  public ShuffleServerGrpcClient(
      String host,
      int port,
      int maxRetryAttempts,
      long rpcTimeoutMs,
      boolean usePlaintext,
      int pageSize,
      int maxOrder,
      int smallCacheSize,
      int nettyEventLoopThreads) {
    super(
        host,
        port,
        maxRetryAttempts,
        usePlaintext,
        pageSize,
        maxOrder,
        smallCacheSize,
        nettyEventLoopThreads);
    blockingStub = ShuffleServerGrpc.newBlockingStub(channel);
    rpcTimeout = rpcTimeoutMs;
  }

  public ShuffleServerBlockingStub getBlockingStub() {
    return blockingStub.withDeadlineAfter(rpcTimeout, TimeUnit.MILLISECONDS);
  }

  private ShuffleRegisterResponse doRegisterShuffle(
      String appId,
      int shuffleId,
      List<PartitionRange> partitionRanges,
      RemoteStorageInfo remoteStorageInfo,
      String user,
      ShuffleDataDistributionType dataDistributionType,
      int maxConcurrencyPerPartitionToWrite,
      int stageAttemptNumber,
      MergeContext mergeContext,
      Map<String, String> properties) {
    ShuffleRegisterRequest.Builder reqBuilder = ShuffleRegisterRequest.newBuilder();
    reqBuilder
        .setAppId(appId)
        .setShuffleId(shuffleId)
        .setUser(user)
        .setShuffleDataDistribution(RssProtos.DataDistribution.valueOf(dataDistributionType.name()))
        .setMaxConcurrencyPerPartitionToWrite(maxConcurrencyPerPartitionToWrite)
        .addAllPartitionRanges(toShufflePartitionRanges(partitionRanges))
        .setStageAttemptNumber(stageAttemptNumber)
        .putAllProperties(properties);
    if (mergeContext != null) {
      reqBuilder.setMergeContext(mergeContext);
    }
    RemoteStorage.Builder rsBuilder = RemoteStorage.newBuilder();
    rsBuilder.setPath(remoteStorageInfo.getPath());
    Map<String, String> remoteStorageConf = remoteStorageInfo.getConfItems();
    if (!remoteStorageConf.isEmpty()) {
      RemoteStorageConfItem.Builder rsConfBuilder = RemoteStorageConfItem.newBuilder();
      for (Map.Entry<String, String> entry : remoteStorageConf.entrySet()) {
        rsConfBuilder.setKey(entry.getKey()).setValue(entry.getValue());
        rsBuilder.addRemoteStorageConf(rsConfBuilder.build());
      }
    }
    reqBuilder.setRemoteStorage(rsBuilder.build());
    return getBlockingStub().registerShuffle(reqBuilder.build());
  }

  private ShuffleCommitResponse doSendCommit(String appId, int shuffleId) {
    ShuffleCommitRequest request =
        ShuffleCommitRequest.newBuilder().setAppId(appId).setShuffleId(shuffleId).build();
    int retryNum = 0;
    while (retryNum <= maxRetryAttempts) {
      try {
        ShuffleCommitResponse response = getBlockingStub().commitShuffleTask(request);
        return response;
      } catch (Exception e) {
        retryNum++;
        LOG.warn(
            "Send commit to host["
                + host
                + "], port["
                + port
                + "] failed, try again, retryNum["
                + retryNum
                + "]",
            e);
      }
    }
    throw new RssException("Send commit to host[" + host + "], port[" + port + "] failed");
  }

  private AppHeartBeatResponse doSendHeartBeat(String appId, long timeout) {
    AppHeartBeatRequest request = AppHeartBeatRequest.newBuilder().setAppId(appId).build();
    return blockingStub.withDeadlineAfter(timeout, TimeUnit.MILLISECONDS).appHeartbeat(request);
  }

  // Only for tests
  @VisibleForTesting
  public long requirePreAllocation(
      String appId, int requireSize, int retryMax, long retryIntervalMax) throws Exception {
    return requirePreAllocation(
        appId,
        0,
        Collections.emptyList(),
        Collections.emptyList(),
        requireSize,
        retryMax,
        retryIntervalMax);
  }

  @VisibleForTesting
  public long requirePreAllocation(
      String appId,
      int shuffleId,
      List<Integer> partitionIds,
      List<Integer> partitionRequireSizes,
      int requireSize,
      int retryMax,
      long retryIntervalMax) {
    return requirePreAllocation(
            appId,
            shuffleId,
            partitionIds,
            partitionRequireSizes,
            requireSize,
            retryMax,
            retryIntervalMax,
            new AtomicReference<>(StatusCode.INTERNAL_ERROR))
        .getLeft();
  }

  public Pair<Long, List<Integer>> requirePreAllocation(
      String appId,
      int shuffleId,
      List<Integer> partitionIds,
      List<Integer> partitionRequireSizes,
      int requireSize,
      int retryMax,
      long retryIntervalMax,
      AtomicReference<StatusCode> failedStatusCodeRef) {
    RequireBufferRequest rpcRequest =
        RequireBufferRequest.newBuilder()
            .setShuffleId(shuffleId)
            .addAllPartitionIds(partitionIds)
            .addAllPartitionRequireSizes(partitionRequireSizes)
            .setAppId(appId)
            .setRequireSize(requireSize)
            .build();

    long start = System.currentTimeMillis();
    int retry = 0;
    long result = FAILED_REQUIRE_ID;
    List<Integer> needSplitPartitionIds = Collections.emptyList();
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Requiring buffer for appId: {}, shuffleId: {}, partitionIds: {} with {} bytes from {}:{}",
          appId,
          shuffleId,
          OutputUtils.listToSegment(partitionIds, 10),
          requireSize,
          host,
          port);
    }
    RequireBufferResponse rpcResponse;
    while (true) {
      try {
        rpcResponse = getBlockingStub().requireBuffer(rpcRequest);
      } catch (Exception e) {
        LOG.error(
            "Exception happened when requiring pre-allocated buffer from {}:{}", host, port, e);
        return Pair.of(result, needSplitPartitionIds);
      }
      if (rpcResponse.getStatus() != NO_BUFFER
          && rpcResponse.getStatus() != RssProtos.StatusCode.NO_BUFFER_FOR_HUGE_PARTITION) {
        break;
      }
      failedStatusCodeRef.set(StatusCode.fromCode(rpcResponse.getStatus().getNumber()));
      if (retry >= retryMax) {
        LOG.warn(
            "ShuffleServer "
                + host
                + ":"
                + port
                + " is full and can't send shuffle"
                + " data successfully due to "
                + rpcResponse.getStatus()
                + " after retry "
                + retryMax
                + " times, cost: {}(ms)",
            System.currentTimeMillis() - start);
        return Pair.of(result, needSplitPartitionIds);
      }
      try {
        LOG.info(
            "Can't require buffer for appId: {}, shuffleId: {}, partitionIds: {} with {} bytes from {}:{} due to {}, sleep and try[{}] again",
            appId,
            shuffleId,
            OutputUtils.listToSegment(partitionIds, 10),
            requireSize,
            host,
            port,
            rpcResponse.getStatus(),
            retry);
        long backoffTime =
            Math.min(
                retryIntervalMax,
                BACK_OFF_BASE * (1L << Math.min(retry, 16)) + random.nextInt(BACK_OFF_BASE));
        Thread.sleep(backoffTime);
      } catch (Exception e) {
        LOG.warn(
            "Exception happened when requiring pre-allocated buffer from {}:{}", host, port, e);
      }
      retry++;
    }
    if (rpcResponse.getStatus() == RssProtos.StatusCode.SUCCESS) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Require preAllocated size of {} from {}:{}, cost: {}(ms)",
            requireSize,
            host,
            port,
            System.currentTimeMillis() - start);
      }
      result = rpcResponse.getRequireBufferId();
      needSplitPartitionIds = rpcResponse.getNeedSplitPartitionIdsList();
    } else if (NOT_RETRY_STATUS_CODES.contains(
        StatusCode.fromCode(rpcResponse.getStatus().getNumber()))) {
      failedStatusCodeRef.set(StatusCode.fromCode(rpcResponse.getStatus().getNumber()));
      String msg =
          "Can't require "
              + requireSize
              + " bytes from "
              + host
              + ":"
              + port
              + ", statusCode="
              + rpcResponse.getStatus()
              + ", errorMsg:"
              + rpcResponse.getRetMsg();
      throw new NotRetryException(msg);
    }
    return Pair.of(result, needSplitPartitionIds);
  }

  private RssProtos.ShuffleUnregisterByAppIdResponse doUnregisterShuffleByAppId(
      String appId, int timeoutSec) {
    RssProtos.ShuffleUnregisterByAppIdRequest request =
        RssProtos.ShuffleUnregisterByAppIdRequest.newBuilder().setAppId(appId).build();
    return blockingStub
        .withDeadlineAfter(timeoutSec, TimeUnit.SECONDS)
        .unregisterShuffleByAppId(request);
  }

  @Override
  public RssUnregisterShuffleByAppIdResponse unregisterShuffleByAppId(
      RssUnregisterShuffleByAppIdRequest request) {
    RssProtos.ShuffleUnregisterByAppIdResponse rpcResponse =
        doUnregisterShuffleByAppId(request.getAppId(), request.getTimeoutSec());

    RssUnregisterShuffleByAppIdResponse response;
    RssProtos.StatusCode statusCode = rpcResponse.getStatus();

    switch (statusCode) {
      case SUCCESS:
        response = new RssUnregisterShuffleByAppIdResponse(StatusCode.SUCCESS);
        break;
      default:
        String msg =
            String.format(
                "Errors on unregistering app from %s:%s for appId[%s] and timeout[%ss], error: %s",
                host, port, request.getAppId(), request.getTimeoutSec(), rpcResponse.getRetMsg());
        LOG.error(msg);
        throw new RssException(msg);
    }

    return response;
  }

  private RssProtos.ShuffleUnregisterResponse doUnregisterShuffle(
      String appId, int shuffleId, int timeoutSec) {
    RssProtos.ShuffleUnregisterRequest request =
        RssProtos.ShuffleUnregisterRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .build();
    return blockingStub.withDeadlineAfter(timeoutSec, TimeUnit.SECONDS).unregisterShuffle(request);
  }

  @Override
  public RssUnregisterShuffleResponse unregisterShuffle(RssUnregisterShuffleRequest request) {
    RssProtos.ShuffleUnregisterResponse rpcResponse =
        doUnregisterShuffle(request.getAppId(), request.getShuffleId(), request.getTimeoutSec());

    RssUnregisterShuffleResponse response;
    RssProtos.StatusCode statusCode = rpcResponse.getStatus();

    switch (statusCode) {
      case SUCCESS:
        response = new RssUnregisterShuffleResponse(StatusCode.SUCCESS);
        break;
      default:
        String msg =
            String.format(
                "Errors on unregistering shuffle from %s:%s for appId[%s].shuffleId[%s] and timeout[%ss], error: %s",
                host,
                port,
                request.getAppId(),
                request.getShuffleId(),
                request.getTimeoutSec(),
                rpcResponse.getRetMsg());
        LOG.error(msg);
        throw new RssException(msg);
    }

    return response;
  }

  @Override
  public RssRegisterShuffleResponse registerShuffle(RssRegisterShuffleRequest request) {
    ShuffleRegisterResponse rpcResponse =
        doRegisterShuffle(
            request.getAppId(),
            request.getShuffleId(),
            request.getPartitionRanges(),
            request.getRemoteStorageInfo(),
            request.getUser(),
            request.getDataDistributionType(),
            request.getMaxConcurrencyPerPartitionToWrite(),
            request.getStageAttemptNumber(),
            request.getMergeContext(),
            request.getProperties());

    RssRegisterShuffleResponse response;
    RssProtos.StatusCode statusCode = rpcResponse.getStatus();
    switch (statusCode) {
      case SUCCESS:
        response = new RssRegisterShuffleResponse(StatusCode.SUCCESS);
        break;
      default:
        String msg =
            "Can't register shuffle to "
                + host
                + ":"
                + port
                + " for appId["
                + request.getAppId()
                + "], shuffleId["
                + request.getShuffleId()
                + "], errorMsg:"
                + rpcResponse.getRetMsg();
        LOG.error(msg);
        throw new RssException(msg);
    }
    return response;
  }

  @Override
  public RssSendShuffleDataResponse sendShuffleData(RssSendShuffleDataRequest request) {
    String appId = request.getAppId();
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks =
        request.getShuffleIdToBlocks();
    int stageAttemptNumber = request.getStageAttemptNumber();

    boolean isSuccessful = true;
    AtomicReference<StatusCode> failedStatusCode = new AtomicReference<>(StatusCode.INTERNAL_ERROR);
    Set<Integer> needSplitPartitionIds = Sets.newHashSet();
    // prepare rpc request based on shuffleId -> partitionId -> blocks
    for (Map.Entry<Integer, Map<Integer, List<ShuffleBlockInfo>>> stb :
        shuffleIdToBlocks.entrySet()) {
      List<ShuffleData> shuffleData = Lists.newArrayList();
      int size = 0;
      int blockNum = 0;
      int shuffleId = stb.getKey();
      List<Integer> partitionIds = new ArrayList<>();
      List<Integer> partitionRequireSizes = new ArrayList<>();

      for (Map.Entry<Integer, List<ShuffleBlockInfo>> ptb : stb.getValue().entrySet()) {
        List<ShuffleBlock> shuffleBlocks = Lists.newArrayList();
        int partitionRequireSize = 0;
        for (ShuffleBlockInfo sbi : ptb.getValue()) {
          shuffleBlocks.add(
              ShuffleBlock.newBuilder()
                  .setBlockId(sbi.getBlockId())
                  .setCrc(sbi.getCrc())
                  .setLength(sbi.getLength())
                  .setTaskAttemptId(sbi.getTaskAttemptId())
                  .setUncompressLength(sbi.getUncompressLength())
                  .setData(UnsafeByteOperations.unsafeWrap(sbi.getData().nioBuffer()))
                  .build());
          partitionRequireSize += sbi.getSize();
          blockNum++;
        }
        size += partitionRequireSize;
        shuffleData.add(
            ShuffleData.newBuilder()
                .setPartitionId(ptb.getKey())
                .addAllBlock(shuffleBlocks)
                .build());
        partitionIds.add(ptb.getKey());
        partitionRequireSizes.add(partitionRequireSize);
      }

      final int allocateSize = size;
      final int finalBlockNum = blockNum;
      try {
        RetryUtils.retryWithCondition(
            () -> {
              // TODO(baoloongmao): support partition split follow netty client
              Pair<Long, List<Integer>> allocationResult =
                  requirePreAllocation(
                      appId,
                      shuffleId,
                      partitionIds,
                      partitionRequireSizes,
                      allocateSize,
                      request.getRetryMax() / maxRetryAttempts,
                      request.getRetryIntervalMax(),
                      failedStatusCode);
              long requireId = allocationResult.getLeft();
              needSplitPartitionIds.addAll(allocationResult.getRight());
              if (requireId == FAILED_REQUIRE_ID) {
                throw new RssException(
                    String.format(
                        "requirePreAllocation failed! size[%s], host[%s], port[%s]",
                        allocateSize, host, port));
              }
              long start = System.currentTimeMillis();
              SendShuffleDataRequest rpcRequest =
                  SendShuffleDataRequest.newBuilder()
                      .setAppId(appId)
                      .setShuffleId(stb.getKey())
                      .setRequireBufferId(requireId)
                      .addAllShuffleData(shuffleData)
                      .setTimestamp(start)
                      .setStageAttemptNumber(stageAttemptNumber)
                      .build();
              SendShuffleDataResponse response = getBlockingStub().sendShuffleData(rpcRequest);
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Do sendShuffleData to {}:{} rpc cost:"
                        + (System.currentTimeMillis() - start)
                        + " ms for "
                        + allocateSize
                        + " bytes with "
                        + finalBlockNum
                        + " blocks",
                    host,
                    port);
              }
              if (response.getStatus() != RssProtos.StatusCode.SUCCESS) {
                String msg =
                    "Can't send shuffle data with "
                        + finalBlockNum
                        + " blocks to "
                        + host
                        + ":"
                        + port
                        + ", statusCode="
                        + response.getStatus()
                        + ", errorMsg:"
                        + response.getRetMsg();
                failedStatusCode.set(StatusCode.fromCode(response.getStatus().getNumber()));
                if (NOT_RETRY_STATUS_CODES.contains(failedStatusCode.get())) {
                  throw new NotRetryException(msg);
                } else {
                  throw new RssException(msg);
                }
              }
              return response;
            },
            null,
            request.getRetryIntervalMax(),
            maxRetryAttempts,
            t -> !(t instanceof OutOfMemoryError) && !(t instanceof NotRetryException));
      } catch (Throwable throwable) {
        LOG.warn("Failed to send shuffle data due to ", throwable);
        isSuccessful = false;
        break;
      }
    }

    RssSendShuffleDataResponse response;
    if (isSuccessful) {
      response = new RssSendShuffleDataResponse(StatusCode.SUCCESS);
    } else {
      response = new RssSendShuffleDataResponse(failedStatusCode.get());
    }
    response.setNeedSplitPartitionIds(needSplitPartitionIds);
    return response;
  }

  @Override
  public RssSendCommitResponse sendCommit(RssSendCommitRequest request) {
    ShuffleCommitResponse rpcResponse = doSendCommit(request.getAppId(), request.getShuffleId());

    RssSendCommitResponse response;
    if (rpcResponse.getStatus() != RssProtos.StatusCode.SUCCESS) {
      String msg =
          "Can't commit shuffle data to "
              + host
              + ":"
              + port
              + " for [appId="
              + request.getAppId()
              + ", shuffleId="
              + request.getShuffleId()
              + "], "
              + "errorMsg:"
              + rpcResponse.getRetMsg();
      LOG.error(msg);
      throw new RssException(msg);
    } else {
      response = new RssSendCommitResponse(StatusCode.SUCCESS);
      response.setCommitCount(rpcResponse.getCommitCount());
    }
    return response;
  }

  @Override
  public RssAppHeartBeatResponse sendHeartBeat(RssAppHeartBeatRequest request) {
    AppHeartBeatResponse appHeartBeatResponse =
        doSendHeartBeat(request.getAppId(), request.getTimeoutMs());
    if (appHeartBeatResponse.getStatus() != RssProtos.StatusCode.SUCCESS) {
      String msg =
          "Can't send heartbeat to "
              + host
              + ":"
              + port
              + " for [appId="
              + request.getAppId()
              + ", timeout="
              + request.getTimeoutMs()
              + "ms], "
              + "errorMsg:"
              + appHeartBeatResponse.getRetMsg();
      LOG.error(msg);
      return new RssAppHeartBeatResponse(StatusCode.INTERNAL_ERROR);
    } else {
      return new RssAppHeartBeatResponse(StatusCode.SUCCESS);
    }
  }

  @Override
  public RssFinishShuffleResponse finishShuffle(RssFinishShuffleRequest request) {
    FinishShuffleRequest rpcRequest =
        FinishShuffleRequest.newBuilder()
            .setAppId(request.getAppId())
            .setShuffleId(request.getShuffleId())
            .build();
    long start = System.currentTimeMillis();
    FinishShuffleResponse rpcResponse = getBlockingStub().finishShuffle(rpcRequest);

    RssFinishShuffleResponse response;
    if (rpcResponse.getStatus() != RssProtos.StatusCode.SUCCESS) {
      String msg =
          "Can't finish shuffle process to "
              + host
              + ":"
              + port
              + " for [appId="
              + request.getAppId()
              + ", shuffleId="
              + request.getShuffleId()
              + "], "
              + "errorMsg:"
              + rpcResponse.getRetMsg();
      LOG.error(msg);
      throw new RssException(msg);
    } else {
      String requestInfo =
          "appId[" + request.getAppId() + "], shuffleId[" + request.getShuffleId() + "]";
      LOG.info(
          "FinishShuffle to {}:{} for {} cost {} ms",
          host,
          port,
          requestInfo,
          System.currentTimeMillis() - start);
      response = new RssFinishShuffleResponse(StatusCode.SUCCESS);
    }
    return response;
  }

  @Override
  public RssReportShuffleResultResponse reportShuffleResult(RssReportShuffleResultRequest request) {
    List<PartitionToBlockIds> partitionToBlockIds = Lists.newArrayList();
    for (Map.Entry<Integer, List<Long>> entry : request.getPartitionToBlockIds().entrySet()) {
      List<Long> blockIds = entry.getValue();
      if (blockIds != null && !blockIds.isEmpty()) {
        partitionToBlockIds.add(
            PartitionToBlockIds.newBuilder()
                .setPartitionId(entry.getKey())
                .addAllBlockIds(entry.getValue())
                .build());
      }
    }

    ReportShuffleResultRequest recRequest =
        ReportShuffleResultRequest.newBuilder()
            .setAppId(request.getAppId())
            .setShuffleId(request.getShuffleId())
            .setTaskAttemptId(request.getTaskAttemptId())
            .setBitmapNum(request.getBitmapNum())
            .addAllPartitionToBlockIds(partitionToBlockIds)
            .build();
    ReportShuffleResultResponse rpcResponse = doReportShuffleResult(recRequest);

    RssProtos.StatusCode statusCode = rpcResponse.getStatus();
    RssReportShuffleResultResponse response;
    switch (statusCode) {
      case SUCCESS:
        response = new RssReportShuffleResultResponse(StatusCode.SUCCESS);
        break;
      default:
        String msg =
            "Can't report shuffle result to "
                + host
                + ":"
                + port
                + " for [appId="
                + request.getAppId()
                + ", shuffleId="
                + request.getShuffleId()
                + ", errorMsg:"
                + rpcResponse.getRetMsg();
        LOG.error(msg);
        throw new RssException(msg);
    }

    return response;
  }

  private ReportShuffleResultResponse doReportShuffleResult(ReportShuffleResultRequest rpcRequest) {
    try {
      return RetryUtils.retryWithCondition(
          () -> getBlockingStub().reportShuffleResult(rpcRequest),
          null, // No specific callback to execute
          0, // No delay between retries, retry immediately
          maxRetryAttempts, // Maximum number of retry attempts
          t -> { // Define retry condition directly in the method call
            if (t instanceof StatusRuntimeException) {
              return !(t.getCause() instanceof InterruptedException);
            }
            return t instanceof Exception; // Retry for all other Exceptions
          });
    } catch (Throwable t) {
      // Handle or rethrow the exception as appropriate
      throw new RssException(
          "Failed to report shuffle result to host[" + host + "], port[" + port + "]", t);
    }
  }

  @Override
  public RssGetShuffleResultResponse getShuffleResult(RssGetShuffleResultRequest request) {
    GetShuffleResultRequest rpcRequest =
        GetShuffleResultRequest.newBuilder()
            .setAppId(request.getAppId())
            .setShuffleId(request.getShuffleId())
            .setPartitionId(request.getPartitionId())
            .setBlockIdLayout(
                RssProtos.BlockIdLayout.newBuilder()
                    .setSequenceNoBits(request.getBlockIdLayout().sequenceNoBits)
                    .setPartitionIdBits(request.getBlockIdLayout().partitionIdBits)
                    .setTaskAttemptIdBits(request.getBlockIdLayout().taskAttemptIdBits)
                    .build())
            .build();
    GetShuffleResultResponse rpcResponse = getBlockingStub().getShuffleResult(rpcRequest);
    RssProtos.StatusCode statusCode = rpcResponse.getStatus();

    RssGetShuffleResultResponse response;
    switch (statusCode) {
      case SUCCESS:
        try {
          response =
              new RssGetShuffleResultResponse(
                  StatusCode.SUCCESS, rpcResponse.getSerializedBitmap().toByteArray());
        } catch (Exception e) {
          throw new RssException(e);
        }
        break;
      default:
        String msg =
            "Can't get shuffle result from "
                + host
                + ":"
                + port
                + " for [appId="
                + request.getAppId()
                + ", shuffleId="
                + request.getShuffleId()
                + ", errorMsg:"
                + rpcResponse.getRetMsg();
        LOG.error(msg);
        throw new RssFetchFailedException(msg);
    }

    return response;
  }

  @Override
  public RssGetShuffleResultResponse getShuffleResultForMultiPart(
      RssGetShuffleResultForMultiPartRequest request) {
    GetShuffleResultForMultiPartRequest rpcRequest =
        GetShuffleResultForMultiPartRequest.newBuilder()
            .setAppId(request.getAppId())
            .setShuffleId(request.getShuffleId())
            .addAllPartitions(request.getPartitions())
            .setBlockIdLayout(
                RssProtos.BlockIdLayout.newBuilder()
                    .setSequenceNoBits(request.getBlockIdLayout().sequenceNoBits)
                    .setPartitionIdBits(request.getBlockIdLayout().partitionIdBits)
                    .setTaskAttemptIdBits(request.getBlockIdLayout().taskAttemptIdBits)
                    .build())
            .build();
    GetShuffleResultForMultiPartResponse rpcResponse =
        getBlockingStub().getShuffleResultForMultiPart(rpcRequest);
    RssProtos.StatusCode statusCode = rpcResponse.getStatus();

    RssGetShuffleResultResponse response;
    switch (statusCode) {
      case SUCCESS:
        try {
          response =
              new RssGetShuffleResultResponse(
                  StatusCode.SUCCESS, rpcResponse.getSerializedBitmap().toByteArray());
        } catch (Exception e) {
          throw new RssException(e);
        }
        break;
      default:
        String msg =
            "Can't get shuffle result from "
                + host
                + ":"
                + port
                + " for [appId="
                + request.getAppId()
                + ", shuffleId="
                + request.getShuffleId()
                + ", errorMsg:"
                + rpcResponse.getRetMsg();
        LOG.error(msg);
        throw new RssFetchFailedException(msg);
    }

    return response;
  }

  @Override
  public RssGetShuffleDataResponse getShuffleData(RssGetShuffleDataRequest request) {
    long start = System.currentTimeMillis();
    GetLocalShuffleDataRequest rpcRequest =
        GetLocalShuffleDataRequest.newBuilder()
            .setAppId(request.getAppId())
            .setShuffleId(request.getShuffleId())
            .setPartitionId(request.getPartitionId())
            .setPartitionNumPerRange(request.getPartitionNumPerRange())
            .setPartitionNum(request.getPartitionNum())
            .setOffset(request.getOffset())
            .setLength(request.getLength())
            .setTimestamp(start)
            .setStorageId(request.getStorageId())
            .build();
    String requestInfo =
        "appId["
            + request.getAppId()
            + "], shuffleId["
            + request.getShuffleId()
            + "], partitionId["
            + request.getPartitionId()
            + "], storageId["
            + request.getStorageId()
            + "]";
    int retry = 0;
    GetLocalShuffleDataResponse rpcResponse;
    while (true) {
      rpcResponse = getBlockingStub().getLocalShuffleData(rpcRequest);
      if (rpcResponse.getStatus() != NO_BUFFER) {
        break;
      }
      waitOrThrow(
          request, retry, requestInfo, StatusCode.fromProto(rpcResponse.getStatus()), start);
      retry++;
    }
    RssGetShuffleDataResponse response;
    switch (rpcResponse.getStatus()) {
      case SUCCESS:
        LOG.info(
            "GetShuffleData from {}:{} for {} cost {} ms",
            host,
            port,
            requestInfo,
            System.currentTimeMillis() - start);
        response =
            new RssGetShuffleDataResponse(
                StatusCode.SUCCESS, ByteBuffer.wrap(rpcResponse.getData().toByteArray()));
        break;
      default:
        String msg =
            "Can't get shuffle data from "
                + host
                + ":"
                + port
                + " for "
                + requestInfo
                + ", errorMsg:"
                + rpcResponse.getRetMsg();
        LOG.error(msg);
        throw new RssFetchFailedException(msg);
    }
    return response;
  }

  @Override
  public RssGetShuffleIndexResponse getShuffleIndex(RssGetShuffleIndexRequest request) {
    GetLocalShuffleIndexRequest rpcRequest =
        GetLocalShuffleIndexRequest.newBuilder()
            .setAppId(request.getAppId())
            .setShuffleId(request.getShuffleId())
            .setPartitionId(request.getPartitionId())
            .setPartitionNumPerRange(request.getPartitionNumPerRange())
            .setPartitionNum(request.getPartitionNum())
            .build();
    String requestInfo =
        "appId["
            + request.getAppId()
            + "], shuffleId["
            + request.getShuffleId()
            + "], partitionId["
            + request.getPartitionId()
            + "]";
    long start = System.currentTimeMillis();
    int retry = 0;
    GetLocalShuffleIndexResponse rpcResponse;
    while (true) {
      rpcResponse = getBlockingStub().getLocalShuffleIndex(rpcRequest);
      if (rpcResponse.getStatus() != NO_BUFFER) {
        break;
      }
      waitOrThrow(
          request, retry, requestInfo, StatusCode.fromProto(rpcResponse.getStatus()), start);
      retry++;
    }
    RssGetShuffleIndexResponse response;
    switch (rpcResponse.getStatus()) {
      case SUCCESS:
        LOG.info(
            "GetShuffleIndex from {}:{} for {} cost {} ms",
            host,
            port,
            requestInfo,
            System.currentTimeMillis() - start);
        response =
            new RssGetShuffleIndexResponse(
                StatusCode.SUCCESS,
                new NettyManagedBuffer(
                    Unpooled.wrappedBuffer(rpcResponse.getIndexData().toByteArray())),
                rpcResponse.getDataFileLen(),
                rpcResponse.getStorageIdsList().stream().mapToInt(Integer::intValue).toArray());

        break;
      default:
        String msg =
            "Can't get shuffle index from "
                + host
                + ":"
                + port
                + " for "
                + requestInfo
                + ", errorMsg:"
                + rpcResponse.getRetMsg();
        LOG.error(msg);
        throw new RssFetchFailedException(msg);
    }
    return response;
  }

  @Override
  public RssGetInMemoryShuffleDataResponse getInMemoryShuffleData(
      RssGetInMemoryShuffleDataRequest request) {
    long start = System.currentTimeMillis();
    ByteString serializedTaskIdsBytes = ByteString.EMPTY;
    try {
      if (request.getExpectedTaskIds() != null) {
        serializedTaskIdsBytes =
            UnsafeByteOperations.unsafeWrap(RssUtils.serializeBitMap(request.getExpectedTaskIds()));
      }
    } catch (Exception e) {
      throw new RssException("Errors on serializing task ids bitmap.", e);
    }

    GetMemoryShuffleDataRequest rpcRequest =
        GetMemoryShuffleDataRequest.newBuilder()
            .setAppId(request.getAppId())
            .setShuffleId(request.getShuffleId())
            .setPartitionId(request.getPartitionId())
            .setLastBlockId(request.getLastBlockId())
            .setReadBufferSize(request.getReadBufferSize())
            .setSerializedExpectedTaskIdsBitmap(serializedTaskIdsBytes)
            .setTimestamp(start)
            .build();
    String requestInfo =
        "appId["
            + request.getAppId()
            + "], shuffleId["
            + request.getShuffleId()
            + "], partitionId["
            + request.getPartitionId()
            + "]";
    int retry = 0;
    GetMemoryShuffleDataResponse rpcResponse;
    while (true) {
      rpcResponse = getBlockingStub().getMemoryShuffleData(rpcRequest);
      if (rpcResponse.getStatus() != NO_BUFFER) {
        break;
      }
      waitOrThrow(
          request, retry, requestInfo, StatusCode.fromProto(rpcResponse.getStatus()), start);
      retry++;
    }
    RssGetInMemoryShuffleDataResponse response;
    switch (rpcResponse.getStatus()) {
      case SUCCESS:
        LOG.info(
            "GetInMemoryShuffleData from {}:{} for "
                + requestInfo
                + " cost "
                + (System.currentTimeMillis() - start)
                + " ms",
            host,
            port);
        response =
            new RssGetInMemoryShuffleDataResponse(
                StatusCode.SUCCESS,
                ByteBuffer.wrap(rpcResponse.getData().toByteArray()),
                toBufferSegments(rpcResponse.getShuffleDataBlockSegmentsList()));
        break;
      default:
        String msg =
            "Can't get shuffle in memory data from "
                + host
                + ":"
                + port
                + " for "
                + requestInfo
                + ", errorMsg:"
                + rpcResponse.getRetMsg();
        LOG.error(msg);
        throw new RssFetchFailedException(msg);
    }
    return response;
  }

  @Override
  public RssStartSortMergeResponse startSortMerge(RssStartSortMergeRequest request) {
    ByteString serializedBlockIdsBytes = ByteString.EMPTY;
    try {
      if (request.getExpectedTaskIds() != null) {
        serializedBlockIdsBytes =
            UnsafeByteOperations.unsafeWrap(RssUtils.serializeBitMap(request.getExpectedTaskIds()));
      }
    } catch (Exception e) {
      throw new RssException("Errors on serializing task ids bitmap.", e);
    }

    RssProtos.StartSortMergeRequest rpcRequest =
        RssProtos.StartSortMergeRequest.newBuilder()
            .setAppId(request.getAppId())
            .setShuffleId(request.getShuffleId())
            .setPartitionId(request.getPartitionId())
            .setUniqueBlocksBitmap(serializedBlockIdsBytes)
            .build();
    long start = System.currentTimeMillis();
    RssProtos.StartSortMergeResponse rpcResponse = getBlockingStub().startSortMerge(rpcRequest);
    String requestInfo =
        "appId["
            + request.getAppId()
            + "], shuffleId["
            + request.getShuffleId()
            + "], partitionId["
            + request.getPartitionId()
            + "]";
    LOG.info(
        "startSortMerge to {}:{} for {} cost {} ms",
        host,
        port,
        requestInfo,
        (System.currentTimeMillis() - start));
    RssProtos.StatusCode statusCode = rpcResponse.getStatus();
    RssStartSortMergeResponse response;
    switch (statusCode) {
      case SUCCESS:
        response = new RssStartSortMergeResponse(StatusCode.SUCCESS);
        break;
      default:
        String msg =
            "Can't report unique block to from "
                + host
                + ":"
                + port
                + " for "
                + requestInfo
                + ", errorMsg:"
                + rpcResponse.getRetMsg();
        LOG.error(msg);
        throw new RssException(msg);
    }
    return response;
  }

  @Override
  public RssGetSortedShuffleDataResponse getSortedShuffleData(
      RssGetSortedShuffleDataRequest request) {
    long start = System.currentTimeMillis();
    RssProtos.GetSortedShuffleDataRequest rpcRequest =
        RssProtos.GetSortedShuffleDataRequest.newBuilder()
            .setAppId(request.getAppId())
            .setShuffleId(request.getShuffleId())
            .setPartitionId(request.getPartitionId())
            .setMergedBlockId(request.getBlockId())
            .setTimestamp(start)
            .build();
    String requestInfo =
        "appId["
            + request.getAppId()
            + "], shuffleId["
            + request.getShuffleId()
            + "], partitionId["
            + request.getPartitionId()
            + "], blockId["
            + request.getBlockId()
            + "]";
    int retry = 0;
    RssProtos.GetSortedShuffleDataResponse rpcResponse;
    while (true) {
      rpcResponse = getBlockingStub().getSortedShuffleData(rpcRequest);
      if (rpcResponse.getStatus() != NO_BUFFER) {
        break;
      }
      waitOrThrow(
          request, retry, requestInfo, StatusCode.fromProto(rpcResponse.getStatus()), start);
      retry++;
    }
    LOG.info(
        "GetSortedShuffleData from {}:{} for {} cost {} ms",
        host,
        port,
        requestInfo,
        System.currentTimeMillis() - start);

    RssProtos.StatusCode statusCode = rpcResponse.getStatus();

    RssGetSortedShuffleDataResponse response;
    switch (statusCode) {
      case SUCCESS:
        response =
            new RssGetSortedShuffleDataResponse(
                StatusCode.SUCCESS,
                rpcResponse.getRetMsg(),
                rpcResponse.getData().asReadOnlyByteBuffer(),
                rpcResponse.getNextBlockId(),
                rpcResponse.getMState());
        break;
      default:
        String msg =
            "Can't get sorted shuffle data from "
                + host
                + ":"
                + port
                + " for "
                + requestInfo
                + ", errorMsg:"
                + rpcResponse.getRetMsg();
        LOG.error(msg);
        throw new RssFetchFailedException(msg);
    }
    return response;
  }

  @Override
  public String getClientInfo() {
    return "ShuffleServerGrpcClient for host[" + host + "], port[" + port + "]";
  }

  protected void waitOrThrow(
      RetryableRequest request, int retry, String requestInfo, StatusCode statusCode, long start) {
    if (retry >= request.getRetryMax()) {
      String msg =
          String.format(
              "ShuffleServer %s:%s is full when %s due to %s, after %d retries, cost %d ms",
              host,
              port,
              request.operationType(),
              statusCode,
              request.getRetryMax(),
              System.currentTimeMillis() - start);
      LOG.error(msg);
      throw new RssFetchFailedException(msg);
    }
    try {
      long backoffTime =
          Math.min(
              request.getRetryIntervalMax(),
              BACK_OFF_BASE * (1L << Math.min(retry, 16)) + random.nextInt(BACK_OFF_BASE));
      LOG.warn(
          "Can't acquire buffer for {} from {}:{} when executing {}, due to {}. "
              + "Will retry {} more time(s) after waiting {} milliseconds.",
          requestInfo,
          host,
          port,
          request.operationType(),
          statusCode,
          request.getRetryMax() - retry,
          backoffTime);
      Thread.sleep(backoffTime);
    } catch (InterruptedException e) {
      LOG.warn(
          "Exception happened when executing {} from {}:{}",
          request.operationType(),
          host,
          port,
          e);
    }
  }

  private List<ShufflePartitionRange> toShufflePartitionRanges(
      List<PartitionRange> partitionRanges) {
    List<ShufflePartitionRange> ret = Lists.newArrayList();
    for (PartitionRange partitionRange : partitionRanges) {
      ret.add(
          ShufflePartitionRange.newBuilder()
              .setStart(partitionRange.getStart())
              .setEnd(partitionRange.getEnd())
              .build());
    }
    return ret;
  }

  protected List<BufferSegment> toBufferSegments(List<ShuffleDataBlockSegment> blockSegments) {
    List<BufferSegment> ret = Lists.newArrayList();
    for (ShuffleDataBlockSegment sdbs : blockSegments) {
      ret.add(
          new BufferSegment(
              sdbs.getBlockId(),
              sdbs.getOffset(),
              sdbs.getLength(),
              sdbs.getUncompressLength(),
              sdbs.getCrc(),
              sdbs.getTaskAttemptId()));
    }
    return ret;
  }

  @VisibleForTesting
  public void adjustTimeout(long timeout) {
    rpcTimeout = timeout;
  }
}
