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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ReconfigurableRegistry;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.audit.RpcAuditContext;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.exception.ExceedHugePartitionHardLimitException;
import org.apache.uniffle.common.exception.FileNotFoundException;
import org.apache.uniffle.common.exception.NoBufferException;
import org.apache.uniffle.common.exception.NoBufferForHugePartitionException;
import org.apache.uniffle.common.exception.NoRegisterException;
import org.apache.uniffle.common.merger.MergeState;
import org.apache.uniffle.common.rpc.ClientContextServerInterceptor;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.ByteBufUtils;
import org.apache.uniffle.common.util.OutputUtils;
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
import org.apache.uniffle.proto.RssProtos.PartitionToBlockIds;
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
import org.apache.uniffle.proto.ShuffleServerGrpc.ShuffleServerImplBase;
import org.apache.uniffle.server.audit.ServerRpcAuditContext;
import org.apache.uniffle.server.block.ShuffleBlockIdManager;
import org.apache.uniffle.server.buffer.PreAllocatedBufferInfo;
import org.apache.uniffle.server.merge.MergeStatus;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.common.StorageReadMetrics;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.server.merge.ShuffleMergeManager.MERGE_APP_SUFFIX;

public class ShuffleServerGrpcService extends ShuffleServerImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServerGrpcService.class);
  private static final Logger AUDIT_LOGGER =
      LoggerFactory.getLogger("SHUFFLE_SERVER_RPC_AUDIT_LOG");
  private final ShuffleServer shuffleServer;
  private boolean isRpcAuditLogEnabled;
  private List<String> rpcAuditExcludeOpList;
  private boolean reportOnUnregisterEnabled;

  public ShuffleServerGrpcService(ShuffleServer shuffleServer) {
    this.shuffleServer = shuffleServer;
    isRpcAuditLogEnabled =
        shuffleServer
            .getShuffleServerConf()
            .getReconfigurableConf(ShuffleServerConf.SERVER_RPC_AUDIT_LOG_ENABLED)
            .get();
    rpcAuditExcludeOpList =
        shuffleServer
            .getShuffleServerConf()
            .getReconfigurableConf(ShuffleServerConf.SERVER_RPC_RPC_AUDIT_LOG_EXCLUDE_LIST)
            .get();
    reportOnUnregisterEnabled =
        shuffleServer
            .getShuffleServerConf()
            .getBoolean(ShuffleServerConf.SERVER_HEARTBEAT_REPORT_ON_UNREGISTER_ENABLED);
    ReconfigurableRegistry.register(
        Sets.newHashSet(
            ShuffleServerConf.SERVER_RPC_AUDIT_LOG_ENABLED.key(),
            ShuffleServerConf.SERVER_RPC_RPC_AUDIT_LOG_EXCLUDE_LIST.key()),
        (conf, changedProperties) -> {
          if (changedProperties == null) {
            return;
          }
          if (changedProperties.contains(ShuffleServerConf.SERVER_RPC_AUDIT_LOG_ENABLED.key())) {
            isRpcAuditLogEnabled = conf.getBoolean(ShuffleServerConf.SERVER_RPC_AUDIT_LOG_ENABLED);
          }
          if (changedProperties.contains(
              ShuffleServerConf.SERVER_RPC_RPC_AUDIT_LOG_EXCLUDE_LIST.key())) {
            rpcAuditExcludeOpList =
                conf.get(ShuffleServerConf.SERVER_RPC_RPC_AUDIT_LOG_EXCLUDE_LIST);
          }
        });
  }

  @Override
  public void unregisterShuffleByAppId(
      RssProtos.ShuffleUnregisterByAppIdRequest request,
      StreamObserver<RssProtos.ShuffleUnregisterByAppIdResponse> responseStreamObserver) {
    try (ServerRpcAuditContext auditContext = createAuditContext("unregisterShuffleByAppId")) {
      String appId = request.getAppId();
      auditContext.withAppId(appId);
      StatusCode status = verifyRequest(appId);
      if (status != StatusCode.SUCCESS) {
        auditContext.withStatusCode(status);
        RssProtos.ShuffleUnregisterByAppIdResponse reply =
            RssProtos.ShuffleUnregisterByAppIdResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(status.toString())
                .build();
        responseStreamObserver.onNext(reply);
        responseStreamObserver.onCompleted();
        return;
      }
      String responseMessage = "OK";
      try {
        if (reportOnUnregisterEnabled) {
          shuffleServer.sendHeartbeat();
        }
        shuffleServer.getShuffleTaskManager().removeShuffleDataAsync(appId);
        if (shuffleServer.isRemoteMergeEnable()) {
          shuffleServer.getShuffleTaskManager().removeShuffleDataAsync(appId + MERGE_APP_SUFFIX);
        }
      } catch (Exception e) {
        status = StatusCode.INTERNAL_ERROR;
        LOG.error("App {} exception while unregisterShuffleByAppId", appId, e);
      }

      auditContext.withStatusCode(status);
      RssProtos.ShuffleUnregisterByAppIdResponse reply =
          RssProtos.ShuffleUnregisterByAppIdResponse.newBuilder()
              .setStatus(status.toProto())
              .setRetMsg(responseMessage)
              .build();
      responseStreamObserver.onNext(reply);
      responseStreamObserver.onCompleted();
    }
  }

  @Override
  public void unregisterShuffle(
      RssProtos.ShuffleUnregisterRequest request,
      StreamObserver<RssProtos.ShuffleUnregisterResponse> responseStreamObserver) {
    try (ServerRpcAuditContext auditContext = createAuditContext("unregisterShuffle")) {
      String appId = request.getAppId();
      int shuffleId = request.getShuffleId();
      auditContext.withAppId(appId).withShuffleId(shuffleId);
      StatusCode status = verifyRequest(appId);
      if (status != StatusCode.SUCCESS) {
        auditContext.withStatusCode(status);
        RssProtos.ShuffleUnregisterResponse reply =
            RssProtos.ShuffleUnregisterResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(status.toString())
                .build();
        responseStreamObserver.onNext(reply);
        responseStreamObserver.onCompleted();
        return;
      }
      String responseMessage = "OK";
      try {
        if (reportOnUnregisterEnabled) {
          shuffleServer.sendHeartbeat();
        }
        shuffleServer.getShuffleTaskManager().removeShuffleDataAsync(appId, shuffleId);
        if (shuffleServer.isRemoteMergeEnable()) {
          shuffleServer
              .getShuffleTaskManager()
              .removeShuffleDataAsync(appId + MERGE_APP_SUFFIX, shuffleId);
        }
      } catch (Exception e) {
        status = StatusCode.INTERNAL_ERROR;
        LOG.error("App {} exception while unregisterShuffle", appId, e);
      }

      auditContext.withStatusCode(status);
      RssProtos.ShuffleUnregisterResponse reply =
          RssProtos.ShuffleUnregisterResponse.newBuilder()
              .setStatus(status.toProto())
              .setRetMsg(responseMessage)
              .build();
      responseStreamObserver.onNext(reply);
      responseStreamObserver.onCompleted();
    }
  }

  @Override
  public void registerShuffle(
      ShuffleRegisterRequest req, StreamObserver<ShuffleRegisterResponse> responseObserver) {
    try (ServerRpcAuditContext auditContext = createAuditContext("registerShuffle")) {
      ShuffleRegisterResponse reply;
      String appId = req.getAppId();
      int shuffleId = req.getShuffleId();
      int stageAttemptNumber = req.getStageAttemptNumber();
      String remoteStoragePath = req.getRemoteStorage().getPath();
      String user = req.getUser();
      auditContext.withAppId(appId).withShuffleId(shuffleId);
      auditContext.withArgs(
          "remoteStoragePath="
              + remoteStoragePath
              + ", user="
              + user
              + ", stageAttemptNumber="
              + stageAttemptNumber);
      // If the Stage is registered for the first time, you do not need to consider the Stage retry
      // and delete the Block data that has been sent.
      ShuffleTaskInfo taskInfo = shuffleServer.getShuffleTaskManager().getShuffleTaskInfo(appId);
      if (taskInfo != null) {
        // Prevents AttemptNumber of multiple stages from modifying the latest AttemptNumber.
        synchronized (taskInfo) {
          int lastAttemptNumber = taskInfo.getLatestStageAttemptNumber(shuffleId);
          if (stageAttemptNumber > 0 && stageAttemptNumber > lastAttemptNumber) {
            taskInfo.refreshLatestStageAttemptNumber(shuffleId, stageAttemptNumber);
            try {
              long start = System.currentTimeMillis();
              shuffleServer
                  .getShuffleTaskManager()
                  .removeShuffleDataSyncRenameAndDelete(appId, shuffleId);
              LOG.info(
                  "Deleted the previous stage attempt data due to stage recomputing for app: {}, "
                      + "shuffleId: {}, stageAttemptNumber: {}. It costs {} ms",
                  appId,
                  shuffleId,
                  lastAttemptNumber,
                  System.currentTimeMillis() - start);
              // Add a check to prevent undeleted metadata.
              ShuffleBlockIdManager shuffleBlockIdManager =
                  shuffleServer
                      .getShuffleTaskManager()
                      .getShuffleTaskInfo(appId)
                      .getShuffleBlockIdManager();
              long blockCountByShuffleId =
                  shuffleBlockIdManager.getBlockCountByShuffleId(
                      appId, Lists.newArrayList(shuffleId));
              if (blockCountByShuffleId != 0) {
                LOG.error(
                    "Metadata is not deleted on clearing previous stage attempt data for app: {}, shuffleId: {}, stageAttemptNumber: {}",
                    appId,
                    shuffleId,
                    lastAttemptNumber);
                StatusCode code = StatusCode.INTERNAL_ERROR;
                auditContext.withStatusCode(code);
                reply = ShuffleRegisterResponse.newBuilder().setStatus(code.toProto()).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
                return;
              }
            } catch (Exception e) {
              LOG.error(
                  "Errors on clearing previous stage attempt data for app: {}, shuffleId: {}, stageAttemptNumber: {}",
                  appId,
                  shuffleId,
                  lastAttemptNumber,
                  e);
              StatusCode code = StatusCode.INTERNAL_ERROR;
              auditContext.withStatusCode(code);
              reply = ShuffleRegisterResponse.newBuilder().setStatus(code.toProto()).build();
              responseObserver.onNext(reply);
              responseObserver.onCompleted();
              return;
            }
          } else if (stageAttemptNumber > 0 && stageAttemptNumber <= lastAttemptNumber) {
            LOG.info(
                "The registration failed. The latest retry count is smaller than the existing retry count. This situation should not exist.");
            // When a Stage retry occurs, the first or last registration of a Stage may need to be
            // ignored and the ignored status quickly returned.
            StatusCode code = StatusCode.STAGE_RETRY_IGNORE;
            auditContext.withStatusCode(code);
            reply = ShuffleRegisterResponse.newBuilder().setStatus(code.toProto()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
            return;
          }
        }
      }

      ShuffleDataDistributionType shuffleDataDistributionType =
          ShuffleDataDistributionType.valueOf(
              Optional.ofNullable(req.getShuffleDataDistribution())
                  .orElse(RssProtos.DataDistribution.NORMAL)
                  .name());

      int maxConcurrencyPerPartitionToWrite = req.getMaxConcurrencyPerPartitionToWrite();

      Map<String, String> remoteStorageConf =
          req.getRemoteStorage().getRemoteStorageConfList().stream()
              .collect(
                  Collectors.toMap(RemoteStorageConfItem::getKey, RemoteStorageConfItem::getValue));

      List<PartitionRange> partitionRanges = toPartitionRanges(req.getPartitionRangesList());
      LOG.info(
          "Get register request for appId["
              + appId
              + "], shuffleId["
              + shuffleId
              + "], remoteStorage["
              + remoteStoragePath
              + "] with "
              + partitionRanges.size()
              + " partition ranges. User: {}",
          user);

      StatusCode result =
          shuffleServer
              .getShuffleTaskManager()
              .registerShuffle(
                  appId,
                  shuffleId,
                  stageAttemptNumber,
                  partitionRanges,
                  new RemoteStorageInfo(remoteStoragePath, remoteStorageConf),
                  user,
                  shuffleDataDistributionType,
                  maxConcurrencyPerPartitionToWrite,
                  req.getPropertiesMap());
      if (StatusCode.SUCCESS == result
          && shuffleServer.isRemoteMergeEnable()
          && req.hasMergeContext()) {
        // The merged block is in a different domain from the original block,
        // so you need to register a new app for holding the merged block.
        result =
            shuffleServer
                .getShuffleTaskManager()
                .registerShuffle(
                    appId + MERGE_APP_SUFFIX,
                    shuffleId,
                    stageAttemptNumber,
                    partitionRanges,
                    new RemoteStorageInfo(remoteStoragePath, remoteStorageConf),
                    user,
                    shuffleDataDistributionType,
                    maxConcurrencyPerPartitionToWrite,
                    req.getPropertiesMap());
        if (result == StatusCode.SUCCESS) {
          result =
              shuffleServer
                  .getShuffleMergeManager()
                  .registerShuffle(appId, shuffleId, req.getMergeContext());
        }
      }
      auditContext.withStatusCode(result);
      reply = ShuffleRegisterResponse.newBuilder().setStatus(result.toProto()).build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void sendShuffleData(
      SendShuffleDataRequest req, StreamObserver<SendShuffleDataResponse> responseObserver) {
    try (ServerRpcAuditContext auditContext = createAuditContext("sendShuffleData")) {
      SendShuffleDataResponse reply;
      String appId = req.getAppId();
      int shuffleId = req.getShuffleId();
      long requireBufferId = req.getRequireBufferId();
      long timestamp = req.getTimestamp();
      int stageAttemptNumber = req.getStageAttemptNumber();

      auditContext.withAppId(appId).withShuffleId(shuffleId);
      auditContext.withArgs(
          "requireBufferId="
              + requireBufferId
              + ", timestamp="
              + timestamp
              + ", stageAttemptNumber="
              + stageAttemptNumber
              + ", shuffleDataSize="
              + req.getShuffleDataCount());

      ShuffleTaskInfo taskInfo = shuffleServer.getShuffleTaskManager().getShuffleTaskInfo(appId);
      if (taskInfo == null) {
        String errorMsg =
            "APP_NOT_FOUND error, requireBufferId["
                + requireBufferId
                + "] for appId["
                + appId
                + "], shuffleId["
                + shuffleId
                + "]";
        LOG.error(errorMsg);
        ShuffleServerMetrics.counterAppNotFound.inc();
        reply =
            SendShuffleDataResponse.newBuilder()
                .setStatus(StatusCode.APP_NOT_FOUND.toProto())
                .setRetMsg(errorMsg)
                .build();
        auditContext.withStatusCode(StatusCode.fromProto(reply.getStatus()));
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
        return;
      }
      Integer latestStageAttemptNumber = taskInfo.getLatestStageAttemptNumber(shuffleId);
      // The Stage retry occurred, and the task before StageNumber was simply ignored and not
      // processed if the task was being sent.
      if (stageAttemptNumber < latestStageAttemptNumber) {
        String responseMessage = "A retry has occurred at the Stage, sending data is invalid.";
        reply =
            SendShuffleDataResponse.newBuilder()
                .setStatus(StatusCode.STAGE_RETRY_IGNORE.toProto())
                .setRetMsg(responseMessage)
                .build();
        auditContext.withStatusCode(StatusCode.fromProto(reply.getStatus()));
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
        return;
      }
      if (timestamp > 0) {
        /*
         * Here we record the transport time, but we don't consider the impact of data size on transport time.
         * The amount of data will not cause great fluctuations in latency. For example, 100K costs 1ms,
         * and 1M costs 10ms. This seems like a normal fluctuation, but it may rise to 10s when the server load is high.
         * In addition, we need to pay attention to that the time of the client machine and the machine
         * time of the Shuffle Server should be kept in sync. TransportTime is accurate only if this condition is met.
         * */
        long transportTime = System.currentTimeMillis() - timestamp;
        if (transportTime > 0) {
          shuffleServer
              .getGrpcMetrics()
              .recordTransportTime(
                  ShuffleServerGrpcMetrics.SEND_SHUFFLE_DATA_METHOD, transportTime);
        }
      }
      int requireSize = shuffleServer.getShuffleTaskManager().getRequireBufferSize(requireBufferId);

      StatusCode ret = StatusCode.SUCCESS;
      String responseMessage = "OK";
      if (req.getShuffleDataCount() > 0) {
        ShuffleServerMetrics.counterTotalReceivedDataSize.inc(requireSize);
        ShuffleTaskManager manager = shuffleServer.getShuffleTaskManager();
        PreAllocatedBufferInfo info = manager.getAndRemovePreAllocatedBuffer(requireBufferId);
        boolean isPreAllocated = info != null;
        if (!isPreAllocated) {
          String errorMsg =
              "Can't find requireBufferId["
                  + requireBufferId
                  + "] for appId["
                  + appId
                  + "], shuffleId["
                  + shuffleId
                  + "]";
          LOG.warn(errorMsg);
          responseMessage = errorMsg;
          reply =
              SendShuffleDataResponse.newBuilder()
                  .setStatus(StatusCode.INTERNAL_ERROR.toProto())
                  .setRetMsg(responseMessage)
                  .build();
          auditContext.withStatusCode(StatusCode.fromProto(reply.getStatus()));
          responseObserver.onNext(reply);
          responseObserver.onCompleted();
          return;
        }
        final long start = System.currentTimeMillis();
        List<ShufflePartitionedData> shufflePartitionedDataList = toPartitionedDataList(req);
        long alreadyReleasedSize = 0;
        boolean hasFailureOccurred = false;
        for (ShufflePartitionedData spd : shufflePartitionedDataList) {
          String shuffleDataInfo =
              "appId["
                  + appId
                  + "], shuffleId["
                  + shuffleId
                  + "], partitionId["
                  + spd.getPartitionId()
                  + "]";
          try {
            ret = manager.cacheShuffleData(appId, shuffleId, isPreAllocated, spd);
            if (ret != StatusCode.SUCCESS) {
              String errorMsg =
                  "Error happened when shuffleEngine.write for "
                      + shuffleDataInfo
                      + ", statusCode="
                      + ret;
              LOG.error(errorMsg);
              responseMessage = errorMsg;
              hasFailureOccurred = true;
              break;
            } else {
              if (shuffleServer.isRemoteMergeEnable()) {
                shuffleServer.getShuffleMergeManager().setDirect(appId, shuffleId, false);
              }
              long toReleasedSize = spd.getTotalBlockEncodedLength();
              // after each cacheShuffleData call, the `preAllocatedSize` is updated timely.
              manager.releasePreAllocatedSize(toReleasedSize);
              alreadyReleasedSize += toReleasedSize;
              manager.updateCachedBlockIds(
                  appId, shuffleId, spd.getPartitionId(), spd.getBlockList());
            }
          } catch (ExceedHugePartitionHardLimitException e) {
            String errorMsg =
                "ExceedHugePartitionHardLimitException Error happened when shuffleEngine.write for "
                    + shuffleDataInfo
                    + ": "
                    + e.getMessage();
            ShuffleServerMetrics.counterTotalHugePartitionExceedHardLimitNum.inc();
            ret = StatusCode.EXCEED_HUGE_PARTITION_HARD_LIMIT;
            responseMessage = errorMsg;
            LOG.error(errorMsg);
            hasFailureOccurred = true;
          } catch (Exception e) {
            String errorMsg =
                "Error happened when shuffleEngine.write for "
                    + shuffleDataInfo
                    + ": "
                    + e.getMessage();
            ret = StatusCode.INTERNAL_ERROR;
            responseMessage = errorMsg;
            LOG.error(errorMsg);
            hasFailureOccurred = true;
            break;
          } finally {
            if (hasFailureOccurred) {
              shuffleServer
                  .getShuffleBufferManager()
                  .releaseMemory(spd.getTotalBlockEncodedLength(), false, false);
            }
          }
        }
        // since the required buffer id is only used once, the shuffle client would try to require
        // another buffer whether
        // current connection succeeded or not. Therefore, the preAllocatedBuffer is first get and
        // removed, then after
        // cacheShuffleData finishes, the preAllocatedSize should be updated accordingly.
        if (info.getRequireSize() > alreadyReleasedSize) {
          manager.releasePreAllocatedSize(info.getRequireSize() - alreadyReleasedSize);
        }
        reply =
            SendShuffleDataResponse.newBuilder()
                .setStatus(ret.toProto())
                .setRetMsg(responseMessage)
                .build();
        long costTime = System.currentTimeMillis() - start;
        shuffleServer
            .getGrpcMetrics()
            .recordProcessTime(ShuffleServerGrpcMetrics.SEND_SHUFFLE_DATA_METHOD, costTime);
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Cache Shuffle Data for appId["
                  + appId
                  + "], shuffleId["
                  + shuffleId
                  + "], cost "
                  + costTime
                  + " ms with "
                  + shufflePartitionedDataList.size()
                  + " blocks and "
                  + requireSize
                  + " bytes");
        }
      } else {
        reply =
            SendShuffleDataResponse.newBuilder()
                .setStatus(StatusCode.INTERNAL_ERROR.toProto())
                .setRetMsg("No data in request")
                .build();
      }

      auditContext.withStatusCode(StatusCode.fromProto(reply.getStatus()));
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void commitShuffleTask(
      ShuffleCommitRequest req, StreamObserver<ShuffleCommitResponse> responseObserver) {
    try (ServerRpcAuditContext auditContext = createAuditContext("commitShuffleTask")) {
      String appId = req.getAppId();
      int shuffleId = req.getShuffleId();
      auditContext.withAppId(appId).withShuffleId(shuffleId);
      org.apache.uniffle.common.StorageType storageType =
          shuffleServer.getShuffleServerConf().get(ShuffleServerConf.RSS_STORAGE_TYPE);
      boolean storageTypeWithMemory =
          StorageType.withMemory(StorageType.valueOf(storageType.name()));
      if (storageTypeWithMemory) {
        String errorMessage =
            String.format(
                "commitShuffleTask should not be called while server-side configured StorageType to %s for appId %s",
                storageType, appId);
        LOG.error(errorMessage);
        throw new UnsupportedOperationException(errorMessage);
      }
      StatusCode status = verifyRequest(appId);
      if (status != StatusCode.SUCCESS) {
        auditContext.withStatusCode(status);
        ShuffleCommitResponse response =
            ShuffleCommitResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(status.toString())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
        return;
      }
      String msg = "OK";
      int commitCount = 0;

      try {
        if (!shuffleServer.getShuffleTaskManager().getAppIds().contains(appId)) {
          throw new IllegalStateException("AppId " + appId + " was removed already");
        }
        commitCount =
            shuffleServer.getShuffleTaskManager().updateAndGetCommitCount(appId, shuffleId);
        auditContext.withReturnValue("commitCount=" + commitCount);
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Get commitShuffleTask request for appId["
                  + appId
                  + "], shuffleId["
                  + shuffleId
                  + "], currentCommitted["
                  + commitCount
                  + "]");
        }
      } catch (Exception e) {
        status = StatusCode.INTERNAL_ERROR;
        msg = "Error happened when commit for appId[" + appId + "], shuffleId[" + shuffleId + "]";
        LOG.error(msg, e);
      }

      auditContext.withStatusCode(status);
      ShuffleCommitResponse reply =
          ShuffleCommitResponse.newBuilder()
              .setCommitCount(commitCount)
              .setStatus(status.toProto())
              .setRetMsg(msg)
              .build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void finishShuffle(
      FinishShuffleRequest req, StreamObserver<FinishShuffleResponse> responseObserver) {
    try (ServerRpcAuditContext auditContext = createAuditContext("finishShuffle")) {
      String appId = req.getAppId();
      int shuffleId = req.getShuffleId();
      auditContext.withAppId(appId).withShuffleId(shuffleId);
      org.apache.uniffle.common.StorageType storageType =
          shuffleServer.getShuffleServerConf().get(ShuffleServerConf.RSS_STORAGE_TYPE);
      boolean storageTypeWithMemory =
          StorageType.withMemory(StorageType.valueOf(storageType.name()));
      if (storageTypeWithMemory) {
        String errorMessage =
            String.format(
                "finishShuffle should not be called while server-side configured StorageType to %s for appId %s",
                storageType, appId);
        LOG.error(errorMessage);
        throw new UnsupportedOperationException(errorMessage);
      }
      StatusCode status = verifyRequest(appId);
      if (status != StatusCode.SUCCESS) {
        auditContext.withStatusCode(status);
        FinishShuffleResponse response =
            FinishShuffleResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(status.toString())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
        return;
      }
      String msg = "OK";
      String errorMsg =
          "Fail to finish shuffle for appId["
              + appId
              + "], shuffleId["
              + shuffleId
              + "], data may be lost";
      try {
        LOG.info(
            "Get finishShuffle request for appId[" + appId + "], shuffleId[" + shuffleId + "]");
        status = shuffleServer.getShuffleTaskManager().commitShuffle(appId, shuffleId);
        if (status != StatusCode.SUCCESS) {
          status = StatusCode.INTERNAL_ERROR;
          msg = errorMsg;
        }
      } catch (Exception e) {
        status = StatusCode.INTERNAL_ERROR;
        msg = errorMsg;
        LOG.error(errorMsg, e);
      }

      auditContext.withStatusCode(status);
      FinishShuffleResponse response =
          FinishShuffleResponse.newBuilder().setStatus(status.toProto()).setRetMsg(msg).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void requireBuffer(
      RequireBufferRequest request, StreamObserver<RequireBufferResponse> responseObserver) {
    try (ServerRpcAuditContext auditContext = createAuditContext("requireBuffer")) {
      String appId = request.getAppId();
      auditContext.withAppId(appId).withShuffleId(request.getShuffleId());
      String auditArgs = "requireSize=" + request.getRequireSize();
      if (request.getPartitionIdsList() != null) {
        auditArgs += ", partitionIdsSize=" + request.getPartitionIdsList().size();
      }
      if (request.getPartitionIdsList() != null) {
        auditArgs +=
            ", partitionIds=" + OutputUtils.listToSegment(request.getPartitionIdsList(), 1, 10);
      }
      auditContext.withArgs(auditArgs);
      StatusCode status = verifyRequest(appId);
      if (status != StatusCode.SUCCESS) {
        auditContext.withStatusCode(status);
        RequireBufferResponse response =
            RequireBufferResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(status.toString())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
        return;
      }
      long requireBufferId = -1;
      String responseMessage = "";
      String shuffleDataInfo = "appId[" + appId + "], shuffleId[" + request.getShuffleId() + "]";
      List<Integer> needSplitPartitionIds = Collections.emptyList();
      try {
        if (StringUtils.isEmpty(appId)) {
          // To be compatible with older client version
          requireBufferId =
              shuffleServer.getShuffleTaskManager().requireBuffer(request.getRequireSize());
        } else {
          Pair<Long, List<Integer>> pair =
              shuffleServer
                  .getShuffleTaskManager()
                  .requireBufferReturnPair(
                      appId,
                      request.getShuffleId(),
                      request.getPartitionIdsList(),
                      request.getPartitionRequireSizesList(),
                      request.getRequireSize());
          requireBufferId = pair.getLeft();
          needSplitPartitionIds = pair.getRight();
        }
      } catch (NoBufferException e) {
        responseMessage = e.getMessage();
        status = StatusCode.NO_BUFFER;
        ShuffleServerMetrics.counterTotalRequireBufferFailedForRegularPartition.inc();
        ShuffleServerMetrics.counterTotalRequireBufferFailed.inc();
      } catch (NoBufferForHugePartitionException e) {
        responseMessage = e.getMessage();
        status = StatusCode.NO_BUFFER_FOR_HUGE_PARTITION;
        ShuffleServerMetrics.counterTotalRequireBufferFailedForHugePartition.inc();
        ShuffleServerMetrics.counterTotalRequireBufferFailed.inc();
      } catch (NoRegisterException e) {
        responseMessage = e.getMessage();
        status = StatusCode.NO_REGISTER;
        ShuffleServerMetrics.counterTotalRequireBufferFailed.inc();
      } catch (ExceedHugePartitionHardLimitException e) {
        status = StatusCode.EXCEED_HUGE_PARTITION_HARD_LIMIT;
        ShuffleServerMetrics.counterTotalHugePartitionExceedHardLimitNum.inc();
        ShuffleServerMetrics.counterTotalRequireBufferFailed.inc();
        responseMessage =
            "ExceedHugePartitionHardLimitException Error happened when requireBuffer for "
                + shuffleDataInfo
                + ": "
                + e.getMessage();
        LOG.error(responseMessage);
      }
      auditContext.withStatusCode(status);
      auditContext.withReturnValue("requireBufferId=" + requireBufferId);
      RequireBufferResponse response =
          RequireBufferResponse.newBuilder()
              .setStatus(status.toProto())
              .setRequireBufferId(requireBufferId)
              .setRetMsg(responseMessage)
              .addAllNeedSplitPartitionIds(needSplitPartitionIds)
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void appHeartbeat(
      AppHeartBeatRequest request, StreamObserver<AppHeartBeatResponse> responseObserver) {
    try (ServerRpcAuditContext auditContext = createAuditContext("appHeartbeat")) {
      String appId = request.getAppId();
      auditContext.withAppId(appId);
      StatusCode status = verifyRequest(appId);
      if (status != StatusCode.SUCCESS) {
        auditContext.withStatusCode(status);
        AppHeartBeatResponse response =
            AppHeartBeatResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(status.toString())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
        return;
      }

      if (Context.current().isCancelled()) {
        auditContext.withStatusCode("CANCELLED");
        responseObserver.onError(
            Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
        LOG.warn("Cancelled by client {} for after deadline.", appId);
        return;
      }

      LOG.info("Get heartbeat from {}", appId);
      auditContext.withStatusCode(StatusCode.SUCCESS);
      shuffleServer.getShuffleTaskManager().refreshAppId(appId);
      if (shuffleServer.isRemoteMergeEnable()) {
        shuffleServer.getShuffleMergeManager().refreshAppId(appId);
      }
      AppHeartBeatResponse response =
          AppHeartBeatResponse.newBuilder()
              .setRetMsg("")
              .setStatus(StatusCode.SUCCESS.toProto())
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void reportShuffleResult(
      ReportShuffleResultRequest request,
      StreamObserver<ReportShuffleResultResponse> responseObserver) {
    try (ServerRpcAuditContext auditContext = createAuditContext("reportShuffleResult")) {
      String appId = request.getAppId();
      int shuffleId = request.getShuffleId();
      long taskAttemptId = request.getTaskAttemptId();
      int bitmapNum = request.getBitmapNum();
      Map<Integer, long[]> partitionToBlockIds =
          toPartitionBlocksMap(request.getPartitionToBlockIdsList());
      auditContext.withAppId(appId).withShuffleId(shuffleId);
      auditContext.withArgs(
          "taskAttemptId="
              + taskAttemptId
              + ", bitmapNum="
              + bitmapNum
              + ", partitionToBlockIdsSize="
              + partitionToBlockIds.size());

      StatusCode status = verifyRequest(appId);
      if (status != StatusCode.SUCCESS) {
        auditContext.withStatusCode(status);
        ReportShuffleResultResponse response =
            ReportShuffleResultResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(status.toString())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
        return;
      }

      String msg = "OK";
      ReportShuffleResultResponse reply;
      String requestInfo =
          "appId["
              + appId
              + "], shuffleId["
              + shuffleId
              + "], taskAttemptId["
              + taskAttemptId
              + "]";

      try {
        long start = System.currentTimeMillis();
        int expectedBlockCount =
            partitionToBlockIds.values().stream().mapToInt(x -> x.length).sum();
        LOG.debug(
            "Accepted blockIds report for {} blocks across {} partitions as shuffle result for task {}",
            expectedBlockCount,
            partitionToBlockIds.size(),
            requestInfo);
        int updatedBlockCount =
            shuffleServer
                .getShuffleTaskManager()
                .addFinishedBlockIds(appId, shuffleId, partitionToBlockIds, bitmapNum);
        long costTime = System.currentTimeMillis() - start;
        shuffleServer
            .getGrpcMetrics()
            .recordProcessTime(ShuffleServerGrpcMetrics.REPORT_SHUFFLE_RESULT_METHOD, costTime);
        if (expectedBlockCount != updatedBlockCount) {
          LOG.warn(
              "Existing {} duplicated blockIds on blockId report for appId: {}, shuffleId: {}",
              expectedBlockCount - updatedBlockCount,
              appId,
              shuffleId);
        }
      } catch (Exception e) {
        status = StatusCode.INTERNAL_ERROR;
        msg = "error happened when report shuffle result, check shuffle server for detail";
        LOG.error("Error happened when report shuffle result for " + requestInfo, e);
      }

      auditContext.withStatusCode(status);
      reply =
          ReportShuffleResultResponse.newBuilder()
              .setStatus(status.toProto())
              .setRetMsg(msg)
              .build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void getShuffleResult(
      GetShuffleResultRequest request, StreamObserver<GetShuffleResultResponse> responseObserver) {
    try (ServerRpcAuditContext auditContext = createAuditContext("getShuffleResult")) {
      String appId = request.getAppId();
      int shuffleId = request.getShuffleId();
      int partitionId = request.getPartitionId();
      BlockIdLayout blockIdLayout =
          BlockIdLayout.from(
              request.getBlockIdLayout().getSequenceNoBits(),
              request.getBlockIdLayout().getPartitionIdBits(),
              request.getBlockIdLayout().getTaskAttemptIdBits());

      auditContext.withAppId(appId).withShuffleId(shuffleId);
      auditContext.withArgs("partitionId=" + partitionId + ", blockIdLayout=" + blockIdLayout);

      StatusCode status = verifyRequest(appId);
      if (status != StatusCode.SUCCESS) {
        auditContext.withStatusCode(status);
        GetShuffleResultResponse response =
            GetShuffleResultResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(status.toString())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
        return;
      }

      String msg = "OK";
      GetShuffleResultResponse reply;
      byte[] serializedBlockIds = null;
      String requestInfo =
          "appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]";
      ByteString serializedBlockIdsBytes = ByteString.EMPTY;

      try {
        serializedBlockIds =
            shuffleServer
                .getShuffleTaskManager()
                .getFinishedBlockIds(appId, shuffleId, Sets.newHashSet(partitionId), blockIdLayout);
        if (serializedBlockIds == null) {
          status = StatusCode.INTERNAL_ERROR;
          msg = "Can't get shuffle result for " + requestInfo;
          LOG.warn(msg);
        } else {
          serializedBlockIdsBytes = UnsafeByteOperations.unsafeWrap(serializedBlockIds);
        }
      } catch (Exception e) {
        status = StatusCode.INTERNAL_ERROR;
        msg = e.getMessage();
        LOG.error("Error happened when get shuffle result for {}", requestInfo, e);
      }

      auditContext.withStatusCode(status);
      reply =
          GetShuffleResultResponse.newBuilder()
              .setStatus(status.toProto())
              .setRetMsg(msg)
              .setSerializedBitmap(serializedBlockIdsBytes)
              .build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void getShuffleResultForMultiPart(
      GetShuffleResultForMultiPartRequest request,
      StreamObserver<GetShuffleResultForMultiPartResponse> responseObserver) {
    try (ServerRpcAuditContext auditContext = createAuditContext("getShuffleResultForMultiPart")) {
      String appId = request.getAppId();
      int shuffleId = request.getShuffleId();
      List<Integer> partitionsList = request.getPartitionsList();

      BlockIdLayout blockIdLayout =
          BlockIdLayout.from(
              request.getBlockIdLayout().getSequenceNoBits(),
              request.getBlockIdLayout().getPartitionIdBits(),
              request.getBlockIdLayout().getTaskAttemptIdBits());

      auditContext.withAppId(appId).withShuffleId(shuffleId);
      String partitionIdsOutput = OutputUtils.listToSegment(partitionsList, 1, 10);
      auditContext.withArgs(
          "partitionsListSize="
              + partitionsList.size()
              + ", partitionIds="
              + partitionIdsOutput
              + ", blockIdLayout="
              + blockIdLayout);

      StatusCode status = verifyRequest(appId);
      if (status != StatusCode.SUCCESS) {
        auditContext.withStatusCode(status);
        GetShuffleResultForMultiPartResponse response =
            GetShuffleResultForMultiPartResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(status.toString())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
        return;
      }

      String msg = "OK";
      GetShuffleResultForMultiPartResponse reply;
      byte[] serializedBlockIds = null;
      String requestInfo =
          "appId[" + appId + "], shuffleId[" + shuffleId + "], partitions=" + partitionIdsOutput;
      ByteString serializedBlockIdsBytes = ByteString.EMPTY;

      try {
        long start = System.currentTimeMillis();
        serializedBlockIds =
            shuffleServer
                .getShuffleTaskManager()
                .getFinishedBlockIds(
                    appId, shuffleId, Sets.newHashSet(partitionsList), blockIdLayout);
        if (serializedBlockIds == null) {
          status = StatusCode.INTERNAL_ERROR;
          msg = "Can't get shuffle result for " + requestInfo;
          LOG.warn(msg);
        } else {
          serializedBlockIdsBytes = UnsafeByteOperations.unsafeWrap(serializedBlockIds);
          long costTime = System.currentTimeMillis() - start;
          shuffleServer
              .getGrpcMetrics()
              .recordProcessTime(
                  ShuffleServerGrpcMetrics.GET_SHUFFLE_RESULT_FOR_MULTI_PART_METHOD, costTime);
        }
      } catch (Exception e) {
        status = StatusCode.INTERNAL_ERROR;
        msg = e.getMessage();
        LOG.error("Error happened when get shuffle result for {}", requestInfo, e);
      }

      auditContext.withStatusCode(status);
      reply =
          GetShuffleResultForMultiPartResponse.newBuilder()
              .setStatus(status.toProto())
              .setRetMsg(msg)
              .setSerializedBitmap(serializedBlockIdsBytes)
              .build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void getLocalShuffleData(
      GetLocalShuffleDataRequest request,
      StreamObserver<GetLocalShuffleDataResponse> responseObserver) {
    try (ServerRpcAuditContext auditContext = createAuditContext("getLocalShuffleData")) {
      String appId = request.getAppId();
      int shuffleId = request.getShuffleId();
      int partitionId = request.getPartitionId();
      int partitionNumPerRange = request.getPartitionNumPerRange();
      int partitionNum = request.getPartitionNum();
      long offset = request.getOffset();
      int length = request.getLength();
      int storageId = request.getStorageId();

      auditContext.withAppId(appId).withShuffleId(shuffleId);
      auditContext.withArgs(
          "partitionId="
              + partitionId
              + ", partitionNumPerRange="
              + partitionNumPerRange
              + ", partitionNum="
              + partitionNum
              + ", offset="
              + offset
              + ", length="
              + length
              + ", storageId="
              + storageId);

      StatusCode status = verifyRequest(appId);
      if (status != StatusCode.SUCCESS) {
        auditContext.withStatusCode(status);
        GetLocalShuffleDataResponse response =
            GetLocalShuffleDataResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(status.toString())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
        return;
      }

      long timestamp = request.getTimestamp();
      if (timestamp > 0) {
        long transportTime = System.currentTimeMillis() - timestamp;
        if (transportTime > 0) {
          shuffleServer
              .getGrpcMetrics()
              .recordTransportTime(ShuffleServerGrpcMetrics.GET_SHUFFLE_DATA_METHOD, transportTime);
        }
      }
      String storageType =
          shuffleServer.getShuffleServerConf().get(RssBaseConf.RSS_STORAGE_TYPE).name();
      String msg = "OK";
      GetLocalShuffleDataResponse reply = null;
      ShuffleDataResult sdr = null;
      String requestInfo =
          "appId["
              + appId
              + "], shuffleId["
              + shuffleId
              + "], partitionId["
              + partitionId
              + "]"
              + "offset["
              + offset
              + "]"
              + "length["
              + length
              + "]";

      int[] range =
          ShuffleStorageUtils.getPartitionRange(partitionId, partitionNumPerRange, partitionNum);
      Storage storage =
          shuffleServer
              .getStorageManager()
              .selectStorage(
                  new ShuffleDataReadEvent(appId, shuffleId, partitionId, range[0], storageId));
      if (storage != null) {
        storage.updateReadMetrics(new StorageReadMetrics(appId, shuffleId));
      }

      if (shuffleServer.getShuffleBufferManager().requireReadMemory(length)) {
        try {
          long start = System.currentTimeMillis();
          sdr =
              shuffleServer
                  .getShuffleTaskManager()
                  .getShuffleData(
                      appId,
                      shuffleId,
                      partitionId,
                      partitionNumPerRange,
                      partitionNum,
                      storageType,
                      offset,
                      length,
                      storageId);
          reply =
              GetLocalShuffleDataResponse.newBuilder()
                  .setStatus(status.toProto())
                  .setRetMsg(msg)
                  .setData(UnsafeByteOperations.unsafeWrap(sdr.getData()))
                  .build();
          long readTime = System.currentTimeMillis() - start;
          ShuffleServerMetrics.counterTotalReadTime.inc(readTime);
          ShuffleServerMetrics.counterTotalReadDataSize.inc(sdr.getDataLength());
          ShuffleServerMetrics.counterTotalReadLocalDataFileSize.inc(sdr.getDataLength());
          ShuffleServerMetrics.gaugeReadLocalDataFileThreadNum.inc();
          ShuffleServerMetrics.gaugeReadLocalDataFileBufferSize.inc(length);
          shuffleServer
              .getGrpcMetrics()
              .recordProcessTime(ShuffleServerGrpcMetrics.GET_SHUFFLE_DATA_METHOD, readTime);
          LOG.info(
              "Successfully getShuffleData cost {} ms for shuffle data with {}",
              readTime,
              requestInfo);
        } catch (Exception e) {
          status = StatusCode.INTERNAL_ERROR;
          msg = "Error happened when get shuffle data for " + requestInfo + ", " + e.getMessage();
          LOG.error(msg, e);
          reply =
              GetLocalShuffleDataResponse.newBuilder()
                  .setStatus(status.toProto())
                  .setRetMsg(msg)
                  .build();
        } finally {
          if (sdr != null) {
            sdr.release();
            ShuffleServerMetrics.gaugeReadLocalDataFileThreadNum.dec();
            ShuffleServerMetrics.gaugeReadLocalDataFileBufferSize.dec(length);
          }
          shuffleServer.getShuffleBufferManager().releaseReadMemory(length);
        }
      } else {
        status = StatusCode.NO_BUFFER;
        msg = "Can't require memory to get shuffle data";
        LOG.warn("{} for {}", msg, requestInfo);
        reply =
            GetLocalShuffleDataResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(msg)
                .build();
      }
      auditContext.withStatusCode(status);
      auditContext.withReturnValue("len=" + (sdr == null ? 0 : sdr.getDataLength()));
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void getLocalShuffleIndex(
      GetLocalShuffleIndexRequest request,
      StreamObserver<GetLocalShuffleIndexResponse> responseObserver) {
    try (ServerRpcAuditContext auditContext = createAuditContext("getLocalShuffleIndex")) {
      String appId = request.getAppId();
      int shuffleId = request.getShuffleId();
      int partitionId = request.getPartitionId();
      int partitionNumPerRange = request.getPartitionNumPerRange();
      int partitionNum = request.getPartitionNum();
      auditContext.withAppId(appId).withShuffleId(shuffleId);
      auditContext.withArgs(
          "partitionId="
              + partitionId
              + ", partitionNumPerRange="
              + partitionNumPerRange
              + ", partitionNum="
              + partitionNum);

      StatusCode status = verifyRequest(appId);
      if (status != StatusCode.SUCCESS) {
        auditContext.withStatusCode(status);
        GetLocalShuffleIndexResponse reply =
            GetLocalShuffleIndexResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(status.toString())
                .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
        return;
      }

      String msg = "OK";
      GetLocalShuffleIndexResponse reply;
      String requestInfo =
          "appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]";

      int[] range =
          ShuffleStorageUtils.getPartitionRange(partitionId, partitionNumPerRange, partitionNum);
      Storage storage =
          shuffleServer
              .getStorageManager()
              .selectStorage(new ShuffleDataReadEvent(appId, shuffleId, partitionId, range[0]));
      if (storage != null) {
        storage.updateReadMetrics(new StorageReadMetrics(appId, shuffleId));
      }
      // Index file is expected small size and won't cause oom problem with the assumed size. An
      // index
      // segment is 40B,
      // with the default size - 2MB, it can support 50k blocks for shuffle data.
      long assumedFileSize =
          shuffleServer
              .getShuffleServerConf()
              .getLong(ShuffleServerConf.SERVER_SHUFFLE_INDEX_SIZE_HINT);
      if (shuffleServer.getShuffleBufferManager().requireReadMemory(assumedFileSize)) {
        ShuffleIndexResult shuffleIndexResult = null;
        try {
          final long start = System.currentTimeMillis();
          shuffleIndexResult =
              shuffleServer
                  .getShuffleTaskManager()
                  .getShuffleIndex(
                      appId, shuffleId, partitionId, partitionNumPerRange, partitionNum);

          ByteBuffer data = shuffleIndexResult.getIndexData();
          ShuffleServerMetrics.counterTotalReadDataSize.inc(data.remaining());
          ShuffleServerMetrics.counterTotalReadLocalIndexFileSize.inc(data.remaining());
          ShuffleServerMetrics.gaugeReadLocalIndexFileThreadNum.inc();
          ShuffleServerMetrics.gaugeReadLocalIndexFileBufferSize.inc(assumedFileSize);
          GetLocalShuffleIndexResponse.Builder builder =
              GetLocalShuffleIndexResponse.newBuilder().setStatus(status.toProto()).setRetMsg(msg);
          builder.setIndexData(UnsafeByteOperations.unsafeWrap(data));
          builder.setDataFileLen(shuffleIndexResult.getDataFileLen());
          builder.addAllStorageIds(
              Arrays.stream(shuffleIndexResult.getStorageIds())
                  .boxed()
                  .collect(Collectors.toList()));
          long readTime = System.currentTimeMillis() - start;
          shuffleServer
              .getGrpcMetrics()
              .recordProcessTime(ShuffleServerGrpcMetrics.GET_SHUFFLE_INDEX_METHOD, readTime);
          LOG.info(
              "Successfully getShuffleIndex cost {} ms for {} bytes with {}",
              readTime,
              data.remaining(),
              requestInfo);
          auditContext.withReturnValue("len=" + shuffleIndexResult.getDataFileLen());
          reply = builder.build();
        } catch (FileNotFoundException indexFileNotFoundException) {
          LOG.warn(
              "Index file for {} is not found, maybe the data has been flushed to cold storage "
                  + "or still in memory buffer pool.",
              requestInfo,
              indexFileNotFoundException);
          reply = GetLocalShuffleIndexResponse.newBuilder().setStatus(status.toProto()).build();
        } catch (Exception e) {
          status = StatusCode.INTERNAL_ERROR;
          msg = "Error happened when get shuffle index for " + requestInfo + ", " + e.getMessage();
          LOG.error(msg, e);
          reply =
              GetLocalShuffleIndexResponse.newBuilder()
                  .setStatus(status.toProto())
                  .setRetMsg(msg)
                  .build();
        } finally {
          if (shuffleIndexResult != null) {
            shuffleIndexResult.release();
            ShuffleServerMetrics.gaugeReadLocalIndexFileThreadNum.dec();
            ShuffleServerMetrics.gaugeReadLocalIndexFileBufferSize.dec(assumedFileSize);
          }
          shuffleServer.getShuffleBufferManager().releaseReadMemory(assumedFileSize);
        }
      } else {
        status = StatusCode.NO_BUFFER;
        msg = "Can't require memory to get shuffle index";
        LOG.warn("{} for {}", msg, requestInfo);
        reply =
            GetLocalShuffleIndexResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(msg)
                .build();
      }
      auditContext.withStatusCode(status);
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void getMemoryShuffleData(
      GetMemoryShuffleDataRequest request,
      StreamObserver<GetMemoryShuffleDataResponse> responseObserver) {
    try (ServerRpcAuditContext auditContext = createAuditContext("getMemoryShuffleData")) {
      String appId = request.getAppId();
      int shuffleId = request.getShuffleId();
      int partitionId = request.getPartitionId();
      long blockId = request.getLastBlockId();
      int readBufferSize = request.getReadBufferSize();

      auditContext.withAppId(appId).withShuffleId(shuffleId);
      auditContext.withArgs(
          "partitionId="
              + partitionId
              + ", blockId="
              + blockId
              + ", readBufferSize="
              + readBufferSize);

      StatusCode status = verifyRequest(appId);
      if (status != StatusCode.SUCCESS) {
        auditContext.withStatusCode(status);
        GetMemoryShuffleDataResponse reply =
            GetMemoryShuffleDataResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(status.toString())
                .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
        return;
      }

      long timestamp = request.getTimestamp();
      if (timestamp > 0) {
        long transportTime = System.currentTimeMillis() - timestamp;
        if (transportTime > 0) {
          shuffleServer
              .getGrpcMetrics()
              .recordTransportTime(
                  ShuffleServerGrpcMetrics.GET_MEMORY_SHUFFLE_DATA_METHOD, transportTime);
        }
      }
      String msg = "OK";
      GetMemoryShuffleDataResponse reply;
      String requestInfo =
          "appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]";

      // todo: if can get the exact memory size?
      if (shuffleServer.getShuffleBufferManager().requireReadMemory(readBufferSize)) {
        ShuffleDataResult shuffleDataResult = null;
        try {
          final long start = System.currentTimeMillis();
          Roaring64NavigableMap expectedTaskIds = null;
          if (request.getSerializedExpectedTaskIdsBitmap() != null
              && !request.getSerializedExpectedTaskIdsBitmap().isEmpty()) {
            expectedTaskIds =
                RssUtils.deserializeBitMap(
                    request.getSerializedExpectedTaskIdsBitmap().toByteArray());
          }
          shuffleDataResult =
              shuffleServer
                  .getShuffleTaskManager()
                  .getInMemoryShuffleData(
                      appId, shuffleId, partitionId, blockId, readBufferSize, expectedTaskIds);
          byte[] data = new byte[] {};
          List<BufferSegment> bufferSegments = Lists.newArrayList();
          if (shuffleDataResult != null) {
            data = shuffleDataResult.getData();
            bufferSegments = shuffleDataResult.getBufferSegments();
            ShuffleServerMetrics.counterTotalReadDataSize.inc(data.length);
            ShuffleServerMetrics.counterTotalReadMemoryDataSize.inc(data.length);
            ShuffleServerMetrics.gaugeReadMemoryDataThreadNum.inc();
            ShuffleServerMetrics.gaugeReadMemoryDataBufferSize.inc(readBufferSize);
          }
          long costTime = System.currentTimeMillis() - start;
          shuffleServer
              .getGrpcMetrics()
              .recordProcessTime(ShuffleServerGrpcMetrics.GET_MEMORY_SHUFFLE_DATA_METHOD, costTime);
          LOG.info(
              "Successfully getInMemoryShuffleData cost {} ms with {} bytes shuffle data for {}",
              costTime,
              data.length,
              requestInfo);
          auditContext.withReturnValue(
              "len=" + data.length + ", bufferSegmentSize=" + bufferSegments.size());
          reply =
              GetMemoryShuffleDataResponse.newBuilder()
                  .setStatus(status.toProto())
                  .setRetMsg(msg)
                  .setData(UnsafeByteOperations.unsafeWrap(data))
                  .addAllShuffleDataBlockSegments(toShuffleDataBlockSegments(bufferSegments))
                  .build();
        } catch (Exception e) {
          status = StatusCode.INTERNAL_ERROR;
          msg =
              "Error happened when get in memory shuffle data for "
                  + requestInfo
                  + ", "
                  + e.getMessage();
          LOG.error(msg, e);
          reply =
              GetMemoryShuffleDataResponse.newBuilder()
                  .setData(UnsafeByteOperations.unsafeWrap(new byte[] {}))
                  .addAllShuffleDataBlockSegments(Lists.newArrayList())
                  .setStatus(status.toProto())
                  .setRetMsg(msg)
                  .build();
        } finally {
          if (shuffleDataResult != null) {
            shuffleDataResult.release();
            ShuffleServerMetrics.gaugeReadMemoryDataThreadNum.dec();
            ShuffleServerMetrics.gaugeReadMemoryDataBufferSize.dec(readBufferSize);
          }
          shuffleServer.getShuffleBufferManager().releaseReadMemory(readBufferSize);
        }
      } else {
        status = StatusCode.NO_BUFFER;
        msg = "Can't require memory to get in memory shuffle data";
        LOG.warn("{} for {}", msg, requestInfo);
        reply =
            GetMemoryShuffleDataResponse.newBuilder()
                .setData(UnsafeByteOperations.unsafeWrap(new byte[] {}))
                .addAllShuffleDataBlockSegments(Lists.newArrayList())
                .setStatus(status.toProto())
                .setRetMsg(msg)
                .build();
      }

      auditContext.withStatusCode(status);
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void startSortMerge(
      RssProtos.StartSortMergeRequest request,
      StreamObserver<RssProtos.StartSortMergeResponse> responseObserver) {
    try (ServerRpcAuditContext auditContext = createAuditContext("startSortMerge")) {
      String appId = request.getAppId();
      int shuffleId = request.getShuffleId();
      int partitionId = request.getPartitionId();

      auditContext
          .withAppId(appId)
          .withShuffleId(shuffleId)
          .withArgs(String.format("partitionId=%d", partitionId));

      StatusCode status = StatusCode.SUCCESS;
      String msg = "OK";
      RssProtos.StartSortMergeResponse reply;
      String requestInfo =
          "appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]";
      try {
        Roaring64NavigableMap expectedBlockIdMap =
            RssUtils.deserializeBitMap(request.getUniqueBlocksBitmap().toByteArray());
        LOG.info(
            "Report "
                + expectedBlockIdMap.getLongCardinality()
                + " unique blocks for "
                + requestInfo);
        if (shuffleServer.isRemoteMergeEnable()) {
          shuffleServer
              .getShuffleMergeManager()
              .startSortMerge(appId, shuffleId, partitionId, expectedBlockIdMap);
        } else {
          status = StatusCode.INTERNAL_ERROR;
          msg = "Remote merge is disabled, can not report StartSortMerge!";
        }
      } catch (IOException e) {
        status = StatusCode.INTERNAL_ERROR;
        msg = e.getMessage();
        LOG.error("Error happened when report unique blocks for {}, {}", requestInfo, e);
      }
      reply =
          RssProtos.StartSortMergeResponse.newBuilder()
              .setStatus(status.toProto())
              .setRetMsg(msg)
              .build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void getSortedShuffleData(
      RssProtos.GetSortedShuffleDataRequest request,
      StreamObserver<RssProtos.GetSortedShuffleDataResponse> responseObserver) {
    try (ServerRpcAuditContext auditContext = createAuditContext("getSortedShuffleData")) {
      String appId = request.getAppId();
      int shuffleId = request.getShuffleId();
      int partitionId = request.getPartitionId();
      long blockId = request.getMergedBlockId();
      long timestamp = request.getTimestamp();
      auditContext
          .withAppId(appId)
          .withShuffleId(shuffleId)
          .withArgs(String.format("partitionId=%d, blockId=%d", partitionId, blockId));

      if (timestamp > 0) {
        long transportTime = System.currentTimeMillis() - timestamp;
        if (transportTime > 0) {
          shuffleServer
              .getGrpcMetrics()
              .recordTransportTime(
                  ShuffleServerGrpcMetrics.GET_SORTED_SHUFFLE_DATA_METHOD, transportTime);
        }
      }
      StatusCode status = StatusCode.SUCCESS;
      String msg = "OK";
      RssProtos.GetSortedShuffleDataResponse reply = null;
      ShuffleDataResult sdr = null;
      String requestInfo =
          "appId["
              + appId
              + "], shuffleId["
              + shuffleId
              + "], partitionId["
              + partitionId
              + "]"
              + "blockId["
              + blockId
              + "]";

      if (!shuffleServer.isRemoteMergeEnable()) {
        msg = "Remote merge is disabled";
        status = StatusCode.INTERNAL_ERROR;
        reply =
            RssProtos.GetSortedShuffleDataResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(msg)
                .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
        return;
      }

      MergeStatus mergeStatus =
          shuffleServer
              .getShuffleMergeManager()
              .tryGetBlock(appId, shuffleId, partitionId, blockId);
      MergeState mergeState = mergeStatus.getState();
      long blockSize = mergeStatus.getSize();
      if (mergeState == MergeState.INITED) {
        msg = MergeState.INITED.name();
        reply =
            RssProtos.GetSortedShuffleDataResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(msg)
                .setMState(mergeState.code())
                .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
        return;
      } else if (mergeState == MergeState.MERGING && blockSize == -1) {
        // Notify the client that all merged data has been read, but there may be data that has not
        // yet been merged.
        msg = MergeState.MERGING.name();
        reply =
            RssProtos.GetSortedShuffleDataResponse.newBuilder()
                .setStatus(status.toProto())
                .setNextBlockId(-1)
                .setRetMsg(msg)
                .setMState(mergeState.code())
                .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
        return;
      } else if (mergeState == MergeState.DONE && blockSize == -1) {
        // Notify the client that all data has been read
        msg = MergeState.DONE.name();
        reply =
            RssProtos.GetSortedShuffleDataResponse.newBuilder()
                .setStatus(status.toProto())
                .setNextBlockId(-1)
                .setRetMsg(msg)
                .setMState(mergeState.code())
                .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
        return;
      } else if (mergeState == MergeState.INTERNAL_ERROR) {
        msg = MergeState.INTERNAL_ERROR.name();
        status = StatusCode.INTERNAL_ERROR;
        reply =
            RssProtos.GetSortedShuffleDataResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(msg)
                .setMState(mergeState.code())
                .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
        return;
      }

      if (shuffleServer.getShuffleBufferManager().requireReadMemory(blockSize)) {
        try {
          long start = System.currentTimeMillis();
          sdr =
              shuffleServer
                  .getShuffleMergeManager()
                  .getShuffleData(appId, shuffleId, partitionId, blockId);
          long readTime = System.currentTimeMillis() - start;
          ShuffleServerMetrics.counterTotalReadTime.inc(readTime);
          ShuffleServerMetrics.counterTotalReadDataSize.inc(sdr.getDataLength());
          ShuffleServerMetrics.counterTotalReadLocalDataFileSize.inc(sdr.getDataLength());
          shuffleServer
              .getGrpcMetrics()
              .recordProcessTime(ShuffleServerGrpcMetrics.GET_SORTED_SHUFFLE_DATA_METHOD, readTime);
          LOG.info(
              "Successfully getSortedShuffleData cost {} ms for shuffle"
                  + " data with {}, length is {}, state is {}",
              readTime,
              requestInfo,
              sdr.getDataLength(),
              mergeState);
          auditContext.withReturnValue("len=" + sdr.getDataLength());
          reply =
              RssProtos.GetSortedShuffleDataResponse.newBuilder()
                  .setNextBlockId(blockId + 1) // next block id
                  .setMState(mergeState.code())
                  .setStatus(status.toProto())
                  .setRetMsg(msg)
                  .setData(UnsafeByteOperations.unsafeWrap(sdr.getData(), 0, sdr.getDataLength()))
                  .build();
        } catch (Exception e) {
          status = StatusCode.INTERNAL_ERROR;
          msg = "Error happened when get shuffle data for " + requestInfo + ", " + e.getMessage();
          LOG.error(msg, e);
          reply =
              RssProtos.GetSortedShuffleDataResponse.newBuilder()
                  .setStatus(status.toProto())
                  .setRetMsg(msg)
                  .build();
        } finally {
          if (sdr != null) {
            sdr.release();
          }
          shuffleServer.getShuffleBufferManager().releaseReadMemory(blockSize);
        }
      } else {
        status = StatusCode.NO_BUFFER;
        msg = "Can't require read memory to get sorted shuffle data";
        LOG.error(msg + " for " + requestInfo);
        reply =
            RssProtos.GetSortedShuffleDataResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(msg)
                .build();
      }
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }

  private List<ShufflePartitionedData> toPartitionedDataList(SendShuffleDataRequest req) {
    List<ShufflePartitionedData> ret = Lists.newArrayList();

    for (ShuffleData data : req.getShuffleDataList()) {
      ret.add(toPartitionedData(data.getPartitionId(), data.getBlockList()));
    }

    return ret;
  }

  private ShufflePartitionedData toPartitionedData(int partitionId, List<ShuffleBlock> blocks) {
    if (blocks == null || blocks.size() == 0) {
      return new ShufflePartitionedData(partitionId, 0L, 0L, new ShufflePartitionedBlock[] {});
    }
    ShufflePartitionedBlock[] ret = new ShufflePartitionedBlock[blocks.size()];
    long encodedLength = 0L;
    long dataLength = 0L;
    int i = 0;
    for (ShuffleBlock block : blocks) {
      ByteBuf data = ByteBufUtils.byteStringToByteBuf(block.getData());
      ret[i] =
          new ShufflePartitionedBlock(
              block.getLength(),
              block.getUncompressLength(),
              block.getCrc(),
              block.getBlockId(),
              block.getTaskAttemptId(),
              data);
      encodedLength += ret[i].getEncodedLength();
      dataLength += ret[i].getDataLength();
      i++;
    }
    return new ShufflePartitionedData(partitionId, encodedLength, dataLength, ret);
  }

  private Map<Integer, long[]> toPartitionBlocksMap(List<PartitionToBlockIds> partitionToBlockIds) {
    Map<Integer, long[]> result = Maps.newHashMap();
    for (PartitionToBlockIds ptb : partitionToBlockIds) {
      List<Long> blockIds = ptb.getBlockIdsList();
      if (blockIds != null && !blockIds.isEmpty()) {
        long[] array = new long[blockIds.size()];
        for (int i = 0; i < array.length; i++) {
          array[i] = blockIds.get(i);
        }
        result.put(ptb.getPartitionId(), array);
      }
    }
    return result;
  }

  private List<PartitionRange> toPartitionRanges(
      List<ShufflePartitionRange> shufflePartitionRanges) {
    List<PartitionRange> partitionRanges = Lists.newArrayList();
    for (ShufflePartitionRange spr : shufflePartitionRanges) {
      partitionRanges.add(new PartitionRange(spr.getStart(), spr.getEnd()));
    }
    return partitionRanges;
  }

  private List<ShuffleDataBlockSegment> toShuffleDataBlockSegments(
      List<BufferSegment> bufferSegments) {
    List<ShuffleDataBlockSegment> shuffleDataBlockSegments = Lists.newArrayList();
    if (bufferSegments != null) {
      for (BufferSegment bs : bufferSegments) {
        shuffleDataBlockSegments.add(
            ShuffleDataBlockSegment.newBuilder()
                .setBlockId(bs.getBlockId())
                .setCrc(bs.getCrc())
                .setOffset(bs.getOffset())
                .setLength(bs.getLength())
                .setTaskAttemptId(bs.getTaskAttemptId())
                .setUncompressLength(bs.getUncompressLength())
                .build());
      }
    }
    return shuffleDataBlockSegments;
  }

  private StatusCode verifyRequest(String appId) {
    if (StringUtils.isNotBlank(appId)
        && shuffleServer.getShuffleTaskManager().isAppExpired(appId)) {
      return StatusCode.NO_REGISTER;
    }
    return StatusCode.SUCCESS;
  }

  /**
   * Creates a {@link ServerRpcAuditContext} instance.
   *
   * @param command the command to be logged by this {@link RpcAuditContext}
   * @return newly-created {@link ServerRpcAuditContext} instance
   */
  private ServerRpcAuditContext createAuditContext(String command) {
    // Audit log may be enabled during runtime
    Logger auditLogger = null;
    if (isRpcAuditLogEnabled && !rpcAuditExcludeOpList.contains(command)) {
      auditLogger = AUDIT_LOGGER;
    }
    ServerRpcAuditContext auditContext = new ServerRpcAuditContext(auditLogger);
    if (auditLogger != null) {
      auditContext
          .withCommand(command)
          .withFrom(ClientContextServerInterceptor.getIpAddress())
          .withCreationTimeNs(System.nanoTime());
    }
    return auditContext;
  }
}
