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

package org.apache.uniffle.server.netty;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ReconfigurableRegistry;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.audit.RpcAuditContext;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.exception.ExceedHugePartitionHardLimitException;
import org.apache.uniffle.common.exception.FileNotFoundException;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.merger.MergeState;
import org.apache.uniffle.common.netty.buffer.ManagedBuffer;
import org.apache.uniffle.common.netty.buffer.NettyManagedBuffer;
import org.apache.uniffle.common.netty.client.TransportClient;
import org.apache.uniffle.common.netty.handle.BaseMessageHandler;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleDataResponse;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleIndexRequest;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleIndexResponse;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleIndexV2Response;
import org.apache.uniffle.common.netty.protocol.GetMemoryShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.GetMemoryShuffleDataResponse;
import org.apache.uniffle.common.netty.protocol.GetSortedShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.GetSortedShuffleDataResponse;
import org.apache.uniffle.common.netty.protocol.RequestMessage;
import org.apache.uniffle.common.netty.protocol.RpcResponse;
import org.apache.uniffle.common.netty.protocol.SendShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.SendShuffleDataRequestV1;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.server.ShuffleDataReadEvent;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.server.ShuffleTaskInfo;
import org.apache.uniffle.server.ShuffleTaskManager;
import org.apache.uniffle.server.audit.ServerRpcAuditContext;
import org.apache.uniffle.server.buffer.PreAllocatedBufferInfo;
import org.apache.uniffle.server.buffer.ShuffleBufferManager;
import org.apache.uniffle.server.merge.MergeStatus;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.common.StorageReadMetrics;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;

public class ShuffleServerNettyHandler implements BaseMessageHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServerNettyHandler.class);
  private static final Logger AUDIT_LOGGER =
      LoggerFactory.getLogger("SHUFFLE_SERVER_RPC_AUDIT_LOG");
  private static final int RPC_TIMEOUT = 60000;
  private final ShuffleServer shuffleServer;
  private boolean isRpcAuditLogEnabled;
  private List<String> rpcAuditExcludeOpList;

  public ShuffleServerNettyHandler(ShuffleServer shuffleServer) {
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
  public void receive(TransportClient client, RequestMessage msg) {
    shuffleServer.getNettyMetrics().incCounter(msg.getClass().getName());
    if (msg instanceof SendShuffleDataRequestV1) {
      handleSendShuffleDataRequest(client, (SendShuffleDataRequestV1) msg);
    } else if (msg instanceof GetLocalShuffleDataRequest) {
      handleGetLocalShuffleData(client, (GetLocalShuffleDataRequest) msg);
    } else if (msg instanceof GetLocalShuffleIndexRequest) {
      handleGetLocalShuffleIndexRequest(client, (GetLocalShuffleIndexRequest) msg);
    } else if (msg instanceof GetMemoryShuffleDataRequest) {
      handleGetMemoryShuffleDataRequest(client, (GetMemoryShuffleDataRequest) msg);
    } else if (msg instanceof GetSortedShuffleDataRequest) {
      handleGetSortedShuffleDataRequest(client, (GetSortedShuffleDataRequest) msg);
    } else {
      throw new RssException("Can not handle message " + msg.type());
    }
    shuffleServer.getNettyMetrics().decCounter(msg.getClass().getName());
  }

  @Override
  public void exceptionCaught(Throwable cause, TransportClient client) {
    LOG.error("exception caught {}", client.getSocketAddress(), cause);
  }

  public void handleSendShuffleDataRequest(TransportClient client, SendShuffleDataRequestV1 req) {
    try (ServerRpcAuditContext auditContext = createAuditContext("sendShuffleData", client)) {
      RpcResponse rpcResponse;
      String appId = req.getAppId();
      int shuffleId = req.getShuffleId();
      long requireBufferId = req.getRequireId();
      int stageAttemptNumber = req.getStageAttemptNumber();
      ShuffleBufferManager shuffleBufferManager = shuffleServer.getShuffleBufferManager();
      ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
      // info is null, means pre-allocated buffer has been removed by preAllocatedBufferCheck
      // thread,
      // otherwise we need to release the required size.
      PreAllocatedBufferInfo info =
          shuffleTaskManager.getAndRemovePreAllocatedBuffer(requireBufferId);
      int requireSize = info == null ? 0 : info.getRequireSize();
      int requireBlocksSize = Math.max(requireSize - req.getDecodedLength(), 0);

      boolean isPreAllocated = info != null;

      auditContext.withAppId(appId).withShuffleId(shuffleId);
      auditContext.withArgs(
          "requireBufferId="
              + requireBufferId
              + ", requireSize="
              + requireSize
              + ", isPreAllocated="
              + isPreAllocated
              + ", requireBlocksSize="
              + requireBlocksSize
              + ", stageAttemptNumber="
              + stageAttemptNumber
              + ", partitionCount="
              + req.getPartitionToBlocks().size());

      ShuffleTaskInfo taskInfo = shuffleServer.getShuffleTaskManager().getShuffleTaskInfo(appId);
      if (taskInfo == null) {
        rpcResponse =
            new RpcResponse(
                req.getRequestId(), StatusCode.APP_NOT_FOUND, "appId: " + appId + " not found");
        String errorMsg =
            "APP_NOT_FOUND error, requireBufferId["
                + requireBufferId
                + "] for appId["
                + appId
                + "], shuffleId["
                + shuffleId
                + "], isPreAllocated["
                + isPreAllocated
                + "]";
        LOG.error(errorMsg);
        ShuffleServerMetrics.counterAppNotFound.inc();
        releaseNettyBufferAndMetrics(
            req,
            appId,
            shuffleId,
            requireBufferId,
            requireBlocksSize,
            shuffleBufferManager,
            info,
            isPreAllocated);
        auditContext.withStatusCode(rpcResponse.getStatusCode());
        client.getChannel().writeAndFlush(rpcResponse);
        return;
      }
      Integer latestStageAttemptNumber = taskInfo.getLatestStageAttemptNumber(shuffleId);
      // The Stage retry occurred, and the task before StageNumber was simply ignored and not
      // processed if the task was being sent.
      if (stageAttemptNumber < latestStageAttemptNumber) {
        String responseMessage = "A retry has occurred at the Stage, sending data is invalid.";
        rpcResponse =
            new RpcResponse(req.getRequestId(), StatusCode.STAGE_RETRY_IGNORE, responseMessage);
        LOG.warn(
            "Stage retry occurred, appId["
                + appId
                + "], shuffleId["
                + shuffleId
                + "], stageAttemptNumber["
                + stageAttemptNumber
                + "], latestStageAttemptNumber["
                + latestStageAttemptNumber
                + "]");
        releaseNettyBufferAndMetrics(
            req,
            appId,
            shuffleId,
            requireBufferId,
            requireBlocksSize,
            shuffleBufferManager,
            info,
            isPreAllocated);
        auditContext.withStatusCode(rpcResponse.getStatusCode());
        client.getChannel().writeAndFlush(rpcResponse);
        return;
      }
      long timestamp = req.getTimestamp();
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
              .getNettyMetrics()
              .recordTransportTime(SendShuffleDataRequest.class.getName(), transportTime);
        }
      }

      StatusCode ret = StatusCode.SUCCESS;
      String responseMessage = "OK";
      if (req.getPartitionToBlocks().size() > 0) {
        ShuffleServerMetrics.counterTotalReceivedDataSize.inc(requireBlocksSize);
        if (!isPreAllocated) {
          req.getPartitionToBlocks().values().stream()
              .flatMap(Collection::stream)
              .forEach(block -> block.getData().release());

          String errorMsg =
              "Can't find requireBufferId["
                  + requireBufferId
                  + "] for appId["
                  + appId
                  + "], shuffleId["
                  + shuffleId
                  + "], probably because the pre-allocated buffer has expired. "
                  + "Please increase the expiration time using "
                  + ShuffleServerConf.SERVER_PRE_ALLOCATION_EXPIRED.key()
                  + " in ShuffleServer's configuration";
          LOG.warn(errorMsg);
          rpcResponse = new RpcResponse(req.getRequestId(), StatusCode.INTERNAL_ERROR, errorMsg);
          auditContext.withStatusCode(rpcResponse.getStatusCode());
          client.getChannel().writeAndFlush(rpcResponse);
          return;
        }
        final long start = System.currentTimeMillis();
        shuffleBufferManager.releaseMemory(req.getDecodedLength(), false, true);
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
            if (hasFailureOccurred) {
              continue;
            }
            ret = shuffleTaskManager.cacheShuffleData(appId, shuffleId, isPreAllocated, spd);
            if (ret != StatusCode.SUCCESS) {
              String errorMsg =
                  "Error happened when shuffleEngine.write for "
                      + shuffleDataInfo
                      + ", statusCode="
                      + ret;
              LOG.error(errorMsg);
              responseMessage = errorMsg;
              hasFailureOccurred = true;
            } else {
              if (shuffleServer.isRemoteMergeEnable()) {
                shuffleServer.getShuffleMergeManager().setDirect(appId, shuffleId, true);
              }
              long toReleasedSize = spd.getTotalBlockEncodedLength();
              // after each cacheShuffleData call, the `preAllocatedSize` is updated timely.
              shuffleTaskManager.releasePreAllocatedSize(toReleasedSize);
              alreadyReleasedSize += toReleasedSize;
              shuffleTaskManager.updateCachedBlockIds(
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
          } finally {
            // Once the cache failure occurs, we should explicitly release data held by byteBuf
            if (hasFailureOccurred) {
              Arrays.stream(spd.getBlockList()).forEach(block -> block.getData().release());
              shuffleBufferManager.releaseMemory(spd.getTotalBlockEncodedLength(), false, false);
            }
          }
        }
        // since the required buffer id is only used once, the shuffle client would try to require
        // another buffer whether
        // current connection succeeded or not. Therefore, the preAllocatedBuffer is first get and
        // removed, then after
        // cacheShuffleData finishes, the preAllocatedSize should be updated accordingly.
        if (requireBlocksSize > alreadyReleasedSize) {
          shuffleTaskManager.releasePreAllocatedSize(requireBlocksSize - alreadyReleasedSize);
        }
        rpcResponse = new RpcResponse(req.getRequestId(), ret, responseMessage);
        long costTime = System.currentTimeMillis() - start;
        shuffleServer
            .getNettyMetrics()
            .recordProcessTime(SendShuffleDataRequest.class.getName(), costTime);
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
                  + requireBlocksSize
                  + " bytes");
        }
      } else {
        rpcResponse =
            new RpcResponse(req.getRequestId(), StatusCode.INTERNAL_ERROR, "No data in request");
      }
      auditContext.withStatusCode(rpcResponse.getStatusCode());
      client.getChannel().writeAndFlush(rpcResponse);
    }
  }

  private static void releaseNettyBufferAndMetrics(
      SendShuffleDataRequestV1 req,
      String appId,
      int shuffleId,
      long requireBufferId,
      long requireBlocksSize,
      ShuffleBufferManager shuffleBufferManager,
      PreAllocatedBufferInfo info,
      boolean isPreAllocated) {
    if (isPreAllocated) {
      shuffleBufferManager.releaseMemory(info.getRequireSize(), false, true);
    }
    if (MapUtils.isNotEmpty(req.getPartitionToBlocks())) {
      // release memory
      req.getPartitionToBlocks().values().stream()
          .flatMap(Collection::stream)
          .forEach(block -> block.getData().release());
      ShuffleServerMetrics.counterTotalReceivedDataSize.inc(requireBlocksSize);
    } else {
      LOG.error(
          "Failed to handle send shuffle data request, no blocks found in this request. appId: {}, shuffleId: {}, requireBufferId: {}",
          appId,
          shuffleId,
          requireBufferId);
    }
  }

  public void handleGetMemoryShuffleDataRequest(
      TransportClient client, GetMemoryShuffleDataRequest req) {
    try (ServerRpcAuditContext auditContext = createAuditContext("getMemoryShuffleData", client)) {
      String appId = req.getAppId();
      int shuffleId = req.getShuffleId();
      int partitionId = req.getPartitionId();
      long blockId = req.getLastBlockId();
      int readBufferSize = req.getReadBufferSize();
      auditContext.withAppId(appId).withShuffleId(shuffleId);
      auditContext.withArgs(
          "requestId="
              + req.getRequestId()
              + ", partitionId="
              + partitionId
              + ", blockId="
              + blockId
              + ", readBufferSize="
              + readBufferSize);
      StatusCode status = verifyRequest(appId);
      if (status != StatusCode.SUCCESS) {
        auditContext.withStatusCode(status);
        GetMemoryShuffleDataResponse response =
            new GetMemoryShuffleDataResponse(
                req.getRequestId(),
                status,
                status.toString(),
                Lists.newArrayList(),
                Unpooled.EMPTY_BUFFER);
        client.getChannel().writeAndFlush(response);
        return;
      }

      long timestamp = req.getTimestamp();
      if (timestamp > 0) {
        long transportTime = System.currentTimeMillis() - timestamp;
        if (transportTime > 0) {
          shuffleServer
              .getNettyMetrics()
              .recordTransportTime(GetMemoryShuffleDataRequest.class.getName(), transportTime);
        }
      }
      String msg = "OK";
      GetMemoryShuffleDataResponse response;
      String requestInfo =
          "appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]";

      // todo: if can get the exact memory size?
      if (shuffleServer.getShuffleBufferManager().requireReadMemory(readBufferSize)) {
        ShuffleDataResult shuffleDataResult = null;
        try {
          final long start = System.currentTimeMillis();
          shuffleDataResult =
              shuffleServer
                  .getShuffleTaskManager()
                  .getInMemoryShuffleData(
                      appId,
                      shuffleId,
                      partitionId,
                      blockId,
                      readBufferSize,
                      req.getExpectedTaskIdsBitmap());
          ManagedBuffer data = NettyManagedBuffer.EMPTY_BUFFER;
          List<BufferSegment> bufferSegments = Lists.newArrayList();
          if (shuffleDataResult != null) {
            data = shuffleDataResult.getManagedBuffer();
            bufferSegments = shuffleDataResult.getBufferSegments();
            ShuffleServerMetrics.counterTotalReadDataSize.inc(data.size());
            ShuffleServerMetrics.counterTotalReadMemoryDataSize.inc(data.size());
            ShuffleServerMetrics.gaugeReadMemoryDataThreadNum.inc();
            ShuffleServerMetrics.gaugeReadMemoryDataBufferSize.inc(readBufferSize);
          }
          auditContext.withStatusCode(status);
          auditContext.withReturnValue(
              "len=" + data.size() + ", bufferSegments=" + bufferSegments.size());
          response =
              new GetMemoryShuffleDataResponse(
                  req.getRequestId(), status, msg, bufferSegments, data);
          ReleaseMemoryAndRecordReadTimeListener listener =
              new ReleaseMemoryAndRecordReadTimeListener(
                  start, readBufferSize, data.size(), requestInfo, req, response, client);
          client.getChannel().writeAndFlush(response).addListener(listener);
          return;
        } catch (Exception e) {
          shuffleServer.getShuffleBufferManager().releaseReadMemory(readBufferSize);
          if (shuffleDataResult != null) {
            shuffleDataResult.release();
          }
          status = StatusCode.INTERNAL_ERROR;
          msg =
              "Error happened when get in memory shuffle data for "
                  + requestInfo
                  + ", "
                  + e.getMessage();
          LOG.error(msg, e);
          response =
              new GetMemoryShuffleDataResponse(
                  req.getRequestId(), status, msg, Lists.newArrayList(), Unpooled.EMPTY_BUFFER);
        }
      } else {
        status = StatusCode.NO_BUFFER;
        msg = "Can't require memory to get in memory shuffle data";
        LOG.warn("{} for {}", msg, requestInfo);
        response =
            new GetMemoryShuffleDataResponse(
                req.getRequestId(), status, msg, Lists.newArrayList(), Unpooled.EMPTY_BUFFER);
      }
      auditContext.withStatusCode(response.getStatusCode());
      client.getChannel().writeAndFlush(response);
    }
  }

  public void handleGetLocalShuffleIndexRequest(
      TransportClient client, GetLocalShuffleIndexRequest req) {
    try (ServerRpcAuditContext auditContext = createAuditContext("getLocalShuffleIndex", client)) {
      String appId = req.getAppId();
      int shuffleId = req.getShuffleId();
      int partitionId = req.getPartitionId();
      int partitionNumPerRange = req.getPartitionNumPerRange();
      int partitionNum = req.getPartitionNum();
      String requestInfo =
          "appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]";
      auditContext.withAppId(appId);
      auditContext.withShuffleId(shuffleId);
      auditContext.withArgs(
          "requestId="
              + req.getRequestId()
              + ", partitionId="
              + partitionId
              + ", partitionNumPerRange="
              + partitionNumPerRange
              + ", partitionNum="
              + partitionNum);
      StatusCode status = verifyRequest(appId);
      if (status != StatusCode.SUCCESS) {
        auditContext.withStatusCode(status);
        GetLocalShuffleIndexResponse response =
            new GetLocalShuffleIndexResponse(
                req.getRequestId(), status, status.toString(), Unpooled.EMPTY_BUFFER, 0L);
        client.getChannel().writeAndFlush(response);
        return;
      }

      String msg = "OK";
      GetLocalShuffleIndexV2Response response;
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
      // index segment is 40B,
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

          ManagedBuffer data = shuffleIndexResult.getManagedBuffer();
          ShuffleServerMetrics.counterTotalReadDataSize.inc(data.size());
          ShuffleServerMetrics.counterTotalReadLocalIndexFileSize.inc(data.size());
          ShuffleServerMetrics.gaugeReadLocalIndexFileThreadNum.inc();
          ShuffleServerMetrics.gaugeReadLocalIndexFileBufferSize.inc(assumedFileSize);
          auditContext.withStatusCode(status);
          auditContext.withReturnValue("len=" + data.size());
          response =
              new GetLocalShuffleIndexV2Response(
                  req.getRequestId(),
                  status,
                  msg,
                  data,
                  shuffleIndexResult.getDataFileLen(),
                  shuffleIndexResult.getStorageIds());
          ReleaseMemoryAndRecordReadTimeListener listener =
              new ReleaseMemoryAndRecordReadTimeListener(
                  start, assumedFileSize, data.size(), requestInfo, req, response, client);
          client.getChannel().writeAndFlush(response).addListener(listener);
          return;
        } catch (FileNotFoundException indexFileNotFoundException) {
          shuffleServer.getShuffleBufferManager().releaseReadMemory(assumedFileSize);
          if (shuffleIndexResult != null) {
            shuffleIndexResult.release();
          }
          LOG.warn(
              "Index file for {} is not found, maybe the data has been flushed to cold storage "
                  + "or partial data still in another shuffle server(when partition split is enabled).",
              requestInfo,
              indexFileNotFoundException);
          response =
              new GetLocalShuffleIndexV2Response(
                  req.getRequestId(), status, msg, Unpooled.EMPTY_BUFFER, 0L);
        } catch (Exception e) {
          shuffleServer.getShuffleBufferManager().releaseReadMemory(assumedFileSize);
          if (shuffleIndexResult != null) {
            shuffleIndexResult.release();
          }
          status = StatusCode.INTERNAL_ERROR;
          msg = "Error happened when get shuffle index for " + requestInfo + ", " + e.getMessage();
          LOG.error(msg, e);
          response =
              new GetLocalShuffleIndexV2Response(
                  req.getRequestId(), status, msg, Unpooled.EMPTY_BUFFER, 0L);
        }
      } else {
        status = StatusCode.NO_BUFFER;
        msg = "Can't require memory to get shuffle index";
        LOG.warn("{} for {}", msg, requestInfo);
        response =
            new GetLocalShuffleIndexV2Response(
                req.getRequestId(), status, msg, Unpooled.EMPTY_BUFFER, 0L);
      }
      auditContext.withStatusCode(response.getStatusCode());
      client.getChannel().writeAndFlush(response);
    }
  }

  public void handleGetLocalShuffleData(TransportClient client, GetLocalShuffleDataRequest req) {
    try (ServerRpcAuditContext auditContext = createAuditContext("getLocalShuffleData", client)) {
      GetLocalShuffleDataResponse response;
      String appId = req.getAppId();
      int shuffleId = req.getShuffleId();
      int partitionId = req.getPartitionId();
      int partitionNumPerRange = req.getPartitionNumPerRange();
      int partitionNum = req.getPartitionNum();
      long offset = req.getOffset();
      int length = req.getLength();
      int storageId = req.getStorageId();
      auditContext.withAppId(appId);
      auditContext.withShuffleId(shuffleId);
      auditContext.withArgs(
          "requestId="
              + req.getRequestId()
              + ", partitionId="
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
        response =
            new GetLocalShuffleDataResponse(
                req.getRequestId(),
                status,
                status.toString(),
                new NettyManagedBuffer(Unpooled.EMPTY_BUFFER));
        client.getChannel().writeAndFlush(response);
        return;
      }
      long timestamp = req.getTimestamp();
      if (timestamp > 0) {
        long transportTime = System.currentTimeMillis() - timestamp;
        if (transportTime > 0) {
          shuffleServer
              .getNettyMetrics()
              .recordTransportTime(GetLocalShuffleDataRequest.class.getName(), transportTime);
        }
      }
      String storageType =
          shuffleServer.getShuffleServerConf().get(RssBaseConf.RSS_STORAGE_TYPE).name();
      String msg = "OK";
      String requestInfo =
          "appId["
              + appId
              + "], shuffleId["
              + shuffleId
              + "], partitionId["
              + partitionId
              + "], offset["
              + offset
              + "], length["
              + length
              + "]";

      int[] range =
          ShuffleStorageUtils.getPartitionRange(partitionId, partitionNumPerRange, partitionNum);
      Storage storage =
          shuffleServer
              .getStorageManager()
              .selectStorage(
                  new ShuffleDataReadEvent(
                      appId, shuffleId, partitionId, range[0], req.getStorageId()));
      if (storage != null) {
        storage.updateReadMetrics(new StorageReadMetrics(appId, shuffleId));
      }

      if (shuffleServer.getShuffleBufferManager().requireReadMemory(length)) {
        ShuffleDataResult sdr = null;
        try {
          final long start = System.currentTimeMillis();
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
          ShuffleServerMetrics.counterTotalReadDataSize.inc(sdr.getDataLength());
          ShuffleServerMetrics.counterTotalReadLocalDataFileSize.inc(sdr.getDataLength());
          ShuffleServerMetrics.gaugeReadLocalDataFileThreadNum.inc();
          ShuffleServerMetrics.gaugeReadLocalDataFileBufferSize.inc(length);
          response =
              new GetLocalShuffleDataResponse(
                  req.getRequestId(), status, msg, sdr.getManagedBuffer());
          ReleaseMemoryAndRecordReadTimeListener listener =
              new ReleaseMemoryAndRecordReadTimeListener(
                  start, length, sdr.getDataLength(), requestInfo, req, response, client);
          client.getChannel().writeAndFlush(response).addListener(listener);
          auditContext.withStatusCode(response.getStatusCode());
          auditContext.withReturnValue("len=" + sdr.getDataLength());
          return;
        } catch (Exception e) {
          shuffleServer.getShuffleBufferManager().releaseReadMemory(length);
          if (sdr != null) {
            sdr.release();
          }
          status = StatusCode.INTERNAL_ERROR;
          msg = "Error happened when get shuffle data for " + requestInfo + ", " + e.getMessage();
          LOG.error(msg, e);
          response =
              new GetLocalShuffleDataResponse(
                  req.getRequestId(), status, msg, new NettyManagedBuffer(Unpooled.EMPTY_BUFFER));
        }
      } else {
        status = StatusCode.NO_BUFFER;
        msg = "Can't require memory to get shuffle data";
        LOG.warn("{} for {}", msg, requestInfo);
        response =
            new GetLocalShuffleDataResponse(
                req.getRequestId(), status, msg, new NettyManagedBuffer(Unpooled.EMPTY_BUFFER));
      }
      auditContext.withStatusCode(response.getStatusCode());
      client.getChannel().writeAndFlush(response);
    }
  }

  public void handleGetSortedShuffleDataRequest(
      TransportClient client, GetSortedShuffleDataRequest req) {
    final long start = System.currentTimeMillis();
    long requestId = req.getRequestId();
    String appId = req.getAppId();
    int shuffleId = req.getShuffleId();
    int partitionId = req.getPartitionId();
    long blockId = req.getBlockId();
    long timestamp = req.getTimestamp();

    if (timestamp > 0) {
      long transportTime = start - timestamp;
      if (transportTime > 0) {
        shuffleServer
            .getNettyMetrics()
            .recordTransportTime(GetSortedShuffleDataRequest.class.getName(), transportTime);
      }
    }
    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";
    GetSortedShuffleDataResponse response;
    String requestInfo =
        "appId["
            + appId
            + "], shuffleId["
            + shuffleId
            + "], partitionId["
            + partitionId
            + "], blockId["
            + blockId
            + "]";
    if (!shuffleServer.isRemoteMergeEnable()) {
      msg = "Remote merge is disabled";
      status = StatusCode.INTERNAL_ERROR;
      response =
          new GetSortedShuffleDataResponse(
              requestId, status, msg, -1, MergeState.INTERNAL_ERROR.code(), Unpooled.EMPTY_BUFFER);
      client.getChannel().writeAndFlush(response);
      return;
    }
    MergeStatus mergeStatus =
        shuffleServer.getShuffleMergeManager().tryGetBlock(appId, shuffleId, partitionId, blockId);
    MergeState mergeState = mergeStatus.getState();
    long readBlockSize = mergeStatus.getSize();

    if (mergeState == MergeState.INITED
        || (mergeState == MergeState.MERGING && readBlockSize == -1)
        || (mergeState == MergeState.DONE && readBlockSize == -1)
        || mergeState == MergeState.INTERNAL_ERROR) {
      msg = mergeState.name();
      response =
          new GetSortedShuffleDataResponse(
              requestId, status, msg, -1, mergeState.code(), Unpooled.EMPTY_BUFFER);
      client.getChannel().writeAndFlush(response);
      return;
    }

    if (shuffleServer.getShuffleBufferManager().requireReadMemory(readBlockSize)) {
      ShuffleDataResult sdr = null;
      try {
        sdr =
            shuffleServer
                .getShuffleMergeManager()
                .getShuffleData(appId, shuffleId, partitionId, blockId);

        response =
            new GetSortedShuffleDataResponse(
                requestId, status, msg, blockId + 1, mergeState.code(), sdr.getManagedBuffer());

        ReleaseMemoryAndRecordReadTimeListener listener =
            new ReleaseMemoryAndRecordReadTimeListener(
                start, readBlockSize, sdr.getDataLength(), requestInfo, req, response, client);

        client.getChannel().writeAndFlush(response).addListener(listener);
      } catch (Exception e) {
        shuffleServer.getShuffleBufferManager().releaseReadMemory(readBlockSize);
        if (sdr != null) {
          sdr.release();
        }
        status = StatusCode.INTERNAL_ERROR;
        msg = "Error happened when get shuffle data for " + requestInfo + ", " + e.getMessage();
        LOG.error(msg, e);
        response =
            new GetSortedShuffleDataResponse(
                requestId,
                status,
                msg,
                -1,
                MergeState.INTERNAL_ERROR.code(),
                Unpooled.EMPTY_BUFFER);
        client.getChannel().writeAndFlush(response);
      }
    } else {
      status = StatusCode.NO_BUFFER;
      msg = "Can't require read memory to get sorted shuffle data";
      LOG.error(msg + " for " + requestInfo);
      response =
          new GetSortedShuffleDataResponse(
              requestId, status, msg, -1, mergeState.code(), Unpooled.EMPTY_BUFFER);
      client.getChannel().writeAndFlush(response);
    }
  }

  private List<ShufflePartitionedData> toPartitionedDataList(SendShuffleDataRequestV1 req) {
    List<ShufflePartitionedData> ret = Lists.newArrayList();

    for (Map.Entry<Integer, List<ShufflePartitionedBlock>> entry :
        req.getPartitionToBlocks().entrySet()) {
      ret.add(toPartitionedData(entry.getKey(), entry.getValue()));
    }
    return ret;
  }

  private ShufflePartitionedData toPartitionedData(
      int partitionId, List<ShufflePartitionedBlock> blocks) {
    if (blocks == null || blocks.isEmpty()) {
      return new ShufflePartitionedData(partitionId, 0L, 0L, new ShufflePartitionedBlock[] {});
    }
    ShufflePartitionedBlock[] ret = new ShufflePartitionedBlock[blocks.size()];
    long encodedLength = 0L;
    long dataLength = 0L;
    int i = 0;
    for (ShufflePartitionedBlock block : blocks) {
      ret[i] = block;
      encodedLength += ret[i].getEncodedLength();
      dataLength += ret[i].getDataLength();
      i++;
    }
    return new ShufflePartitionedData(partitionId, encodedLength, dataLength, ret);
  }

  private StatusCode verifyRequest(String appId) {
    if (StringUtils.isNotBlank(appId)
        && shuffleServer.getShuffleTaskManager().isAppExpired(appId)) {
      return StatusCode.NO_REGISTER;
    }
    return StatusCode.SUCCESS;
  }

  class ReleaseMemoryAndRecordReadTimeListener implements ChannelFutureListener {
    private final long readStartedTime;
    private final long readBufferSize;
    private final long dataSize;
    private final String requestInfo;
    private final RequestMessage request;
    private final RpcResponse response;
    private final TransportClient client;

    ReleaseMemoryAndRecordReadTimeListener(
        long readStartedTime,
        long readBufferSize,
        long dataSize,
        String requestInfo,
        RequestMessage request,
        RpcResponse response,
        TransportClient client) {
      this.readStartedTime = readStartedTime;
      this.readBufferSize = readBufferSize;
      this.dataSize = dataSize;
      this.requestInfo = requestInfo;
      this.request = request;
      this.response = response;
      this.client = client;
    }

    @Override
    public void operationComplete(ChannelFuture future) {
      shuffleServer.getShuffleBufferManager().releaseReadMemory(readBufferSize);
      long readTime = System.currentTimeMillis() - readStartedTime;
      ShuffleServerMetrics.counterTotalReadTime.inc(readTime);
      shuffleServer.getNettyMetrics().recordProcessTime(request.getClass().getName(), readTime);
      if (request instanceof GetLocalShuffleDataRequest) {
        ShuffleServerMetrics.gaugeReadLocalDataFileThreadNum.dec();
        ShuffleServerMetrics.gaugeReadLocalDataFileBufferSize.dec(readBufferSize);
      } else if (request instanceof GetLocalShuffleIndexRequest) {
        ShuffleServerMetrics.gaugeReadLocalIndexFileThreadNum.dec();
        ShuffleServerMetrics.gaugeReadLocalIndexFileBufferSize.dec(readBufferSize);
      } else if (request instanceof GetMemoryShuffleDataRequest) {
        GetMemoryShuffleDataResponse getMemoryShuffleDataResponse =
            (GetMemoryShuffleDataResponse) response;
        if (CollectionUtils.isNotEmpty(getMemoryShuffleDataResponse.getBufferSegments())) {
          ShuffleServerMetrics.gaugeReadMemoryDataThreadNum.dec();
          ShuffleServerMetrics.gaugeReadMemoryDataBufferSize.dec(readBufferSize);
        }
      }
      if (!future.isSuccess()) {
        Throwable cause = future.cause();
        String errorMsg =
            "Error happened when executing "
                + request.getOperationType()
                + " for "
                + requestInfo
                + ", "
                + cause.getMessage();
        if (future.channel().isWritable()) {
          RpcResponse errorResponse;
          if (request instanceof GetLocalShuffleDataRequest) {
            errorResponse =
                new GetLocalShuffleDataResponse(
                    request.getRequestId(),
                    StatusCode.INTERNAL_ERROR,
                    errorMsg,
                    new NettyManagedBuffer(Unpooled.EMPTY_BUFFER));
          } else if (request instanceof GetLocalShuffleIndexRequest) {
            errorResponse =
                new GetLocalShuffleIndexResponse(
                    request.getRequestId(),
                    StatusCode.INTERNAL_ERROR,
                    errorMsg,
                    Unpooled.EMPTY_BUFFER,
                    0L);
          } else if (request instanceof GetMemoryShuffleDataRequest) {
            errorResponse =
                new GetMemoryShuffleDataResponse(
                    request.getRequestId(),
                    StatusCode.INTERNAL_ERROR,
                    errorMsg,
                    Lists.newArrayList(),
                    Unpooled.EMPTY_BUFFER);
          } else if (request instanceof GetSortedShuffleDataRequest) {
            errorResponse =
                new GetSortedShuffleDataResponse(
                    request.getRequestId(),
                    StatusCode.INTERNAL_ERROR,
                    errorMsg,
                    -1L,
                    MergeState.INTERNAL_ERROR.code(),
                    Unpooled.EMPTY_BUFFER);
          } else {
            LOG.error("Cannot handle request {}", request.type(), cause);
            return;
          }
          client.getChannel().writeAndFlush(errorResponse);
        }
        LOG.error(
            "Failed to execute {} for {}. Took {} ms and could not retrieve {} bytes of data",
            request.getOperationType(),
            requestInfo,
            readTime,
            dataSize,
            cause);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Successfully executed {} for {}. Took {} ms and retrieved {} bytes of data",
              request.getOperationType(),
              requestInfo,
              readTime,
              dataSize);
        }
      }
    }
  }

  /**
   * Creates a {@link ServerRpcAuditContext} instance.
   *
   * @param command the command to be logged by this {@link RpcAuditContext}
   * @return newly-created {@link ServerRpcAuditContext} instance
   */
  private ServerRpcAuditContext createAuditContext(
      String command, TransportClient transportClient) {
    // Audit log may be enabled during runtime
    Logger auditLogger = null;
    if (isRpcAuditLogEnabled && !rpcAuditExcludeOpList.contains(command)) {
      auditLogger = AUDIT_LOGGER;
    }
    ServerRpcAuditContext auditContext = new ServerRpcAuditContext(auditLogger);
    if (auditLogger != null) {
      auditContext
          .withCommand(command)
          .withFrom(transportClient.getSocketAddress().toString())
          .withCreationTimeNs(System.nanoTime());
    }
    return auditContext;
  }
}
