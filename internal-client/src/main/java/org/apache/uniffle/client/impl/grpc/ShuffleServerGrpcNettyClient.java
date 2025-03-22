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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hbase.thirdparty.org.glassfish.jersey.internal.guava.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.request.RssGetInMemoryShuffleDataRequest;
import org.apache.uniffle.client.request.RssGetShuffleDataRequest;
import org.apache.uniffle.client.request.RssGetShuffleIndexRequest;
import org.apache.uniffle.client.request.RssGetSortedShuffleDataRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.client.response.RssGetInMemoryShuffleDataResponse;
import org.apache.uniffle.client.response.RssGetShuffleDataResponse;
import org.apache.uniffle.client.response.RssGetShuffleIndexResponse;
import org.apache.uniffle.client.response.RssGetSortedShuffleDataResponse;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.NotRetryException;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.exception.RssFetchFailedException;
import org.apache.uniffle.common.netty.client.TransportClient;
import org.apache.uniffle.common.netty.client.TransportClientFactory;
import org.apache.uniffle.common.netty.client.TransportConf;
import org.apache.uniffle.common.netty.client.TransportContext;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleDataResponse;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleDataV2Request;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleIndexRequest;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleIndexResponse;
import org.apache.uniffle.common.netty.protocol.GetMemoryShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.GetMemoryShuffleDataResponse;
import org.apache.uniffle.common.netty.protocol.GetSortedShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.GetSortedShuffleDataResponse;
import org.apache.uniffle.common.netty.protocol.RpcResponse;
import org.apache.uniffle.common.netty.protocol.SendShuffleDataRequest;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.RetryUtils;

import static org.apache.uniffle.common.config.RssClientConf.RSS_CLIENT_GRPC_EVENT_LOOP_THREADS;

public class ShuffleServerGrpcNettyClient extends ShuffleServerGrpcClient {
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServerGrpcNettyClient.class);
  private int nettyPort;
  private TransportClientFactory clientFactory;

  @VisibleForTesting
  public ShuffleServerGrpcNettyClient(String host, int grpcPort, int nettyPort) {
    this(new RssConf(), host, grpcPort, nettyPort);
  }

  public ShuffleServerGrpcNettyClient(RssConf rssConf, String host, int grpcPort, int nettyPort) {
    this(
        rssConf == null ? new RssConf() : rssConf,
        host,
        grpcPort,
        nettyPort,
        rssConf == null
            ? RssClientConf.RPC_MAX_ATTEMPTS.defaultValue()
            : rssConf.getInteger(RssClientConf.RPC_MAX_ATTEMPTS),
        rssConf == null
            ? RssClientConf.RPC_TIMEOUT_MS.defaultValue()
            : rssConf.getLong(RssClientConf.RPC_TIMEOUT_MS),
        rssConf == null
            ? RssClientConf.RPC_NETTY_PAGE_SIZE.defaultValue()
            : rssConf.getInteger(RssClientConf.RPC_NETTY_PAGE_SIZE),
        rssConf == null
            ? RssClientConf.RPC_NETTY_MAX_ORDER.defaultValue()
            : rssConf.getInteger(RssClientConf.RPC_NETTY_MAX_ORDER),
        rssConf == null
            ? RssClientConf.RPC_NETTY_SMALL_CACHE_SIZE.defaultValue()
            : rssConf.getInteger(RssClientConf.RPC_NETTY_SMALL_CACHE_SIZE));
  }

  public ShuffleServerGrpcNettyClient(
      RssConf rssConf,
      String host,
      int grpcPort,
      int nettyPort,
      int maxRetryAttempts,
      long rpcTimeoutMs,
      int pageSize,
      int maxOrder,
      int smallCacheSize) {
    super(
        host,
        grpcPort,
        maxRetryAttempts,
        rpcTimeoutMs,
        true,
        pageSize,
        maxOrder,
        smallCacheSize,
        rssConf.get(RSS_CLIENT_GRPC_EVENT_LOOP_THREADS));
    this.nettyPort = nettyPort;
    TransportContext transportContext = new TransportContext(new TransportConf(rssConf));
    this.clientFactory = new TransportClientFactory(transportContext);
  }

  @Override
  public String getClientInfo() {
    return "ShuffleServerGrpcNettyClient for host["
        + host
        + "], port["
        + port
        + "], nettyPort["
        + nettyPort
        + "]";
  }

  @Override
  public RssSendShuffleDataResponse sendShuffleData(RssSendShuffleDataRequest request) {
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks =
        request.getShuffleIdToBlocks();
    int stageAttemptNumber = request.getStageAttemptNumber();
    boolean isSuccessful = true;
    AtomicReference<StatusCode> failedStatusCode = new AtomicReference<>(StatusCode.INTERNAL_ERROR);
    Set<Integer> needSplitPartitionIds = Sets.newHashSet();
    for (Map.Entry<Integer, Map<Integer, List<ShuffleBlockInfo>>> stb :
        shuffleIdToBlocks.entrySet()) {
      int shuffleId = stb.getKey();
      int size = 0;
      int blockNum = 0;
      List<Integer> partitionIds = new ArrayList<>();
      List<Integer> partitionRequireSizes = new ArrayList<>();
      for (Map.Entry<Integer, List<ShuffleBlockInfo>> ptb : stb.getValue().entrySet()) {
        int partitionRequireSize = 0;
        for (ShuffleBlockInfo sbi : ptb.getValue()) {
          partitionRequireSize += sbi.getSize();
          blockNum++;
        }
        size += partitionRequireSize;
        partitionIds.add(ptb.getKey());
        partitionRequireSizes.add(partitionRequireSize);
      }

      SendShuffleDataRequest sendShuffleDataRequest =
          new SendShuffleDataRequest(
              requestId(),
              request.getAppId(),
              shuffleId,
              stageAttemptNumber,
              0L,
              stb.getValue(),
              System.currentTimeMillis());
      int allocateSize = size + sendShuffleDataRequest.encodedLength();
      int finalBlockNum = blockNum;
      try {
        RetryUtils.retryWithCondition(
            () -> {
              final TransportClient transportClient = getTransportClient();
              Pair<Long, List<Integer>> result =
                  requirePreAllocation(
                      request.getAppId(),
                      shuffleId,
                      partitionIds,
                      partitionRequireSizes,
                      allocateSize,
                      request.getRetryMax(),
                      request.getRetryIntervalMax(),
                      failedStatusCode);
              long requireId = result.getLeft();
              needSplitPartitionIds.addAll(result.getRight());
              if (requireId == FAILED_REQUIRE_ID) {
                throw new RssException(
                    String.format(
                        "requirePreAllocation failed! size[%s], host[%s], port[%s]",
                        allocateSize, host, port));
              }
              sendShuffleDataRequest.setRequireId(requireId);
              sendShuffleDataRequest.setTimestamp(System.currentTimeMillis());
              long start = System.currentTimeMillis();
              RpcResponse rpcResponse =
                  transportClient.sendRpcSync(sendShuffleDataRequest, rpcTimeout);
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
              if (rpcResponse.getStatusCode() != StatusCode.SUCCESS) {
                failedStatusCode.set(StatusCode.fromCode(rpcResponse.getStatusCode().statusCode()));
                String msg =
                    "Can't send shuffle data with "
                        + finalBlockNum
                        + " blocks to "
                        + host
                        + ":"
                        + port
                        + ", statusCode="
                        + rpcResponse.getStatusCode()
                        + ", errorMsg:"
                        + rpcResponse.getRetMessage();
                if (NOT_RETRY_STATUS_CODES.contains(rpcResponse.getStatusCode())) {
                  throw new NotRetryException(msg);
                } else {
                  throw new RssException(msg);
                }
              }
              return rpcResponse;
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
  public RssGetInMemoryShuffleDataResponse getInMemoryShuffleData(
      RssGetInMemoryShuffleDataRequest request) {
    TransportClient transportClient = getTransportClient();
    GetMemoryShuffleDataRequest getMemoryShuffleDataRequest =
        new GetMemoryShuffleDataRequest(
            requestId(),
            request.getAppId(),
            request.getShuffleId(),
            request.getPartitionId(),
            request.getLastBlockId(),
            request.getReadBufferSize(),
            System.currentTimeMillis(),
            request.getExpectedTaskIds());
    String requestInfo =
        "appId["
            + request.getAppId()
            + "], shuffleId["
            + request.getShuffleId()
            + "], partitionId["
            + request.getPartitionId()
            + "], lastBlockId["
            + request.getLastBlockId()
            + "]";
    long start = System.currentTimeMillis();
    int retry = 0;
    RpcResponse rpcResponse;
    GetMemoryShuffleDataResponse getMemoryShuffleDataResponse;
    while (true) {
      rpcResponse = transportClient.sendRpcSync(getMemoryShuffleDataRequest, rpcTimeout);
      getMemoryShuffleDataResponse = (GetMemoryShuffleDataResponse) rpcResponse;
      if (rpcResponse.getStatusCode() != StatusCode.NO_BUFFER) {
        break;
      }
      waitOrThrow(request, retry, requestInfo, rpcResponse.getStatusCode(), start);
      retry++;
    }
    switch (rpcResponse.getStatusCode()) {
      case SUCCESS:
        LOG.info(
            "GetInMemoryShuffleData size:{}(bytes) from {}:{} for {} cost:{}(ms)",
            getMemoryShuffleDataResponse.body().size(),
            host,
            nettyPort,
            requestInfo,
            System.currentTimeMillis() - start);
        return new RssGetInMemoryShuffleDataResponse(
            StatusCode.SUCCESS,
            getMemoryShuffleDataResponse.body(),
            getMemoryShuffleDataResponse.getBufferSegments());
      default:
        String msg =
            "Can't get shuffle in memory data from "
                + host
                + ":"
                + nettyPort
                + " for "
                + requestInfo
                + ", errorMsg:"
                + getMemoryShuffleDataResponse.getRetMessage();
        LOG.error(msg);
        throw new RssFetchFailedException(msg);
    }
  }

  @Override
  public RssGetShuffleIndexResponse getShuffleIndex(RssGetShuffleIndexRequest request) {
    TransportClient transportClient = getTransportClient();
    GetLocalShuffleIndexRequest getLocalShuffleIndexRequest =
        new GetLocalShuffleIndexRequest(
            requestId(),
            request.getAppId(),
            request.getShuffleId(),
            request.getPartitionId(),
            request.getPartitionNumPerRange(),
            request.getPartitionNum());
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
    RpcResponse rpcResponse;
    GetLocalShuffleIndexResponse getLocalShuffleIndexResponse;
    while (true) {
      rpcResponse = transportClient.sendRpcSync(getLocalShuffleIndexRequest, rpcTimeout);
      getLocalShuffleIndexResponse = (GetLocalShuffleIndexResponse) rpcResponse;
      if (rpcResponse.getStatusCode() != StatusCode.NO_BUFFER) {
        break;
      }
      waitOrThrow(request, retry, requestInfo, rpcResponse.getStatusCode(), start);
      retry++;
    }
    switch (rpcResponse.getStatusCode()) {
      case SUCCESS:
        LOG.info(
            "GetShuffleIndex size:{}(bytes) from {}:{} for {} cost:{}(ms)",
            getLocalShuffleIndexResponse.body().size(),
            host,
            nettyPort,
            requestInfo,
            System.currentTimeMillis() - start);
        return new RssGetShuffleIndexResponse(
            StatusCode.SUCCESS,
            getLocalShuffleIndexResponse.body(),
            getLocalShuffleIndexResponse.getFileLength(),
            getLocalShuffleIndexResponse.getStorageIds());
      default:
        String msg =
            "Can't get shuffle index from "
                + host
                + ":"
                + nettyPort
                + " for "
                + requestInfo
                + ", errorMsg:"
                + getLocalShuffleIndexResponse.getRetMessage();
        LOG.error(msg);
        throw new RssFetchFailedException(msg);
    }
  }

  @Override
  public RssGetShuffleDataResponse getShuffleData(RssGetShuffleDataRequest request) {
    TransportClient transportClient = getTransportClient();
    // Construct old version or v2 get shuffle data request to compatible with old server
    GetLocalShuffleDataRequest getLocalShuffleIndexRequest =
        request.storageIdSpecified()
            ? new GetLocalShuffleDataV2Request(
                requestId(),
                request.getAppId(),
                request.getShuffleId(),
                request.getPartitionId(),
                request.getPartitionNumPerRange(),
                request.getPartitionNum(),
                request.getOffset(),
                request.getLength(),
                request.getStorageId(),
                System.currentTimeMillis())
            : new GetLocalShuffleDataRequest(
                requestId(),
                request.getAppId(),
                request.getShuffleId(),
                request.getPartitionId(),
                request.getPartitionNumPerRange(),
                request.getPartitionNum(),
                request.getOffset(),
                request.getLength(),
                System.currentTimeMillis());
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
    RpcResponse rpcResponse;
    GetLocalShuffleDataResponse getLocalShuffleDataResponse;
    while (true) {
      rpcResponse = transportClient.sendRpcSync(getLocalShuffleIndexRequest, rpcTimeout);
      getLocalShuffleDataResponse = (GetLocalShuffleDataResponse) rpcResponse;
      if (rpcResponse.getStatusCode() != StatusCode.NO_BUFFER) {
        break;
      }
      waitOrThrow(request, retry, requestInfo, rpcResponse.getStatusCode(), start);
      retry++;
    }
    switch (rpcResponse.getStatusCode()) {
      case SUCCESS:
        LOG.info(
            "GetShuffleData size:{}(bytes) from {}:{} for {} cost:{}(ms)",
            getLocalShuffleDataResponse.body().size(),
            host,
            nettyPort,
            requestInfo,
            System.currentTimeMillis() - start);
        return new RssGetShuffleDataResponse(
            StatusCode.SUCCESS, getLocalShuffleDataResponse.body());
      default:
        String msg =
            "Can't get shuffle data from "
                + host
                + ":"
                + nettyPort
                + " for "
                + requestInfo
                + ", errorMsg:"
                + getLocalShuffleDataResponse.getRetMessage();
        LOG.error(msg);
        throw new RssFetchFailedException(msg);
    }
  }

  @Override
  public RssGetSortedShuffleDataResponse getSortedShuffleData(
      RssGetSortedShuffleDataRequest request) {
    TransportClient transportClient = getTransportClient();
    GetSortedShuffleDataRequest getSortedShuffleDataRequest =
        new GetSortedShuffleDataRequest(
            requestId(),
            request.getAppId(),
            request.getShuffleId(),
            request.getPartitionId(),
            request.getBlockId(),
            0,
            System.currentTimeMillis());

    String requestInfo =
        String.format(
            "appId[%s], shuffleId[%d], partitionId[%d], blockId[%d]",
            request.getAppId(),
            request.getShuffleId(),
            request.getPartitionId(),
            request.getBlockId());

    long start = System.currentTimeMillis();
    int retry = 0;
    RpcResponse rpcResponse;
    GetSortedShuffleDataResponse getSortedShuffleDataResponse;

    while (true) {
      rpcResponse = transportClient.sendRpcSync(getSortedShuffleDataRequest, rpcTimeout);
      getSortedShuffleDataResponse = (GetSortedShuffleDataResponse) rpcResponse;
      if (rpcResponse.getStatusCode() != StatusCode.NO_BUFFER) {
        break;
      }
      waitOrThrow(request, retry, requestInfo, rpcResponse.getStatusCode(), start);
      retry++;
    }

    switch (rpcResponse.getStatusCode()) {
      case SUCCESS:
        LOG.info(
            "GetSortedShuffleData from {}:{} for {} cost {} ms",
            host,
            nettyPort,
            requestInfo,
            System.currentTimeMillis() - start);
        return new RssGetSortedShuffleDataResponse(
            StatusCode.SUCCESS,
            getSortedShuffleDataResponse.getRetMessage(),
            getSortedShuffleDataResponse.body(),
            getSortedShuffleDataResponse.getNextBlockId(),
            getSortedShuffleDataResponse.getMergeState());
      default:
        String msg =
            String.format(
                "Can't get sorted shuffle data from %s:%d for %s, errorMsg: %s",
                host, nettyPort, requestInfo, getSortedShuffleDataResponse.getRetMessage());
        LOG.error(msg);
        throw new RssFetchFailedException(msg);
    }
  }

  private static final AtomicLong counter = new AtomicLong();

  public static long requestId() {
    return counter.getAndIncrement();
  }

  private TransportClient getTransportClient() {
    TransportClient transportClient;
    try {
      transportClient = clientFactory.createClient(host, nettyPort);
    } catch (Exception e) {
      throw new RssException("create transport client failed", e);
    }
    return transportClient;
  }
}
