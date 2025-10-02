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

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.request.RssGetShuffleResultForMultiPartRequest;
import org.apache.uniffle.client.request.RssGetShuffleResultRequest;
import org.apache.uniffle.client.request.RssPartitionToShuffleServerRequest;
import org.apache.uniffle.client.request.RssReassignOnBlockSendFailureRequest;
import org.apache.uniffle.client.request.RssReportShuffleFetchFailureRequest;
import org.apache.uniffle.client.request.RssReportShuffleReadMetricRequest;
import org.apache.uniffle.client.request.RssReportShuffleResultRequest;
import org.apache.uniffle.client.request.RssReportShuffleWriteFailureRequest;
import org.apache.uniffle.client.request.RssReportShuffleWriteMetricRequest;
import org.apache.uniffle.client.response.RssGetShuffleResultResponse;
import org.apache.uniffle.client.response.RssReassignOnBlockSendFailureResponse;
import org.apache.uniffle.client.response.RssReassignOnStageRetryResponse;
import org.apache.uniffle.client.response.RssReportShuffleFetchFailureResponse;
import org.apache.uniffle.client.response.RssReportShuffleReadMetricResponse;
import org.apache.uniffle.client.response.RssReportShuffleResultResponse;
import org.apache.uniffle.client.response.RssReportShuffleWriteFailureResponse;
import org.apache.uniffle.client.response.RssReportShuffleWriteMetricResponse;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.RetryUtils;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.proto.RssProtos.ReportShuffleFetchFailureRequest;
import org.apache.uniffle.proto.RssProtos.ReportShuffleFetchFailureResponse;
import org.apache.uniffle.proto.ShuffleManagerGrpc;

public class ShuffleManagerGrpcClient extends GrpcClient implements ShuffleManagerClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleManagerGrpcClient.class);
  private final long rpcTimeout;
  private ShuffleManagerGrpc.ShuffleManagerBlockingStub blockingStub;

  public ShuffleManagerGrpcClient(String host, int port, long rpcTimeout) {
    this(host, port, rpcTimeout, 3);
  }

  public ShuffleManagerGrpcClient(String host, int port, long rpcTimeout, int maxRetryAttempts) {
    this(
        host,
        port,
        rpcTimeout,
        maxRetryAttempts,
        true,
        RssClientConf.RPC_RETRY_BACKOFF_MS.defaultValue());
  }

  public ShuffleManagerGrpcClient(
      String host,
      int port,
      long rpcTimeout,
      int maxRetryAttempts,
      boolean usePlaintext,
      long rpcRetryBackoffMs) {
    super(host, port, maxRetryAttempts, usePlaintext, rpcRetryBackoffMs);
    blockingStub = ShuffleManagerGrpc.newBlockingStub(channel);
    this.rpcTimeout = rpcTimeout;
  }

  public ShuffleManagerGrpc.ShuffleManagerBlockingStub getBlockingStub() {
    return blockingStub.withDeadlineAfter(rpcTimeout, TimeUnit.MILLISECONDS);
  }

  public String getDesc() {
    return "Shuffle manager grpc client ref " + host + ":" + port;
  }

  @Override
  public RssReportShuffleFetchFailureResponse reportShuffleFetchFailure(
      RssReportShuffleFetchFailureRequest request) {
    ReportShuffleFetchFailureRequest protoRequest = request.toProto();
    try {
      ReportShuffleFetchFailureResponse response =
          RetryUtils.retryWithCondition(
              () -> getBlockingStub().reportShuffleFetchFailure(protoRequest),
              null,
              rpcRetryBackoffMs,
              maxRetryAttempts,
              e -> e instanceof Exception);
      return RssReportShuffleFetchFailureResponse.fromProto(response);
    } catch (Throwable e) {
      String msg = "Report shuffle fetch failure to host:port[" + host + ":" + port + "] failed";
      LOG.warn(msg, e);
      throw new RssException(msg, e);
    }
  }

  @Override
  public RssReassignOnStageRetryResponse getPartitionToShufflerServerWithStageRetry(
      RssPartitionToShuffleServerRequest req) {
    RssProtos.PartitionToShuffleServerRequest protoRequest = req.toProto();
    try {
      RssProtos.ReassignOnStageRetryResponse partitionToShufflerServer =
          RetryUtils.retryWithCondition(
              () -> getBlockingStub().getPartitionToShufflerServerWithStageRetry(protoRequest),
              null,
              rpcRetryBackoffMs,
              maxRetryAttempts,
              e -> e instanceof Exception);
      return RssReassignOnStageRetryResponse.fromProto(partitionToShufflerServer);
    } catch (Throwable e) {
      String msg =
          "Get partition to shuffle server with stage retry from host:port["
              + host
              + ":"
              + port
              + "] failed";
      LOG.warn(msg, e);
      throw new RssException(msg, e);
    }
  }

  @Override
  public RssReassignOnBlockSendFailureResponse getPartitionToShufflerServerWithBlockRetry(
      RssPartitionToShuffleServerRequest req) {
    RssProtos.PartitionToShuffleServerRequest protoRequest = req.toProto();
    try {
      RssProtos.ReassignOnBlockSendFailureResponse partitionToShufflerServer =
          RetryUtils.retryWithCondition(
              () -> getBlockingStub().getPartitionToShufflerServerWithBlockRetry(protoRequest),
              null,
              rpcRetryBackoffMs,
              maxRetryAttempts,
              e -> e instanceof Exception);
      return RssReassignOnBlockSendFailureResponse.fromProto(partitionToShufflerServer);
    } catch (Throwable e) {
      String msg =
          "Get partition to shuffle server with block retry from host:port["
              + host
              + ":"
              + port
              + "] failed";
      LOG.warn(msg, e);
      throw new RssException(msg, e);
    }
  }

  @Override
  public RssReportShuffleWriteFailureResponse reportShuffleWriteFailure(
      RssReportShuffleWriteFailureRequest request) {
    RssProtos.ReportShuffleWriteFailureRequest protoRequest = request.toProto();
    try {
      RssProtos.ReportShuffleWriteFailureResponse response =
          RetryUtils.retryWithCondition(
              () -> getBlockingStub().reportShuffleWriteFailure(protoRequest),
              null,
              rpcRetryBackoffMs,
              maxRetryAttempts,
              e -> e instanceof Exception);
      return RssReportShuffleWriteFailureResponse.fromProto(response);
    } catch (Throwable e) {
      String msg = "Report shuffle fetch failure to host:port[" + host + ":" + port + "] failed";
      LOG.warn(msg, e);
      throw new RssException(msg, e);
    }
  }

  @Override
  public RssReassignOnBlockSendFailureResponse reassignOnBlockSendFailure(
      RssReassignOnBlockSendFailureRequest request) {
    RssProtos.RssReassignOnBlockSendFailureRequest protoReq =
        RssReassignOnBlockSendFailureRequest.toProto(request);
    try {
      RssProtos.ReassignOnBlockSendFailureResponse response =
          RetryUtils.retryWithCondition(
              () -> getBlockingStub().reassignOnBlockSendFailure(protoReq),
              null,
              rpcRetryBackoffMs,
              maxRetryAttempts,
              e -> e instanceof Exception);
      return RssReassignOnBlockSendFailureResponse.fromProto(response);
    } catch (Throwable e) {
      String msg =
          "Reassign on block send failure from host:port[" + host + ":" + port + "] failed";
      LOG.warn(msg, e);
      throw new RssException(msg, e);
    }
  }

  @Override
  public RssGetShuffleResultResponse getShuffleResult(RssGetShuffleResultRequest request) {
    try {
      RssProtos.GetShuffleResultResponse response =
          RetryUtils.retryWithCondition(
              () -> getBlockingStub().getShuffleResult(request.toProto()),
              null,
              rpcRetryBackoffMs,
              maxRetryAttempts,
              e -> e instanceof Exception);
      return RssGetShuffleResultResponse.fromProto(response);
    } catch (Throwable e) {
      String msg = "Get shuffle result from host:port[" + host + ":" + port + "] failed";
      LOG.warn(msg, e);
      throw new RssException(msg, e);
    }
  }

  @Override
  public RssGetShuffleResultResponse getShuffleResultForMultiPart(
      RssGetShuffleResultForMultiPartRequest request) {
    try {
      RssProtos.GetShuffleResultForMultiPartResponse response =
          RetryUtils.retryWithCondition(
              () -> getBlockingStub().getShuffleResultForMultiPart(request.toProto()),
              null,
              rpcRetryBackoffMs,
              maxRetryAttempts,
              e -> e instanceof Exception);
      return RssGetShuffleResultResponse.fromProto(response);
    } catch (Throwable e) {
      String msg =
          "Get shuffle result for multiport from host:port[" + host + ":" + port + "] failed";
      LOG.warn(msg, e);
      throw new RssException(msg, e);
    }
  }

  @Override
  public RssReportShuffleResultResponse reportShuffleResult(RssReportShuffleResultRequest request) {
    try {
      RssProtos.ReportShuffleResultResponse response =
          RetryUtils.retryWithCondition(
              () -> getBlockingStub().reportShuffleResult(request.toProto()),
              null,
              rpcRetryBackoffMs,
              maxRetryAttempts,
              e -> e instanceof Exception);
      return RssReportShuffleResultResponse.fromProto(response);
    } catch (Throwable e) {
      String msg = "Report shuffle result to host:port[" + host + ":" + port + "] failed";
      LOG.warn(msg, e);
      throw new RssException(msg, e);
    }
  }

  @Override
  public RssReportShuffleWriteMetricResponse reportShuffleWriteMetric(
      RssReportShuffleWriteMetricRequest request) {
    try {
      RssProtos.ReportShuffleWriteMetricResponse response =
          RetryUtils.retryWithCondition(
              () -> getBlockingStub().reportShuffleWriteMetric(request.toProto()),
              null,
              rpcRetryBackoffMs,
              maxRetryAttempts,
              e -> e instanceof Exception);
      return RssReportShuffleWriteMetricResponse.fromProto(response);
    } catch (Throwable e) {
      String msg = "Report shuffle write metric to host:port[" + host + ":" + port + "] failed";
      LOG.warn(msg, e);
      throw new RssException(msg, e);
    }
  }

  @Override
  public RssReportShuffleReadMetricResponse reportShuffleReadMetric(
      RssReportShuffleReadMetricRequest request) {
    try {
      RssProtos.ReportShuffleReadMetricResponse response =
          RetryUtils.retryWithCondition(
              () -> getBlockingStub().reportShuffleReadMetric(request.toProto()),
              null,
              rpcRetryBackoffMs,
              maxRetryAttempts,
              e -> e instanceof Exception);
      return RssReportShuffleReadMetricResponse.fromProto(response);
    } catch (Throwable e) {
      String msg = "Report shuffle read metric to host:port[" + host + ":" + port + "] failed";
      LOG.warn(msg, e);
      throw new RssException(msg, e);
    }
  }

  @Override
  public boolean isClosed() {
    return channel.isShutdown() || channel.isTerminated();
  }
}
