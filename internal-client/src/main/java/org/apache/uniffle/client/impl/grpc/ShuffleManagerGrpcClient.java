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
import org.apache.uniffle.client.request.RssReportShuffleResultRequest;
import org.apache.uniffle.client.request.RssReportShuffleWriteFailureRequest;
import org.apache.uniffle.client.response.RssGetShuffleResultResponse;
import org.apache.uniffle.client.response.RssReassignOnBlockSendFailureResponse;
import org.apache.uniffle.client.response.RssReassignOnStageRetryResponse;
import org.apache.uniffle.client.response.RssReportShuffleFetchFailureResponse;
import org.apache.uniffle.client.response.RssReportShuffleResultResponse;
import org.apache.uniffle.client.response.RssReportShuffleWriteFailureResponse;
import org.apache.uniffle.common.exception.RssException;
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
    this(host, port, rpcTimeout, maxRetryAttempts, true);
  }

  public ShuffleManagerGrpcClient(
      String host, int port, long rpcTimeout, int maxRetryAttempts, boolean usePlaintext) {
    super(host, port, maxRetryAttempts, usePlaintext);
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
          getBlockingStub().reportShuffleFetchFailure(protoRequest);
      return RssReportShuffleFetchFailureResponse.fromProto(response);
    } catch (Exception e) {
      String msg = "Report shuffle fetch failure to host:port[" + host + ":" + port + "] failed";
      LOG.warn(msg, e);
      throw new RssException(msg, e);
    }
  }

  @Override
  public RssReassignOnStageRetryResponse getPartitionToShufflerServerWithStageRetry(
      RssPartitionToShuffleServerRequest req) {
    RssProtos.PartitionToShuffleServerRequest protoRequest = req.toProto();
    RssProtos.ReassignOnStageRetryResponse partitionToShufflerServer =
        getBlockingStub().getPartitionToShufflerServerWithStageRetry(protoRequest);
    RssReassignOnStageRetryResponse rssReassignOnStageRetryResponse =
        RssReassignOnStageRetryResponse.fromProto(partitionToShufflerServer);
    return rssReassignOnStageRetryResponse;
  }

  @Override
  public RssReassignOnBlockSendFailureResponse getPartitionToShufflerServerWithBlockRetry(
      RssPartitionToShuffleServerRequest req) {
    RssProtos.PartitionToShuffleServerRequest protoRequest = req.toProto();
    RssProtos.ReassignOnBlockSendFailureResponse partitionToShufflerServer =
        getBlockingStub().getPartitionToShufflerServerWithBlockRetry(protoRequest);
    RssReassignOnBlockSendFailureResponse rssReassignOnBlockSendFailureResponse =
        RssReassignOnBlockSendFailureResponse.fromProto(partitionToShufflerServer);
    return rssReassignOnBlockSendFailureResponse;
  }

  @Override
  public RssReportShuffleWriteFailureResponse reportShuffleWriteFailure(
      RssReportShuffleWriteFailureRequest request) {
    RssProtos.ReportShuffleWriteFailureRequest protoRequest = request.toProto();
    try {
      RssProtos.ReportShuffleWriteFailureResponse response =
          getBlockingStub().reportShuffleWriteFailure(protoRequest);
      return RssReportShuffleWriteFailureResponse.fromProto(response);
    } catch (Exception e) {
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
    RssProtos.ReassignOnBlockSendFailureResponse response =
        getBlockingStub().reassignOnBlockSendFailure(protoReq);
    return RssReassignOnBlockSendFailureResponse.fromProto(response);
  }

  @Override
  public RssGetShuffleResultResponse getShuffleResult(RssGetShuffleResultRequest request) {
    RssProtos.GetShuffleResultResponse response =
        getBlockingStub().getShuffleResult(request.toProto());
    return RssGetShuffleResultResponse.fromProto(response);
  }

  @Override
  public RssGetShuffleResultResponse getShuffleResultForMultiPart(
      RssGetShuffleResultForMultiPartRequest request) {
    RssProtos.GetShuffleResultForMultiPartResponse response =
        getBlockingStub().getShuffleResultForMultiPart(request.toProto());
    return RssGetShuffleResultResponse.fromProto(response);
  }

  @Override
  public RssReportShuffleResultResponse reportShuffleResult(RssReportShuffleResultRequest request) {
    RssProtos.ReportShuffleResultResponse response =
        getBlockingStub().reportShuffleResult(request.toProto());
    return RssReportShuffleResultResponse.fromProto(response);
  }

  @Override
  public boolean isClosed() {
    return channel.isShutdown() || channel.isTerminated();
  }
}
