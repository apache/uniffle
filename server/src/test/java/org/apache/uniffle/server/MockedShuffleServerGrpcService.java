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

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.proto.RssProtos;

public class MockedShuffleServerGrpcService extends ShuffleServerGrpcService {

  private static final Logger LOG = LoggerFactory.getLogger(MockedShuffleServerGrpcService.class);

  // appId -> shuffleId -> partitionRequestNum
  private Map<String, Map<Integer, AtomicInteger>> appToPartitionRequest =
      JavaUtils.newConcurrentMap();

  private long mockedTimeout = -1L;

  private boolean mockSendDataFailed = false;
  private int mockSendDataFailedStageNumber = -1;

  private boolean mockRequireBufferFailedWithNoBuffer = false;
  private boolean isMockRequireBufferFailedWithNoBufferForHugePartition = false;

  private boolean recordGetShuffleResult = false;

  private long numOfFailedReadRequest = 0;
  private AtomicInteger failedGetShuffleResultRequest = new AtomicInteger(0);
  private AtomicInteger failedGetShuffleResultForMultiPartRequest = new AtomicInteger(0);
  private AtomicInteger failedGetMemoryShuffleDataRequest = new AtomicInteger(0);
  private AtomicInteger failedGetLocalShuffleDataRequest = new AtomicInteger(0);
  private AtomicInteger failedGetLocalShuffleIndexRequest = new AtomicInteger(0);

  public void enableMockedTimeout(long timeout) {
    mockedTimeout = timeout;
  }

  public void enableMockSendDataFailed(boolean mockSendDataFailed) {
    this.mockSendDataFailed = mockSendDataFailed;
  }

  public void enableMockRequireBufferFailWithNoBuffer() {
    this.mockRequireBufferFailedWithNoBuffer = true;
  }

  public void enableMockRequireBufferFailWithNoBufferForHugePartition() {
    this.isMockRequireBufferFailedWithNoBufferForHugePartition = true;
  }

  public void enableRecordGetShuffleResult() {
    recordGetShuffleResult = true;
  }

  public void disableMockedTimeout() {
    mockedTimeout = -1;
  }

  public void enableFirstNReadRequestToFail(int n) {
    numOfFailedReadRequest = n;
  }

  public void resetFirstNReadRequestToFail() {
    numOfFailedReadRequest = 0;
    failedGetShuffleResultRequest.set(0);
    failedGetShuffleResultForMultiPartRequest.set(0);
    failedGetMemoryShuffleDataRequest.set(0);
    failedGetLocalShuffleDataRequest.set(0);
    failedGetLocalShuffleIndexRequest.set(0);
  }

  public MockedShuffleServerGrpcService(ShuffleServer shuffleServer) {
    super(shuffleServer);
  }

  @Override
  public void requireBuffer(
      RssProtos.RequireBufferRequest request,
      StreamObserver<RssProtos.RequireBufferResponse> responseObserver) {
    if (mockRequireBufferFailedWithNoBuffer
        || isMockRequireBufferFailedWithNoBufferForHugePartition) {
      LOG.info("Make require buffer mocked failed.");
      StatusCode code =
          mockRequireBufferFailedWithNoBuffer
              ? StatusCode.NO_BUFFER
              : StatusCode.NO_BUFFER_FOR_HUGE_PARTITION;
      RssProtos.RequireBufferResponse response =
          RssProtos.RequireBufferResponse.newBuilder()
              .setStatus(code.toProto())
              .setRequireBufferId(-1)
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      return;
    }

    super.requireBuffer(request, responseObserver);
  }

  @Override
  public void sendShuffleData(
      RssProtos.SendShuffleDataRequest request,
      StreamObserver<RssProtos.SendShuffleDataResponse> responseObserver) {
    if (mockSendDataFailed) {
      RssProtos.SendShuffleDataResponse reply;
      String errorMsg = "This write request is failed as mocked failure！";
      LOG.warn(errorMsg);
      reply =
          RssProtos.SendShuffleDataResponse.newBuilder()
              .setStatus(StatusCode.INTERNAL_ERROR.toProto())
              .setRetMsg(errorMsg)
              .build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
      return;
    }
    if (mockSendDataFailedStageNumber == request.getStageAttemptNumber()) {
      LOG.info(
          "Add a mocked sendData failed on sendShuffleData with the stage number={}",
          mockSendDataFailedStageNumber);
      throw new RuntimeException("This write request is failed as mocked failure！");
    }
    if (mockedTimeout > 0) {
      LOG.info("Add a mocked timeout on sendShuffleData");
      Uninterruptibles.sleepUninterruptibly(mockedTimeout, TimeUnit.MILLISECONDS);
    }
    super.sendShuffleData(request, responseObserver);
  }

  @Override
  public void reportShuffleResult(
      RssProtos.ReportShuffleResultRequest request,
      StreamObserver<RssProtos.ReportShuffleResultResponse> responseObserver) {
    if (mockedTimeout > 0) {
      LOG.info("Add a mocked timeout on reportShuffleResult");
      Uninterruptibles.sleepUninterruptibly(mockedTimeout, TimeUnit.MILLISECONDS);
    }
    super.reportShuffleResult(request, responseObserver);
  }

  @Override
  public void getShuffleResult(
      RssProtos.GetShuffleResultRequest request,
      StreamObserver<RssProtos.GetShuffleResultResponse> responseObserver) {
    if (mockedTimeout > 0) {
      LOG.info("Add a mocked timeout on getShuffleResult");
      Uninterruptibles.sleepUninterruptibly(mockedTimeout, TimeUnit.MILLISECONDS);
    }
    if (numOfFailedReadRequest > 0) {
      int currentFailedReadRequest = failedGetShuffleResultRequest.getAndIncrement();
      if (currentFailedReadRequest < numOfFailedReadRequest) {
        LOG.info(
            "This request is failed as mocked failure, current/firstN: {}/{}",
            currentFailedReadRequest,
            numOfFailedReadRequest);
        throw new RuntimeException("This request is failed as mocked failure");
      }
    }
    super.getShuffleResult(request, responseObserver);
  }

  @Override
  public void getShuffleResultForMultiPart(
      RssProtos.GetShuffleResultForMultiPartRequest request,
      StreamObserver<RssProtos.GetShuffleResultForMultiPartResponse> responseObserver) {
    if (mockedTimeout > 0) {
      LOG.info("Add a mocked timeout on getShuffleResultForMultiPart");
      Uninterruptibles.sleepUninterruptibly(mockedTimeout, TimeUnit.MILLISECONDS);
    }
    if (numOfFailedReadRequest > 0) {
      int currentFailedReadRequest = failedGetShuffleResultForMultiPartRequest.getAndIncrement();
      if (currentFailedReadRequest < numOfFailedReadRequest) {
        LOG.info(
            "This request is failed as mocked failure, current/firstN: {}/{}",
            currentFailedReadRequest,
            numOfFailedReadRequest);
        throw new RuntimeException("This request is failed as mocked failure");
      }
    }
    if (recordGetShuffleResult) {
      List<Integer> requestPartitions = request.getPartitionsList();
      Map<Integer, AtomicInteger> shuffleIdToPartitionRequestNum =
          appToPartitionRequest.computeIfAbsent(
              request.getAppId(), x -> JavaUtils.newConcurrentMap());
      AtomicInteger partitionRequestNum =
          shuffleIdToPartitionRequestNum.computeIfAbsent(
              request.getShuffleId(), x -> new AtomicInteger(0));
      partitionRequestNum.addAndGet(requestPartitions.size());
    }
    super.getShuffleResultForMultiPart(request, responseObserver);
  }

  public Map<String, Map<Integer, AtomicInteger>> getShuffleIdToPartitionRequest() {
    return appToPartitionRequest;
  }

  @Override
  public void getMemoryShuffleData(
      RssProtos.GetMemoryShuffleDataRequest request,
      StreamObserver<RssProtos.GetMemoryShuffleDataResponse> responseObserver) {
    if (numOfFailedReadRequest > 0) {
      int currentFailedReadRequest = failedGetMemoryShuffleDataRequest.getAndIncrement();
      if (currentFailedReadRequest < numOfFailedReadRequest) {
        LOG.info(
            "This request is failed as mocked failure, current/firstN: {}/{}",
            currentFailedReadRequest,
            numOfFailedReadRequest);
        StatusCode status = StatusCode.NO_BUFFER;
        String msg =
            "Can't require memory to get in memory shuffle data (This request is failed as mocked failure)";
        RssProtos.GetMemoryShuffleDataResponse reply =
            RssProtos.GetMemoryShuffleDataResponse.newBuilder()
                .setData(UnsafeByteOperations.unsafeWrap(new byte[] {}))
                .addAllShuffleDataBlockSegments(Lists.newArrayList())
                .setStatus(status.toProto())
                .setRetMsg(msg)
                .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
        return;
      }
    }
    super.getMemoryShuffleData(request, responseObserver);
  }

  @Override
  public void getLocalShuffleData(
      RssProtos.GetLocalShuffleDataRequest request,
      StreamObserver<RssProtos.GetLocalShuffleDataResponse> responseObserver) {
    if (numOfFailedReadRequest > 0) {
      int currentFailedReadRequest = failedGetLocalShuffleDataRequest.getAndIncrement();
      if (currentFailedReadRequest < numOfFailedReadRequest) {
        LOG.info(
            "This request is failed as mocked failure, current/firstN: {}/{}",
            currentFailedReadRequest,
            numOfFailedReadRequest);
        StatusCode status = StatusCode.NO_BUFFER;
        String msg =
            "Can't require memory to get shuffle data (This request is failed as mocked failure)";
        RssProtos.GetLocalShuffleDataResponse reply =
            RssProtos.GetLocalShuffleDataResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(msg)
                .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
        return;
      }
    }
    super.getLocalShuffleData(request, responseObserver);
  }

  @Override
  public void getLocalShuffleIndex(
      RssProtos.GetLocalShuffleIndexRequest request,
      StreamObserver<RssProtos.GetLocalShuffleIndexResponse> responseObserver) {
    if (numOfFailedReadRequest > 0) {
      int currentFailedReadRequest = failedGetLocalShuffleIndexRequest.getAndIncrement();
      if (currentFailedReadRequest < numOfFailedReadRequest) {
        LOG.info(
            "This request is failed as mocked failure, current/firstN: {}/{}",
            currentFailedReadRequest,
            numOfFailedReadRequest);
        StatusCode status = StatusCode.NO_BUFFER;
        String msg =
            "Can't require memory to get shuffle index (This request is failed as mocked failure)";
        RssProtos.GetLocalShuffleIndexResponse reply =
            RssProtos.GetLocalShuffleIndexResponse.newBuilder()
                .setStatus(status.toProto())
                .setRetMsg(msg)
                .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
        return;
      }
    }
    super.getLocalShuffleIndex(request, responseObserver);
  }

  public int getMockSendDataFailedStageNumber() {
    return mockSendDataFailedStageNumber;
  }

  public void setMockSendDataFailedStageNumber(int mockSendDataFailedStageNumber) {
    this.mockSendDataFailedStageNumber = mockSendDataFailedStageNumber;
  }
}
