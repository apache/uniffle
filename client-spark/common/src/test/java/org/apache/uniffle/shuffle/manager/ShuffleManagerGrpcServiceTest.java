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

package org.apache.uniffle.shuffle.manager;

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.apache.uniffle.proto.RssProtos.ReportShuffleFetchFailureRequest;
import org.apache.uniffle.proto.RssProtos.ReportShuffleFetchFailureResponse;
import org.apache.uniffle.proto.RssProtos.StatusCode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class ShuffleManagerGrpcServiceTest {
  // create mock of RssShuffleManagerInterface.
  private static RssShuffleManagerBase mockShuffleManager;
  private static final String appId = "app-123";
  private static final int maxFetchFailures = 2;
  private static final int shuffleId = 0;
  private static final int numMaps = 100;
  private static final int numReduces = 10;

  private static class MockedStreamObserver<T> implements StreamObserver<T> {
    T value;
    Throwable error;
    boolean completed;

    @Override
    public void onNext(T value) {
      this.value = value;
    }

    @Override
    public void onError(Throwable t) {
      this.error = t;
    }

    @Override
    public void onCompleted() {
      this.completed = true;
    }
  }

  @BeforeAll
  public static void setup() {
    mockShuffleManager = mock(RssShuffleManagerBase.class);
    Mockito.when(mockShuffleManager.getAppId()).thenReturn(appId);
    Mockito.when(mockShuffleManager.getNumMaps(shuffleId)).thenReturn(numMaps);
    Mockito.when(mockShuffleManager.getPartitionNum(shuffleId)).thenReturn(numReduces);
    Mockito.when(mockShuffleManager.getMaxFetchFailures()).thenReturn(maxFetchFailures);
  }

  @Test
  public void testShuffleManagerGrpcService() {
    ShuffleManagerGrpcService service = new ShuffleManagerGrpcService(mockShuffleManager);
    MockedStreamObserver<ReportShuffleFetchFailureResponse> appIdResponseObserver =
        new MockedStreamObserver<>();
    ReportShuffleFetchFailureRequest req =
        ReportShuffleFetchFailureRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setUniffleShuffleId(shuffleId)
            .setStageAttemptId(1)
            .setStageAttemptNumber(1)
            .setPartitionId(1)
            .buildPartial();

    service.reportShuffleFetchFailure(req, appIdResponseObserver);
    assertTrue(appIdResponseObserver.completed);
    // the first call of ReportShuffleFetchFailureRequest should be successful.
    assertEquals(StatusCode.SUCCESS, appIdResponseObserver.value.getStatus());
    assertFalse(appIdResponseObserver.value.getReSubmitWholeStage());

    // req with wrong appId should fail.
    req =
        ReportShuffleFetchFailureRequest.newBuilder()
            .mergeFrom(req)
            .setAppId("wrong-app-id")
            .build();
    service.reportShuffleFetchFailure(req, appIdResponseObserver);
    assertEquals(StatusCode.INVALID_REQUEST, appIdResponseObserver.value.getStatus());
    // forwards the stageAttemptId to 1 to mock invalid request
    req =
        ReportShuffleFetchFailureRequest.newBuilder()
            .mergeFrom(req)
            .setAppId(appId)
            .setStageAttemptId(1)
            .setStageAttemptNumber(0)
            .build();
    service.reportShuffleFetchFailure(req, appIdResponseObserver);
    assertEquals(StatusCode.INVALID_REQUEST, appIdResponseObserver.value.getStatus());
    assertTrue(appIdResponseObserver.value.getMsg().contains("old stage"));
  }
}
