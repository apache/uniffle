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

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import com.google.protobuf.Message;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.proto.RssProtos.GetLocalShuffleDataRequest;
import org.apache.uniffle.proto.RssProtos.GetLocalShuffleDataResponse;
import org.apache.uniffle.proto.RssProtos.GetLocalShuffleIndexRequest;
import org.apache.uniffle.proto.RssProtos.GetLocalShuffleIndexResponse;
import org.apache.uniffle.proto.RssProtos.GetMemoryShuffleDataRequest;
import org.apache.uniffle.proto.RssProtos.GetMemoryShuffleDataResponse;
import org.apache.uniffle.proto.RssProtos.GetSortedShuffleDataRequest;
import org.apache.uniffle.proto.RssProtos.GetSortedShuffleDataResponse;
import org.apache.uniffle.proto.RssProtos.RequireBufferRequest;
import org.apache.uniffle.proto.RssProtos.RequireBufferResponse;
import org.apache.uniffle.proto.RssProtos.ShufflePartitionRange;
import org.apache.uniffle.proto.RssProtos.ShuffleRegisterRequest;
import org.apache.uniffle.proto.RssProtos.ShuffleRegisterResponse;
import org.apache.uniffle.proto.RssProtos.ShuffleUnregisterByAppIdRequest;
import org.apache.uniffle.proto.RssProtos.ShuffleUnregisterByAppIdResponse;
import org.apache.uniffle.proto.RssProtos.StartSortMergeRequest;
import org.apache.uniffle.proto.RssProtos.StartSortMergeResponse;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The class is to test the appId handling of {@link ShuffleServerGrpcService} */
public class ShuffleServerGrpcServiceTest {

  private static final String VALID_APP_ID = "application_1699999999999_0001";

  // None of them is a plain directory name, while all of them used to be accepted as an appId.
  private static final List<String> MALFORMED_APP_IDS =
      Arrays.asList(
          "", " ", ".", "..", "../outside", "a/../../outside", "/tmp", "a\\b", "app\n", "..\\..");

  @TempDir File tempDir;

  private File storageBase;
  private File outside;
  private ShuffleServer shuffleServer;
  private ShuffleServerGrpcService service;

  @BeforeEach
  public void beforeEach() throws Exception {
    ShuffleServerMetrics.register();
    storageBase = new File(tempDir, "storage");
    outside = new File(tempDir, "outside");
    assertTrue(outside.mkdirs());

    shuffleServer = createShuffleServer(StorageType.LOCALFILE, Collections.emptyList());
    service = new ShuffleServerGrpcService(shuffleServer);
  }

  @AfterEach
  public void afterEach() throws Exception {
    if (shuffleServer != null) {
      shuffleServer.stopServer();
      shuffleServer = null;
    }
    ShuffleServerMetrics.clear();
  }

  private ShuffleServer createShuffleServer(
      StorageType storageType, List<String> allowedRemotePaths) throws Exception {
    ShuffleServerConf conf =
        new ShuffleServerConf(ClassLoader.getSystemResource("server.conf").getFile());
    conf.set(ShuffleServerConf.RPC_SERVER_PORT, 1234);
    conf.set(ShuffleServerConf.RSS_COORDINATOR_QUORUM, "localhost:9527");
    conf.set(ShuffleServerConf.JETTY_HTTP_PORT, 12345);
    conf.set(ShuffleServerConf.JETTY_CORE_POOL_SIZE, 64);
    conf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 1000L);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 50.0);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 0.0);
    conf.set(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 10000L);
    conf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 100000L);
    conf.set(ShuffleServerConf.HEALTH_CHECK_ENABLE, false);
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), storageType.name());
    conf.set(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    conf.setString(ShuffleServerConf.RSS_STORAGE_BASE_PATH.key(), storageBase.getAbsolutePath());
    if (!allowedRemotePaths.isEmpty()) {
      conf.set(ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS, allowedRemotePaths);
    }
    return new ShuffleServer(conf);
  }

  /** Replaces the server with one that writes the shuffle data to a remote storage as well. */
  private void useRemoteStorage(List<String> allowedRemotePaths) throws Exception {
    shuffleServer.stopServer();
    shuffleServer = createShuffleServer(StorageType.MEMORY_LOCALFILE_HDFS, allowedRemotePaths);
    service = new ShuffleServerGrpcService(shuffleServer);
  }

  @Test
  public void testRegisterShuffleRejectsRemoteStorageByDefault() throws Exception {
    useRemoteStorage(Collections.emptyList());
    ResponseRecorder<ShuffleRegisterResponse> recorder = new ResponseRecorder<>();
    service.registerShuffle(registerRequest(VALID_APP_ID, "hdfs://path1/rss"), recorder);

    assertNull(recorder.error);
    assertNotNull(recorder.response);
    assertEquals(StatusCode.INVALID_REQUEST, StatusCode.fromProto(recorder.response.getStatus()));
    // the reason is told to the client, so that the missing configuration is easy to find
    assertTrue(
        recorder
            .response
            .getRetMsg()
            .contains(ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS.key()));
    // nothing is registered for the app, neither the buffers nor the remote storage
    assertFalse(shuffleServer.getShuffleTaskManager().getAppIds().contains(VALID_APP_ID));
  }

  @Test
  public void testRegisterShuffleRejectsRemoteStorageOutsideAllowedPaths() throws Exception {
    useRemoteStorage(Collections.singletonList("hdfs://path1/rss"));
    for (String remoteStorage :
        new String[] {
          "hdfs://path2/rss",
          "hdfs://path1/rss2",
          "hdfs://path1/rss/../other",
          "hdfs://path1",
          "file:///tmp/rss",
          "/rss"
        }) {
      ResponseRecorder<ShuffleRegisterResponse> recorder = new ResponseRecorder<>();
      service.registerShuffle(registerRequest(VALID_APP_ID, remoteStorage), recorder);
      assertEquals(
          StatusCode.INVALID_REQUEST,
          StatusCode.fromProto(recorder.response.getStatus()),
          "remoteStorage[" + remoteStorage + "]");
      assertFalse(shuffleServer.getShuffleTaskManager().getAppIds().contains(VALID_APP_ID));
    }
  }

  @Test
  public void testRegisterShuffleAcceptsRemoteStorageUnderAllowedPaths() throws Exception {
    useRemoteStorage(Collections.singletonList("hdfs://path1/rss"));
    assertEquals(StatusCode.SUCCESS, registerShuffle(VALID_APP_ID, "hdfs://path1/rss"));
    assertTrue(shuffleServer.getShuffleTaskManager().getAppIds().contains(VALID_APP_ID));

    // a request without remote storage is not affected
    assertEquals(StatusCode.SUCCESS, registerShuffle(VALID_APP_ID + "2", ""));
    assertTrue(shuffleServer.getShuffleTaskManager().getAppIds().contains(VALID_APP_ID + "2"));
  }

  @Test
  public void testRegisterShuffleRejectsMalformedAppId() {
    for (String appId : MALFORMED_APP_IDS) {
      assertEquals(StatusCode.INVALID_REQUEST, registerShuffle(appId), "appId[" + appId + "]");
      // nothing is registered for it, so no storage path will be built from it afterwards
      assertFalse(shuffleServer.getShuffleTaskManager().getAppIds().contains(appId));
    }
  }

  @Test
  public void testUnregisterShuffleByAppIdRejectsMalformedAppId() throws Exception {
    // the shuffle data of another application in the storage base directory
    File otherAppData = new File(storageBase, "otherApp/0/0-1/data");
    assertTrue(otherAppData.getParentFile().mkdirs());
    assertTrue(otherAppData.createNewFile());
    File outsideData = new File(outside, "keep");
    assertTrue(outsideData.createNewFile());

    for (String appId : MALFORMED_APP_IDS) {
      assertEquals(
          StatusCode.INVALID_REQUEST,
          call(
              o ->
                  service.unregisterShuffleByAppId(
                      ShuffleUnregisterByAppIdRequest.newBuilder().setAppId(appId).build(), o),
              ShuffleUnregisterByAppIdResponse::getStatus),
          "appId[" + appId + "]");
    }

    // an empty appId used to resolve to the storage base directory itself
    assertTrue(otherAppData.exists());
    assertTrue(outsideData.exists());
  }

  @Test
  public void testHeartbeatRejectsMalformedAppId() {
    for (String appId : MALFORMED_APP_IDS) {
      assertEquals(StatusCode.INVALID_REQUEST, appHeartbeat(appId), "appId[" + appId + "]");
      // an app registered by the heartbeat would be purged once it expires
      assertFalse(shuffleServer.getShuffleTaskManager().getAppIds().contains(appId));
    }
  }

  @Test
  public void testOtherRequestsRejectMalformedAppId() {
    for (String appId : MALFORMED_APP_IDS) {
      assertEquals(
          StatusCode.INVALID_REQUEST,
          call(
              o ->
                  service.unregisterShuffle(
                      RssProtos.ShuffleUnregisterRequest.newBuilder()
                          .setAppId(appId)
                          .setShuffleId(0)
                          .build(),
                      o),
              RssProtos.ShuffleUnregisterResponse::getStatus),
          "unregisterShuffle appId[" + appId + "]");
      assertEquals(
          StatusCode.INVALID_REQUEST,
          call(
              o ->
                  service.reportShuffleResult(
                      RssProtos.ReportShuffleResultRequest.newBuilder()
                          .setAppId(appId)
                          .setShuffleId(0)
                          .build(),
                      o),
              RssProtos.ReportShuffleResultResponse::getStatus),
          "reportShuffleResult appId[" + appId + "]");
      assertEquals(
          StatusCode.INVALID_REQUEST,
          call(
              o ->
                  service.getShuffleResult(
                      RssProtos.GetShuffleResultRequest.newBuilder()
                          .setAppId(appId)
                          .setShuffleId(0)
                          .setBlockIdLayout(blockIdLayout())
                          .build(),
                      o),
              RssProtos.GetShuffleResultResponse::getStatus),
          "getShuffleResult appId[" + appId + "]");
      assertEquals(
          StatusCode.INVALID_REQUEST,
          call(
              o ->
                  service.getShuffleResultForMultiPart(
                      RssProtos.GetShuffleResultForMultiPartRequest.newBuilder()
                          .setAppId(appId)
                          .setShuffleId(0)
                          .setBlockIdLayout(blockIdLayout())
                          .build(),
                      o),
              RssProtos.GetShuffleResultForMultiPartResponse::getStatus),
          "getShuffleResultForMultiPart appId[" + appId + "]");
      assertEquals(
          StatusCode.INVALID_REQUEST,
          call(
              o ->
                  service.commitShuffleTask(
                      RssProtos.ShuffleCommitRequest.newBuilder()
                          .setAppId(appId)
                          .setShuffleId(0)
                          .build(),
                      o),
              RssProtos.ShuffleCommitResponse::getStatus),
          "commitShuffleTask appId[" + appId + "]");
      assertEquals(
          StatusCode.INVALID_REQUEST,
          call(
              o ->
                  service.finishShuffle(
                      RssProtos.FinishShuffleRequest.newBuilder()
                          .setAppId(appId)
                          .setShuffleId(0)
                          .build(),
                      o),
              RssProtos.FinishShuffleResponse::getStatus),
          "finishShuffle appId[" + appId + "]");
      assertEquals(
          StatusCode.INVALID_REQUEST,
          call(
              o ->
                  service.getLocalShuffleData(
                      GetLocalShuffleDataRequest.newBuilder()
                          .setAppId(appId)
                          .setShuffleId(0)
                          .setPartitionId(0)
                          .setPartitionNumPerRange(1)
                          .setPartitionNum(1)
                          .setLength(1)
                          .build(),
                      o),
              GetLocalShuffleDataResponse::getStatus),
          "getLocalShuffleData appId[" + appId + "]");
      assertEquals(
          StatusCode.INVALID_REQUEST,
          call(
              o ->
                  service.getLocalShuffleIndex(
                      GetLocalShuffleIndexRequest.newBuilder()
                          .setAppId(appId)
                          .setShuffleId(0)
                          .setPartitionId(0)
                          .setPartitionNumPerRange(1)
                          .setPartitionNum(1)
                          .build(),
                      o),
              GetLocalShuffleIndexResponse::getStatus),
          "getLocalShuffleIndex appId[" + appId + "]");
      assertEquals(
          StatusCode.INVALID_REQUEST,
          call(
              o ->
                  service.getMemoryShuffleData(
                      GetMemoryShuffleDataRequest.newBuilder()
                          .setAppId(appId)
                          .setShuffleId(0)
                          .setPartitionId(0)
                          .setReadBufferSize(1024)
                          .build(),
                      o),
              GetMemoryShuffleDataResponse::getStatus),
          "getMemoryShuffleData appId[" + appId + "]");
      assertEquals(
          StatusCode.INVALID_REQUEST,
          call(
              o ->
                  service.startSortMerge(
                      StartSortMergeRequest.newBuilder()
                          .setAppId(appId)
                          .setShuffleId(0)
                          .setPartitionId(0)
                          .build(),
                      o),
              StartSortMergeResponse::getStatus),
          "startSortMerge appId[" + appId + "]");
      assertEquals(
          StatusCode.INVALID_REQUEST,
          call(
              o ->
                  service.getSortedShuffleData(
                      GetSortedShuffleDataRequest.newBuilder()
                          .setAppId(appId)
                          .setShuffleId(0)
                          .setPartitionId(0)
                          .build(),
                      o),
              GetSortedShuffleDataResponse::getStatus),
          "getSortedShuffleData appId[" + appId + "]");
    }
  }

  @Test
  public void testRequireBufferKeepsAcceptingEmptyAppId() {
    // an empty appId means the client is of an old version
    ResponseRecorder<RequireBufferResponse> recorder = new ResponseRecorder<>();
    service.requireBuffer(RequireBufferRequest.newBuilder().setRequireSize(16).build(), recorder);

    assertNull(recorder.error);
    assertNotNull(recorder.response);
    assertEquals(StatusCode.SUCCESS, StatusCode.fromProto(recorder.response.getStatus()));
    assertTrue(recorder.response.getRequireBufferId() >= 0);
  }

  @Test
  public void testRequireBufferRejectsMalformedAppId() {
    for (String appId : MALFORMED_APP_IDS) {
      StatusCode status =
          call(
              o ->
                  service.requireBuffer(
                      RequireBufferRequest.newBuilder().setAppId(appId).setRequireSize(16).build(),
                      o),
              RequireBufferResponse::getStatus);
      // only the exactly empty appId tells that the client is of an old version
      assertEquals(
          appId.isEmpty() ? StatusCode.SUCCESS : StatusCode.INVALID_REQUEST,
          status,
          "requireBuffer appId[" + appId + "]");
    }
  }

  @Test
  public void testValidAppIdIsAccepted() {
    assertEquals(StatusCode.SUCCESS, registerShuffle(VALID_APP_ID));
    assertTrue(shuffleServer.getShuffleTaskManager().getAppIds().contains(VALID_APP_ID));
    assertEquals(StatusCode.SUCCESS, appHeartbeat(VALID_APP_ID));

    assertEquals(
        StatusCode.SUCCESS,
        call(
            o ->
                service.unregisterShuffleByAppId(
                    ShuffleUnregisterByAppIdRequest.newBuilder().setAppId(VALID_APP_ID).build(), o),
            ShuffleUnregisterByAppIdResponse::getStatus));
  }

  private StatusCode registerShuffle(String appId) {
    return registerShuffle(appId, "");
  }

  private StatusCode registerShuffle(String appId, String remoteStorage) {
    return call(
        o -> service.registerShuffle(registerRequest(appId, remoteStorage), o),
        ShuffleRegisterResponse::getStatus);
  }

  private static ShuffleRegisterRequest registerRequest(String appId, String remoteStorage) {
    return ShuffleRegisterRequest.newBuilder()
        .setAppId(appId)
        .setShuffleId(0)
        .addPartitionRanges(ShufflePartitionRange.newBuilder().setStart(0).setEnd(1))
        .setRemoteStorage(RssProtos.RemoteStorage.newBuilder().setPath(remoteStorage))
        .build();
  }

  private StatusCode appHeartbeat(String appId) {
    return call(
        o ->
            service.appHeartbeat(
                RssProtos.AppHeartBeatRequest.newBuilder().setAppId(appId).build(), o),
        RssProtos.AppHeartBeatResponse::getStatus);
  }

  /** The default block id layout of the clients, which is (18, 24, 21). */
  private static RssProtos.BlockIdLayout blockIdLayout() {
    return RssProtos.BlockIdLayout.newBuilder()
        .setSequenceNoBits(18)
        .setPartitionIdBits(24)
        .setTaskAttemptIdBits(21)
        .build();
  }

  private <T extends Message> StatusCode call(
      RpcCall<T> rpc, Function<T, RssProtos.StatusCode> getStatus) {
    ResponseRecorder<T> recorder = new ResponseRecorder<>();
    rpc.execute(recorder);
    assertNull(recorder.error, "the rpc should be answered instead of failing");
    assertNotNull(recorder.response, "the rpc should be answered");
    return StatusCode.fromProto(getStatus.apply(recorder.response));
  }

  private interface RpcCall<T> {
    void execute(StreamObserver<T> observer);
  }

  private static class ResponseRecorder<T extends Message> implements StreamObserver<T> {
    private T response;
    private Throwable error;

    @Override
    public void onNext(T value) {
      response = value;
    }

    @Override
    public void onError(Throwable t) {
      error = t;
    }

    @Override
    public void onCompleted() {}
  }
}
