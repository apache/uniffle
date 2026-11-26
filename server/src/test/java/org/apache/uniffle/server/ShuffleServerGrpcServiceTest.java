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
import java.util.Map;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.RssUtils;
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
import org.apache.uniffle.proto.RssProtos.SendShuffleDataRequest;
import org.apache.uniffle.proto.RssProtos.SendShuffleDataResponse;
import org.apache.uniffle.proto.RssProtos.ShufflePartitionRange;
import org.apache.uniffle.proto.RssProtos.ShuffleRegisterRequest;
import org.apache.uniffle.proto.RssProtos.ShuffleRegisterResponse;
import org.apache.uniffle.proto.RssProtos.ShuffleUnregisterByAppIdRequest;
import org.apache.uniffle.proto.RssProtos.ShuffleUnregisterByAppIdResponse;
import org.apache.uniffle.proto.RssProtos.StartSortMergeRequest;
import org.apache.uniffle.proto.RssProtos.StartSortMergeResponse;
import org.apache.uniffle.server.buffer.ShuffleBufferType;
import org.apache.uniffle.server.storage.HadoopStorageManager;
import org.apache.uniffle.server.storage.HybridStorageManager;
import org.apache.uniffle.server.storage.StorageManager;
import org.apache.uniffle.storage.common.HadoopStorage;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.server.merge.ShuffleMergeManager.MERGE_APP_SUFFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
  }

  @AfterEach
  public void afterEach() throws Exception {
    if (shuffleServer != null) {
      shuffleServer.stopServer();
      shuffleServer = null;
    }
    ShuffleServerMetrics.clear();
  }

  /**
   * Starts the server of the test on demand, so that a test gets exactly one. The threads of a
   * server outlive stopServer(), so the less of them the better.
   */
  private ShuffleServer createShuffleServer(
      StorageType storageType, List<String> allowedRemotePaths, boolean remoteMergeEnabled)
      throws Exception {
    assertNull(shuffleServer, "a test gets at most one server");
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
    conf.set(ShuffleServerConf.SERVER_MERGE_ENABLE, remoteMergeEnabled);
    if (remoteMergeEnabled) {
      // the remote merge only works with the skip list buffer
      conf.set(ShuffleServerConf.SERVER_SHUFFLE_BUFFER_TYPE, ShuffleBufferType.SKIP_LIST);
    }
    conf.setString(ShuffleServerConf.RSS_STORAGE_BASE_PATH.key(), storageBase.getAbsolutePath());
    if (!allowedRemotePaths.isEmpty()) {
      conf.set(ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS, allowedRemotePaths);
    }
    shuffleServer = new ShuffleServer(conf);
    service = new ShuffleServerGrpcService(shuffleServer);
    return shuffleServer;
  }

  /** Uses a server that keeps the shuffle data on the local disks only. */
  private void useLocalStorage() throws Exception {
    createShuffleServer(StorageType.LOCALFILE, Collections.emptyList(), false);
  }

  /** Uses a server that writes the shuffle data to a remote storage as well. */
  private void useRemoteStorage(List<String> allowedRemotePaths) throws Exception {
    useRemoteStorage(allowedRemotePaths, false);
  }

  private void useRemoteStorage(List<String> allowedRemotePaths, boolean remoteMergeEnabled)
      throws Exception {
    createShuffleServer(StorageType.MEMORY_LOCALFILE_HDFS, allowedRemotePaths, remoteMergeEnabled);
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

  private Map<String, HadoopStorage> registeredRemoteStorages() {
    StorageManager coldStorageManager =
        ((HybridStorageManager) shuffleServer.getStorageManager()).getColdStorageManager();
    return ((HadoopStorageManager) coldStorageManager).getAppIdToStorages();
  }

  @Test
  public void testRegisterShuffleToleratesBlankAllowedPaths() throws Exception {
    // a blank entry never denotes a location, and a padded one is the location without the spaces
    useRemoteStorage(Arrays.asList(" ", " hdfs://path1/rss "));
    assertEquals(StatusCode.SUCCESS, registerShuffle(VALID_APP_ID, "hdfs://path1/rss"));
    assertEquals("hdfs://path1/rss", registeredRemoteStorages().get(VALID_APP_ID).getStoragePath());
    // the skipped entries allow nothing else
    assertEquals(
        StatusCode.INVALID_REQUEST, registerShuffle(VALID_APP_ID + "2", "hdfs://path2/rss"));
  }

  @Test
  public void testRegisterShuffleOfRegisteredAppWithDisallowedRemoteStorage() throws Exception {
    useRemoteStorage(Collections.singletonList("hdfs://path1/rss"));
    assertEquals(StatusCode.SUCCESS, registerShuffle(VALID_APP_ID, "hdfs://path1/rss"));

    // the app is registered already, a disallowed remote storage is still refused and the
    // registered one is left as it is
    assertEquals(StatusCode.INVALID_REQUEST, registerShuffle(VALID_APP_ID, "hdfs://path2/rss"));
    assertEquals("hdfs://path1/rss", registeredRemoteStorages().get(VALID_APP_ID).getStoragePath());
  }

  @Test
  public void testRegisterShuffleOfUnknownFileSystemScheme() throws Exception {
    useRemoteStorage(Collections.singletonList("myfs://path1/rss"));
    // the client brings the implementation of the file system itself, which is not trusted
    ResponseRecorder<ShuffleRegisterResponse> recorder = new ResponseRecorder<>();
    service.registerShuffle(
        registerRequest(
            VALID_APP_ID,
            "myfs://path1/rss",
            ImmutableMap.of("fs.myfs.impl", "org.apache.hadoop.fs.LocalFileSystem")),
        recorder);

    // it's refused at the registration, instead of failing at the first write
    assertEquals(StatusCode.INVALID_REQUEST, StatusCode.fromProto(recorder.response.getStatus()));
    assertTrue(recorder.response.getRetMsg().contains("myfs"), recorder.response.getRetMsg());
    assertFalse(shuffleServer.getShuffleTaskManager().getAppIds().contains(VALID_APP_ID));
    assertTrue(registeredRemoteStorages().isEmpty());
  }

  @Test
  public void testRegisterShuffleRejectsAppIdTooLongForRemoteMerge() throws Exception {
    useRemoteStorage(Collections.singletonList("hdfs://path1/rss"), true);
    // the id fits the limit, but the remote merge stores the merged blocks under the id with a
    // suffix, which does not fit any more
    String tooLongAppId =
        StringUtils.repeat('a', RssUtils.MAX_APP_ID_LENGTH - MERGE_APP_SUFFIX.length() + 1);
    ResponseRecorder<ShuffleRegisterResponse> recorder = new ResponseRecorder<>();
    service.registerShuffle(registerRequestWithMerge(tooLongAppId, "hdfs://path1/rss"), recorder);

    assertEquals(StatusCode.INVALID_REQUEST, StatusCode.fromProto(recorder.response.getStatus()));
    // neither the app nor the one of the merged blocks is registered
    assertFalse(shuffleServer.getShuffleTaskManager().getAppIds().contains(tooLongAppId));
    assertFalse(
        shuffleServer
            .getShuffleTaskManager()
            .getAppIds()
            .contains(tooLongAppId + MERGE_APP_SUFFIX));

    // an id that fits with the suffix passes the check: with a usable merge context the app of
    // the merged blocks gets registered and the remote merge is set up for the shuffle
    String fittingAppId =
        StringUtils.repeat('b', RssUtils.MAX_APP_ID_LENGTH - MERGE_APP_SUFFIX.length());
    ResponseRecorder<ShuffleRegisterResponse> fittingRecorder = new ResponseRecorder<>();
    service.registerShuffle(
        registerRequestWithUsableMerge(fittingAppId, "hdfs://path1/rss"), fittingRecorder);
    assertEquals(
        StatusCode.SUCCESS,
        StatusCode.fromProto(fittingRecorder.response.getStatus()),
        fittingRecorder.response.getRetMsg());
    assertTrue(shuffleServer.getShuffleTaskManager().getAppIds().contains(fittingAppId));
    assertTrue(
        shuffleServer
            .getShuffleTaskManager()
            .getAppIds()
            .contains(fittingAppId + MERGE_APP_SUFFIX));
  }

  @Test
  public void testHeartbeatDoesNotRegisterTooLongAppIdOfRemoteMerge() throws Exception {
    useRemoteStorage(Collections.singletonList("hdfs://path1/rss"), true);
    // the id fits itself, but not with the suffix of the remote merge, so the app of the merged
    // blocks must not be registered by the heartbeat
    String tooLongAppId =
        StringUtils.repeat('a', RssUtils.MAX_APP_ID_LENGTH - MERGE_APP_SUFFIX.length() + 1);
    // the heartbeat is only accepted for a registered app
    assertEquals(StatusCode.SUCCESS, registerShuffle(tooLongAppId, "hdfs://path1/rss"));
    assertEquals(StatusCode.SUCCESS, appHeartbeat(tooLongAppId));
    assertTrue(shuffleServer.getShuffleTaskManager().getAppIds().contains(tooLongAppId));
    assertFalse(
        shuffleServer
            .getShuffleTaskManager()
            .getAppIds()
            .contains(tooLongAppId + MERGE_APP_SUFFIX));

    // an id that fits refreshes the app of the merged blocks as before
    String fittingAppId =
        StringUtils.repeat('b', RssUtils.MAX_APP_ID_LENGTH - MERGE_APP_SUFFIX.length());
    assertEquals(StatusCode.SUCCESS, registerShuffle(fittingAppId, "hdfs://path1/rss"));
    assertEquals(StatusCode.SUCCESS, appHeartbeat(fittingAppId));
    assertTrue(
        shuffleServer
            .getShuffleTaskManager()
            .getAppIds()
            .contains(fittingAppId + MERGE_APP_SUFFIX));
  }

  @Test
  public void testRegisterShuffleWithMergeContextWhenRemoteMergeIsDisabled() throws Exception {
    useLocalStorage();
    // without the remote merge there is no app of the merged blocks, so it does not matter
    // whether the appId fits with the suffix
    String tooLongAppId =
        StringUtils.repeat('a', RssUtils.MAX_APP_ID_LENGTH - MERGE_APP_SUFFIX.length() + 1);
    assertEquals(StatusCode.SUCCESS, registerShuffle(registerRequestWithMerge(tooLongAppId, "")));
    assertTrue(shuffleServer.getShuffleTaskManager().getAppIds().contains(tooLongAppId));
  }

  @Test
  public void testUnregisterSkipsTooLongAppIdOfRemoteMerge() throws Exception {
    useRemoteStorage(Collections.singletonList("hdfs://path1/rss"), true);
    // One app for each of the two unregister requests, because unregistering an app removes it
    // in the background, after which a second request for it is answered with NO_REGISTER
    String[] tooLongAppIds = {
      StringUtils.repeat('a', RssUtils.MAX_APP_ID_LENGTH - MERGE_APP_SUFFIX.length() + 1),
      StringUtils.repeat('c', RssUtils.MAX_APP_ID_LENGTH - MERGE_APP_SUFFIX.length() + 1)
    };
    String[] fittingAppIds = {
      StringUtils.repeat('b', RssUtils.MAX_APP_ID_LENGTH - MERGE_APP_SUFFIX.length()),
      StringUtils.repeat('d', RssUtils.MAX_APP_ID_LENGTH - MERGE_APP_SUFFIX.length())
    };
    // The apps are registered without a remote storage: the remote merge is a flag of the
    // server, which the service under test reads from a mock below, and a registered remote
    // storage would be deleted in the background afterwards
    for (String appId : tooLongAppIds) {
      assertEquals(StatusCode.SUCCESS, registerShuffle(appId));
    }
    for (String appId : fittingAppIds) {
      assertEquals(StatusCode.SUCCESS, registerShuffle(appId));
    }

    // The service talks to a spy of the task manager of the server, so that the apps it's asked
    // to remove can be seen
    ShuffleTaskManager taskManager = spy(shuffleServer.getShuffleTaskManager());
    ShuffleServer serverView = mock(ShuffleServer.class);
    when(serverView.getShuffleTaskManager()).thenReturn(taskManager);
    when(serverView.getShuffleServerConf()).thenReturn(shuffleServer.getShuffleServerConf());
    when(serverView.isRemoteMergeEnable()).thenReturn(true);
    ShuffleServerGrpcService spiedService = new ShuffleServerGrpcService(serverView);
    for (String appId : new String[] {tooLongAppIds[0], fittingAppIds[0]}) {
      assertEquals(
          StatusCode.SUCCESS,
          call(
              o ->
                  spiedService.unregisterShuffleByAppId(
                      ShuffleUnregisterByAppIdRequest.newBuilder().setAppId(appId).build(), o),
              ShuffleUnregisterByAppIdResponse::getStatus));
    }
    for (String appId : new String[] {tooLongAppIds[1], fittingAppIds[1]}) {
      assertEquals(
          StatusCode.SUCCESS,
          call(
              o ->
                  spiedService.unregisterShuffle(
                      RssProtos.ShuffleUnregisterRequest.newBuilder()
                          .setAppId(appId)
                          .setShuffleId(0)
                          .build(),
                      o),
              RssProtos.ShuffleUnregisterResponse::getStatus));
    }

    // The app itself is always removed. The app of the merged blocks is only removed for an id
    // that fits with the suffix: the first too-long id went through unregisterShuffleByAppId,
    // which takes the one-argument method, the second one through unregisterShuffle.
    verify(taskManager).removeShuffleDataAsync(tooLongAppIds[0]);
    verify(taskManager, never()).removeShuffleDataAsync(tooLongAppIds[0] + MERGE_APP_SUFFIX);
    verify(taskManager).removeShuffleDataAsync(tooLongAppIds[1], 0);
    verify(taskManager, never()).removeShuffleDataAsync(tooLongAppIds[1] + MERGE_APP_SUFFIX, 0);
    verify(taskManager).removeShuffleDataAsync(fittingAppIds[0]);
    verify(taskManager).removeShuffleDataAsync(fittingAppIds[0] + MERGE_APP_SUFFIX);
    verify(taskManager).removeShuffleDataAsync(fittingAppIds[1], 0);
    verify(taskManager).removeShuffleDataAsync(fittingAppIds[1] + MERGE_APP_SUFFIX, 0);
  }

  @Test
  public void testSendShuffleDataRejectsMalformedAppId() throws Exception {
    useLocalStorage();
    ShuffleTaskManager taskManager = shuffleServer.getShuffleTaskManager();
    long requireBufferId = taskManager.requireBuffer(16);
    for (String appId : MALFORMED_APP_IDS) {
      assertEquals(
          StatusCode.INVALID_REQUEST,
          sendShuffleData(appId, requireBufferId),
          "sendShuffleData appId[" + appId + "]");
    }
    // the reply is a fixed text, the malformed appId is not echoed back
    ResponseRecorder<SendShuffleDataResponse> recorder = new ResponseRecorder<>();
    service.sendShuffleData(sendShuffleDataRequest("app\n", requireBufferId), recorder);
    assertEquals("appId is blank or contains illegal characters", recorder.response.getRetMsg());
    // a valid appId that was not registered is answered as before
    assertEquals(StatusCode.APP_NOT_FOUND, sendShuffleData(VALID_APP_ID, requireBufferId));
    // nothing has been taken for the refused requests, the pre-allocation is still there
    assertEquals(16, taskManager.getRequireBufferSize(requireBufferId));
  }

  @Test
  public void testRegisterShuffleRejectsMalformedAppId() throws Exception {
    useLocalStorage();
    for (String appId : MALFORMED_APP_IDS) {
      assertEquals(StatusCode.INVALID_REQUEST, registerShuffle(appId), "appId[" + appId + "]");
      // nothing is registered for it, so no storage path will be built from it afterwards
      assertFalse(shuffleServer.getShuffleTaskManager().getAppIds().contains(appId));
    }
  }

  @Test
  public void testUnregisterShuffleByAppIdRejectsMalformedAppId() throws Exception {
    useLocalStorage();
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
  public void testHeartbeatRejectsMalformedAppId() throws Exception {
    useLocalStorage();
    for (String appId : MALFORMED_APP_IDS) {
      assertEquals(StatusCode.INVALID_REQUEST, appHeartbeat(appId), "appId[" + appId + "]");
      // an app registered by the heartbeat would be purged once it expires
      assertFalse(shuffleServer.getShuffleTaskManager().getAppIds().contains(appId));
    }
  }

  @Test
  public void testOtherRequestsRejectMalformedAppId() throws Exception {
    useLocalStorage();
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
  public void testRequireBufferKeepsAcceptingEmptyAppId() throws Exception {
    useLocalStorage();
    // an empty appId means the client is of an old version
    ResponseRecorder<RequireBufferResponse> recorder = new ResponseRecorder<>();
    service.requireBuffer(RequireBufferRequest.newBuilder().setRequireSize(16).build(), recorder);

    assertNull(recorder.error);
    assertNotNull(recorder.response);
    assertEquals(StatusCode.SUCCESS, StatusCode.fromProto(recorder.response.getStatus()));
    assertTrue(recorder.response.getRequireBufferId() >= 0);
  }

  @Test
  public void testRequireBufferRejectsMalformedAppId() throws Exception {
    useLocalStorage();
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
  public void testValidAppIdIsAccepted() throws Exception {
    useLocalStorage();
    assertEquals(StatusCode.SUCCESS, registerShuffle(VALID_APP_ID));
    assertTrue(shuffleServer.getShuffleTaskManager().getAppIds().contains(VALID_APP_ID));
    assertEquals(StatusCode.SUCCESS, appHeartbeat(VALID_APP_ID));
    // The remote merge is disabled on this server, so its manager doesn't exist and the
    // heartbeat would fail if it still tried to refresh the app of the merged blocks: the
    // SUCCESS above is the check, the app of the merged blocks doesn't exist either way.
    assertFalse(
        shuffleServer
            .getShuffleTaskManager()
            .getAppIds()
            .contains(VALID_APP_ID + MERGE_APP_SUFFIX));

    assertEquals(
        StatusCode.SUCCESS,
        call(
            o ->
                service.unregisterShuffleByAppId(
                    ShuffleUnregisterByAppIdRequest.newBuilder().setAppId(VALID_APP_ID).build(), o),
            ShuffleUnregisterByAppIdResponse::getStatus));
    assertFalse(
        shuffleServer
            .getShuffleTaskManager()
            .getAppIds()
            .contains(VALID_APP_ID + MERGE_APP_SUFFIX));
  }

  private StatusCode registerShuffle(String appId) {
    return registerShuffle(appId, "");
  }

  private StatusCode registerShuffle(String appId, String remoteStorage) {
    return registerShuffle(registerRequest(appId, remoteStorage));
  }

  private StatusCode registerShuffle(ShuffleRegisterRequest request) {
    return call(o -> service.registerShuffle(request, o), ShuffleRegisterResponse::getStatus);
  }

  private StatusCode sendShuffleData(String appId, long requireBufferId) {
    return call(
        o -> service.sendShuffleData(sendShuffleDataRequest(appId, requireBufferId), o),
        SendShuffleDataResponse::getStatus);
  }

  private static SendShuffleDataRequest sendShuffleDataRequest(String appId, long requireBufferId) {
    return SendShuffleDataRequest.newBuilder()
        .setAppId(appId)
        .setShuffleId(0)
        .setRequireBufferId(requireBufferId)
        .build();
  }

  private static ShuffleRegisterRequest registerRequest(String appId, String remoteStorage) {
    return registerRequest(appId, remoteStorage, Collections.emptyMap());
  }

  private static ShuffleRegisterRequest registerRequest(
      String appId, String remoteStorage, Map<String, String> remoteStorageConf) {
    RssProtos.RemoteStorage.Builder remoteStorageBuilder =
        RssProtos.RemoteStorage.newBuilder().setPath(remoteStorage);
    remoteStorageConf.forEach(
        (key, value) ->
            remoteStorageBuilder.addRemoteStorageConf(
                RssProtos.RemoteStorageConfItem.newBuilder().setKey(key).setValue(value)));
    return ShuffleRegisterRequest.newBuilder()
        .setAppId(appId)
        .setShuffleId(0)
        .addPartitionRanges(ShufflePartitionRange.newBuilder().setStart(0).setEnd(1))
        .setRemoteStorage(remoteStorageBuilder)
        .build();
  }

  /** A request with an empty merge context, which carries nothing the server could use. */
  private static ShuffleRegisterRequest registerRequestWithMerge(
      String appId, String remoteStorage) {
    return registerRequestWithMergeContext(
        appId, remoteStorage, RssProtos.MergeContext.newBuilder());
  }

  /** A request with a merge context the server can work with: the key and the value are strings. */
  private static ShuffleRegisterRequest registerRequestWithUsableMerge(
      String appId, String remoteStorage) {
    return registerRequestWithMergeContext(
        appId,
        remoteStorage,
        RssProtos.MergeContext.newBuilder()
            .setKeyClass(String.class.getName())
            .setValueClass(String.class.getName())
            .setMergedBlockSize(1024)
            .setMergeClassLoader(""));
  }

  private static ShuffleRegisterRequest registerRequestWithMergeContext(
      String appId, String remoteStorage, RssProtos.MergeContext.Builder mergeContext) {
    return ShuffleRegisterRequest.newBuilder()
        .mergeFrom(registerRequest(appId, remoteStorage))
        .setMergeContext(mergeContext)
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
