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

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.netty.client.TransportClient;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleIndexRequest;
import org.apache.uniffle.common.netty.protocol.GetMemoryShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.GetSortedShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.RpcResponse;
import org.apache.uniffle.common.netty.protocol.SendShuffleDataRequestV1;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.server.ShuffleTaskManager;
import org.apache.uniffle.server.buffer.ShuffleBufferManager;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** The class is to test the appId handling of {@link ShuffleServerNettyHandler} */
public class ShuffleServerNettyHandlerTest {

  // None of them is a plain directory name, while all of them used to be accepted as an appId.
  private static final List<String> MALFORMED_APP_IDS =
      Arrays.asList(
          "", " ", ".", "..", "../outside", "a/../../outside", "/tmp", "a\\b", "app\n", "..\\..");

  @TempDir File tempDir;

  private ShuffleServer shuffleServer;
  private ShuffleServerNettyHandler handler;
  private TransportClient client;
  private Channel channel;

  @BeforeEach
  public void beforeEach() throws Exception {
    ShuffleServerMetrics.register();

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
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE.name());
    conf.set(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    conf.set(ShuffleServerConf.SERVER_MERGE_ENABLE, false);
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_BASE_PATH.key(),
        new File(tempDir, "storage").getAbsolutePath());

    shuffleServer = new ShuffleServer(conf);
    handler = new ShuffleServerNettyHandler(shuffleServer);

    client = mock(TransportClient.class);
    channel = mock(Channel.class);
    when(client.getChannel()).thenReturn(channel);
    when(client.getSocketAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 40000));
  }

  @AfterEach
  public void afterEach() throws Exception {
    if (shuffleServer != null) {
      shuffleServer.stopServer();
      shuffleServer = null;
    }
    ShuffleServerMetrics.clear();
  }

  @Test
  public void testReadRequestsRejectMalformedAppId() {
    for (String appId : MALFORMED_APP_IDS) {
      handler.handleGetMemoryShuffleDataRequest(
          client, new GetMemoryShuffleDataRequest(1L, appId, 0, 0, -1L, 1024, 0L, null));
      handler.handleGetLocalShuffleIndexRequest(
          client, new GetLocalShuffleIndexRequest(1L, appId, 0, 0, 1, 1));
      handler.handleGetLocalShuffleData(
          client, new GetLocalShuffleDataRequest(1L, appId, 0, 0, 1, 1, 0L, 1, 0L));
    }

    assertAllResponsesAre(StatusCode.INVALID_REQUEST, MALFORMED_APP_IDS.size() * 3);
  }

  @Test
  public void testGetSortedShuffleDataRejectsMalformedAppId() {
    for (String appId : MALFORMED_APP_IDS) {
      handler.handleGetSortedShuffleDataRequest(
          client, new GetSortedShuffleDataRequest(1L, appId, 0, 0, 0L, 0, 0L));
    }

    assertAllResponsesAre(StatusCode.INVALID_REQUEST, MALFORMED_APP_IDS.size());
  }

  @Test
  public void testSendShuffleDataRejectsMalformedAppId() {
    ShuffleTaskManager taskManager = shuffleServer.getShuffleTaskManager();
    ShuffleBufferManager bufferManager = shuffleServer.getShuffleBufferManager();
    // A second pre-allocation is kept for the whole test. The counters of the memory stop at
    // zero, so without it a release of too much memory would look the same as the right one.
    long keptRequireBufferId = taskManager.requireBuffer(64);
    for (String appId : MALFORMED_APP_IDS) {
      // every request carries a pre-allocated buffer and a block, both have to be released
      long requireBufferId = taskManager.requireBuffer(16);
      // the memory of the pre-allocation is counted as used and as pre-allocated
      final long preAllocatedSize = bufferManager.getPreAllocatedSize();
      final long usedMemory = bufferManager.getUsedMemory();
      ByteBuf data = Unpooled.buffer(32).writeZero(32);
      SendShuffleDataRequestV1 req = mock(SendShuffleDataRequestV1.class);
      when(req.getRequestId()).thenReturn(1L);
      when(req.getAppId()).thenReturn(appId);
      when(req.getRequireId()).thenReturn(requireBufferId);
      when(req.getPartitionToBlocks())
          .thenReturn(
              Collections.singletonMap(
                  0,
                  Collections.singletonList(new ShufflePartitionedBlock(32, 32, 0, 1, 1, data))));

      handler.handleSendShuffleDataRequest(client, req);

      ArgumentCaptor<Object> responses = ArgumentCaptor.forClass(Object.class);
      verify(channel).writeAndFlush(responses.capture());
      assertEquals(
          StatusCode.INVALID_REQUEST,
          ((RpcResponse) responses.getValue()).getStatusCode(),
          "appId[" + appId + "]");
      // the reply is a fixed text, the malformed appId is not echoed back
      assertEquals(
          "appId is blank or contains illegal characters",
          ((RpcResponse) responses.getValue()).getRetMessage());
      // the pre-allocated buffer is taken and its memory is given back, the block is released
      // too, and nothing of the request is kept
      assertEquals(0, taskManager.getRequireBufferSize(requireBufferId), "appId[" + appId + "]");
      assertEquals(
          preAllocatedSize - 16,
          bufferManager.getPreAllocatedSize(),
          "pre-allocated memory, appId[" + appId + "]");
      assertEquals(
          usedMemory - 16, bufferManager.getUsedMemory(), "used memory, appId[" + appId + "]");
      assertEquals(0, data.refCnt(), "block buffer, appId[" + appId + "]");
      assertEquals(64, taskManager.getRequireBufferSize(keptRequireBufferId));
      Mockito.clearInvocations(channel);
    }
  }

  @Test
  public void testValidAppIdPassesTheCheck() {
    String appId = "application_1699999999999_0001";
    handler.handleGetSortedShuffleDataRequest(
        client, new GetSortedShuffleDataRequest(1L, appId, 0, 0, 0L, 0, 0L));

    // the request goes on and fails for another reason, the remote merge is disabled here
    ArgumentCaptor<Object> responses = ArgumentCaptor.forClass(Object.class);
    verify(channel).writeAndFlush(responses.capture());
    RpcResponse response = (RpcResponse) responses.getValue();
    assertEquals(StatusCode.INTERNAL_ERROR, response.getStatusCode());
    assertEquals("Remote merge is disabled", response.getRetMessage());
  }

  private void assertAllResponsesAre(StatusCode expected, int expectedResponseNum) {
    ArgumentCaptor<Object> responses = ArgumentCaptor.forClass(Object.class);
    verify(channel, times(expectedResponseNum)).writeAndFlush(responses.capture());
    for (Object response : responses.getAllValues()) {
      assertEquals(expected, ((RpcResponse) response).getStatusCode());
    }
  }
}
