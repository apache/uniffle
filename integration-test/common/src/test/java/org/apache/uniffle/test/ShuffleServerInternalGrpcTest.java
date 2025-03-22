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

package org.apache.uniffle.test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import io.grpc.StatusRuntimeException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.impl.grpc.ShuffleServerInternalGrpcClient;
import org.apache.uniffle.client.request.RssCancelDecommissionRequest;
import org.apache.uniffle.client.request.RssDecommissionRequest;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssUnregisterShuffleRequest;
import org.apache.uniffle.client.response.RssCancelDecommissionResponse;
import org.apache.uniffle.client.response.RssDecommissionResponse;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ServerStatus;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ShuffleServerInternalGrpcTest extends IntegrationTestBase {

  private ShuffleServerGrpcClient shuffleServerClient;
  private ShuffleServerInternalGrpcClient shuffleServerInternalClient;

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    coordinatorConf.setLong(CoordinatorConf.COORDINATOR_APP_EXPIRED, 2000);
    storeCoordinatorConf(coordinatorConf);

    ShuffleServerConf shuffleServerConf = shuffleServerConfWithoutPort(0, tmpDir, ServerType.GRPC);
    shuffleServerConf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 5000L);
    shuffleServerConf.set(ShuffleServerConf.SERVER_DECOMMISSION_CHECK_INTERVAL, 500L);
    storeShuffleServerConf(shuffleServerConf);

    startServersWithRandomPorts();
  }

  @BeforeEach
  public void createClient() {
    shuffleServerClient =
        new ShuffleServerGrpcClient(LOCALHOST, grpcShuffleServers.get(0).getGrpcPort());
    shuffleServerInternalClient =
        new ShuffleServerInternalGrpcClient(LOCALHOST, grpcShuffleServers.get(0).getGrpcPort());
  }

  @AfterEach
  public void closeClient() {
    shuffleServerClient.close();
    shuffleServerInternalClient.close();
  }

  @Test
  public void decommissionTest() {
    String appId = "decommissionTest";
    int shuffleId = 0;
    shuffleServerClient.registerShuffle(
        new RssRegisterShuffleRequest(
            appId, shuffleId, Lists.newArrayList(new PartitionRange(0, 1)), ""));

    ShuffleServer shuffleServer = grpcShuffleServers.get(0);
    RssDecommissionResponse response =
        shuffleServerInternalClient.decommission(new RssDecommissionRequest());
    assertEquals(StatusCode.SUCCESS, response.getStatusCode());
    assertEquals(ServerStatus.DECOMMISSIONING, shuffleServer.getServerStatus());
    RssCancelDecommissionResponse cancelResponse =
        shuffleServerInternalClient.cancelDecommission(new RssCancelDecommissionRequest());
    assertEquals(StatusCode.SUCCESS, cancelResponse.getStatusCode());
    assertEquals(ServerStatus.ACTIVE, shuffleServer.getServerStatus());

    // Clean all apps, shuffle server will be shutdown right now.
    shuffleServerClient.unregisterShuffle(new RssUnregisterShuffleRequest(appId, shuffleId, 1));
    response = shuffleServerInternalClient.decommission(new RssDecommissionRequest());
    assertEquals(StatusCode.SUCCESS, response.getStatusCode());
    assertEquals(ServerStatus.DECOMMISSIONING, shuffleServer.getServerStatus());
    Awaitility.await().timeout(10, TimeUnit.SECONDS).until(() -> !shuffleServer.isRunning());
    // Server is already shutdown, so io exception should be thrown here.
    assertThrows(
        StatusRuntimeException.class,
        () -> shuffleServerInternalClient.cancelDecommission(new RssCancelDecommissionRequest()));
  }
}
