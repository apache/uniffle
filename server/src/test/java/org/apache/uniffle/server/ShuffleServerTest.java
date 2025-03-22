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
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ServerStatus;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.util.ExitUtils;
import org.apache.uniffle.common.util.ExitUtils.ExitException;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.server.ShuffleServerConf.SERVER_DECOMMISSION_CHECK_INTERVAL;
import static org.apache.uniffle.server.ShuffleServerConf.SERVER_DECOMMISSION_SHUTDOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ShuffleServerTest {

  @TempDir private File tempDir;

  @Test
  public void startTest() throws Exception {
    ShuffleServerConf serverConf = createShuffleServerConf(ServerType.GRPC);
    ShuffleServer ss1 = new ShuffleServer(serverConf);
    ss1.start();
    ExitUtils.disableSystemExit();
    ShuffleServer ss2 = new ShuffleServer(serverConf);
    String expectMessage = "Fail to start jetty http server";
    final int expectStatus = 1;
    try {
      ss2.start();
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith(expectMessage));
      assertEquals(expectStatus, ((ExitException) e).getStatus());
    }

    serverConf.setInteger("rss.jetty.http.port", 0);
    ss2 = new ShuffleServer(serverConf);
    expectMessage = "Fail to start grpc server";
    try {
      ss2.start();
    } catch (Exception e) {
      assertEquals(expectMessage, e.getMessage());
      assertEquals(expectStatus, ((ExitException) e).getStatus());
    }
    ss1.stopServer();

    final Thread t =
        new Thread(
            null,
            () -> {
              throw new AssertionError("TestUncaughtException");
            },
            "testThread");
    t.start();
    t.join();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void decommissionTest(boolean shutdown) throws Exception {
    ShuffleServerConf serverConf = createShuffleServerConf(ServerType.GRPC);
    serverConf.set(SERVER_DECOMMISSION_CHECK_INTERVAL, 1000L);
    serverConf.set(SERVER_DECOMMISSION_SHUTDOWN, shutdown);
    serverConf.set(ShuffleServerConf.RPC_SERVER_PORT, 19527);
    serverConf.set(ShuffleServerConf.JETTY_HTTP_PORT, 19528);
    ShuffleServer shuffleServer = new ShuffleServer(serverConf);
    shuffleServer.start();
    assertEquals(ServerStatus.ACTIVE, shuffleServer.getServerStatus());
    // Shuffle server is not decommissioning, but we can also cancel it.
    shuffleServer.cancelDecommission();
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    String appId = "decommissionTest_appId_" + shutdown;
    shuffleTaskManager.registerShuffle(
        appId, 0, Lists.newArrayList(), new RemoteStorageInfo("/tmp"), "");
    shuffleServer.decommission();
    assertEquals(ServerStatus.DECOMMISSIONING, shuffleServer.getServerStatus());
    // Shuffle server is decommissioning, but we can also decommission it again.
    shuffleServer.decommission();
    shuffleServer.cancelDecommission();
    shuffleTaskManager.removeResources(appId, false);
    // Wait for 2 seconds, make sure cancel command is work.
    Thread.sleep(2000);
    assertEquals(ServerStatus.ACTIVE, shuffleServer.getServerStatus());
    shuffleServer.decommission();
    if (shutdown) {
      Awaitility.await().timeout(10, TimeUnit.SECONDS).until(() -> !shuffleServer.isRunning());
    } else {
      Awaitility.await()
          .timeout(10, TimeUnit.SECONDS)
          .until(() -> ServerStatus.DECOMMISSIONED.equals(shuffleServer.getServerStatus()));
      assertEquals(true, shuffleServer.isRunning());
      shuffleServer.stopServer();
    }
  }

  private ShuffleServerConf createShuffleServerConf(ServerType serverType) throws Exception {
    ShuffleServerConf serverConf = new ShuffleServerConf();
    serverConf.setInteger(ShuffleServerConf.RPC_SERVER_PORT, 9527);
    serverConf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE.name());
    serverConf.setBoolean(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    serverConf.setInteger(ShuffleServerConf.JETTY_HTTP_PORT, 9528);
    serverConf.setString(ShuffleServerConf.RSS_COORDINATOR_QUORUM, "localhost:0");
    serverConf.set(
        ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(tempDir.getAbsolutePath()));
    serverConf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L * 1024L * 1024L);
    serverConf.setLong(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 100);
    serverConf.setLong(ShuffleServerConf.SERVER_READ_BUFFER_CAPACITY, 10);
    serverConf.set(ShuffleServerConf.RPC_SERVER_TYPE, serverType);
    return serverConf;
  }

  @Test
  public void nettyServerTest() throws Exception {
    ShuffleServerConf serverConf = createShuffleServerConf(ServerType.GRPC_NETTY);
    serverConf.set(ShuffleServerConf.NETTY_SERVER_PORT, 29999);
    ShuffleServer ss1 = new ShuffleServer(serverConf);
    ss1.start();
    ExitUtils.disableSystemExit();
    serverConf.set(ShuffleServerConf.RPC_SERVER_PORT, 19997);
    serverConf.set(ShuffleServerConf.JETTY_HTTP_PORT, 19996);
    serverConf.set(ShuffleServerConf.SERVER_PORT_MAX_RETRIES, 1);
    ShuffleServer ss2 = new ShuffleServer(serverConf);
    String expectMessage = "Fail to start stream server";
    final int expectStatus = 1;
    try {
      ss2.start();
    } catch (Exception e) {
      assertEquals(expectMessage, e.getMessage());
      assertEquals(expectStatus, ((ExitException) e).getStatus());
      ss1.stopServer();
      return;
    }
    fail();
  }
}
