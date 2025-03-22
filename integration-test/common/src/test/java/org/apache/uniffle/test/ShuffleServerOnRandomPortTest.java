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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShuffleServerOnRandomPortTest extends CoordinatorTestBase {
  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    coordinatorConf.set(RssBaseConf.RPC_METRICS_ENABLED, true);
    coordinatorConf.setString(CoordinatorConf.COORDINATOR_ASSIGNMENT_STRATEGY.key(), "BASIC");
    coordinatorConf.setLong("rss.coordinator.app.expired", 2000);
    coordinatorConf.setLong("rss.coordinator.server.heartbeat.timeout", 3000);
    storeCoordinatorConf(coordinatorConf);

    storeShuffleServerConf(buildShuffleServerConf(0, tmpDir, ServerType.GRPC));
    storeShuffleServerConf(buildShuffleServerConf(1, tmpDir, ServerType.GRPC));
    storeShuffleServerConf(buildShuffleServerConf(2, tmpDir, ServerType.GRPC_NETTY));
    storeShuffleServerConf(buildShuffleServerConf(3, tmpDir, ServerType.GRPC_NETTY));

    startServersWithRandomPorts();
  }

  private static ShuffleServerConf buildShuffleServerConf(
      int subDirIndex, File tempDir, ServerType serverType) {
    ShuffleServerConf shuffleServerConf =
        shuffleServerConfWithoutPort(subDirIndex, tempDir, serverType);
    shuffleServerConf.setInteger("rss.server.netty.port", 0);
    shuffleServerConf.setInteger("rss.rpc.server.port", 0);
    shuffleServerConf.setInteger("rss.random.port.min", 30000);
    shuffleServerConf.setInteger("rss.random.port.max", 40000);
    return shuffleServerConf;
  }

  @Test
  public void startStreamServerOnRandomPort(@TempDir File tmpDir) throws Exception {
    CoordinatorTestUtils.waitForRegister(coordinatorClient, 4);
    Thread.sleep(5000);
    int actualPort = nettyShuffleServers.get(0).getNettyPort();
    assertTrue(actualPort >= 30000 && actualPort <= 40000);
    actualPort = nettyShuffleServers.get(1).getNettyPort();
    assertTrue(actualPort >= 30000 && actualPort <= 40000);

    int maxRetries = 100;
    ShuffleServerConf shuffleServerConf =
        shuffleServerConfWithoutPort(0, tmpDir, ServerType.GRPC_NETTY);
    // start netty server with already bind port
    shuffleServerConf.setInteger("rss.server.netty.port", actualPort);
    shuffleServerConf.setInteger("rss.port.max.retry", maxRetries);
    shuffleServerConf.setString("rss.coordinator.quorum", getQuorum());
    ShuffleServer ss = new ShuffleServer(shuffleServerConf);
    ss.start();
    assertTrue(ss.getNettyPort() > actualPort && actualPort <= actualPort + maxRetries);
    ss.stopServer();
  }

  @Test
  public void startGrpcServerOnRandomPort(@TempDir File tmpDir) throws Exception {
    CoordinatorTestUtils.waitForRegister(coordinatorClient, 4);
    Thread.sleep(5000);
    int actualPort = grpcShuffleServers.get(0).getGrpcPort();
    assertTrue(actualPort >= 30000 && actualPort <= 40000);
    actualPort = grpcShuffleServers.get(1).getGrpcPort();
    assertTrue(actualPort >= 30000 && actualPort <= 40000);

    int maxRetries = 100;
    ShuffleServerConf shuffleServerConf =
        shuffleServerConfWithoutPort(0, tmpDir, ServerType.GRPC_NETTY);
    // start grpc server with already bind port
    shuffleServerConf.setInteger("rss.rpc.server.port", actualPort);
    shuffleServerConf.setInteger("rss.port.max.retry", maxRetries);
    shuffleServerConf.setString("rss.coordinator.quorum", getQuorum());
    ShuffleServer ss = new ShuffleServer(shuffleServerConf);
    ss.start();
    assertTrue(ss.getGrpcPort() > actualPort && actualPort <= actualPort + maxRetries);
    ss.stopServer();
  }
}
