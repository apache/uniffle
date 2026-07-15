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

import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
    return shuffleServerConf;
  }

  @Test
  public void startStreamServerOnRandomPort(@TempDir File tmpDir) throws Exception {
    CoordinatorTestUtils.waitForRegister(coordinatorClient, 4);
    Thread.sleep(5000);
    int firstPort = nettyShuffleServers.get(0).getNettyPort();
    int actualPort = nettyShuffleServers.get(1).getNettyPort();
    assertTrue(firstPort > 0);
    assertTrue(actualPort > 0);
    assertNotEquals(firstPort, actualPort);

    ShuffleServerConf shuffleServerConf =
        shuffleServerConfWithoutPort(0, tmpDir, ServerType.GRPC_NETTY);
    // Start a Netty server on an already-bound port and verify that it falls back to an
    // OS-assigned port.
    shuffleServerConf.setInteger("rss.server.netty.port", actualPort);
    shuffleServerConf.setString("rss.coordinator.quorum", getQuorum());
    ShuffleServer ss = new ShuffleServer(shuffleServerConf);
    try {
      ss.start();
      assertTrue(ss.getNettyPort() > 0);
      assertNotEquals(actualPort, ss.getNettyPort());
    } finally {
      ss.stopServer();
    }
  }

  @Test
  public void startGrpcServerOnRandomPort(@TempDir File tmpDir) throws Exception {
    CoordinatorTestUtils.waitForRegister(coordinatorClient, 4);
    Thread.sleep(5000);
    int firstPort = grpcShuffleServers.get(0).getGrpcPort();
    int actualPort = grpcShuffleServers.get(1).getGrpcPort();
    assertTrue(firstPort > 0);
    assertTrue(actualPort > 0);
    assertNotEquals(firstPort, actualPort);

    ShuffleServerConf shuffleServerConf =
        shuffleServerConfWithoutPort(0, tmpDir, ServerType.GRPC_NETTY);
    // Start a gRPC server on an already-bound port and verify that it falls back to an
    // OS-assigned port.
    shuffleServerConf.setInteger("rss.rpc.server.port", actualPort);
    shuffleServerConf.setString("rss.coordinator.quorum", getQuorum());
    ShuffleServer ss = new ShuffleServer(shuffleServerConf);
    try {
      ss.start();
      assertTrue(ss.getGrpcPort() > 0);
      assertNotEquals(actualPort, ss.getGrpcPort());
    } finally {
      ss.stopServer();
    }
  }
}
