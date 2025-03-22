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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.handler.impl.MemoryClientReadHandler;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ShuffleServerWithLocalOfExceptionTest extends ShuffleReadWriteBase {

  private ShuffleServerGrpcClient shuffleServerClient;

  private static int rpcPort;

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    storeCoordinatorConf(coordinatorConf);

    ShuffleServerConf shuffleServerConf = shuffleServerConfWithoutPort(0, tmpDir, ServerType.GRPC);
    shuffleServerConf.setString("rss.server.app.expired.withoutHeartbeat", "5000");
    storeShuffleServerConf(shuffleServerConf);

    startServersWithRandomPorts();
  }

  @BeforeEach
  public void createClient() {
    shuffleServerClient = new ShuffleServerGrpcClient(LOCALHOST, rpcPort);
  }

  @AfterEach
  public void closeClient() {
    shuffleServerClient.close();
  }

  @Test
  public void testReadWhenConnectionFailedShouldThrowException() throws Exception {
    String testAppId = "testReadWhenException";
    int shuffleId = 0;
    int partitionId = 0;

    MemoryClientReadHandler memoryClientReadHandler =
        new MemoryClientReadHandler(
            testAppId,
            shuffleId,
            partitionId,
            150,
            shuffleServerClient,
            Roaring64NavigableMap.bitmapOf());
    grpcShuffleServers.get(0).stopServer();
    try {
      memoryClientReadHandler.readShuffleData();
      fail("Should throw connection exception directly.");
    } catch (RssException rssException) {
      assertTrue(rssException.getMessage().contains("Failed to read in memory shuffle data with"));
    }
  }
}
