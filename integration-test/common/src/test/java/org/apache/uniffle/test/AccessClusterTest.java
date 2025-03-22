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
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.client.request.RssAccessClusterRequest;
import org.apache.uniffle.client.response.RssAccessClusterResponse;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.coordinator.AccessManager;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.access.AccessCheckResult;
import org.apache.uniffle.coordinator.access.AccessInfo;
import org.apache.uniffle.coordinator.access.checker.AccessChecker;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AccessClusterTest extends CoordinatorTestBase {

  public static class MockedAccessChecker implements AccessChecker {
    final String key = "key";
    final List<String> legalNames = Arrays.asList("v1", "v2", "v3");

    public MockedAccessChecker(AccessManager accessManager) throws Exception {
      // ignore
    }

    @Override
    public AccessCheckResult check(AccessInfo accessInfo) {
      Map<String, String> reservedData = accessInfo.getExtraProperties();
      if (legalNames.contains(reservedData.get(key))) {
        return new AccessCheckResult(true, "");
      }
      return new AccessCheckResult(false, "");
    }

    @Override
    public void refreshAccessChecker() {
      // ignore.
    }

    @Override
    public void close() throws IOException {
      // ignore.
    }
  }

  @AfterEach
  public void afterEach() throws Exception {
    shutdownServers();
    CoordinatorMetrics.clear();
  }

  @Test
  public void testUsingCustomExtraProperties() throws Exception {
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    coordinatorConf.setString(
        "rss.coordinator.access.checkers",
        "org.apache.uniffle.test.AccessClusterTest$MockedAccessChecker");
    storeCoordinatorConf(coordinatorConf);
    startServersWithRandomPorts();
    Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
    // case1: empty map
    String accessID = "acessid";
    RssAccessClusterRequest request =
        new RssAccessClusterRequest(
            accessID, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION), 2000, "user");
    createClient();
    RssAccessClusterResponse response = coordinatorClient.accessCluster(request);
    assertEquals(StatusCode.ACCESS_DENIED, response.getStatusCode());

    // case2: illegal names
    Map<String, String> extraProperties = new HashMap<>();
    extraProperties.put("key", "illegalName");
    request =
        new RssAccessClusterRequest(
            accessID,
            Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION),
            2000,
            extraProperties,
            "user");
    response = coordinatorClient.accessCluster(request);
    assertEquals(StatusCode.ACCESS_DENIED, response.getStatusCode());

    // case3: legal names
    extraProperties.clear();
    extraProperties.put("key", "v1");
    request =
        new RssAccessClusterRequest(
            accessID,
            Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION),
            2000,
            extraProperties,
            "user");
    response = coordinatorClient.accessCluster(request);
    assertEquals(StatusCode.SUCCESS, response.getStatusCode());
  }

  @Test
  public void test(@TempDir File tempDir) throws Exception {
    File cfgFile = File.createTempFile("tmp", ".conf", tempDir);
    FileWriter fileWriter = new FileWriter(cfgFile);
    PrintWriter printWriter = new PrintWriter(fileWriter);
    printWriter.println("9527");
    printWriter.println(" 135 ");
    printWriter.println("2 ");
    printWriter.flush();
    printWriter.close();

    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    coordinatorConf.setInteger("rss.coordinator.access.loadChecker.serverNum.threshold", 2);
    coordinatorConf.setString("rss.coordinator.access.candidates.path", cfgFile.getAbsolutePath());
    coordinatorConf.setString(
        "rss.coordinator.access.checkers",
        "org.apache.uniffle.coordinator.access.checker.AccessCandidatesChecker,"
            + "org.apache.uniffle.coordinator.access.checker.AccessClusterLoadChecker");
    storeCoordinatorConf(coordinatorConf);

    storeShuffleServerConf(shuffleServerConfWithoutPort(0, tempDir, ServerType.GRPC));
    startServersWithRandomPorts();
    Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
    String accessId = "111111";
    RssAccessClusterRequest request =
        new RssAccessClusterRequest(
            accessId, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION), 2000, "user");
    createClient();
    RssAccessClusterResponse response = coordinatorClient.accessCluster(request);
    assertEquals(StatusCode.ACCESS_DENIED, response.getStatusCode());
    assertTrue(response.getMessage().startsWith("Denied by AccessCandidatesChecker"));

    accessId = "135";
    request =
        new RssAccessClusterRequest(
            accessId, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION), 2000, "user");
    response = coordinatorClient.accessCluster(request);
    assertEquals(StatusCode.ACCESS_DENIED, response.getStatusCode());
    assertTrue(response.getMessage().startsWith("Denied by AccessClusterLoadChecker"));
    ShuffleServerConf shuffleServerConf = shuffleServerConfWithoutPort(0, tempDir, ServerType.GRPC);
    shuffleServerConf.setString("rss.coordinator.quorum", getQuorum());
    ShuffleServer shuffleServer = new ShuffleServer(shuffleServerConf);
    shuffleServer.start();
    // this make sure the server can be shutdown
    grpcShuffleServers.add(shuffleServer);
    Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

    request =
        new RssAccessClusterRequest(
            accessId, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION), 2000, "user");
    response = coordinatorClient.accessCluster(request);
    assertEquals(StatusCode.SUCCESS, response.getStatusCode());
    assertTrue(response.getMessage().startsWith("SUCCESS"));
  }
}
