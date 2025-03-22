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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ServerStatus;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.metrics.TestUtils;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.web.resource.Response;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.CoordinatorServer;
import org.apache.uniffle.coordinator.ServerNode;
import org.apache.uniffle.coordinator.SimpleClusterManager;
import org.apache.uniffle.coordinator.web.request.CancelDecommissionRequest;
import org.apache.uniffle.coordinator.web.request.DecommissionRequest;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ServletTest extends IntegrationTestBase {
  private static int coordinatorHttpPort;
  private static final String URL_PREFIX = "http://127.0.0.1:%s/api/";
  private static final String SINGLE_NODE_URL = URL_PREFIX + "server/nodes/%s";
  private static final String NODES_URL = URL_PREFIX + "server/nodes";
  private static final String LOSTNODES_URL = URL_PREFIX + "server/nodes?status=LOST";
  private static final String UNHEALTHYNODES_URL = URL_PREFIX + "server/nodes?status=UNHEALTHY";
  private static final String DECOMMISSIONEDNODES_URL =
      URL_PREFIX + "server/nodes?status=DECOMMISSIONED";
  private static final String DECOMMISSION_URL = URL_PREFIX + "server/decommission";
  private static final String CANCEL_DECOMMISSION_URL = URL_PREFIX + "server/cancelDecommission";
  private static final String DECOMMISSION_SINGLENODE_URL = URL_PREFIX + "server/%s/decommission";
  private static final String CANCEL_DECOMMISSION_SINGLENODE_URL =
      URL_PREFIX + "server/%s/cancelDecommission";
  private static final String AUTHORIZATION_CREDENTIALS = "dW5pZmZsZTp1bmlmZmxlMTIz";
  private static final Map<String, String> authorizationHeader =
      ImmutableMap.of("Authorization", "Basic " + AUTHORIZATION_CREDENTIALS);
  private static CoordinatorServer coordinatorServer;
  private ObjectMapper objectMapper = new ObjectMapper();

  private static void prepareShuffleServerConf(int subDirIndex, File tmpDir) throws Exception {
    ShuffleServerConf shuffleServerConf =
        shuffleServerConfWithoutPort(subDirIndex, tmpDir, ServerType.GRPC);
    shuffleServerConf.set(ShuffleServerConf.SERVER_DECOMMISSION_SHUTDOWN, false);
    storeShuffleServerConf(shuffleServerConf);
  }

  @BeforeAll
  public static void setUp(@TempDir File tmpDir) throws Exception {

    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    coordinatorConf.set(RssBaseConf.JETTY_CORE_POOL_SIZE, 128);
    coordinatorConf.set(RssBaseConf.REST_AUTHORIZATION_CREDENTIALS, AUTHORIZATION_CREDENTIALS);
    storeCoordinatorConf(coordinatorConf);

    prepareShuffleServerConf(0, tmpDir);
    prepareShuffleServerConf(1, tmpDir);
    prepareShuffleServerConf(2, tmpDir);
    prepareShuffleServerConf(3, tmpDir);

    startServersWithRandomPorts();
    coordinatorServer = coordinators.get(0);
    coordinatorHttpPort = coordinatorServer.getJettyPort();
    Awaitility.await()
        .timeout(30, TimeUnit.SECONDS)
        .until(() -> coordinatorServer.getClusterManager().list().size() == 4);
  }

  @Test
  public void testGetSingleNode() throws Exception {
    ShuffleServer shuffleServer = grpcShuffleServers.get(0);
    String content =
        TestUtils.httpGet(
            String.format(SINGLE_NODE_URL, coordinatorHttpPort, shuffleServer.getId()));
    Response<HashMap<String, Object>> response =
        objectMapper.readValue(content, new TypeReference<Response<HashMap<String, Object>>>() {});
    HashMap<String, Object> server = response.getData();
    assertEquals(0, response.getCode());
    assertEquals(
        grpcShuffleServers.get(0).getGrpcPort(),
        Integer.parseInt(server.get("grpcPort").toString()));
    assertEquals(ServerStatus.ACTIVE.toString(), server.get("status"));
  }

  @Test
  public void testNodesServlet() throws Exception {
    String content = TestUtils.httpGet(String.format(NODES_URL, coordinatorHttpPort));
    Response<List<HashMap<String, Object>>> response =
        objectMapper.readValue(
            content, new TypeReference<Response<List<HashMap<String, Object>>>>() {});
    List<HashMap<String, Object>> serverList = response.getData();
    assertEquals(0, response.getCode());
    assertEquals(4, serverList.size());
    Set<Integer> portSet =
        grpcShuffleServers.stream().map(server -> server.getGrpcPort()).collect(Collectors.toSet());
    for (int i = 0; i < serverList.size(); i++) {
      assertEquals(ServerStatus.ACTIVE.toString(), serverList.get(i).get("status"));
      assertTrue(portSet.contains(Integer.parseInt(serverList.get(i).get("grpcPort").toString())));
    }
  }

  @Test
  public void testLostNodesServlet() throws IOException {
    try (SimpleClusterManager clusterManager =
        (SimpleClusterManager) coordinatorServer.getClusterManager()) {
      ShuffleServer shuffleServer3 = grpcShuffleServers.get(2);
      ShuffleServer shuffleServer4 = grpcShuffleServers.get(3);
      Map<String, ServerNode> servers = clusterManager.getServers();
      servers.get(shuffleServer3.getId()).setTimestamp(System.currentTimeMillis() - 40000);
      servers.get(shuffleServer4.getId()).setTimestamp(System.currentTimeMillis() - 40000);
      clusterManager.nodesCheckTest();
      List<String> expectShuffleIds = Arrays.asList(shuffleServer3.getId(), shuffleServer4.getId());
      List<String> shuffleIds = new ArrayList<>();
      Response<List<HashMap<String, Object>>> response =
          objectMapper.readValue(
              TestUtils.httpGet(String.format(LOSTNODES_URL, coordinatorHttpPort)),
              new TypeReference<Response<List<HashMap<String, Object>>>>() {});
      List<HashMap<String, Object>> serverList = response.getData();
      for (HashMap<String, Object> stringObjectHashMap : serverList) {
        String shuffleId = (String) stringObjectHashMap.get("id");
        shuffleIds.add(shuffleId);
      }
      assertTrue(CollectionUtils.isEqualCollection(expectShuffleIds, shuffleIds));
    }
  }

  @Test
  public void testDecommissionedNodeServlet() {
    ShuffleServer shuffleServer = grpcShuffleServers.get(1);
    shuffleServer.decommission();
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .until(
            () -> {
              Response<List<HashMap<String, Object>>> response =
                  objectMapper.readValue(
                      TestUtils.httpGet(
                          String.format(DECOMMISSIONEDNODES_URL, coordinatorHttpPort)),
                      new TypeReference<Response<List<HashMap<String, Object>>>>() {});
              List<HashMap<String, Object>> serverList = response.getData();
              for (HashMap<String, Object> stringObjectHashMap : serverList) {
                String shuffleId = (String) stringObjectHashMap.get("id");
                return shuffleServer.getId().equals(shuffleId);
              }
              return false;
            });
    shuffleServer.cancelDecommission();
  }

  @Test
  public void testUnhealthyNodesServlet() {
    ShuffleServer shuffleServer3 = grpcShuffleServers.get(2);
    ShuffleServer shuffleServer4 = grpcShuffleServers.get(3);
    shuffleServer3.markUnhealthy();
    shuffleServer4.markUnhealthy();
    List<String> expectShuffleIds = Arrays.asList(shuffleServer3.getId(), shuffleServer4.getId());
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .until(
            () -> {
              Response<List<HashMap<String, Object>>> response =
                  objectMapper.readValue(
                      TestUtils.httpGet(String.format(UNHEALTHYNODES_URL, coordinatorHttpPort)),
                      new TypeReference<Response<List<HashMap<String, Object>>>>() {});
              List<HashMap<String, Object>> serverList = response.getData();
              if (serverList.size() != 2) {
                return false;
              }

              List<String> shuffleIds =
                  serverList.stream()
                      .map(serverInfo -> (String) serverInfo.get("id"))
                      .collect(Collectors.toList());

              assertTrue(CollectionUtils.isEqualCollection(expectShuffleIds, shuffleIds));
              return true;
            });
  }

  @Test
  public void testDecommissionServlet() throws Exception {
    ShuffleServer shuffleServer = grpcShuffleServers.get(0);
    assertEquals(ServerStatus.ACTIVE, shuffleServer.getServerStatus());
    DecommissionRequest decommissionRequest = new DecommissionRequest();
    decommissionRequest.setServerIds(Sets.newHashSet("not_exist_serverId"));
    String content =
        TestUtils.httpPost(
            String.format(CANCEL_DECOMMISSION_URL, coordinatorHttpPort),
            objectMapper.writeValueAsString(decommissionRequest),
            authorizationHeader);
    Response<?> response = objectMapper.readValue(content, Response.class);
    assertEquals(-1, response.getCode());
    assertNotNull(response.getErrMsg());
    CancelDecommissionRequest cancelDecommissionRequest = new CancelDecommissionRequest();
    cancelDecommissionRequest.setServerIds(Sets.newHashSet(shuffleServer.getId()));
    content =
        TestUtils.httpPost(
            String.format(CANCEL_DECOMMISSION_URL, coordinatorHttpPort),
            objectMapper.writeValueAsString(cancelDecommissionRequest),
            authorizationHeader);
    response = objectMapper.readValue(content, Response.class);
    assertEquals(0, response.getCode());

    // Register shuffle, avoid server exiting immediately.
    ShuffleServerGrpcClient shuffleServerClient =
        new ShuffleServerGrpcClient(LOCALHOST, grpcShuffleServers.get(0).getGrpcPort());
    shuffleServerClient.registerShuffle(
        new RssRegisterShuffleRequest(
            "testDecommissionServlet_appId", 0, Lists.newArrayList(new PartitionRange(0, 1)), ""));
    decommissionRequest.setServerIds(Sets.newHashSet(shuffleServer.getId()));
    content =
        TestUtils.httpPost(
            String.format(DECOMMISSION_URL, coordinatorHttpPort),
            objectMapper.writeValueAsString(decommissionRequest),
            authorizationHeader);
    response = objectMapper.readValue(content, Response.class);
    assertEquals(0, response.getCode());
    assertEquals(ServerStatus.DECOMMISSIONING, shuffleServer.getServerStatus());

    // Wait until shuffle server send heartbeat to coordinator.
    Awaitility.await()
        .timeout(10, TimeUnit.SECONDS)
        .until(
            () ->
                ServerStatus.DECOMMISSIONING.equals(
                    coordinatorServer
                        .getClusterManager()
                        .getServerNodeById(shuffleServer.getId())
                        .getStatus()));
    // Cancel decommission.
    content =
        TestUtils.httpPost(
            String.format(CANCEL_DECOMMISSION_URL, coordinatorHttpPort),
            objectMapper.writeValueAsString(cancelDecommissionRequest),
            authorizationHeader);
    response = objectMapper.readValue(content, Response.class);
    assertEquals(0, response.getCode());
    assertEquals(ServerStatus.ACTIVE, shuffleServer.getServerStatus());
  }

  @Test
  public void testDecommissionSingleNode() throws Exception {
    ShuffleServer shuffleServer = grpcShuffleServers.get(0);
    assertEquals(ServerStatus.ACTIVE, shuffleServer.getServerStatus());
    String content =
        TestUtils.httpPost(
            String.format(
                CANCEL_DECOMMISSION_SINGLENODE_URL, coordinatorHttpPort, "not_exist_serverId"),
            null,
            authorizationHeader);
    Response<?> response = objectMapper.readValue(content, Response.class);
    assertEquals(-1, response.getCode());
    assertNotNull(response.getErrMsg());
    content =
        TestUtils.httpPost(
            String.format(
                CANCEL_DECOMMISSION_SINGLENODE_URL, coordinatorHttpPort, shuffleServer.getId()),
            null,
            authorizationHeader);
    response = objectMapper.readValue(content, Response.class);
    assertEquals(0, response.getCode());

    // Register shuffle, avoid server exiting immediately.
    ShuffleServerGrpcClient shuffleServerClient =
        new ShuffleServerGrpcClient(LOCALHOST, grpcShuffleServers.get(0).getGrpcPort());
    shuffleServerClient.registerShuffle(
        new RssRegisterShuffleRequest(
            "testDecommissionServlet_appId", 0, Lists.newArrayList(new PartitionRange(0, 1)), ""));
    content =
        TestUtils.httpPost(
            String.format(DECOMMISSION_SINGLENODE_URL, coordinatorHttpPort, shuffleServer.getId()),
            null,
            authorizationHeader);
    response = objectMapper.readValue(content, Response.class);
    assertEquals(0, response.getCode());
    assertEquals(ServerStatus.DECOMMISSIONING, shuffleServer.getServerStatus());

    // Wait until shuffle server send heartbeat to coordinator.
    Awaitility.await()
        .timeout(10, TimeUnit.SECONDS)
        .until(
            () ->
                ServerStatus.DECOMMISSIONING.equals(
                    coordinatorServer
                        .getClusterManager()
                        .getServerNodeById(shuffleServer.getId())
                        .getStatus()));
    // Cancel decommission.
    content =
        TestUtils.httpPost(
            String.format(
                CANCEL_DECOMMISSION_SINGLENODE_URL, coordinatorHttpPort, shuffleServer.getId()),
            null,
            authorizationHeader);
    response = objectMapper.readValue(content, Response.class);
    assertEquals(0, response.getCode());
    assertEquals(ServerStatus.ACTIVE, shuffleServer.getServerStatus());
  }

  @Test
  public void testRequestWithWrongCredentials() throws Exception {
    DecommissionRequest decommissionRequest = new DecommissionRequest();
    decommissionRequest.setServerIds(Sets.newHashSet("not_exist_serverId"));
    String wrongCredentials = "dW5pZmZsZTp1bmlmZmxlMTIz1";
    String content =
        TestUtils.httpPost(
            String.format(CANCEL_DECOMMISSION_URL, coordinatorHttpPort),
            objectMapper.writeValueAsString(decommissionRequest),
            ImmutableMap.of("Authorization", "Basic " + wrongCredentials));
    assertEquals("Authentication Failed", content);
  }
}
