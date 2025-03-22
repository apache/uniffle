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
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.client.factory.ShuffleServerClientFactory;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.CoordinatorServer;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;
import org.apache.uniffle.server.MockedShuffleServer;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.storage.HadoopTestBase;
import org.apache.uniffle.storage.util.StorageType;

public abstract class IntegrationTestBase extends HadoopTestBase {

  protected static final String LOCALHOST;

  static {
    try {
      LOCALHOST = RssUtils.getHostIp();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected static List<ShuffleServer> grpcShuffleServers = Lists.newArrayList();
  protected static List<ShuffleServer> nettyShuffleServers = Lists.newArrayList();
  protected static List<CoordinatorServer> coordinators = Lists.newArrayList();

  private static List<ShuffleServerConf> shuffleServerConfList = Lists.newArrayList();
  private static List<ShuffleServerConf> mockShuffleServerConfList = Lists.newArrayList();
  protected static List<CoordinatorConf> coordinatorConfList = Lists.newArrayList();

  static @TempDir File tempDir;

  public static String getQuorum() {
    return coordinators.stream()
        .map(CoordinatorServer::getRpcListenPort)
        .map(port -> LOCALHOST + ":" + port)
        .collect(Collectors.joining(","));
  }

  public static List<Integer> generateNonExistingPorts(int num) {
    Set<Integer> portExistsSet = Sets.newHashSet();
    coordinators.forEach(
        server -> {
          portExistsSet.add(server.getJettyPort());
          portExistsSet.add(server.getRpcListenPort());
        });
    grpcShuffleServers.forEach(
        server -> {
          portExistsSet.add(server.getJettyPort());
          portExistsSet.add(server.getGrpcPort());
        });
    nettyShuffleServers.forEach(
        server -> {
          portExistsSet.add(server.getJettyPort());
          portExistsSet.add(server.getGrpcPort());
          portExistsSet.add(server.getNettyPort());
        });
    int i = 0;
    List<Integer> fakePorts = new ArrayList<>(num);
    while (i < num) {
      int port = ThreadLocalRandom.current().nextInt(1024, 65535);
      if (portExistsSet.add(port)) {
        fakePorts.add(port);
        i++;
      }
    }
    return fakePorts;
  }

  public static void startServersWithRandomPorts() throws Exception {
    for (CoordinatorConf coordinatorConf : coordinatorConfList) {
      coordinatorConf.setInteger(CoordinatorConf.JETTY_HTTP_PORT, 0);
      coordinatorConf.setInteger(CoordinatorConf.RPC_SERVER_PORT, 0);
      createCoordinatorServer(coordinatorConf);
    }
    for (CoordinatorServer coordinator : coordinators) {
      coordinator.start();
    }
    String quorum = getQuorum();

    for (ShuffleServerConf serverConf : shuffleServerConfList) {
      serverConf.setInteger(RssBaseConf.JETTY_HTTP_PORT, 0);
      serverConf.setInteger(RssBaseConf.RPC_SERVER_PORT, 0);
      serverConf.setString(RssBaseConf.RSS_COORDINATOR_QUORUM, quorum);
      createShuffleServer(serverConf);
    }
    for (ShuffleServerConf serverConf : mockShuffleServerConfList) {
      serverConf.setInteger(RssBaseConf.JETTY_HTTP_PORT, 0);
      serverConf.setInteger(RssBaseConf.RPC_SERVER_PORT, 0);
      serverConf.setString(RssBaseConf.RSS_COORDINATOR_QUORUM, quorum);
      createMockedShuffleServer(serverConf);
    }
    for (ShuffleServer server : grpcShuffleServers) {
      server.start();
    }
    for (ShuffleServer server : nettyShuffleServers) {
      server.getShuffleServerConf().setInteger(ShuffleServerConf.NETTY_SERVER_PORT, 0);
      server.start();
    }
  }

  @AfterAll
  public static void shutdownServers() throws Exception {
    for (CoordinatorServer coordinator : coordinators) {
      coordinator.stopServer();
    }
    for (ShuffleServer shuffleServer : grpcShuffleServers) {
      shuffleServer.stopServer();
    }
    for (ShuffleServer shuffleServer : nettyShuffleServers) {
      shuffleServer.stopServer();
    }
    grpcShuffleServers.clear();
    nettyShuffleServers.clear();
    coordinators.clear();
    shuffleServerConfList.clear();
    mockShuffleServerConfList.clear();
    coordinatorConfList.clear();
    ShuffleServerMetrics.clear();
    CoordinatorMetrics.clear();
    ShuffleServerClientFactory.getInstance().cleanupCache();
  }

  protected static CoordinatorConf coordinatorConfWithoutPort() {
    CoordinatorConf coordinatorConf = new CoordinatorConf();
    coordinatorConf.setInteger(CoordinatorConf.RPC_EXECUTOR_SIZE, 10);
    return coordinatorConf;
  }

  protected static void addDynamicConf(
      CoordinatorConf coordinatorConf, Map<String, String> dynamicConf) throws Exception {
    File file = createDynamicConfFile(dynamicConf);
    coordinatorConf.setBoolean(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_ENABLED, true);
    coordinatorConf.setString(
        CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_PATH, file.getAbsolutePath());
    coordinatorConf.setInteger(
        CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_UPDATE_INTERVAL_SEC, 5);
  }

  private static ShuffleServerConf getShuffleServerConf(ServerType serverType, String quorum) {
    ShuffleServerConf serverConf = new ShuffleServerConf();
    serverConf.setInteger("rss.rpc.server.port", 0);
    serverConf.setString("rss.storage.type", StorageType.MEMORY_LOCALFILE_HDFS.name());
    serverConf.setString("rss.storage.basePath", tempDir.getAbsolutePath());
    serverConf.setString("rss.server.buffer.capacity", "671088640");
    serverConf.setString("rss.server.memory.shuffle.highWaterMark", "50.0");
    serverConf.setString("rss.server.memory.shuffle.lowWaterMark", "0.0");
    serverConf.setString("rss.server.read.buffer.capacity", "335544320");
    serverConf.setString("rss.coordinator.quorum", quorum);
    serverConf.setString("rss.server.heartbeat.delay", "1000");
    serverConf.setString("rss.server.heartbeat.interval", "1000");
    serverConf.setInteger("rss.jetty.corePool.size", 64);
    serverConf.setInteger("rss.rpc.executor.size", 10);
    serverConf.setString("rss.server.hadoop.dfs.replication", "2");
    serverConf.setLong("rss.server.disk.capacity", 10L * 1024L * 1024L * 1024L);
    serverConf.setBoolean("rss.server.health.check.enable", false);
    serverConf.setBoolean(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    serverConf.set(ShuffleServerConf.SERVER_TRIGGER_FLUSH_CHECK_INTERVAL, 500L);
    serverConf.set(ShuffleServerConf.RPC_SERVER_TYPE, serverType);
    if (serverType == ServerType.GRPC_NETTY) {
      serverConf.setInteger(ShuffleServerConf.NETTY_SERVER_PORT, 0);
    }
    return serverConf;
  }

  protected static ShuffleServerConf shuffleServerConfWithoutPort(
      int subDirIndex, File tmpDir, ServerType serverType) {
    ShuffleServerConf shuffleServerConf = getShuffleServerConf(serverType, "");
    if (tmpDir != null) {
      File dataDir1 = new File(tmpDir, subDirIndex + "_1");
      File dataDir2 = new File(tmpDir, subDirIndex + "_2");
      String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
      shuffleServerConf.setString("rss.storage.basePath", basePath);
    }
    return shuffleServerConf;
  }

  protected static void createCoordinatorServer(CoordinatorConf coordinatorConf) throws Exception {
    coordinators.add(new CoordinatorServer(coordinatorConf));
  }

  protected static void storeCoordinatorConf(CoordinatorConf coordinatorConf) {
    coordinatorConfList.add(coordinatorConf);
  }

  protected static void createShuffleServer(ShuffleServerConf serverConf) throws Exception {
    ServerType serverType = serverConf.get(ShuffleServerConf.RPC_SERVER_TYPE);
    switch (serverType) {
      case GRPC:
        grpcShuffleServers.add(new ShuffleServer(serverConf));
        break;
      case GRPC_NETTY:
        nettyShuffleServers.add(new ShuffleServer(serverConf));
        break;
      default:
        throw new UnsupportedOperationException("Unsupported server type " + serverType);
    }
  }

  protected static void storeShuffleServerConf(ShuffleServerConf serverConf) {
    shuffleServerConfList.add(serverConf);
  }

  protected static void storeMockShuffleServerConf(ShuffleServerConf serverConf) {
    mockShuffleServerConfList.add(serverConf);
  }

  protected static void createMockedShuffleServer(ShuffleServerConf serverConf) throws Exception {
    ServerType serverType = serverConf.get(ShuffleServerConf.RPC_SERVER_TYPE);
    switch (serverType) {
      case GRPC:
        grpcShuffleServers.add(new MockedShuffleServer(serverConf));
        break;
      case GRPC_NETTY:
        nettyShuffleServers.add(new MockedShuffleServer(serverConf));
        break;
      default:
        throw new UnsupportedOperationException("Unsupported server type " + serverType);
    }
  }

  protected static File createDynamicConfFile(Map<String, String> dynamicConf) throws Exception {
    File dynamicConfFile = Files.createTempFile("dynamicConf", "conf").toFile();
    writeRemoteStorageConf(dynamicConfFile, dynamicConf);
    return dynamicConfFile;
  }

  protected static void writeRemoteStorageConf(File cfgFile, Map<String, String> dynamicConf)
      throws Exception {
    // sleep 2 secs to make sure the modified time will be updated
    Thread.sleep(2000);
    FileWriter fileWriter = new FileWriter(cfgFile);
    PrintWriter printWriter = new PrintWriter(fileWriter);
    for (Map.Entry<String, String> entry : dynamicConf.entrySet()) {
      printWriter.println(entry.getKey() + " " + entry.getValue());
    }
    printWriter.flush();
    printWriter.close();
  }
}
