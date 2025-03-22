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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.common.RssTezConfig;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.RssDAGAppMaster;
import org.apache.tez.test.MiniTezCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.tez.dag.api.TezConfiguration.TEZ_LIB_URIS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TezIntegrationTestBase extends IntegrationTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TezIntegrationTestBase.class);
  private static final String TEST_ROOT_DIR =
      "target" + Path.SEPARATOR + TezIntegrationTestBase.class.getName() + "-tmpDir";

  private Path remoteStagingDir = null;
  protected static MiniTezCluster miniTezCluster;

  @BeforeAll
  public static void beforeClass() throws Exception {
    LOG.info("Starting mini tez clusters");
    if (miniTezCluster == null) {
      miniTezCluster = new MiniTezCluster(TezIntegrationTestBase.class.getName(), 1, 1, 1);
      miniTezCluster.init(conf);
      miniTezCluster.start();
    }
  }

  protected static void setupServers(ShuffleServerConf serverConf) throws Exception {
    LOG.info("Starting coordinators and shuffle servers");
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    Map<String, String> dynamicConf = new HashMap<>();
    dynamicConf.put(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_PATH.key(), HDFS_URI + "rss/test");
    dynamicConf.put(RssTezConfig.RSS_STORAGE_TYPE, StorageType.MEMORY_LOCALFILE_HDFS.name());
    addDynamicConf(coordinatorConf, dynamicConf);
    storeCoordinatorConf(coordinatorConf);
    ShuffleServerConf grpcShuffleServerConf =
        shuffleServerConfWithoutPort(0, null, ServerType.GRPC);
    if (serverConf != null) {
      grpcShuffleServerConf.addAll(serverConf);
    }
    storeShuffleServerConf(grpcShuffleServerConf);
    ShuffleServerConf nettyShuffleServerConf =
        shuffleServerConfWithoutPort(0, null, ServerType.GRPC_NETTY);
    if (serverConf != null) {
      nettyShuffleServerConf.addAll(serverConf);
    }
    storeShuffleServerConf(nettyShuffleServerConf);
    startServersWithRandomPorts();
  }

  @AfterAll
  public static void tearDown() {
    if (miniTezCluster != null) {
      LOG.info("Stopping MiniTezCluster");
      miniTezCluster.stop();
      miniTezCluster = null;
    }
  }

  @BeforeEach
  public void setup() throws Exception {
    remoteStagingDir =
        fs.makeQualified(new Path(TEST_ROOT_DIR, String.valueOf(new Random().nextInt(100000))));
    TezClientUtils.ensureStagingDirExists(conf, remoteStagingDir);
  }

  @AfterEach
  public void tearDownEach() throws Exception {
    if (this.remoteStagingDir != null) {
      fs.delete(this.remoteStagingDir, true);
    }
  }

  public void run() throws Exception {
    // 1 Run original Tez examples
    TezConfiguration appConf = new TezConfiguration(miniTezCluster.getConfig());
    updateCommonConfiguration(appConf);
    runTezApp(appConf, getTestTool(), getTestArgs("origin"));
    final String originPath = getOutputDir("origin");

    // Run RSS tests with different configurations
    runRssTest(ClientType.GRPC, null, "rss-grpc", originPath);
    runRssTest(ClientType.GRPC, "/tmp/spill-grpc", "rss-spill-grpc", originPath);
    runRssTest(ClientType.GRPC_NETTY, null, "rss-netty", originPath);
    runRssTest(ClientType.GRPC_NETTY, "/tmp/spill-netty", "rss-spill-netty", originPath);
  }

  private void runRssTest(
      ClientType clientType, String spillPath, String testName, String originPath)
      throws Exception {
    TezConfiguration appConf = new TezConfiguration(miniTezCluster.getConfig());
    if (spillPath != null) {
      appConf.setBoolean(RssTezConfig.RSS_REDUCE_REMOTE_SPILL_ENABLED, true);
      appConf.set(RssTezConfig.RSS_REMOTE_SPILL_STORAGE_PATH, spillPath);
    }
    updateRssConfiguration(appConf, clientType);
    appendAndUploadRssJars(appConf);
    runTezApp(appConf, getTestTool(), getTestArgs(testName));
    verifyResults(originPath, getOutputDir(testName));
  }

  public Tool getTestTool() {
    Assertions.fail("getTestTool is not implemented");
    return null;
  }

  public String[] getTestArgs(String uniqueOutputName) {
    Assertions.fail("getTestArgs is not implemented");
    return new String[0];
  }

  public String getOutputDir(String uniqueOutputName) {
    Assertions.fail("getOutputDir is not implemented");
    return null;
  }

  public void verifyResults(String originPath, String rssPath) throws Exception {
    verifyResultEqual(originPath, rssPath);
  }

  public void updateCommonConfiguration(Configuration appConf) throws Exception {
    appConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());
    appConf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 512);
    appConf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, " -Xmx384m");
    appConf.setInt(TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB, 512);
    appConf.set(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, " -Xmx384m");
  }

  public void updateRssConfiguration(Configuration appConf, ClientType clientType) {
    appConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());
    appConf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 512);
    appConf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, " -Xmx384m");
    appConf.setInt(TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB, 512);
    appConf.set(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, " -Xmx384m");
    appConf.set(RssTezConfig.RSS_COORDINATOR_QUORUM, getQuorum());
    appConf.set(RssTezConfig.RSS_CLIENT_TYPE, clientType.name());
    appConf.set(
        TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS,
        TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS_DEFAULT + " " + RssDAGAppMaster.class.getName());
  }

  protected static void appendAndUploadRssJars(TezConfiguration tezConf) throws IOException {
    String uris = tezConf.get(TEZ_LIB_URIS);
    Assertions.assertNotNull(uris);

    // Get the rss client tez shaded jar file.
    URL url = TezIntegrationTestBase.class.getResource("/");
    final String parentPath =
        new Path(url.getPath()).getParent().getParent().getParent().getParent().toString();
    File file = new File(parentPath, "client-tez/target/shaded");
    File[] jars = file.listFiles();
    File rssJar = null;
    for (File jar : jars) {
      if (jar.getName().startsWith("rss-client-tez")) {
        rssJar = jar;
        break;
      }
    }

    // upload rss jars
    Path testRootDir =
        fs.makeQualified(new Path("target", TezIntegrationTestBase.class.getName() + "-tmpDir"));
    Path appRemoteJar = new Path(testRootDir, "rss-tez-client-shaded.jar");
    fs.copyFromLocalFile(new Path(rssJar.toString()), appRemoteJar);
    fs.setPermission(appRemoteJar, new FsPermission("777"));

    // update tez.lib.uris
    tezConf.set(TEZ_LIB_URIS, uris + "," + appRemoteJar);
  }

  protected void runTezApp(TezConfiguration tezConf, Tool tool, String[] args) throws Exception {
    assertEquals(0, ToolRunner.run(tezConf, tool, args), tool.getClass().getName() + " failed");
  }

  public static void verifyResultEqual(String originPath, String rssPath) throws Exception {
    if (originPath == null && rssPath == null) {
      return;
    }
    Path originPathFs = new Path(originPath);
    Path rssPathFs = new Path(rssPath);
    FileStatus[] originFiles = fs.listStatus(originPathFs);
    FileStatus[] rssFiles = fs.listStatus(rssPathFs);
    long originLen = 0;
    long rssLen = 0;
    List<String> originFileList = new ArrayList<>();
    List<String> rssFileList = new ArrayList<>();
    for (FileStatus file : originFiles) {
      originLen += file.getLen();
      String name = file.getPath().getName();
      if (!name.equals("_SUCCESS")) {
        originFileList.add(name);
      }
    }
    for (FileStatus file : rssFiles) {
      rssLen += file.getLen();
      String name = file.getPath().getName();
      if (!name.equals("_SUCCESS")) {
        rssFileList.add(name);
      }
    }
    assertEquals(originFileList.size(), rssFileList.size());
    for (int i = 0; i < originFileList.size(); i++) {
      assertEquals(originFileList.get(i), rssFileList.get(i));
      Path p1 = new Path(originPath, originFileList.get(i));
      FSDataInputStream f1 = fs.open(p1);
      Path p2 = new Path(rssPath, rssFileList.get(i));
      FSDataInputStream f2 = fs.open(p2);
      boolean isNotEof1 = true;
      boolean isNotEof2 = true;
      while (isNotEof1 && isNotEof2) {
        byte b1 = 1;
        byte b2 = 1;
        try {
          b1 = f1.readByte();
        } catch (EOFException ee) {
          isNotEof1 = false;
        }
        try {
          b2 = f2.readByte();
        } catch (EOFException ee) {
          isNotEof2 = false;
        }
        assertEquals(b1, b2);
      }
      assertEquals(isNotEof1, isNotEof2);
    }
    assertEquals(originLen, rssLen);
  }

  public static void verifyResultsSameSet(String originPath, String rssPath) throws Exception {
    if (originPath == null && rssPath == null) {
      return;
    }
    // 1 List the originalPath and rssPath, make sure generated file are same.
    Path originPathFs = new Path(originPath);
    Path rssPathFs = new Path(rssPath);
    FileStatus[] originFiles = fs.listStatus(originPathFs);
    FileStatus[] rssFiles = fs.listStatus(rssPathFs);
    long originLen = 0;
    long rssLen = 0;
    List<String> originFileList = new ArrayList<>();
    List<String> rssFileList = new ArrayList<>();
    for (FileStatus file : originFiles) {
      originLen += file.getLen();
      String name = file.getPath().getName();
      if (!name.equals("_SUCCESS")) {
        originFileList.add(name);
      }
    }
    for (FileStatus file : rssFiles) {
      rssLen += file.getLen();
      String name = file.getPath().getName();
      if (!name.equals("_SUCCESS")) {
        rssFileList.add(name);
      }
    }
    assertEquals(originFileList.size(), rssFileList.size());

    // 2 Load original result and rss result to hashmap
    Map<String, Integer> originalResults = new HashMap<>();
    for (String file : originFileList) {
      Path path = new Path(originPath, file);
      LineReader lineReader = new LineReader(fs.open(path));
      Text line = new Text();
      while (lineReader.readLine(line) > 0) {
        if (!originalResults.containsKey(line.toString())) {
          originalResults.put(line.toString(), 1);
        } else {
          originalResults.put(line.toString(), originalResults.get(line.toString()) + 1);
        }
      }
    }

    Map<String, Integer> rssResults = new HashMap<>();
    for (String file : rssFileList) {
      Path path = new Path(rssPath, file);
      LineReader lineReader = new LineReader(fs.open(path));
      Text line = new Text();
      while (lineReader.readLine(line) > 0) {
        if (!rssResults.containsKey(line.toString())) {
          rssResults.put(line.toString(), 1);
        } else {
          rssResults.put(line.toString(), rssResults.get(line.toString()) + 1);
        }
      }
    }

    // 3 Compare the hashmap
    Assertions.assertEquals(
        originalResults.size(),
        rssResults.size(),
        "The size of cartesian product set is not equal");
    for (Map.Entry<String, Integer> entry : originalResults.entrySet()) {
      Assertions.assertTrue(
          rssResults.containsKey(entry.getKey()),
          entry.getKey() + " is not found in rss cartesian product result");
      Assertions.assertEquals(
          entry.getValue(),
          rssResults.get(entry.getKey()),
          "the value of " + entry.getKey() + " is not equal to in rss cartesian product result");
    }
  }
}
