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

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.CallerContext;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.common.RssTezConfig;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.app.RssDAGAppMasterForWordCountWithFailures;
import org.apache.tez.examples.WordCount;
import org.apache.tez.hadoop.shim.HadoopShim;
import org.apache.tez.hadoop.shim.HadoopShimsLoader;
import org.apache.tez.test.MiniTezCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.tez.common.RssTezConfig.RSS_AVOID_RECOMPUTE_SUCCEEDED_TASK;
import static org.apache.tez.dag.api.TezConfiguration.TEZ_AM_MAX_TASK_FAILURES_PER_NODE;
import static org.apache.tez.dag.api.TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED;
import static org.apache.tez.dag.api.TezConfiguration.TEZ_AM_NODE_BLACKLISTING_IGNORE_THRESHOLD;
import static org.apache.tez.dag.api.TezConfiguration.TEZ_AM_NODE_UNHEALTHY_RESCHEDULE_TASKS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TezWordCountWithFailuresTest extends IntegrationTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TezWordCountWithFailuresTest.class);
  private static final String TEST_ROOT_DIR =
      "target" + Path.SEPARATOR + TezWordCountWithFailuresTest.class.getName() + "-tmpDir";

  private Path remoteStagingDir = null;
  private final String inputPath = "word_count_input";
  private final String outputPath = "word_count_output";
  private final List<String> wordTable =
      Lists.newArrayList(
          "apple", "banana", "fruit", "cherry", "Chinese", "America", "Japan", "tomato");

  protected static MiniTezCluster miniTezCluster;

  @BeforeAll
  public static void beforeClass() throws Exception {
    LOG.info("Starting mini tez clusters");
    if (miniTezCluster == null) {
      miniTezCluster = new MiniTezCluster(TezIntegrationTestBase.class.getName(), 3, 1, 1);
      miniTezCluster.init(conf);
      miniTezCluster.start();
    }
    LOG.info("Starting coordinators and shuffle servers");
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    Map<String, String> dynamicConf = new HashMap<>();
    dynamicConf.put(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_PATH.key(), HDFS_URI + "rss/test");
    dynamicConf.put(RssTezConfig.RSS_STORAGE_TYPE, StorageType.MEMORY_LOCALFILE_HDFS.name());
    addDynamicConf(coordinatorConf, dynamicConf);
    storeCoordinatorConf(coordinatorConf);

    storeShuffleServerConf(shuffleServerConfWithoutPort(0, null, ServerType.GRPC));
    storeShuffleServerConf(shuffleServerConfWithoutPort(0, null, ServerType.GRPC_NETTY));
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
    generateInputFile();
  }

  private void generateInputFile() throws Exception {
    assertTrue(fs.mkdirs(new Path(inputPath)));
    for (int j = 0; j < 5; j++) {
      FSDataOutputStream outputStream = fs.create(new Path(inputPath + "/file." + j));
      Random random = new Random();
      for (int i = 0; i < 100; i++) {
        int index = random.nextInt(wordTable.size());
        String str = wordTable.get(index) + "\n";
        outputStream.writeBytes(str);
      }
      outputStream.close();
    }
    FileStatus[] fileStatus = fs.listStatus(new Path(inputPath));
    for (FileStatus status : fileStatus) {
      System.out.println("status is " + status);
    }
  }

  @AfterEach
  public void tearDownEach() throws Exception {
    if (this.remoteStagingDir != null) {
      fs.delete(this.remoteStagingDir, true);
    }
    for (int j = 0; j < 5; j++) {
      fs.delete(new Path(inputPath + "/file." + j), true);
    }
  }

  @Test
  public void wordCountTestWithTaskFailureWhenAvoidRecomputeEnable() throws Exception {
    runWordCountTestForBothClientTypes(true, false, 1);
  }

  @Test
  public void wordCountTestWithTaskFailureWhenAvoidRecomputeDisable() throws Exception {
    runWordCountTestForBothClientTypes(false, false, 1);
  }

  @Test
  public void wordCountTestWithNodeUnhealthyWhenAvoidRecomputeEnable() throws Exception {
    runWordCountTestForBothClientTypes(true, true, 100);
  }

  @Test
  public void wordCountTestWithNodeUnhealthyWhenAvoidRecomputeDisable() throws Exception {
    runWordCountTestForBothClientTypes(false, true, 100);
  }

  private void runWordCountTestForBothClientTypes(
      boolean avoidRecompute, boolean rescheduleWhenUnhealthy, int maxFailures) throws Exception {
    String originPath = runOriginalWordCount();
    runWordCountTest(
        ClientType.GRPC, avoidRecompute, rescheduleWhenUnhealthy, maxFailures, originPath);
    runWordCountTest(
        ClientType.GRPC_NETTY, avoidRecompute, rescheduleWhenUnhealthy, maxFailures, originPath);
  }

  private void runWordCountTest(
      ClientType clientType,
      boolean avoidRecompute,
      boolean rescheduleWhenUnhealthy,
      int maxFailures,
      String originPath)
      throws Exception {
    int testMode = rescheduleWhenUnhealthy ? 1 : 0;
    int expectedVerifyMode = avoidRecompute ? 0 : 1;
    TezConfiguration rssConf = new TezConfiguration(miniTezCluster.getConfig());
    updateRssConfiguration(
        rssConf, testMode, avoidRecompute, rescheduleWhenUnhealthy, maxFailures, clientType);
    TezIntegrationTestBase.appendAndUploadRssJars(rssConf);
    String testName = "rss-" + clientType.name().toLowerCase();
    runTezApp(rssConf, getTestArgs(testName), expectedVerifyMode);
    String rssPath = getOutputDir(testName);
    TezIntegrationTestBase.verifyResultEqual(originPath, rssPath);
  }

  private String runOriginalWordCount() throws Exception {
    TezConfiguration originalConf = new TezConfiguration(miniTezCluster.getConfig());
    updateCommonConfiguration(originalConf);
    runTezApp(originalConf, getTestArgs("origin"), -1);
    return getOutputDir("origin");
  }

  /*
   * Two verify mode are supported:
   * (a) verifyMode 0
   *     tez.rss.avoid.recompute.succeeded.task is enabled, should not recompute the task when this node is
   *     black-listed for unhealthy.
   *
   * (b) verifyMode 1
   *     tez.rss.avoid.recompute.succeeded.task is disable, will recompute the task when this node is
   *     black-listed for unhealthy.
   * */
  protected void runTezApp(TezConfiguration tezConf, String[] args, int verifyMode)
      throws Exception {
    assertEquals(
        0,
        ToolRunner.run(tezConf, new WordCountWithFailures(verifyMode), args),
        "WordCountWithFailures failed");
  }

  public String[] getTestArgs(String uniqueOutputName) {
    return new String[] {
      "-disableSplitGrouping", inputPath, outputPath + "/" + uniqueOutputName, "2"
    };
  }

  public String getOutputDir(String uniqueOutputName) {
    return outputPath + "/" + uniqueOutputName;
  }

  /*
   * In this integration test, mini cluster have three NM with 4G
   * (YarnConfiguration.DEFAULT_YARN_MINICLUSTER_NM_PMEM_MB). The request of am is 4G, the request of task is 2G.
   * It means that one node only runs one am container so that won't label the node which am container runs as
   * black-list or unhealthy node.
   * */
  public void updateRssConfiguration(
      Configuration appConf,
      int testMode,
      boolean avoidRecompute,
      boolean rescheduleWhenUnhealthy,
      int maxFailures,
      ClientType clientType) {
    appConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());
    appConf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 4096);
    appConf.setInt(TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB, 4096);
    appConf.setBoolean(TEZ_AM_NODE_BLACKLISTING_ENABLED, true);
    appConf.setInt(TEZ_AM_NODE_BLACKLISTING_IGNORE_THRESHOLD, 99);
    appConf.setInt(TEZ_AM_MAX_TASK_FAILURES_PER_NODE, maxFailures);
    appConf.set(RssTezConfig.RSS_COORDINATOR_QUORUM, getQuorum());
    appConf.set(RssTezConfig.RSS_CLIENT_TYPE, clientType.name());
    appConf.set(
        TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS,
        TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS_DEFAULT
            + " "
            + RssDAGAppMasterForWordCountWithFailures.class.getName()
            + " --testMode"
            + testMode);
    appConf.setBoolean(RSS_AVOID_RECOMPUTE_SUCCEEDED_TASK, avoidRecompute);
    appConf.setBoolean(TEZ_AM_NODE_UNHEALTHY_RESCHEDULE_TASKS, rescheduleWhenUnhealthy);
  }

  public void updateCommonConfiguration(Configuration appConf) {
    appConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());
    appConf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 512);
    appConf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, " -Xmx384m");
    appConf.setInt(TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB, 512);
    appConf.set(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, " -Xmx384m");
  }

  public static class WordCountWithFailures extends WordCount {

    TezClient tezClientInternal = null;
    private HadoopShim hadoopShim;
    int verifyMode;

    WordCountWithFailures(int assertMode) {
      this.verifyMode = assertMode;
    }

    @Override
    protected int runJob(String[] args, TezConfiguration tezConf, TezClient tezClient)
        throws Exception {
      this.tezClientInternal = tezClient;
      Method method =
          WordCount.class.getDeclaredMethod(
              "createDAG", TezConfiguration.class, String.class, String.class, int.class);
      method.setAccessible(true);
      DAG dag =
          (DAG)
              method.invoke(
                  this,
                  tezConf,
                  args[0],
                  args[1],
                  args.length == 3 ? Integer.parseInt(args[2]) : 1);
      LOG.info("Running WordCountWithFailures");
      return runDag(dag, isCountersLog(), LOG);
    }

    public int runDag(DAG dag, boolean printCounters, Logger logger)
        throws TezException, InterruptedException, IOException {
      tezClientInternal.waitTillReady();

      CallerContext callerContext =
          CallerContext.create("TezExamples", "Tez Example DAG: " + dag.getName());
      ApplicationId appId = tezClientInternal.getAppMasterApplicationId();
      if (hadoopShim == null) {
        Configuration conf = (getConf() == null ? new Configuration(false) : getConf());
        hadoopShim = new HadoopShimsLoader(conf).getHadoopShim();
      }

      if (appId != null) {
        TezUtilsInternal.setHadoopCallerContext(hadoopShim, appId);
        callerContext.setCallerIdAndType(appId.toString(), "TezExampleApplication");
      }
      dag.setCallerContext(callerContext);

      DAGClient dagClient = tezClientInternal.submitDAG(dag);
      Set<StatusGetOpts> getOpts = Sets.newHashSet();
      if (printCounters) {
        getOpts.add(StatusGetOpts.GET_COUNTERS);
      }

      DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(getOpts);
      if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
        logger.info("DAG diagnostics: {}", dagStatus.getDiagnostics());
        return -1;
      }

      Map<String, Progress> progressMap = dagStatus.getVertexProgress();
      if (verifyMode == 0) {
        // verifyMode is 0: avoid recompute succeeded task is true
        Assertions.assertEquals(0, progressMap.get("Tokenizer").getKilledTaskAttemptCount());
      } else if (verifyMode == 1) {
        // verifyMode is 1: avoid recompute succeeded task is false
        Assertions.assertTrue(progressMap.get("Tokenizer").getKilledTaskAttemptCount() > 0);
      }
      return 0;
    }
  }
}
