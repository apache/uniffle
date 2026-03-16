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

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.RangeMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.KerberizedHadoopBase;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.util.ChecksumUtils;
import org.apache.uniffle.server.buffer.ShuffleBuffer;
import org.apache.uniffle.server.buffer.ShuffleBufferManager;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KerberizedShuffleTaskManagerTest extends KerberizedHadoopBase {

  private static final AtomicInteger ATOMIC_INT = new AtomicInteger(0);

  private ShuffleServer shuffleServer;
  protected static String hdfsUri;
  protected static FileSystem fs;

  @BeforeAll
  public static void beforeAll() throws Exception {
    testRunner = KerberizedShuffleTaskManagerTest.class;
    KerberizedHadoopBase.init();
    fs = kerberizedHadoop.getFileSystem();
    hdfsUri = fs.getUri().toString();
  }

  @BeforeEach
  public void beforeEach() {
    ShuffleServerMetrics.clear();
    ShuffleServerMetrics.register();
  }

  @AfterEach
  public void afterEach() throws Exception {
    if (shuffleServer != null) {
      shuffleServer.stopServer();
      shuffleServer = null;
    }
    ShuffleServerMetrics.clear();
  }

  /**
   * Clean up the shuffle data of stage level for one app
   *
   * @throws Exception
   */
  @Test
  public void removeShuffleDataWithHdfsTest() throws Exception {
    String confFile = ClassLoader.getSystemResource("server.conf").getFile();
    ShuffleServerConf conf = new ShuffleServerConf(confFile);
    String storageBasePath = hdfsUri + "/rss/removeShuffleDataWithHdfsTest";
    conf.set(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    conf.set(ShuffleServerConf.RPC_SERVER_PORT, 1234);
    conf.set(ShuffleServerConf.RSS_COORDINATOR_QUORUM, "localhost:9527");
    conf.set(ShuffleServerConf.JETTY_HTTP_PORT, 12345);
    conf.set(ShuffleServerConf.JETTY_CORE_POOL_SIZE, 64);
    conf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 128L);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 50.0);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 0.0);
    conf.set(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 10000L);
    conf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, Long.MAX_VALUE);
    conf.set(ShuffleServerConf.HEALTH_CHECK_ENABLE, false);
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    conf.set(ShuffleServerConf.RSS_SECURITY_HADOOP_KERBEROS_ENABLE, true);
    conf.set(
        ShuffleServerConf.RSS_SECURITY_HADOOP_KRB5_CONF_FILE, kerberizedHadoop.getKrb5ConfFile());
    conf.set(
        ShuffleServerConf.RSS_SECURITY_HADOOP_KERBEROS_KEYTAB_FILE,
        kerberizedHadoop.getHdfsKeytab());
    conf.set(
        ShuffleServerConf.RSS_SECURITY_HADOOP_KERBEROS_PRINCIPAL,
        kerberizedHadoop.getHdfsPrincipal());
    conf.setString(
        ShuffleServerConf.PREFIX_HADOOP_CONF + ".hadoop.security.authentication", "kerberos");

    shuffleServer = new ShuffleServer(conf);

    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();

    String appId = "removeShuffleDataTest1";
    for (int i = 0; i < 4; i++) {
      shuffleTaskManager.registerShuffle(
          appId,
          i,
          Lists.newArrayList(new PartitionRange(0, 1)),
          new RemoteStorageInfo(storageBasePath, Maps.newHashMap()),
          user);
    }
    shuffleTaskManager.refreshAppId(appId);

    assertEquals(1, shuffleTaskManager.getAppIds().size());

    ShufflePartitionedData partitionedData0 = createPartitionedData(1, 1, 35);

    shuffleTaskManager.requireBuffer(35);
    shuffleTaskManager.requireBuffer(35);
    shuffleTaskManager.cacheShuffleData(appId, 0, false, partitionedData0);
    shuffleTaskManager.updateCachedBlockIds(appId, 0, partitionedData0);
    shuffleTaskManager.cacheShuffleData(appId, 1, false, partitionedData0);
    shuffleTaskManager.updateCachedBlockIds(appId, 1, partitionedData0);
    shuffleTaskManager.refreshAppId(appId);
    shuffleTaskManager.checkResourceStatus();

    assertEquals(1, shuffleTaskManager.getAppIds().size());

    ShuffleBufferManager shuffleBufferManager = shuffleServer.getShuffleBufferManager();
    RangeMap<Integer, ShuffleBuffer> rangeMap =
        shuffleBufferManager.getBufferPool().get(appId).get(0);
    assertFalse(rangeMap.asMapOfRanges().isEmpty());
    shuffleTaskManager.commitShuffle(appId, 0);

    // Before removing shuffle resources
    String appBasePath = ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePath, appId);
    String shufflePath0 = ShuffleStorageUtils.getFullShuffleDataFolder(appBasePath, "0");
    assertTrue(fs.exists(new Path(shufflePath0)));

    // After removing the shuffle id of 0 resources
    shuffleTaskManager.removeShuffleDataSync(appId, 0);
    assertFalse(fs.exists(new Path(shufflePath0)));
    assertTrue(fs.exists(new Path(appBasePath)));
    assertNull(shuffleBufferManager.getBufferPool().get(appId).get(0));
    assertNotNull(shuffleBufferManager.getBufferPool().get(appId).get(1));
    shuffleTaskManager.removeResources(appId, false);
    assertFalse(fs.exists(new Path(appBasePath)));
    assertNull(shuffleBufferManager.getBufferPool().get(appId));
  }

  private ShufflePartitionedData createPartitionedData(
      int partitionId, int blockNum, int dataLength) {
    ShufflePartitionedBlock[] blocks = createBlock(blockNum, dataLength);
    return new ShufflePartitionedData(partitionId, blocks);
  }

  private ShufflePartitionedBlock[] createBlock(int num, int length) {
    ShufflePartitionedBlock[] blocks = new ShufflePartitionedBlock[num];
    for (int i = 0; i < num; i++) {
      byte[] buf = new byte[length];
      new Random().nextBytes(buf);
      blocks[i] =
          new ShufflePartitionedBlock(
              length, length, ChecksumUtils.getCrc32(buf), ATOMIC_INT.incrementAndGet(), 0, buf);
    }
    return blocks;
  }
}
