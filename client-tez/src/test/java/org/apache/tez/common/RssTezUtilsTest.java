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

package org.apache.tez.common;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.tez.common.RssTezConfig.RSS_STORAGE_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RssTezUtilsTest {
  @Test
  public void testSharedWriterBufferDefault() {
    Configuration conf = new Configuration(false);
    assertEquals(
        3 * 1024 * 1024L,
        conf.getLong(
            RssTezConfig.RSS_WRITER_BUFFER_SIZE, RssTezConfig.RSS_DEFAULT_WRITER_BUFFER_SIZE));
    conf.setLong(RssTezConfig.RSS_WRITER_BUFFER_SIZE, 4 * 1024 * 1024L);
    assertEquals(
        4 * 1024 * 1024L,
        conf.getLong(
            RssTezConfig.RSS_WRITER_BUFFER_SIZE, RssTezConfig.RSS_DEFAULT_WRITER_BUFFER_SIZE));
  }

  @Test
  public void baskAttemptIdTest() {
    long taskAttemptId = 0x1000ad12;
    ApplicationId appId = ApplicationId.newInstance(9999, 72);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vId = TezVertexID.getInstance(dagId, 35);
    TezTaskID taskId = TezTaskID.getInstance(vId, (int) taskAttemptId);
    TezTaskAttemptID tezTaskAttemptId = TezTaskAttemptID.getInstance(taskId, 3);

    boolean isException = false;
    try {
      RssTezUtils.createRssTaskAttemptId(tezTaskAttemptId, 3);
    } catch (RssException e) {
      isException = true;
    }
    assertTrue(isException);

    taskId = TezTaskID.getInstance(vId, 1 << 21);
    tezTaskAttemptId = TezTaskAttemptID.getInstance(taskId, 2);
    isException = false;
    try {
      RssTezUtils.createRssTaskAttemptId(tezTaskAttemptId, 3);
    } catch (RssException e) {
      isException = true;
    }
    assertTrue(isException);
  }

  @Test
  public void blockConvertTest() {
    ApplicationId appId = ApplicationId.newInstance(9999, 72);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vId = TezVertexID.getInstance(dagId, 35);
    TezTaskID tId = TezTaskID.getInstance(vId, 389);
    TezTaskAttemptID tezTaskAttemptId = TezTaskAttemptID.getInstance(tId, 2);
    long taskAttemptId = RssTezUtils.createRssTaskAttemptId(tezTaskAttemptId, 3);
    long blockId = RssTezUtils.getBlockId(1, taskAttemptId, 0);
    long newTaskAttemptId = RssTezUtils.getTaskAttemptId(blockId);
    assertEquals(taskAttemptId, newTaskAttemptId);
    blockId = RssTezUtils.getBlockId(2, taskAttemptId, 2);
    newTaskAttemptId = RssTezUtils.getTaskAttemptId(blockId);
    assertEquals(taskAttemptId, newTaskAttemptId);
  }

  @Test
  public void testPartitionIdConvertBlock() {
    BlockIdLayout layout = BlockIdLayout.DEFAULT;
    ApplicationId appId = ApplicationId.newInstance(9999, 72);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vId = TezVertexID.getInstance(dagId, 35);
    TezTaskID tId = TezTaskID.getInstance(vId, 389);
    TezTaskAttemptID tezTaskAttemptId = TezTaskAttemptID.getInstance(tId, 2);
    long taskAttemptId = RssTezUtils.createRssTaskAttemptId(tezTaskAttemptId, 3);
    long mask = (1L << layout.partitionIdBits) - 1;
    for (int partitionId = 0; partitionId <= 3000; partitionId++) {
      for (int seqNo = 0; seqNo <= 10; seqNo++) {
        long blockId = RssTezUtils.getBlockId(partitionId, taskAttemptId, seqNo);
        int newPartitionId = Math.toIntExact((blockId >> layout.taskAttemptIdBits) & mask);
        assertEquals(partitionId, newPartitionId);
      }
    }
  }

  @Test
  public void testEstimateTaskConcurrency() {
    Configuration jobConf = new Configuration();
    int mapNum = 500;
    int reduceNum = 20;
    assertEquals(495, RssTezUtils.estimateTaskConcurrency(jobConf, mapNum, reduceNum));

    jobConf.setDouble(Constants.MR_SLOW_START, 1.0);
    assertEquals(500, RssTezUtils.estimateTaskConcurrency(jobConf, mapNum, reduceNum));
    jobConf.setInt(Constants.MR_MAP_LIMIT, 200);
    jobConf.setInt(Constants.MR_REDUCE_LIMIT, 200);
    assertEquals(200, RssTezUtils.estimateTaskConcurrency(jobConf, mapNum, reduceNum));

    jobConf.setDouble("mapreduce.rss.estimate.task.concurrency.dynamic.factor", 0.5);
    assertEquals(200, RssTezUtils.estimateTaskConcurrency(jobConf, mapNum, reduceNum));
  }

  @Test
  public void testGetRequiredShuffleServerNumber() {
    Configuration jobConf = new Configuration();
    int mapNum = 500;
    int reduceNum = 20;
    jobConf.setInt(RssTezConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER, 10);
    assertEquals(10, RssTezUtils.getRequiredShuffleServerNumber(jobConf, mapNum, reduceNum));
    jobConf.setBoolean(RssTezConfig.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED, true);
    assertEquals(10, RssTezUtils.getRequiredShuffleServerNumber(jobConf, mapNum, reduceNum));
    jobConf.unset(RssTezConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER);
    assertEquals(7, RssTezUtils.getRequiredShuffleServerNumber(jobConf, mapNum, reduceNum));
    jobConf.setDouble(Constants.MR_SLOW_START, 1.0);
    assertEquals(7, RssTezUtils.getRequiredShuffleServerNumber(jobConf, mapNum, reduceNum));
    jobConf.setInt(Constants.MR_MAP_LIMIT, 200);
    jobConf.setInt(Constants.MR_REDUCE_LIMIT, 200);
    assertEquals(3, RssTezUtils.getRequiredShuffleServerNumber(jobConf, mapNum, reduceNum));
    jobConf.setDouble("mapreduce.rss.estimate.task.concurrency.dynamic.factor", 0.5);
    assertEquals(3, RssTezUtils.getRequiredShuffleServerNumber(jobConf, mapNum, reduceNum));
  }

  @Test
  public void testComputeShuffleId() {
    int dagId = 1;
    int upVertexId = 1;
    int downVertexID = 2;
    assertEquals(1001002, RssTezUtils.computeShuffleId(dagId, upVertexId, downVertexID));
  }

  @Test
  public void testTaskIdStrToTaskId() {
    assertEquals(
        0, RssTezUtils.taskIdStrToTaskId("attempt_1680867852986_0012_1_01_000000_0_10003"));
  }

  @Test
  public void attemptTaskIdTest() {
    String tezTaskAttemptId = "attempt_1677051234358_0091_1_00_000000_0";
    TezTaskAttemptID originalTezTaskAttemptID = TezTaskAttemptID.fromString(tezTaskAttemptId);
    String uniqueIdentifier = String.format("%s_%05d", tezTaskAttemptId, 3);
    String uniqueIdentifierToAttemptId = RssTezUtils.uniqueIdentifierToAttemptId(uniqueIdentifier);
    assertEquals(tezTaskAttemptId, uniqueIdentifierToAttemptId);
    TezTaskAttemptID tezTaskAttemptID = TezTaskAttemptID.fromString(uniqueIdentifierToAttemptId);
    assertEquals(originalTezTaskAttemptID, tezTaskAttemptID);
  }

  @Test
  public void testParseRssWorker() {
    Map<Integer, Set<ShuffleServerInfo>> rssWorker = new HashMap<>();
    int shuffleId = 1001602;
    // 0_1_2_3 is consist of partition id.
    String hostnameInfo =
        "localhost;1001602=172.19.193.152:19999+0_1_2_3,172.19.193.153:19999+2_3_4_5";
    RssTezUtils.parseRssWorker(rssWorker, shuffleId, hostnameInfo);

    assertEquals(6, rssWorker.size());

    int partitionId = 0;
    Set<ShuffleServerInfo> shuffleServerInfo = rssWorker.get(partitionId);
    ShuffleServerInfo server = new ShuffleServerInfo("172.19.193.152", 19999);
    assertEquals(ImmutableSet.of(server), shuffleServerInfo);

    partitionId = 3;
    shuffleServerInfo = rssWorker.get(partitionId);
    ShuffleServerInfo server2 = new ShuffleServerInfo("172.19.193.153", 19999);
    assertEquals(ImmutableSet.of(server, server2), shuffleServerInfo);

    partitionId = 18;
    shuffleServerInfo = rssWorker.get(partitionId);
    assertNull(shuffleServerInfo);

    Integer[] expectPartitionArr = new Integer[] {0, 1, 2, 3, 4, 5};
    assertTrue(Arrays.equals(expectPartitionArr, rssWorker.keySet().toArray(new Integer[0])));
  }

  @Test
  public void testApplyDynamicClientConf() {
    Configuration conf = new Configuration(false);
    conf.set("tez.config1", "value1");
    conf.set(RSS_STORAGE_TYPE, StorageType.MEMORY_LOCALFILE_HDFS.name());
    Map<String, String> dynamic = new HashMap<>();
    dynamic.put(RSS_STORAGE_TYPE, StorageType.LOCALFILE.name());
    dynamic.put("config2", "value2");
    RssTezUtils.applyDynamicClientConf(conf, dynamic);
    assertEquals("value1", conf.get("tez.config1"));
    assertEquals("value2", conf.get("tez.config2"));
    assertEquals(StorageType.LOCALFILE.name(), conf.get(RSS_STORAGE_TYPE));
  }

  @Test
  public void testFilterRssConf() {
    Configuration conf1 = new Configuration(false);
    conf1.set("tez.config1", "value1");
    conf1.set("config2", "value2");
    Configuration conf2 = RssTezUtils.filterRssConf(conf1);
    assertEquals("value1", conf2.get("tez.config1"));
    assertNull(conf2.get("config2"));
  }

  @Test
  public void testParseDagId() {
    int shuffleId = RssTezUtils.computeShuffleId(1, 2, 3);
    assertEquals(1, RssTezUtils.parseDagId(shuffleId));
    assertThrows(IllegalArgumentException.class, () -> RssTezUtils.parseDagId(-1));
    assertThrows(RssException.class, () -> RssTezUtils.parseDagId(100));
  }
}
