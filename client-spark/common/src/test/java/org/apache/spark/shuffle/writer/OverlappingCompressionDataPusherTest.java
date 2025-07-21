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

package org.apache.spark.shuffle.writer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.impl.FailedBlockSendTracker;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.JavaUtils;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_WRITE_OVERLAPPING_COMPRESSION_THREADS_PER_VCORE;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OverlappingCompressionDataPusherTest {

  @Test
  public void testSend() {
    DataPusherTest.FakedShuffleWriteClient shuffleWriteClient =
        new DataPusherTest.FakedShuffleWriteClient();

    Map<String, Set<Long>> taskToSuccessBlockIds = Maps.newConcurrentMap();
    Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker = JavaUtils.newConcurrentMap();
    Set<String> failedTaskIds = new HashSet<>();

    RssConf rssConf = new RssConf();
    int threads = rssConf.get(RSS_WRITE_OVERLAPPING_COMPRESSION_THREADS_PER_VCORE);

    // case1: Illegal thread number of compression
    Assertions.assertThrows(
        RssException.class,
        () -> {
          new OverlappingCompressionDataPusher(
              shuffleWriteClient,
              taskToSuccessBlockIds,
              taskToFailedBlockSendTracker,
              failedTaskIds,
              1,
              2,
              threads);
        });

    // case2: Propagated into the underlying data pusher
    DataPusher pusher =
        new OverlappingCompressionDataPusher(
            shuffleWriteClient,
            taskToSuccessBlockIds,
            taskToFailedBlockSendTracker,
            failedTaskIds,
            1,
            2,
            1);
    pusher.setRssAppId("testSend");

    String taskId = "taskId1";
    List<ShuffleServerInfo> server1 =
        Collections.singletonList(new ShuffleServerInfo("0", "localhost", 1234));
    ShuffleBlockInfo staleBlock1 =
        new ShuffleBlockInfo(
            1, 1, 3, 1, 1, new byte[1], server1, 1, 100, 1, integer -> Collections.emptyList());

    // case1: will fast fail due to the stale assignment
    AddBlockEvent event = new AddBlockEvent(taskId, Arrays.asList(staleBlock1));
    CompletableFuture<Long> f1 = pusher.send(event);
    assertEquals(f1.join(), 0);
  }
}
