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

package org.apache.uniffle.client.impl;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.StatusCode;

import static org.awaitility.Awaitility.await;

public class FailedBlockSendTrackerTest {
  @Test
  public void test() throws Exception {
    FailedBlockSendTracker tracker = new FailedBlockSendTracker();
    ShuffleServerInfo shuffleServerInfo1 = new ShuffleServerInfo("host1", 19999);
    ShuffleServerInfo shuffleServerInfo2 = new ShuffleServerInfo("host2", 19999);
    ShuffleServerInfo shuffleServerInfo3 = new ShuffleServerInfo("host3", 19999);
    List<ShuffleServerInfo> shuffleServerInfos1 =
        Lists.newArrayList(shuffleServerInfo1, shuffleServerInfo2);
    ShuffleBlockInfo shuffleBlockInfo1 =
        new ShuffleBlockInfo(0, 0, 1L, 0, 0L, new byte[] {}, shuffleServerInfos1, 0, 0L, 0L);
    List<ShuffleServerInfo> shuffleServerInfos2 =
        Lists.newArrayList(shuffleServerInfo3, shuffleServerInfo2);
    ShuffleBlockInfo shuffleBlockInfo2 =
        new ShuffleBlockInfo(0, 0, 2L, 0, 0L, new byte[] {}, shuffleServerInfos2, 0, 0L, 0L);
    new Thread(
            () -> {
              tracker.add(shuffleBlockInfo1, shuffleServerInfo1, StatusCode.INTERNAL_ERROR);
              tracker.add(shuffleBlockInfo1, shuffleServerInfo2, StatusCode.INTERNAL_ERROR);
              tracker.add(shuffleBlockInfo2, shuffleServerInfo3, StatusCode.INTERNAL_ERROR);
              tracker.add(shuffleBlockInfo2, shuffleServerInfo2, StatusCode.INTERNAL_ERROR);
            })
        .start();
    List<String> expected =
        Lists.newArrayList(
            shuffleServerInfo1.getId(), shuffleServerInfo2.getId(), shuffleServerInfo3.getId());
    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(
            () -> {
              if (tracker.getFailedBlockIds().size() != 2) {
                return false;
              }
              List<String> actual =
                  tracker.getFaultyShuffleServers().stream()
                      .map(ShuffleServerInfo::getId)
                      .sorted()
                      .collect(Collectors.toList());
              return CollectionUtils.isEqualCollection(expected, actual);
            });
  }
}
