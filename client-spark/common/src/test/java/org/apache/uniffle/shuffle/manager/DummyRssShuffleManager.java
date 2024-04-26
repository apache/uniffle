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

package org.apache.uniffle.shuffle.manager;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.shuffle.handle.MutableShuffleHandleInfo;
import org.apache.spark.shuffle.handle.ShuffleHandleInfoBase;

import org.apache.uniffle.common.ReceivingFailureServer;

import static org.mockito.Mockito.mock;

public class DummyRssShuffleManager implements RssShuffleManagerInterface {
  public Set<Integer> unregisteredShuffleIds = new LinkedHashSet<>();

  @Override
  public String getAppId() {
    return "testAppId";
  }

  @Override
  public int getMaxFetchFailures() {
    return 2;
  }

  @Override
  public int getPartitionNum(int shuffleId) {
    return 16;
  }

  @Override
  public int getNumMaps(int shuffleId) {
    return 8;
  }

  @Override
  public void unregisterAllMapOutput(int shuffleId) {
    unregisteredShuffleIds.add(shuffleId);
  }

  @Override
  public ShuffleHandleInfoBase getShuffleHandleInfoByShuffleId(int shuffleId) {
    return null;
  }

  @Override
  public void addFailuresShuffleServerInfos(String shuffleServerId) {}

  @Override
  public boolean reassignAllShuffleServersForWholeStage(
      int stageId, int stageAttemptNumber, int shuffleId, int numMaps) {
    return false;
  }

  @Override
  public MutableShuffleHandleInfo reassignOnBlockSendFailure(
      int shuffleId, Map<Integer, List<ReceivingFailureServer>> partitionToFailureServers) {
    return mock(MutableShuffleHandleInfo.class);
  }
}
