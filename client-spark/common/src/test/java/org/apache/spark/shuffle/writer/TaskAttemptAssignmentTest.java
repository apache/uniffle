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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.shuffle.handle.ShuffleHandleInfo;
import org.apache.spark.shuffle.handle.split.PartitionSplitInfo;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.common.PartitionSplitMode;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleServerInfo;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TaskAttemptAssignmentTest {

  class MockShuffleHandleInfo implements ShuffleHandleInfo {

    @Override
    public Set<ShuffleServerInfo> getServers() {
      return Collections.emptySet();
    }

    @Override
    public Map<Integer, List<ShuffleServerInfo>> getAvailablePartitionServersForWriter(
        Map<Integer, List<ShuffleServerInfo>> exclusivePartitionServers) {
      return Collections.emptyMap();
    }

    @Override
    public Map<Integer, List<ShuffleServerInfo>> getAllPartitionServersForReader() {
      return Collections.emptyMap();
    }

    @Override
    public PartitionDataReplicaRequirementTracking createPartitionReplicaTracking() {
      return null;
    }

    @Override
    public int getShuffleId() {
      return 0;
    }

    @Override
    public RemoteStorageInfo getRemoteStorage() {
      return null;
    }

    @Override
    public PartitionSplitInfo getPartitionSplitInfo(int partitionId) {
      return new PartitionSplitInfo(1, true, PartitionSplitMode.LOAD_BALANCE, null);
    }
  }

  @Test
  public void testUpdatePartitionSplitAssignment() {
    TaskAttemptAssignment assignment = new TaskAttemptAssignment(1, new MockShuffleHandleInfo());
    assertTrue(
        assignment.updatePartitionSplitAssignment(
            1, Arrays.asList(new ShuffleServerInfo("localhost", 122))));
  }
}
