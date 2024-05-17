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

package org.apache.uniffle.shuffle;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.flink.shuffle.RssApplication;
import org.apache.uniffle.flink.shuffle.RssApplicationStateStore;

public class TestRssApplicationStateStore {
  @Test
  public void testGetRssTaskShuffleId() {
    // Prepare jobId
    JobID jobId = new JobID(10, 2);

    // Prepare partitionDescriptor
    IntermediateDataSetID resultId = new IntermediateDataSetID();
    RssApplication flinkApplication = new RssApplication();
    String shuffleId = flinkApplication.getTaskShuffleId(jobId, resultId);

    RssApplicationStateStore stateStore = new RssApplicationStateStore();

    for (int i = 0; i < 50; i++) {
      Integer rssTaskShuffleId = stateStore.getRssTaskShuffleId(shuffleId);
      Assertions.assertEquals(0, rssTaskShuffleId);
    }
  }
}
