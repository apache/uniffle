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

package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.common.IdUtils;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;

// Although RMRssShuffleScheduler is extended from ShuffleScheduler, it is only used
// to handle DME events. Because the protobuf-java version of tez is different from
// uniffle, it is not convenient to parse DME events. It means there is no need to
// start RMRssShuffleScheduler.
public class RMRssShuffleScheduler extends ShuffleScheduler {

  private RMRssShuffle rssShuffle;

  public RMRssShuffleScheduler(
      InputContext inputContext,
      Configuration conf,
      int numberOfInputs,
      ExceptionReporter exceptionReporter,
      MergeManager mergeManager,
      FetchedInputAllocatorOrderedGrouped allocator,
      long startTime,
      CompressionCodec codec,
      boolean ifileReadAhead,
      int ifileReadAheadLength,
      String srcNameTrimmed,
      RMRssShuffle rssShuffle)
      throws IOException {
    super(
        inputContext,
        conf,
        numberOfInputs,
        exceptionReporter,
        mergeManager,
        allocator,
        startTime,
        codec,
        ifileReadAhead,
        ifileReadAheadLength,
        srcNameTrimmed);
    this.rssShuffle = rssShuffle;
  }

  @Override
  public synchronized void addKnownMapOutput(
      String inputHostName, int port, int partitionId, CompositeInputAttemptIdentifier srcAttempt) {
    super.addKnownMapOutput(inputHostName, port, partitionId, srcAttempt);
    rssShuffle.partitionIds.add(partitionId);
    if (!rssShuffle.partitionIdToSuccessMapTaskAttempts.containsKey(partitionId)) {
      rssShuffle.partitionIdToSuccessMapTaskAttempts.put(partitionId, new HashSet<>());
    }
    rssShuffle.partitionIdToSuccessMapTaskAttempts.get(partitionId).add(srcAttempt);

    String pathComponent = srcAttempt.getPathComponent();
    TezTaskAttemptID tezTaskAttemptId = IdUtils.convertTezTaskAttemptID(pathComponent);
    if (!rssShuffle.partitionIdToSuccessTezTasks.containsKey(partitionId)) {
      rssShuffle.partitionIdToSuccessTezTasks.put(partitionId, new HashSet<>());
    }
    rssShuffle.partitionIdToSuccessTezTasks.get(partitionId).add(tezTaskAttemptId.getTaskID());
  }
}
