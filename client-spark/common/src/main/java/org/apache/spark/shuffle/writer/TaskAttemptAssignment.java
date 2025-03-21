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

import java.util.List;
import java.util.Map;

import org.apache.spark.shuffle.handle.ShuffleHandleInfo;
import org.apache.spark.shuffle.handle.split.PartitionSplitInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.PartitionSplitMode;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;

/** This class is to get the partition assignment for ShuffleWriter. */
public class TaskAttemptAssignment {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskAttemptAssignment.class);

  private Map<Integer, List<ShuffleServerInfo>> assignment;
  private ShuffleHandleInfo handle;
  private final long taskAttemptId;

  public TaskAttemptAssignment(long taskAttemptId, ShuffleHandleInfo shuffleHandleInfo) {
    this.update(shuffleHandleInfo);
    this.handle = shuffleHandleInfo;
    this.taskAttemptId = taskAttemptId;
  }

  /**
   * Retrieving the partition's current available shuffleServers.
   *
   * @param partitionId
   * @return
   */
  public List<ShuffleServerInfo> retrieve(int partitionId) {
    return assignment.get(partitionId);
  }

  public void update(ShuffleHandleInfo handle) {
    if (handle == null) {
      throw new RssException("Errors on updating shuffle handle by the empty handleInfo.");
    }
    this.assignment = handle.getAvailablePartitionServersForWriter();
    this.handle = handle;
  }

  public boolean isSkipPartitionSplit(int partitionId) {
    // for those load balance partition split, once split, skip the following split.
    PartitionSplitInfo splitInfo = this.handle.getPartitionSplitInfo(partitionId);
    return splitInfo.isSplit() && splitInfo.getMode() == PartitionSplitMode.LOAD_BALANCE;
  }
}
