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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.ExceedHugePartitionHardLimitException;
import org.apache.uniffle.server.buffer.ShuffleBuffer;
import org.apache.uniffle.server.buffer.ShuffleBufferManager;

/** Huge partition utils. */
public class HugePartitionUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HugePartitionUtils.class);

  /**
   * Check if the partition size exceeds the huge partition size threshold limit.
   *
   * @param shuffleBufferManager the shuffle buffer manager
   * @param usedPartitionDataSize the used partition data size
   * @return true if the partition size exceeds the huge partition size threshold limit, otherwise
   *     false
   */
  public static boolean isHugePartition(
      ShuffleBufferManager shuffleBufferManager, long usedPartitionDataSize) {
    return usedPartitionDataSize > shuffleBufferManager.getHugePartitionSizeThreshold();
  }

  /**
   * Check if the partition is huge partition.
   *
   * @param shuffleTaskManager the shuffle task manager
   * @param appId the app id
   * @param shuffleId the shuffle id
   * @param partitionId the partition id
   * @return true if the partition is huge partition, otherwise false
   */
  public static boolean isHugePartition(
      ShuffleTaskManager shuffleTaskManager, String appId, int shuffleId, int partitionId) {
    return shuffleTaskManager != null
        && shuffleTaskManager.getShuffleTaskInfo(appId) != null
        && shuffleTaskManager.getShuffleTaskInfo(appId).isHugePartition(shuffleId, partitionId);
  }

  /**
   * Check if the partition size exceeds the huge partition size hard limit.
   *
   * @param shuffleBufferManager the shuffle buffer manager
   * @param usedPartitionDataSize the used partition data size
   * @return true if the partition size exceeds the huge partition size hard limit, otherwise false
   */
  public static boolean hasPartitionExceededHugeHardLimit(
      ShuffleBufferManager shuffleBufferManager, long usedPartitionDataSize) {
    return usedPartitionDataSize > shuffleBufferManager.getHugePartitionSizeHardLimit();
  }

  /**
   * Mark the partition as huge partition if the partition size exceeds the huge partition threshold
   * size.
   *
   * @param shuffleBufferManager the shuffle buffer manager
   * @param shuffleTaskInfo the shuffle task info
   * @param shuffleId the shuffle id
   * @param partitionId the partition id
   * @param partitionSize the partition size
   */
  public static void markHugePartition(
      ShuffleBufferManager shuffleBufferManager,
      ShuffleTaskInfo shuffleTaskInfo,
      int shuffleId,
      int partitionId,
      long partitionSize) {
    if (isHugePartition(shuffleBufferManager, partitionSize)) {
      shuffleTaskInfo.markHugePartition(shuffleId, partitionId);
    }
  }

  /**
   * Check if the partition size exceeds the huge hard limit size.
   *
   * @param operation the operation name
   * @param shuffleBufferManager the shuffle buffer manager
   * @param partitionSize the partition size
   * @param increaseSize the increase size
   */
  public static void checkExceedPartitionHardLimit(
      String operation,
      ShuffleBufferManager shuffleBufferManager,
      long partitionSize,
      long increaseSize) {
    if (HugePartitionUtils.hasPartitionExceededHugeHardLimit(shuffleBufferManager, partitionSize)) {
      throw new ExceedHugePartitionHardLimitException(
          operation
              + ": Current partition size: "
              + partitionSize
              + " exceeded the huge hard limit size: "
              + shuffleBufferManager.getHugePartitionSizeHardLimit()
              + " if cache this shuffle data with size: "
              + increaseSize);
    }
  }

  /**
   * Check if the partition size exceeds the huge memory limit, if so, trigger memory limitation.
   *
   * @param shuffleBufferManager the shuffle buffer manager
   * @param appId the app id
   * @param shuffleId the shuffle id
   * @param partitionId the partition id
   * @param usedPartitionDataSize the used partition data size
   */
  public static boolean limitHugePartition(
      ShuffleBufferManager shuffleBufferManager,
      String appId,
      int shuffleId,
      int partitionId,
      long usedPartitionDataSize) {
    if (usedPartitionDataSize > shuffleBufferManager.getHugePartitionSizeThreshold()) {
      ShuffleBuffer buffer =
          shuffleBufferManager.getShuffleBufferEntry(appId, shuffleId, partitionId).getValue();
      long memoryUsed = buffer.getInFlushSize() + buffer.getEncodedLength();
      if (memoryUsed > shuffleBufferManager.getHugePartitionMemoryLimitSize()) {
        LOG.warn(
            "AppId: {}, shuffleId: {}, partitionId: {}, memory used: {}, "
                + "huge partition triggered memory limitation.",
            appId,
            shuffleId,
            partitionId,
            memoryUsed);
        return true;
      }
    }
    return false;
  }

  public static boolean hasExceedPartitionSplitLimit(
      ShuffleBufferManager shuffleBufferManager, long usedPartitionDataSize) {
    return usedPartitionDataSize > shuffleBufferManager.getHugePartitionSplitLimit();
  }
}
