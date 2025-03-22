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

package org.apache.uniffle.server.block;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.server.ShuffleTaskInfo;

/** The implementation of this interface to manage the shuffle block ids. */
public interface ShuffleBlockIdManager {
  void registerAppId(String appId);

  int addFinishedBlockIds(
      ShuffleTaskInfo taskInfo,
      String appId,
      Integer shuffleId,
      Map<Integer, long[]> partitionToBlockIds,
      int bitmapNum);

  byte[] getFinishedBlockIds(
      ShuffleTaskInfo taskInfo,
      String appId,
      Integer shuffleId,
      Set<Integer> partitions,
      BlockIdLayout blockIdLayout)
      throws IOException;

  void removeBlockIdByShuffleId(String appId, List<Integer> shuffleIds);

  void removeBlockIdByAppId(String appId);

  long getTotalBlockCount();

  long getBlockCountByShuffleId(String appId, List<Integer> shuffleIds);

  boolean contains(String testAppId);

  long getBitmapNum(String appId, int shuffleId);
}
