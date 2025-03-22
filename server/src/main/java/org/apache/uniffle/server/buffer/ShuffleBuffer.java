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

package org.apache.uniffle.server.buffer;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.server.ShuffleDataFlushEvent;

public interface ShuffleBuffer {
  long append(ShufflePartitionedData data);

  ShuffleDataFlushEvent toFlushEvent(
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      Supplier<Boolean> isValid,
      ShuffleDataDistributionType dataDistributionType);

  /** Only for test */
  ShuffleDataFlushEvent toFlushEvent(
      String appId, int shuffleId, int startPartition, int endPartition, Supplier<Boolean> isValid);

  ShuffleDataResult getShuffleData(long lastBlockId, int readBufferSize);

  ShuffleDataResult getShuffleData(
      long lastBlockId, int readBufferSize, Roaring64NavigableMap expectedTaskIds);

  /** @return the buffer memory size include encoded length */
  long getEncodedLength();

  /** @return the buffer block data size */
  long getDataLength();

  long getInFlushSize();

  /** Only for test */
  Set<ShufflePartitionedBlock> getBlocks();

  int getBlockCount();

  long getInFlushBlockCount();

  long release();

  void clearInFlushBuffer(long eventId);

  @VisibleForTesting
  Map<Long, Set<ShufflePartitionedBlock>> getInFlushBlockMap();
}
