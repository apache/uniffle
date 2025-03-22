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

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleFlushManager;

public class ShuffleBufferWithSkipList extends AbstractShuffleBuffer {
  private ConcurrentSkipListMap<Long, ShufflePartitionedBlock> blocksMap;
  private final Map<Long, ConcurrentSkipListMap<Long, ShufflePartitionedBlock>> inFlushBlockMap;
  private int blockCount;

  public ShuffleBufferWithSkipList() {
    this.blocksMap = newConcurrentSkipListMap();
    this.inFlushBlockMap = JavaUtils.newConcurrentMap();
  }

  private ConcurrentSkipListMap<Long, ShufflePartitionedBlock> newConcurrentSkipListMap() {
    // We just need to ensure the order of taskAttemptId here for we need sort blocks when flush.
    // taskAttemptId is in the lowest bits of blockId, so we should reverse it when making
    // comparisons.
    return new ConcurrentSkipListMap<>(Comparator.comparingLong(Long::reverse));
  }

  @Override
  public synchronized long append(ShufflePartitionedData data) {
    if (evicted) {
      return BUFFER_EVICTED;
    }
    long currentEncodedLength = 0;
    long currentDataLength = 0;

    for (ShufflePartitionedBlock block : data.getBlockList()) {
      // If sendShuffleData retried, we may receive duplicate block. The duplicate
      // block would gc without release. Here we must release the duplicated block.
      if (!blocksMap.containsKey(block.getBlockId())) {
        blocksMap.put(block.getBlockId(), block);
        blockCount++;
        currentEncodedLength += block.getEncodedLength();
        currentDataLength += block.getDataLength();
      } else {
        block.getData().release();
      }
    }
    this.encodedLength += currentEncodedLength;
    this.dataLength += currentDataLength;

    return currentEncodedLength;
  }

  @Override
  public synchronized ShuffleDataFlushEvent toFlushEvent(
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      Supplier<Boolean> isValid,
      ShuffleDataDistributionType dataDistributionType) {
    if (blocksMap.isEmpty()) {
      return null;
    }
    Collection<ShufflePartitionedBlock> spBlocks = blocksMap.values();
    long eventId = ShuffleFlushManager.ATOMIC_EVENT_ID.getAndIncrement();
    final ShuffleDataFlushEvent event =
        new ShuffleDataFlushEvent(
            eventId,
            appId,
            shuffleId,
            startPartition,
            endPartition,
            encodedLength,
            dataLength,
            spBlocks,
            isValid,
            this);
    event.addCleanupCallback(
        () -> {
          this.clearInFlushBuffer(event.getEventId());
          spBlocks.forEach(spb -> spb.getData().release());
          inFlushSize.addAndGet(-event.getEncodedLength());
        });
    inFlushBlockMap.put(eventId, blocksMap);
    blocksMap = newConcurrentSkipListMap();
    blockCount = 0;
    inFlushSize.addAndGet(encodedLength);
    encodedLength = 0;
    dataLength = 0;
    return event;
  }

  @Override
  public Set<ShufflePartitionedBlock> getBlocks() {
    return new LinkedHashSet<>(blocksMap.values());
  }

  @Override
  public int getBlockCount() {
    return blockCount;
  }

  @Override
  public long getInFlushBlockCount() {
    return inFlushBlockMap.values().stream().mapToLong(Map::size).sum();
  }

  @Override
  public synchronized long release() {
    Throwable lastException = null;
    int failedToReleaseSize = 0;
    long releasedSize = 0;
    evicted = true;
    for (ShufflePartitionedBlock spb : blocksMap.values()) {
      try {
        spb.getData().release();
        releasedSize += spb.getEncodedLength();
      } catch (Throwable t) {
        lastException = t;
        failedToReleaseSize += spb.getEncodedLength();
      }
    }
    if (lastException != null) {
      LOG.warn(
          "Failed to release shuffle blocks with size (). Maybe it has been released by others.",
          failedToReleaseSize,
          lastException);
    }
    return releasedSize;
  }

  @Override
  public synchronized void clearInFlushBuffer(long eventId) {
    inFlushBlockMap.remove(eventId);
  }

  @Override
  public Map<Long, Set<ShufflePartitionedBlock>> getInFlushBlockMap() {
    return inFlushBlockMap.entrySet().stream()
        .collect(
            Collectors.toMap(Map.Entry::getKey, e -> new LinkedHashSet<>(e.getValue().values())));
  }

  @Override
  protected void updateBufferSegmentsAndResultBlocks(
      long lastBlockId,
      long readBufferSize,
      List<BufferSegment> bufferSegments,
      List<ShufflePartitionedBlock> resultBlocks,
      Roaring64NavigableMap expectedTaskIds) {
    long nextBlockId = lastBlockId;
    List<Long> eventIdList = Lists.newArrayList(inFlushBlockMap.keySet());
    List<Long> sortedEventId = sortFlushingEventId(eventIdList);
    int offset = 0;
    boolean hasLastBlockId = false;
    // read from inFlushBlockMap first to make sure the order of
    // data read is according to the order of data received
    // The number of events means how many batches are in flushing status,
    // it should be less than 5, or there has some problem with storage
    if (!inFlushBlockMap.isEmpty()) {
      for (Long eventId : sortedEventId) {
        hasLastBlockId =
            updateSegments(
                offset,
                inFlushBlockMap.get(eventId),
                readBufferSize,
                nextBlockId,
                bufferSegments,
                resultBlocks,
                expectedTaskIds);
        // if last blockId is found, read from begin with next cached blocks
        if (hasLastBlockId) {
          // reset blockId to read from begin in next cached blocks
          nextBlockId = Constants.INVALID_BLOCK_ID;
        }
        if (!bufferSegments.isEmpty()) {
          offset = calculateDataLength(bufferSegments);
        }
        if (offset >= readBufferSize) {
          break;
        }
      }
    }
    // try to read from cached blocks which is not in flush queue
    if (!blocksMap.isEmpty() && offset < readBufferSize) {
      hasLastBlockId =
          updateSegments(
              offset,
              blocksMap,
              readBufferSize,
              nextBlockId,
              bufferSegments,
              resultBlocks,
              expectedTaskIds);
    }
    if ((!inFlushBlockMap.isEmpty() || !blocksMap.isEmpty()) && offset == 0 && !hasLastBlockId) {
      // can't find lastBlockId, it should be flushed
      // but there still has data in memory
      // try read again with blockId = Constants.INVALID_BLOCK_ID
      updateBufferSegmentsAndResultBlocks(
          Constants.INVALID_BLOCK_ID,
          readBufferSize,
          bufferSegments,
          resultBlocks,
          expectedTaskIds);
    }
  }

  private boolean updateSegments(
      int offset,
      ConcurrentSkipListMap<Long, ShufflePartitionedBlock> cachedBlocks,
      long readBufferSize,
      long lastBlockId,
      List<BufferSegment> bufferSegments,
      List<ShufflePartitionedBlock> readBlocks,
      Roaring64NavigableMap expectedTaskIds) {
    int currentOffset = offset;
    ConcurrentNavigableMap<Long, ShufflePartitionedBlock> remainingBlocks;
    boolean hasLastBlockId;
    if (lastBlockId == Constants.INVALID_BLOCK_ID) {
      remainingBlocks = cachedBlocks;
    } else {
      if (cachedBlocks.get(lastBlockId) == null) {
        return false;
      }
      remainingBlocks = cachedBlocks.tailMap(lastBlockId, false);
    }

    hasLastBlockId = true;
    for (ShufflePartitionedBlock block : remainingBlocks.values()) {
      if (expectedTaskIds != null && !expectedTaskIds.contains(block.getTaskAttemptId())) {
        continue;
      }
      // add bufferSegment with block
      bufferSegments.add(
          new BufferSegment(
              block.getBlockId(),
              currentOffset,
              block.getDataLength(),
              block.getUncompressLength(),
              block.getCrc(),
              block.getTaskAttemptId()));
      readBlocks.add(block);
      // update offset
      currentOffset += block.getDataLength();
      if (currentOffset >= readBufferSize) {
        break;
      }
    }
    return hasLastBlockId;
  }

  public synchronized ShufflePartitionedBlock getBlock(long blockId) {
    ShufflePartitionedBlock block = blocksMap.get(blockId);
    if (block == null) {
      for (ConcurrentSkipListMap<Long, ShufflePartitionedBlock> map : inFlushBlockMap.values()) {
        if (map.containsKey(blockId)) {
          return map.get(blockId);
        }
      }
    }
    return block;
  }
}
