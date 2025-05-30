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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.google.common.collect.Lists;
import io.netty.buffer.CompositeByteBuf;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.NettyUtils;
import org.apache.uniffle.server.ShuffleDataFlushEvent;

public abstract class AbstractShuffleBuffer implements ShuffleBuffer {

  protected static final Logger LOG = LoggerFactory.getLogger(AbstractShuffleBuffer.class);

  /** The memory cost size include encoded length */
  protected long encodedLength;
  /** The data size of this shuffle block */
  protected long dataLength;

  protected AtomicLong inFlushSize = new AtomicLong();

  protected volatile boolean evicted;
  public static final long BUFFER_EVICTED = -1L;

  public AbstractShuffleBuffer() {
    this.encodedLength = 0;
    this.evicted = false;
  }

  /** Only for test */
  @Override
  public synchronized ShuffleDataFlushEvent toFlushEvent(
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      Supplier<Boolean> isValid) {
    return toFlushEvent(
        appId,
        shuffleId,
        startPartition,
        endPartition,
        isValid,
        ShuffleDataDistributionType.NORMAL);
  }

  @Override
  public long getEncodedLength() {
    return encodedLength;
  }

  @Override
  public long getDataLength() {
    return dataLength;
  }

  @Override
  public synchronized ShuffleDataResult getShuffleData(long lastBlockId, int readBufferSize) {
    return getShuffleData(lastBlockId, readBufferSize, null);
  }

  // 1. generate buffer segments and other info: if blockId exist, start with which eventId
  // 2. according to info from step 1, generate data
  // todo: if block was flushed, it's possible to get duplicated data
  @Override
  public synchronized ShuffleDataResult getShuffleData(
      long lastBlockId, int readBufferSize, Roaring64NavigableMap expectedTaskIds) {
    try {
      List<BufferSegment> bufferSegments = Lists.newArrayList();
      List<ShufflePartitionedBlock> readBlocks = Lists.newArrayList();
      updateBufferSegmentsAndResultBlocks(
          lastBlockId, readBufferSize, bufferSegments, readBlocks, expectedTaskIds);
      if (!bufferSegments.isEmpty()) {
        CompositeByteBuf byteBuf =
            new CompositeByteBuf(
                NettyUtils.getSharedUnpooledByteBufAllocator(true),
                true,
                Constants.COMPOSITE_BYTE_BUF_MAX_COMPONENTS);
        // copy result data
        updateShuffleData(readBlocks, byteBuf);
        return new ShuffleDataResult(byteBuf, bufferSegments);
      }
    } catch (Exception e) {
      LOG.error("Exception happened when getShuffleData in buffer", e);
    }
    return new ShuffleDataResult();
  }

  // here is the rule to read data in memory:
  // 1. read from inFlushBlockMap order by eventId asc, then from blocks
  // 2. if can't find lastBlockId, means related data may be flushed to storage, repeat step 1
  protected abstract void updateBufferSegmentsAndResultBlocks(
      long lastBlockId,
      long readBufferSize,
      List<BufferSegment> bufferSegments,
      List<ShufflePartitionedBlock> resultBlocks,
      Roaring64NavigableMap expectedTaskIds);

  protected int calculateDataLength(List<BufferSegment> bufferSegments) {
    BufferSegment bufferSegment = bufferSegments.get(bufferSegments.size() - 1);
    return bufferSegment.getOffset() + bufferSegment.getLength();
  }

  private void updateShuffleData(List<ShufflePartitionedBlock> readBlocks, CompositeByteBuf data) {
    int offset = 0;
    for (ShufflePartitionedBlock block : readBlocks) {
      // fill shuffle data
      try {
        data.addComponent(true, block.getData().retain());
      } catch (Exception e) {
        LOG.error(
            "Unexpected exception for System.arraycopy, length["
                + block.getDataLength()
                + "], offset["
                + offset
                + "], dataLength["
                + data.capacity()
                + "]",
            e);
        throw e;
      }
      offset += block.getDataLength();
    }
  }

  protected List<Long> sortFlushingEventId(List<Long> eventIdList) {
    eventIdList.sort(
        (id1, id2) -> {
          if (id1 > id2) {
            return 1;
          }
          return -1;
        });
    return eventIdList;
  }

  protected void updateSegmentsWithoutBlockId(
      int offset,
      Collection<ShufflePartitionedBlock> cachedBlocks,
      long readBufferSize,
      List<BufferSegment> bufferSegments,
      List<ShufflePartitionedBlock> readBlocks,
      Roaring64NavigableMap expectedTaskIds) {
    int currentOffset = offset;
    // read from first block
    for (ShufflePartitionedBlock block : cachedBlocks) {
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
      // check if length >= request buffer size
      if (currentOffset >= readBufferSize) {
        break;
      }
    }
  }

  @Override
  public long getInFlushSize() {
    return inFlushSize.get();
  }
}
