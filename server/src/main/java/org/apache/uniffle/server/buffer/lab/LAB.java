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

package org.apache.uniffle.server.buffer.lab;

import java.util.LinkedList;
import java.util.List;

import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.server.ShuffleServerMetrics;

/**
 * Local allocation buffer.
 *
 * <p>The LAB is basically a bump-the-pointer allocator that allocates big (100K) chunks from and
 * then doles it out to threads that request slices into the array. These chunks can get pooled as
 * well. See {@link ChunkCreator}.
 *
 * <p>The purpose of this is to combat heap fragmentation in the shuffle server. By ensuring that
 * all blocks in a given partition refer only to large chunks of contiguous memory, we ensure that
 * large blocks get freed up when the partition is flushed.
 *
 * <p>Without the LAB, the byte array allocated during insertion end up interleaved throughout the
 * heap, and the old generation gets progressively more fragmented until a stop-the-world compacting
 * collection occurs.
 *
 * <p>This manages the large sized chunks. When blocks are to be added to partition, LAB's {@link
 * #tryCopyBlockToChunk(ShufflePartitionedBlock)} gets called. This allocates enough size in the
 * chunk to hold this block's data and copies into this area and then recreate a
 * LABShufflePartitionedBlock over this copied data.
 *
 * <p>
 *
 * @see ChunkCreator
 */
public class LAB {
  private Chunk currChunk;

  List<Integer> chunks = new LinkedList<>();
  private final int maxAlloc;
  private final ChunkCreator chunkCreator;

  public LAB() {
    this.chunkCreator = ChunkCreator.getInstance();
    maxAlloc = chunkCreator.getMaxAlloc();
  }

  public ShufflePartitionedBlock tryCopyBlockToChunk(ShufflePartitionedBlock block) {
    int size = block.getDataLength();
    if (size > maxAlloc) {
      ShuffleServerMetrics.counterBlockNotOnLAB.inc();
      return block;
    }
    Chunk c;
    int allocOffset;
    while (true) {
      // Try to get the chunk
      c = getOrMakeChunk();
      // Try to allocate from this chunk
      allocOffset = c.getAllocOffset(size);
      if (allocOffset != -1) {
        break;
      }
      // not enough space!
      currChunk = null;
    }
    c.getData().writeBytes(block.getData());
    ShuffleServerMetrics.counterBlockOnLAB.inc();
    return new LABShufflePartitionedBlock(
        block.getDataLength(),
        block.getUncompressLength(),
        block.getCrc(),
        block.getBlockId(),
        block.getTaskAttemptId(),
        c.getData().slice(allocOffset, size));
  }

  public void close() {
    recycleChunks();
  }

  private void recycleChunks() {
    chunkCreator.putBackChunks(chunks);
  }

  /** Get the current chunk, or, if there is no current chunk, allocate a new one. */
  private Chunk getOrMakeChunk() {
    Chunk c = currChunk;
    if (c != null) {
      return c;
    }
    c = this.chunkCreator.getChunk();
    currChunk = c;
    chunks.add(c.getId());
    return c;
  }
}
