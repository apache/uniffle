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
      return block;
    }
    Chunk c;
    int allocOffset;
    while (true) {
      // Try to get the chunk
      c = getOrMakeChunk();
      // Try to allocate from this chunk
      if (c != null) {
        allocOffset = c.alloc(size);
        if (allocOffset != -1) {
          break;
        }
        // not enough space!
        currChunk = null;
      }
    }
    c.getData().writeBytes(block.getData());
    block.getData().release();
    return new ShufflePartitionedBlock(
        block.getDataLength(),
        block.getUncompressLength(),
        block.getCrc(),
        block.getBlockId(),
        block.getTaskAttemptId(),
        c.getData().slice(allocOffset, size),
        true);
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
