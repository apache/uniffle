/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.uniffle.server.buffer.lab;

import org.apache.uniffle.common.ShufflePartitionedBlock;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class LAB {

  private AtomicReference<Chunk> currChunk = new AtomicReference<>();
  // Lock to manage multiple handlers requesting for a chunk
  private ReentrantLock lock = new ReentrantLock();

  List<Integer> chunks = new LinkedList<>();
  private final int dataChunkSize;
  private final int maxAlloc;
  private final ChunkCreator chunkCreator;

  public LAB() {
    this.chunkCreator = ChunkCreator.getInstance();
    dataChunkSize = chunkCreator.getChunkSize();
    maxAlloc = dataChunkSize / 5;
  }
  public ShufflePartitionedBlock tryCopyBlockToChunk(ShufflePartitionedBlock block) {
    int size = block.getLength();
    // Callers should satisfy large allocations from JVM heap so limit fragmentation.
    if (size > maxAlloc) {
      return block;
    }
    Chunk c = null;
    int allocOffset = 0;
    while (true) {
      // Try to get the chunk
      c = getOrMakeChunk();
      // We may get null because the some other thread succeeded in getting the lock
      // and so the current thread has to try again to make its chunk or grab the chunk
      // that the other thread created
      // Try to allocate from this chunk
      if (c != null) {
        allocOffset = c.alloc(size);
        if (allocOffset != -1) {
          break;
        }
        // not enough space!
        // try to retire this chunk
        tryRetireChunk(c);
      }
    }
    c.getData().writeBytes(block.getData());
    block.getData().release();
    return new ShufflePartitionedBlock(
        block.getLength(),
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

  /**
   * Try to retire the current chunk if it is still
   * <code>c</code>. Postcondition is that curChunk.get()
   * != c
   * @param c the chunk to retire
   */
  private void tryRetireChunk(Chunk c) {
    currChunk.compareAndSet(c, null);
    // If the CAS succeeds, that means that we won the race
    // to retire the chunk. We could use this opportunity to
    // update metrics on external fragmentation.
    //
    // If the CAS fails, that means that someone else already
    // retired the chunk for us.
  }

  /**
   * Get the current chunk, or, if there is no current chunk,
   * allocate a new one from the JVM.
   */
  private Chunk getOrMakeChunk() {
    // Try to get the chunk
    Chunk c = currChunk.get();
    if (c != null) {
      return c;
    }
    // No current chunk, so we want to allocate one. We race
    // against other allocators to CAS in an uninitialized chunk
    // (which is cheap to allocate)
    if (lock.tryLock()) {
      try {
        // once again check inside the lock
        c = currChunk.get();
        if (c != null) {
          return c;
        }
        c = this.chunkCreator.getChunk();
        if (c != null) {
          // set the curChunk. No need of CAS as only one thread will be here
          currChunk.set(c);
          chunks.add(c.getId());
          return c;
        }
      } finally {
        lock.unlock();
      }
    }
    return null;
  }
}

