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

import io.netty.buffer.ByteBuf;

/** A chunk of memory out of which allocations are sliced. */
public abstract class Chunk {
  /** Actual underlying data */
  protected ByteBuf data;
  /** Size of chunk in bytes */
  protected final int size;
  // The unique id associated with the chunk.
  private final int id;
  private final boolean fromPool;

  /**
   * Create an uninitialized chunk. Note that memory is not allocated yet, so this is cheap.
   *
   * @param size in bytes
   * @param id the chunk id
   */
  public Chunk(int size, int id) {
    this(size, id, false);
  }

  /**
   * Create an uninitialized chunk. Note that memory is not allocated yet, so this is cheap.
   *
   * @param size in bytes
   * @param id the chunk id
   * @param fromPool if the chunk is formed by pool
   */
  public Chunk(int size, int id, boolean fromPool) {
    this.size = size;
    this.id = id;
    this.fromPool = fromPool;
  }

  int getId() {
    return this.id;
  }

  boolean isFromPool() {
    return this.fromPool;
  }

  /**
   * Actually claim the memory for this chunk. This should only be called from the thread that
   * constructed the chunk. It is thread-safe against other threads calling alloc(), who will block
   * until the allocation is complete.
   */
  public void init() {
    allocateDataBuffer();
  }

  abstract void allocateDataBuffer();

  /**
   * Try to get the allocated offset from the chunk.
   *
   * @return the offset of the successful allocation, or -1 to indicate not-enough-space
   */
  public int getAllocOffset(int size) {
    if (data.writerIndex() + size > data.capacity()) {
      return -1;
    }
    return data.writerIndex();
  }

  void reset() {
    data.clear();
  }

  /** @return This chunk's backing data. */
  ByteBuf getData() {
    return this.data;
  }
}
