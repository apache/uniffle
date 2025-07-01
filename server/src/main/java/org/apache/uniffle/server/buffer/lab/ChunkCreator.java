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

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Does the management of LAB chunk creations. A monotonically incrementing id is associated with
 * every chunk
 */
public class ChunkCreator {
  private static final Logger LOG = LoggerFactory.getLogger(ChunkCreator.class);
  // monotonically increasing chunkid. Starts at 1.
  private final AtomicInteger chunkID = new AtomicInteger(1);

  // mapping from chunk IDs to chunks
  private final Map<Integer, Chunk> chunkIdMap = new ConcurrentHashMap<Integer, Chunk>();
  static ChunkCreator instance;
  private final int maxAlloc;
  private final ChunkPool chunksPool;
  private final int chunkSize;

  ChunkCreator(int chunkSize, long bufferCapacity, int maxAlloc) {
    this.chunkSize = chunkSize;
    this.maxAlloc = maxAlloc;
    this.chunksPool = initializePool(bufferCapacity, chunkSize);
  }

  /**
   * Initializes the instance of ChunkCreator
   *
   * @param chunkSize the chunkSize
   * @param bufferCapacity the buffer capacity
   * @return singleton ChunkCreator
   */
  public static synchronized void initialize(int chunkSize, long bufferCapacity, int maxAlloc) {
    if (instance != null) {
      LOG.warn("ChunkCreator instance is already initialized.");
    }
    instance = new ChunkCreator(chunkSize, bufferCapacity, maxAlloc);
  }

  public static ChunkCreator getInstance() {
    return instance;
  }

  /**
   * Creates and inits a chunk with specific index type and type.
   *
   * @return the chunk that was initialized
   */
  Chunk getChunk() {
    return getChunk(chunksPool.getChunkSize());
  }

  /**
   * Creates and inits a chunk.
   *
   * @return the chunk that was initialized
   * @param size the size of the chunk to be allocated, in bytes
   */
  Chunk getChunk(int size) {
    Chunk chunk = null;
    ChunkPool pool = null;

    // if the size is suitable for one of the pools
    if (chunksPool != null && size == chunksPool.getChunkSize()) {
      pool = chunksPool;
    }

    if (pool != null) {
      chunk = pool.getChunk();
      if (chunk == null) {
        LOG.warn(
            "The chunk pool is full. Reached maxCount= "
                + pool.getMaxCount()
                + ". Creating chunk outside of the pool.");
      }
    }

    if (chunk == null) {
      chunk = createChunk(false, size);
    }
    chunk.init();
    return chunk;
  }

  /**
   * Creates the chunk
   *
   * @param pool indicates if the chunks have to be created which will be used by the Pool
   * @param size the size of the chunk to be allocated, in bytes
   * @return the chunk
   */
  private Chunk createChunk(boolean pool, int size) {
    Chunk chunk;
    int id = chunkID.getAndIncrement();
    // if chunkID overflow, reset it.
    if (id <= 0) {
      chunkID.compareAndSet(id, chunksPool.maxCount + 1);
      id = chunkID.getAndIncrement();
    }
    Preconditions.checkArgument(id > 0, "chunkId should be positive.");
    chunk = new OffheapChunk(size, id, pool);
    this.chunkIdMap.put(chunk.getId(), chunk);
    return chunk;
  }

  // Chunks from pool are created covered with strong references anyway
  private Chunk createChunkForPool(int chunkSize) {
    if (chunkSize != chunksPool.getChunkSize()) {
      return null;
    }
    return createChunk(true, chunkSize);
  }

  private void removeChunks(List<Integer> chunkIDs) {
    chunkIDs.forEach(this::removeChunk);
  }

  Chunk removeChunk(int chunkId) {
    Chunk c = this.chunkIdMap.remove(chunkId);
    c.getData().release();
    return c;
  }

  /**
   * A pool of {@link Chunk} instances.
   *
   * <p>ChunkPool caches a number of retired chunks for reusing, it could decrease allocating bytes
   * when writing, thereby optimizing the garbage collection on JVM.
   */
  private class ChunkPool {
    private final int chunkSize;
    private final int maxCount;

    // A queue of reclaimed chunks
    private final BlockingQueue<Chunk> reclaimedChunks;

    /** Statistics thread schedule pool */
    private final ScheduledExecutorService scheduleThreadPool;
    /** Statistics thread */
    private static final int statThreadPeriod = 60 * 5;

    private final AtomicLong chunkCount = new AtomicLong();
    private final LongAdder reusedChunkCount = new LongAdder();

    ChunkPool(int chunkSize, int maxCount) {
      this.chunkSize = chunkSize;
      this.maxCount = maxCount;
      this.reclaimedChunks = new LinkedBlockingQueue<>();
      final String n = Thread.currentThread().getName();
      scheduleThreadPool =
          Executors.newScheduledThreadPool(
              1,
              new ThreadFactoryBuilder()
                  .setNameFormat(n + "-ChunkPool Statistics")
                  .setDaemon(true)
                  .build());
      this.scheduleThreadPool.scheduleAtFixedRate(
          new StatisticsThread(), statThreadPeriod, statThreadPeriod, TimeUnit.SECONDS);
    }

    /**
     * Poll a chunk from the pool, reset it if not null, else create a new chunk to return if we
     * have not yet created max allowed chunks count. When we have already created max allowed
     * chunks and no free chunks as of now, return null. It is the responsibility of the caller to
     * make a chunk then. Note: Chunks returned by this pool must be put back to the pool after its
     * use.
     *
     * @return a chunk
     * @see #putbackChunks(Chunk)
     */
    Chunk getChunk() {
      Chunk chunk = reclaimedChunks.poll();
      if (chunk != null) {
        chunk.reset();
        reusedChunkCount.increment();
      } else {
        // Make a chunk if we have not yet created the maxCount chunks
        while (true) {
          long created = this.chunkCount.get();
          if (created < this.maxCount) {
            if (this.chunkCount.compareAndSet(created, created + 1)) {
              chunk = createChunkForPool(chunkSize);
              break;
            }
          } else {
            break;
          }
        }
      }
      return chunk;
    }

    int getChunkSize() {
      return chunkSize;
    }

    /**
     * Add the chunks to the pool, when the pool achieves the max size, it will skip the remaining
     * chunks
     */
    private void putbackChunks(Chunk c) {
      int toAdd = this.maxCount - reclaimedChunks.size();
      if (c.isFromPool() && c.size == chunkSize && toAdd > 0) {
        reclaimedChunks.add(c);
      } else {
        // remove the chunk (that is not going to pool)
        ChunkCreator.this.removeChunk(c.getId());
      }
    }

    private class StatisticsThread extends Thread {
      StatisticsThread() {
        super("MemStoreChunkPool.StatisticsThread");
        setDaemon(true);
      }

      @Override
      public void run() {
        logStats();
      }

      private void logStats() {
        long created = chunkCount.get();
        long reused = reusedChunkCount.sum();
        long total = created + reused;
        LOG.info(
            "ChunkPool stats (chunk size={}): current pool size={}, created chunk count={}, "
                + "reused chunk count={}, reuseRatio={}",
            chunkSize,
            reclaimedChunks.size(),
            created,
            reused,
            (total == 0 ? "0" : StringUtils.formatPercent((float) reused / (float) total, 2)));
      }
    }

    private int getMaxCount() {
      return this.maxCount;
    }
  }

  private ChunkPool initializePool(long bufferCapacity, int chunkSize) {
    int maxCount = Math.max((int) (bufferCapacity / chunkSize), 1);
    LOG.info(
        "Allocating ChunkPool with chunk size {}, max count {}",
        StringUtils.byteDesc(chunkSize),
        maxCount);
    return new ChunkPool(chunkSize, maxCount);
  }

  int getChunkSize() {
    return chunkSize;
  }

  int getMaxAlloc() {
    return maxAlloc;
  }

  synchronized void putBackChunks(List<Integer> chunks) {
    // if there is no pool just try to clear the chunkIdMap in case there is something
    if (chunksPool == null) {
      this.removeChunks(chunks);
      return;
    }

    // if there is a pool, go over all chunk IDs that came back, the chunks may be from pool or not
    for (int chunkID : chunks) {
      // translate chunk ID to chunk, if chunk initially wasn't in pool
      // this translation will (most likely) return null
      Chunk chunk = chunkIdMap.get(chunkID);
      if (chunk != null) {
        if (chunk.isFromPool()) {
          chunksPool.putbackChunks(chunk);
        } else {
          // chunks which are not from one of the pools
          // should be released without going to the pools.
          this.removeChunk(chunkID);
        }
      } else {
        LOG.warn("Chunk {} can not be found in chunkIdMap, ignore it", chunkID);
      }
    }
  }
}
