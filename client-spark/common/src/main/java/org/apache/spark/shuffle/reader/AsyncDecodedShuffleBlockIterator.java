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

package org.apache.spark.shuffle.reader;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.response.ShuffleBlock;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.common.util.ThreadUtils;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_READ_OVERLAPPING_DECOMPRESSION_THREADS;

class AsyncDecodedShuffleBlockIterator implements DecodedShuffleBlockIterator {
  private final ShuffleReadClient shuffleReadClient;
  private final Optional<Codec> codec;
  private final ExecutorService executorService;
  private final Queue<CompletableFuture<DecodedShuffleBlock>> pendingBlocks = new ArrayDeque<>();
  private final DecodedBufferLimiter decodedBufferLimiter;
  private final int maxInFlightBlocks;

  private final AtomicLong backgroundDecompressMillis = new AtomicLong(0);
  private final AtomicLong uncompressedBytes = new AtomicLong(0);
  private final AtomicLong rawBytes = new AtomicLong(0);
  private long foregroundDecompressMillis;
  private long readMillis;
  private long sequence;
  private boolean end;
  private boolean closed;

  AsyncDecodedShuffleBlockIterator(
      ShuffleReadClient shuffleReadClient, Optional<Codec> codec, RssConf rssConf) {
    this.shuffleReadClient = shuffleReadClient;
    this.codec = codec;
    int threads = Math.max(1, rssConf.get(RSS_READ_OVERLAPPING_DECOMPRESSION_THREADS));
    this.executorService =
        Executors.newFixedThreadPool(
            threads, ThreadUtils.getThreadFactory("rss-async-decompression"));
    this.maxInFlightBlocks =
        Math.max(
            1,
            rssConf.get(RssClientConf.RSS_READ_OVERLAPPING_DECOMPRESSION_MAX_CONCURRENT_SEGMENTS));
    long decodedBudget =
        rssConf.getSizeAsBytes(
            RssClientConf.RSS_CLIENT_READ_BUFFER_SIZE.key(),
            RssClientConf.RSS_CLIENT_READ_BUFFER_SIZE.defaultValue());
    this.decodedBufferLimiter = new DecodedBufferLimiter(decodedBudget);
  }

  @Override
  public boolean hasNext() {
    fill();
    return !pendingBlocks.isEmpty();
  }

  @Override
  public DecodedShuffleBlock next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    CompletableFuture<DecodedShuffleBlock> future = pendingBlocks.poll();
    try {
      long startWait = System.currentTimeMillis();
      DecodedShuffleBlock block = future.get();
      long waitMillis = System.currentTimeMillis() - startWait;
      block.addDecodeWaitMillis(waitMillis);
      foregroundDecompressMillis += waitMillis;
      fill();
      return block;
    } catch (Exception e) {
      throw new RssException(e);
    }
  }

  private void fill() {
    while (!closed && !end && pendingBlocks.size() < maxInFlightBlocks) {
      long startRead = System.currentTimeMillis();
      ShuffleBlock shuffleBlock = shuffleReadClient.readRetainedShuffleBlockData();
      long blockReadMillis = System.currentTimeMillis() - startRead;
      readMillis += blockReadMillis;
      if (shuffleBlock == null) {
        end = true;
        return;
      }
      long blockSequence = sequence++;
      pendingBlocks.add(
          CompletableFuture.supplyAsync(
              () -> decode(shuffleBlock, blockSequence, blockReadMillis), executorService));
    }
  }

  private DecodedShuffleBlock decode(
      ShuffleBlock shuffleBlock, long blockSequence, long blockReadMillis) {
    DecodedBufferLimiter.Permit permit = null;
    boolean rawBlockClosed = false;
    try {
      ByteBuffer rawData = shuffleBlock.getByteBuffer();
      if (rawData == null) {
        permit = decodedBufferLimiter.acquire(blockSequence, 0);
        permit.release();
        shuffleBlock.close();
        throw new RssException("Shuffle block data buffer is null");
      }

      rawBytes.addAndGet(shuffleBlock.getCompressedLength());
      int uncompressedLength = shuffleBlock.getUncompressLength();
      if (uncompressedLength < 0) {
        throw new RssException("Uncompressed length is negative: " + uncompressedLength);
      }
      if (codec.isPresent()) {
        permit = decodedBufferLimiter.acquire(blockSequence, uncompressedLength);
        ByteBuffer decoded =
            rawData.isDirect()
                ? ByteBuffer.allocateDirect(uncompressedLength)
                : ByteBuffer.allocate(uncompressedLength);
        long startDecompress = System.currentTimeMillis();
        codec.get().decompress(rawData, uncompressedLength, decoded, 0);
        backgroundDecompressMillis.addAndGet(System.currentTimeMillis() - startDecompress);
        uncompressedBytes.addAndGet(uncompressedLength);
        decoded.position(0);
        decoded.limit(uncompressedLength);
        shuffleBlock.close();
        rawBlockClosed = true;
        DecodedBufferLimiter.Permit decodedPermit = permit;
        permit = null;
        return new DecodedShuffleBlock(
            decoded,
            shuffleBlock.getTaskAttemptId(),
            shuffleBlock.getCompressedLength(),
            uncompressedLength,
            blockReadMillis,
            0,
            () -> {
              RssUtils.releaseByteBuffer(decoded);
              decodedPermit.release();
            });
      }

      permit = decodedBufferLimiter.acquire(blockSequence, 0);
      permit.release();
      permit = null;
      uncompressedBytes.addAndGet(uncompressedLength);
      return new DecodedShuffleBlock(
          rawData,
          shuffleBlock.getTaskAttemptId(),
          shuffleBlock.getCompressedLength(),
          uncompressedLength,
          blockReadMillis,
          0,
          shuffleBlock::close);
    } catch (Exception e) {
      if (permit != null) {
        permit.release();
      }
      if (!rawBlockClosed) {
        shuffleBlock.close();
      }
      throw new RssException(e);
    }
  }

  @Override
  public long foregroundDecompressionMillis() {
    return foregroundDecompressMillis;
  }

  @Override
  public long backgroundDecompressionMillis() {
    return backgroundDecompressMillis.get();
  }

  @Override
  public long uncompressedBytes() {
    return uncompressedBytes.get();
  }

  @Override
  public long rawBytes() {
    return rawBytes.get();
  }

  @Override
  public long readMillis() {
    return readMillis;
  }

  @Override
  public void close() {
    closed = true;
    decodedBufferLimiter.close();
    while (!pendingBlocks.isEmpty()) {
      CompletableFuture<DecodedShuffleBlock> future = pendingBlocks.poll();
      try {
        DecodedShuffleBlock block = future.get();
        if (block != null) {
          block.close();
        }
      } catch (Exception e) {
        // Decode failures have already closed their retained input blocks.
      }
    }
    executorService.shutdownNow();
  }
}
