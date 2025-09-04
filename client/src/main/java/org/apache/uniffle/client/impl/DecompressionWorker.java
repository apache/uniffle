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

package org.apache.uniffle.client.impl;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.response.DecompressedShuffleBlock;
import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.util.JavaUtils;

public class DecompressionWorker {
  private static final Logger LOG = LoggerFactory.getLogger(DecompressionWorker.class);

  private final ExecutorService executorService;
  private final ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, DecompressedShuffleBlock>>
      tasks;
  private Codec codec;
  private final ThreadLocal<ByteBuffer> bufferLocal =
      ThreadLocal.withInitial(() -> ByteBuffer.allocate(0));

  public DecompressionWorker(Codec codec, int threads) {
    if (codec == null) {
      throw new IllegalArgumentException("Codec cannot be null");
    }
    if (threads <= 0) {
      throw new IllegalArgumentException("Threads must be greater than 0");
    }
    this.tasks = JavaUtils.newConcurrentMap();
    this.executorService = Executors.newFixedThreadPool(threads);
    this.codec = codec;
  }

  public void add(int batchIndex, ShuffleDataResult shuffleDataResult) {
    List<BufferSegment> bufferSegments = shuffleDataResult.getBufferSegments();
    ByteBuffer sharedByteBuffer = shuffleDataResult.getDataBuffer();
    int index = 0;
    LOG.debug(
        "Adding {} segments with batch index:{} to decompression worker",
        bufferSegments.size(),
        batchIndex);
    for (BufferSegment bufferSegment : bufferSegments) {
      CompletableFuture<ByteBuffer> f =
          CompletableFuture.supplyAsync(
              () -> {
                int offset = bufferSegment.getOffset();
                int length = bufferSegment.getLength();
                ByteBuffer buffer = sharedByteBuffer.duplicate();
                buffer.position(offset);
                buffer.limit(offset + length);

                int uncompressedLen = bufferSegment.getUncompressLength();
                ByteBuffer dst =
                    buffer.isDirect()
                        ? ByteBuffer.allocateDirect(uncompressedLen)
                        : ByteBuffer.allocate(uncompressedLen);
                codec.decompress(buffer, uncompressedLen, dst, 0);
                return dst;
              },
              executorService);
      ConcurrentHashMap<Integer, DecompressedShuffleBlock> blocks =
          tasks.computeIfAbsent(batchIndex, k -> new ConcurrentHashMap<>());
      blocks.put(index++, new DecompressedShuffleBlock(f));
    }
  }

  public DecompressedShuffleBlock get(int batchIndex, int segmentIndex) {
    ConcurrentHashMap<Integer, DecompressedShuffleBlock> blocks = tasks.get(batchIndex);
    if (blocks == null) {
      return null;
    }
    DecompressedShuffleBlock block = blocks.remove(segmentIndex);
    return block;
  }

  public void close() {
    executorService.shutdown();
  }
}
