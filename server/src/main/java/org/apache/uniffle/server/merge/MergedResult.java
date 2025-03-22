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

package org.apache.uniffle.server.merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.SerOutputStream;
import org.apache.uniffle.common.util.NettyUtils;

import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MERGE_DEFAULT_MERGED_BLOCK_SIZE;

public class MergedResult {

  private final RssConf rssConf;
  private final long mergedBlockSize;
  // raw offset by blockId
  private final List<Long> offsets = new ArrayList<>();
  private final CacheMergedBlockFuntion cachedMergedBlock;
  private final Partition partition;

  public MergedResult(
      RssConf rssConf,
      CacheMergedBlockFuntion cachedMergedBlock,
      int mergedBlockSize,
      Partition partition) {
    this.rssConf = rssConf;
    this.cachedMergedBlock = cachedMergedBlock;
    this.mergedBlockSize =
        mergedBlockSize > 0
            ? mergedBlockSize
            : this.rssConf.getSizeAsBytes(
                SERVER_MERGE_DEFAULT_MERGED_BLOCK_SIZE.key(),
                SERVER_MERGE_DEFAULT_MERGED_BLOCK_SIZE.defaultValue());
    this.partition = partition;
    offsets.add(0L);
  }

  public SerOutputStream getOutputStream(boolean direct, long totalBytes) {
    return new MergedSegmentOutputStream(direct, totalBytes);
  }

  public boolean isOutOfBound(long blockId) {
    return blockId >= offsets.size();
  }

  public long getBlockSize(long blockId) {
    return offsets.get((int) blockId) - offsets.get((int) (blockId - 1));
  }

  @FunctionalInterface
  public interface CacheMergedBlockFuntion {
    boolean cache(ByteBuf byteBuf, long blockId, int length);
  }

  public class MergedSegmentOutputStream extends SerOutputStream {

    private final boolean direct;
    private final long totalBytes;
    private ByteBuf byteBuf;
    private long written = 0;

    MergedSegmentOutputStream(boolean direct, long totalBytes) {
      this.direct = direct;
      this.totalBytes = totalBytes;
    }

    public void finalizeBlock() throws IOException {
      // Avoid write empty block.
      if (written <= offsets.get(offsets.size() - 1)) {
        return;
      }
      int requireSize = byteBuf.readableBytes();
      // In fact, requireBuffer makes more sense before creating ByteBuf.
      // However, it is not easy to catch exceptions to release buffer.
      if (partition == null) {
        throw new IOException("Can't find partition!");
      }
      partition.requireMemory(requireSize);
      boolean success = false;
      try {
        success = cachedMergedBlock.cache(byteBuf, offsets.size(), requireSize);
      } finally {
        if (!success) {
          partition.releaseMemory(requireSize);
        }
      }
      offsets.add(written);
    }

    // If some record is bigger than mergedBlockSize, we should allocate enough buffer for this.
    // So when preallocate, we need to make sure the allocated buffer can write for the big record.
    private void allocateNewBuffer(int preAllocateSize) {
      if (this.byteBuf != null) {
        byteBuf.release();
        byteBuf = null;
      }
      int alloc = Math.max((int) Math.min(mergedBlockSize, totalBytes - written), preAllocateSize);
      UnpooledByteBufAllocator allocator = NettyUtils.getSharedUnpooledByteBufAllocator(true);
      // In grpc mode, we may use array to visit the underlying buffer directly.
      // We may still use array after release. But The pooled buffer may change
      // the underlying buffer. So we can not use pooled buffer.
      this.byteBuf = direct ? allocator.directBuffer(alloc) : Unpooled.buffer(alloc);
    }

    @Override
    public void write(ByteBuf from) throws IOException {
      preAllocate(from.readableBytes());
      int c = from.readableBytes();
      this.byteBuf.writeBytes(from);
      written += c;
    }

    @Override
    public void write(int b) throws IOException {
      preAllocate(1);
      this.byteBuf.writeByte((byte) (b & 0xFF));
      written++;
    }

    @Override
    public void preAllocate(int length) throws IOException {
      if (this.byteBuf == null || this.byteBuf.writableBytes() < length) {
        finalizeBlock();
        allocateNewBuffer(length);
      }
    }

    // Unlike the traditional flush, this flush can and must
    // only be called once before close.
    @Override
    public void flush() throws IOException {
      finalizeBlock();
    }

    @Override
    public void close() throws IOException {
      if (this.byteBuf != null) {
        this.byteBuf.release();
        this.byteBuf = null;
      }
    }
  }
}
