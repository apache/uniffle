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
import java.util.Optional;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.response.ShuffleBlock;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.RssUtils;

class SyncDecodedShuffleBlockIterator implements DecodedShuffleBlockIterator {
  private final ShuffleReadClient shuffleReadClient;
  private final Optional<Codec> codec;

  private DecodedShuffleBlock nextBlock;
  private boolean end;
  private long readMillis;
  private long decompressMillis;
  private long uncompressedBytes;
  private long rawBytes;

  SyncDecodedShuffleBlockIterator(ShuffleReadClient shuffleReadClient, Optional<Codec> codec) {
    this.shuffleReadClient = shuffleReadClient;
    this.codec = codec;
  }

  @Override
  public boolean hasNext() {
    if (nextBlock != null) {
      return true;
    }
    if (end) {
      return false;
    }

    long startRead = System.currentTimeMillis();
    ShuffleBlock shuffleBlock = shuffleReadClient.readShuffleBlockData();
    long blockReadMillis = System.currentTimeMillis() - startRead;
    readMillis += blockReadMillis;
    if (shuffleBlock == null) {
      end = true;
      return false;
    }

    nextBlock = decode(shuffleBlock, blockReadMillis);
    return nextBlock != null;
  }

  @Override
  public DecodedShuffleBlock next() {
    if (!hasNext()) {
      throw new java.util.NoSuchElementException();
    }
    DecodedShuffleBlock block = nextBlock;
    nextBlock = null;
    return block;
  }

  private DecodedShuffleBlock decode(ShuffleBlock shuffleBlock, long blockReadMillis) {
    ByteBuffer rawData = null;
    boolean rawBlockClosed = false;
    try {
      rawData = shuffleBlock.getByteBuffer();
      if (rawData == null) {
        shuffleBlock.close();
        throw new RssException("Shuffle block data buffer is null");
      }

      rawBytes += shuffleBlock.getCompressedLength();
      int uncompressedLength = shuffleBlock.getUncompressLength();
      if (uncompressedLength < 0) {
        throw new RssException("Uncompressed length is negative: " + uncompressedLength);
      }
      if (codec.isPresent()) {
        ByteBuffer decoded =
            rawData.isDirect()
                ? ByteBuffer.allocateDirect(uncompressedLength)
                : ByteBuffer.allocate(uncompressedLength);
        long startDecompress = System.currentTimeMillis();
        codec.get().decompress(rawData, uncompressedLength, decoded, 0);
        long blockDecompressMillis = System.currentTimeMillis() - startDecompress;
        decompressMillis += blockDecompressMillis;
        uncompressedBytes += uncompressedLength;
        decoded.position(0);
        decoded.limit(uncompressedLength);
        shuffleBlock.close();
        rawBlockClosed = true;
        return new DecodedShuffleBlock(
            decoded,
            shuffleBlock.getTaskAttemptId(),
            shuffleBlock.getCompressedLength(),
            uncompressedLength,
            blockReadMillis,
            blockDecompressMillis,
            () -> RssUtils.releaseByteBuffer(decoded));
      }

      uncompressedBytes += uncompressedLength;
      return new DecodedShuffleBlock(
          rawData,
          shuffleBlock.getTaskAttemptId(),
          shuffleBlock.getCompressedLength(),
          uncompressedLength,
          blockReadMillis,
          0,
          shuffleBlock::close);
    } catch (Exception e) {
      if (!rawBlockClosed) {
        shuffleBlock.close();
      }
      throw new RssException(e);
    }
  }

  @Override
  public long foregroundDecompressionMillis() {
    return decompressMillis;
  }

  @Override
  public long backgroundDecompressionMillis() {
    return 0;
  }

  @Override
  public long uncompressedBytes() {
    return uncompressedBytes;
  }

  @Override
  public long rawBytes() {
    return rawBytes;
  }

  @Override
  public long readMillis() {
    return readMillis;
  }

  @Override
  public void close() {
    if (nextBlock != null) {
      nextBlock.close();
      nextBlock = null;
    }
  }
}
