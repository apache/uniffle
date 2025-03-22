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

package org.apache.uniffle.common.compression;

import java.nio.ByteBuffer;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;

import static org.apache.uniffle.common.config.RssClientConf.ZSTD_COMPRESSION_LEVEL;
import static org.apache.uniffle.common.config.RssClientConf.ZSTD_COMPRESSION_WORKER_NUMBER;

public class ZstdCodec extends Codec {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZstdCodec.class);

  private int compressionLevel;
  private int workerNumber;

  private static class LazyHolder {
    static final ZstdCodec INSTANCE = new ZstdCodec();
  }

  public static ZstdCodec getInstance(RssConf conf) {
    LazyHolder.INSTANCE.compressionLevel = conf.get(ZSTD_COMPRESSION_LEVEL);
    LazyHolder.INSTANCE.workerNumber = conf.get(ZSTD_COMPRESSION_WORKER_NUMBER);
    return LazyHolder.INSTANCE;
  }

  @Override
  public void decompress(ByteBuffer src, int uncompressedLen, ByteBuffer dst, int dstOffset) {
    if (src.isDirect() && dst.isDirect()) {
      long size =
          Zstd.decompressDirectByteBuffer(
              dst, dstOffset, uncompressedLen, src, src.position(), src.limit() - src.position());
      if (size != uncompressedLen) {
        throw new RssException(
            "This should not happen that the decompressed data size is not equals to original size.");
      }
      return;
    }

    if (!src.isDirect() && !dst.isDirect()) {
      Zstd.decompressByteArray(
          dst.array(),
          dstOffset,
          uncompressedLen,
          src.array(),
          src.position(),
          src.limit() - src.position());
      return;
    }

    throw new IllegalStateException(
        "Zstd only supports the same type of bytebuffer decompression.");
  }

  @Override
  public byte[] compress(byte[] src) {
    ZstdCompressCtx ctx = new ZstdCompressCtx();
    try {
      ctx.setLevel(compressionLevel);
      if (workerNumber > 0) {
        ctx.setWorkers(workerNumber);
      }
      return ctx.compress(src);
    } finally {
      ctx.close();
    }
  }

  @Override
  public int compress(ByteBuffer src, ByteBuffer dest) {
    try {
      if (src.isDirect() && dest.isDirect()) {
        return Zstd.compress(dest, src.duplicate(), compressionLevel);
      }
      if (!src.isDirect() && !dest.isDirect()) {
        int destOff = dest.position();
        int compressedSize =
            (int)
                Zstd.compressByteArray(
                    dest.array(),
                    dest.position(),
                    dest.remaining(),
                    src.array(),
                    src.position(),
                    src.remaining(),
                    compressionLevel);
        dest.position(destOff + compressedSize);
        return compressedSize;
      }
    } catch (Exception e) {
      throw new RssException("Failed to compress by Zstd", e);
    }
    throw new IllegalStateException("Zstd only supports the same type of bytebuffer compression.");
  }

  @Override
  public int maxCompressedLength(int sourceLength) {
    return (int) Zstd.compressBound(sourceLength);
  }
}
