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

import net.jpountz.lz4.LZ4Factory;

import org.apache.uniffle.common.exception.RssException;

public class Lz4Codec extends Codec {

  private LZ4Factory lz4Factory;

  private static class LazyHolder {
    static final Lz4Codec INSTANCE = new Lz4Codec();
  }

  public static Lz4Codec getInstance() {
    return LazyHolder.INSTANCE;
  }

  public Lz4Codec() {
    this.lz4Factory = LZ4Factory.safeInstance();
  }

  @Override
  public void decompress(ByteBuffer src, int uncompressedLen, ByteBuffer dest, int destOffset) {
    lz4Factory
        .fastDecompressor()
        .decompress(src, src.position(), dest, destOffset, uncompressedLen);
  }

  @Override
  public byte[] compress(byte[] src) {
    return lz4Factory.fastCompressor().compress(src);
  }

  @Override
  public int compress(ByteBuffer src, ByteBuffer dest) {
    try {
      int destOff = dest.position();
      lz4Factory.fastCompressor().compress(src.duplicate(), dest);
      return dest.position() - destOff;
    } catch (Exception e) {
      throw new RssException("Failed to compress by Lz4", e);
    }
  }

  @Override
  public int maxCompressedLength(int sourceLength) {
    return lz4Factory.fastCompressor().maxCompressedLength(sourceLength);
  }
}
