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

public class NoOpCodec extends Codec {

  private static class LazyHolder {
    static final NoOpCodec INSTANCE = new NoOpCodec();
  }

  public static NoOpCodec getInstance() {
    return LazyHolder.INSTANCE;
  }

  @Override
  public void decompress(ByteBuffer src, int uncompressedLen, ByteBuffer dest, int destOffset) {
    ByteBuffer destDuplicated = dest.duplicate();
    destDuplicated.position(destOffset);
    destDuplicated.put(src.duplicate());
  }

  @Override
  public byte[] compress(byte[] src) {
    byte[] dst = new byte[src.length];
    System.arraycopy(src, 0, dst, 0, src.length);
    return dst;
  }

  @Override
  public int compress(ByteBuffer src, ByteBuffer dest) {
    int destOff = dest.position();
    dest.put(src.duplicate());
    return dest.position() - destOff;
  }

  @Override
  public int maxCompressedLength(int sourceLength) {
    return sourceLength;
  }
}
