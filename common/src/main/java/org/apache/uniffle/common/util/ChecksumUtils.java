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

package org.apache.uniffle.common.util;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import io.netty.buffer.ByteBuf;

public class ChecksumUtils {

  private static final int LENGTH_PER_CRC = 4 * 1024;

  public static long getCrc32(byte[] buf) {
    return getCrc32(buf, 0, buf.length);
  }

  public static long getCrc32(byte[] buf, int offset, int length) {
    CRC32 crc32 = new CRC32();

    for (int i = 0; i < length; ) {
      int len = Math.min(LENGTH_PER_CRC, length - i);
      crc32.update(buf, i + offset, len);
      i += len;
    }

    return crc32.getValue();
  }

  public static long getCrc32(ByteBuffer byteBuffer) {
    return getCrc32(byteBuffer, byteBuffer.position(), byteBuffer.limit() - byteBuffer.position());
  }

  public static long getCrc32(ByteBuffer byteBuffer, int offset, int length) {
    CRC32 crc32 = new CRC32();
    ByteBuffer crcBuffer = byteBuffer.duplicate();
    crcBuffer.position(offset);
    for (int i = 0; i < length; ) {
      int len = Math.min(LENGTH_PER_CRC, length - i);
      crcBuffer.limit(crcBuffer.position() + len);
      crc32.update(crcBuffer);
      i += len;
    }
    return crc32.getValue();
  }

  public static long getCrc32(ByteBuf byteBuf) {
    final int offset = byteBuf.readerIndex();
    final int length = byteBuf.readableBytes();
    if (length == 0) {
      return 0L;
    }

    // Avoid coalescing/copy for composite buffers by iterating over nioBuffers.
    if (byteBuf.nioBufferCount() == 1) {
      return getCrc32(byteBuf.nioBuffer(offset, length));
    }

    CRC32 crc32 = new CRC32();
    for (ByteBuffer bb : byteBuf.nioBuffers(offset, length)) {
      // `nioBuffers` returns fresh ByteBuffer views; CRC32.update(ByteBuffer) only advances
      // the ByteBuffer position and won't affect the underlying ByteBuf indices.
      crc32.update(bb);
    }
    return crc32.getValue();
  }
}
