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
import java.nio.charset.StandardCharsets;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ByteBufUtils {

  public static int encodedLength(String s) {
    if (s == null) {
      return Integer.BYTES;
    }
    return Integer.BYTES + s.getBytes(StandardCharsets.UTF_8).length;
  }

  public static int encodedLength(ByteBuf buf) {
    return 4 + buf.readableBytes();
  }

  public static final void writeLengthAndString(ByteBuf buf, String str) {
    if (str == null) {
      buf.writeInt(-1);
      return;
    }

    byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
    buf.writeInt(bytes.length);
    buf.writeBytes(bytes);
  }

  public static final String readLengthAndString(ByteBuf buf) {
    int length = buf.readInt();
    if (length == -1) {
      return null;
    }

    byte[] bytes = new byte[length];
    buf.readBytes(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public static final void copyByteBuf(ByteBuf from, ByteBuf to) {
    to.writeInt(from.readableBytes());
    to.writeBytes(from);
    from.resetReaderIndex();
  }

  public static final byte[] readByteArray(ByteBuf byteBuf) {
    int length = byteBuf.readInt();
    if (length < 0) {
      return null;
    }
    byte[] data = new byte[length];
    byteBuf.readBytes(data);
    return data;
  }

  public static final ByteBuf readSlice(ByteBuf from) {
    int length = from.readInt();
    return from.retain().readSlice(length);
  }

  public static final byte[] readBytes(ByteBuf byteBuf) {
    ByteBuf buf = byteBuf.duplicate();
    byte[] bytes = new byte[buf.readableBytes()];
    buf.readBytes(bytes);
    return bytes;
  }

  public static void readBytes(ByteBuf from, byte[] to, int offset, int length) {
    from.readBytes(to, offset, length);
    from.resetReaderIndex();
  }

  public static ByteBuf byteStringToByteBuf(ByteString bytes) {
    final ByteBuffer byteBuffer = bytes.asReadOnlyByteBuffer();
    return Unpooled.wrappedBuffer(byteBuffer);
  }
}
