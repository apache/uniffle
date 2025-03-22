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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.ByteBufferUtils;

import static org.apache.uniffle.common.config.RssClientConf.COMPRESSION_TYPE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CompressionTest {

  static List<Arguments> testCompression() {
    int[] sizes = {1, 1024, 128 * 1024, 512 * 1024, 1024 * 1024, 4 * 1024 * 1024};
    Codec.Type[] types = {Codec.Type.ZSTD, Codec.Type.LZ4, Codec.Type.SNAPPY, Codec.Type.NOOP};

    List<Arguments> arguments = new ArrayList<>();
    for (int size : sizes) {
      for (Codec.Type type : types) {
        arguments.add(Arguments.of(size, type));
      }
    }
    return arguments;
  }

  @ParameterizedTest
  @MethodSource
  public void testCompression(int size, Codec.Type type) {
    byte[] data = RandomUtils.nextBytes(size);
    RssConf conf = new RssConf();
    conf.set(COMPRESSION_TYPE, type);

    // case1: heap bytebuffer
    Codec codec = Codec.newInstance(conf).get();
    byte[] compressed = codec.compress(data);

    ByteBuffer dest = ByteBuffer.allocate(size);
    codec.decompress(ByteBuffer.wrap(compressed), size, dest, 0);

    assertArrayEquals(data, dest.array());

    // case2: non-heap bytebuffer
    ByteBuffer src = ByteBuffer.allocateDirect(compressed.length);
    src.put(compressed);
    src.flip();
    ByteBuffer dst = ByteBuffer.allocateDirect(size);
    codec.decompress(src, size, dst, 0);
    byte[] res = new byte[size];
    dst.get(res);
    assertArrayEquals(data, res);

    // case3: use the recycled bytebuffer
    ByteBuffer recycledDst = ByteBuffer.allocate(size + 10);
    codec.decompress(ByteBuffer.wrap(compressed), size, recycledDst, 0);
    recycledDst.get(res);
    assertArrayEquals(data, res);

    // case4: use off heap bytebuffer compress
    ByteBuffer srcBuffer = ByteBuffer.allocateDirect(size);
    ByteBuffer destBuffer = ByteBuffer.allocateDirect(codec.maxCompressedLength(size));
    testCompressWithByteBuffer(codec, data, srcBuffer, destBuffer, 0);

    // case5: use on heap bytebuffer compress
    srcBuffer = ByteBuffer.allocate(size);
    destBuffer = ByteBuffer.allocate(codec.maxCompressedLength(size));
    testCompressWithByteBuffer(codec, data, srcBuffer, destBuffer, 0);

    // case6: src buffer is on heap && dest buffer is off heap
    srcBuffer = ByteBuffer.allocate(size);
    destBuffer = ByteBuffer.allocateDirect(codec.maxCompressedLength(size));
    testCompressWithByteBuffer(codec, data, srcBuffer, destBuffer, 0);

    // case7: src buffer is off heap && dest buffer is on heap
    srcBuffer = ByteBuffer.allocateDirect(size);
    destBuffer = ByteBuffer.allocate(codec.maxCompressedLength(size));
    testCompressWithByteBuffer(codec, data, srcBuffer, destBuffer, 0);

    // case8: use src&dest bytebuffer with offset
    int destOffset = 10;
    srcBuffer = ByteBuffer.allocateDirect(size + destOffset);
    destBuffer = ByteBuffer.allocateDirect(codec.maxCompressedLength(size) + destOffset);
    testCompressWithByteBuffer(codec, data, srcBuffer, destBuffer, destOffset);
  }

  private void testCompressWithByteBuffer(
      Codec codec, byte[] originData, ByteBuffer srcBuffer, ByteBuffer destBuffer, int destOffset) {
    srcBuffer.position(destOffset);
    srcBuffer.put(originData);
    srcBuffer.flip();
    srcBuffer.position(destOffset);
    destBuffer.position(destOffset);
    if (!isSameType(srcBuffer, destBuffer)
        && (codec instanceof SnappyCodec || codec instanceof ZstdCodec)) {
      try {
        codec.compress(srcBuffer, destBuffer);
      } catch (Exception e) {
        assertTrue(e instanceof IllegalStateException);
      }
    } else {
      int compressedLength = codec.compress(srcBuffer, destBuffer);
      assertEquals(destBuffer.position(), destOffset + compressedLength);
      assertEquals(srcBuffer.position(), destOffset);
      destBuffer.flip();
      destBuffer.position(destOffset);
      srcBuffer.clear();
      checkCompressedData(codec, originData, srcBuffer, destBuffer);
    }
  }

  private boolean isSameType(ByteBuffer srcBuffer, ByteBuffer destBuffer) {
    if (srcBuffer == null || destBuffer == null) {
      return false;
    }
    return (srcBuffer.isDirect() && destBuffer.isDirect())
        || (!srcBuffer.isDirect() && !destBuffer.isDirect());
  }

  private void checkCompressedData(
      Codec codec, byte[] originData, ByteBuffer dest, ByteBuffer src) {
    codec.decompress(src, originData.length, dest, 0);
    byte[] res = new byte[originData.length];
    dest.get(res);
    assertArrayEquals(originData, res);
  }

  @Test
  public void checkDecompressBufferOffsets() {
    byte[] data = RandomUtils.nextBytes(1024);
    // Snappy decompression does not support non-zero offset for destination direct ByteBuffer
    Codec.Type[] types = {Codec.Type.ZSTD, Codec.Type.LZ4, Codec.Type.NOOP};
    Boolean[] isDirects = {true, false};
    for (Boolean isDirect : isDirects) {
      for (Codec.Type type : types) {
        Codec codec = Codec.newInstance(new RssConf().set(COMPRESSION_TYPE, type)).get();
        byte[] compressed = codec.compress(data);

        ByteBuffer src;
        if (isDirect) {
          src = ByteBuffer.allocateDirect(compressed.length);
        } else {
          src = ByteBuffer.allocate(compressed.length);
        }
        src.put(compressed);
        src.flip();

        ByteBuffer dest;
        if (isDirect) {
          dest = ByteBuffer.allocateDirect(2048);
        } else {
          dest = ByteBuffer.allocate(2048);
        }
        codec.decompress(src, 1024, dest, 0);
        assertEquals(0, src.position());
        assertEquals(compressed.length, src.limit());
        assertEquals(0, dest.position());
        assertEquals(2048, dest.limit());
        assertArrayEquals(
            data, Arrays.copyOfRange(ByteBufferUtils.bufferToArray(dest.duplicate()), 0, 1024));

        codec.decompress(src, 1024, dest, 1024);
        assertEquals(0, src.position());
        assertEquals(compressed.length, src.limit());
        assertEquals(0, dest.position());
        assertEquals(2048, dest.limit());
        assertArrayEquals(
            data, Arrays.copyOfRange(ByteBufferUtils.bufferToArray(dest.duplicate()), 1024, 2048));
      }
    }
  }
}
