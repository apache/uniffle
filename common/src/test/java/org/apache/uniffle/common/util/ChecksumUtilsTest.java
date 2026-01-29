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

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.zip.CRC32;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ChecksumUtilsTest {

  @TempDir File tempDir;

  @Test
  public void crc32TestWithByte() {
    byte[] data = new byte[32 * 1024 * 1024];
    new Random().nextBytes(data);
    CRC32 crc32 = new CRC32();
    crc32.update(data);
    long expected = crc32.getValue();
    assertEquals(expected, ChecksumUtils.getCrc32(data));

    data = new byte[32 * 1024];
    new Random().nextBytes(data);
    crc32 = new CRC32();
    crc32.update(data);
    expected = crc32.getValue();
    assertEquals(expected, ChecksumUtils.getCrc32(data));
  }

  @Test
  public void crc32TestWithByteBuff() throws Exception {
    int length = 32 * 1024 * 1024;
    byte[] data = new byte[length];
    new Random().nextBytes(data);

    File file = new File(tempDir, "crc_test.txt");
    file.createNewFile();
    file.deleteOnExit();

    try (FileOutputStream outputStream = new FileOutputStream(file)) {
      outputStream.write(data);
    }

    // test direct ByteBuffer
    Path path = Paths.get(file.getAbsolutePath());
    FileChannel fileChannel = FileChannel.open(path);
    ByteBuffer buffer = ByteBuffer.allocateDirect(length);
    int bytesRead = fileChannel.read(buffer);
    fileChannel.close();
    assertEquals(length, bytesRead);
    buffer.flip();
    long expectedChecksum = ChecksumUtils.getCrc32(data);
    assertEquals(expectedChecksum, ChecksumUtils.getCrc32(buffer));

    // test heap ByteBuffer
    path = Paths.get(file.getAbsolutePath());
    fileChannel = FileChannel.open(path);
    buffer = ByteBuffer.allocate(length);
    bytesRead = fileChannel.read(buffer);
    fileChannel.close();
    assertEquals(length, bytesRead);
    buffer.flip();
    assertEquals(expectedChecksum, ChecksumUtils.getCrc32(buffer));
  }

  @Test
  public void crc32ByteBufferTest() throws Exception {
    int length = 32 * 1024 * 1024;
    byte[] data = new byte[length];
    Random random = new Random();
    random.nextBytes(data);
    long expectCrc = ChecksumUtils.getCrc32(data);
    ByteBuffer originBuffer = ByteBuffer.allocateDirect(length);
    originBuffer.put(data);
    originBuffer.flip();
    assertEquals(expectCrc, ChecksumUtils.getCrc32(ByteBuffer.wrap(data)));
    ByteBuffer directBuffer = ByteBuffer.allocateDirect(length);
    directBuffer.put(data);
    directBuffer.flip();
    assertEquals(expectCrc, ChecksumUtils.getCrc32(directBuffer));
    assertEquals(originBuffer, directBuffer);
    int offset = random.nextInt(15);
    ByteBuffer directOffsetBuffer = ByteBuffer.allocateDirect(length + offset);
    byte[] dataOffset = new byte[offset];
    random.nextBytes(dataOffset);
    directOffsetBuffer.put(dataOffset);
    directOffsetBuffer.put(data);
    assertEquals(expectCrc, ChecksumUtils.getCrc32(directOffsetBuffer, offset, length));
  }

  @Test
  public void crc32ByteBufEmptyReadableBytesShouldReturnZero() {
    ByteBuf byteBuf = Unpooled.buffer(16);
    assertEquals(0, byteBuf.readableBytes());
    assertEquals(0L, ChecksumUtils.getCrc32(byteBuf));
  }

  @Test
  public void crc32ByteBufShouldRespectReaderIndexAndNotChangeIt() {
    byte[] data = new byte[1024];
    new Random().nextBytes(data);
    ByteBuf byteBuf = Unpooled.wrappedBuffer(data);

    int readerIndex = 17;
    byteBuf.readerIndex(readerIndex);

    CRC32 crc32 = new CRC32();
    crc32.update(data, readerIndex, data.length - readerIndex);
    long expected = crc32.getValue();

    assertEquals(expected, ChecksumUtils.getCrc32(byteBuf));
    assertEquals(readerIndex, byteBuf.readerIndex());
  }

  @Test
  public void crc32CompositeByteBufShouldIterateOverNioBuffers() {
    byte[] part1 = new byte[128];
    byte[] part2 = new byte[256];
    Random random = new Random();
    random.nextBytes(part1);
    random.nextBytes(part2);

    CompositeByteBuf composite = Unpooled.compositeBuffer();
    composite.addComponent(true, Unpooled.wrappedBuffer(part1));
    composite.addComponent(true, Unpooled.wrappedBuffer(part2));

    // Ensure this test hits the composite path (nioBufferCount > 1).
    // Note: CompositeByteBuf.nioBufferCount() depends on readerIndex/readableBytes, so check it
    // here.
    assertTrue(composite.nioBufferCount() > 1);

    int skip = 13; // cross-component offsets are fine; expected CRC is computed on readable bytes.
    composite.skipBytes(skip);

    CRC32 crc32 = new CRC32();
    crc32.update(part1, skip, part1.length - skip);
    crc32.update(part2, 0, part2.length);
    long expected = crc32.getValue();

    int readerIndex = composite.readerIndex();
    assertEquals(expected, ChecksumUtils.getCrc32(composite));
    assertEquals(readerIndex, composite.readerIndex());
  }
}
