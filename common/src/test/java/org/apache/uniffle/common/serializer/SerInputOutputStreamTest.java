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

package org.apache.uniffle.common.serializer;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class SerInputOutputStreamTest {

  private static final int BYTES_LEN = 10240;
  private static final int LOOP = 10;
  @TempDir private static File tempDir;

  @Test
  public void testReadMemoryInputStream() throws IOException {
    SerOutputStream outputStream = new DynBufferSerOutputStream();
    for (int i = 0; i < BYTES_LEN; i++) {
      outputStream.write((byte) (i & 0x7F));
    }
    ByteBuf testBuf = outputStream.toByteBuf();

    // 1 test whole buffer
    testRandomReadMemory(testBuf, 0, BYTES_LEN);

    // 2 test from start to random end
    Random random = new Random();
    for (int i = 0; i < LOOP; i++) {
      testRandomReadMemory(testBuf, 0, random.nextInt(BYTES_LEN - 1));
    }

    // 3 test from random start to end
    for (int i = 0; i < LOOP; i++) {
      testRandomReadMemory(testBuf, random.nextInt(BYTES_LEN - 1), BYTES_LEN);
    }

    // 4 test from random start to random end
    for (int i = 0; i < LOOP; i++) {
      int r1 = random.nextInt(BYTES_LEN - 2) + 1;
      int r2 = random.nextInt(BYTES_LEN - 2) + 1;
      testRandomReadMemory(testBuf, Math.min(r1, r2), Math.max(r1, r2));
    }

    // 5 Test when bytes is from start to start
    testRandomReadMemory(testBuf, 0, 0);

    // 6 Test when bytes is from end to end
    testRandomReadMemory(testBuf, BYTES_LEN, BYTES_LEN);

    // 7 Test when bytes is from random to this random
    for (int i = 0; i < LOOP; i++) {
      int r = random.nextInt(BYTES_LEN - 2) + 1;
      testRandomReadMemory(testBuf, r, r);
    }
    testBuf.release();
  }

  @Test
  public void testReadNullBytes() throws IOException {
    // Test when bytes is byte[0]
    SerOutputStream outputStream = new DynBufferSerOutputStream();
    ByteBuf testBuf = outputStream.toByteBuf();
    SerInputStream input = SerInputStream.newInputStream(testBuf, 0, 0);
    assertEquals(0, input.available());
    assertEquals(-1, input.read());
    input.close();
    testBuf.release();
  }

  @Test
  public void testReadFileInputStream() throws IOException {
    File tempFile = new File(tempDir, "data");
    ;
    SerOutputStream outputStream = new FileSerOutputStream(tempFile);
    for (int i = 0; i < BYTES_LEN; i++) {
      outputStream.write((byte) (i & 0x7F));
    }

    // 1 test whole file
    testRandomReadFile(tempFile, 0, BYTES_LEN);

    // 2 test from start to random end
    Random random = new Random();
    for (int i = 0; i < LOOP; i++) {
      testRandomReadFile(tempFile, 0, random.nextInt(BYTES_LEN - 1));
    }

    // 3 test from random start to end
    for (int i = 0; i < LOOP; i++) {
      testRandomReadFile(tempFile, random.nextInt(BYTES_LEN - 1), BYTES_LEN);
    }

    // 4 test from random start to random end
    for (int i = 0; i < LOOP; i++) {
      int r1 = random.nextInt(BYTES_LEN - 2) + 1;
      int r2 = random.nextInt(BYTES_LEN - 2) + 1;
      testRandomReadFile(tempFile, Math.min(r1, r2), Math.max(r1, r2));
    }

    // 5 Test when bytes is from start to start
    testRandomReadFile(tempFile, 0, 0);

    // 6 Test when bytes is from end to end
    testRandomReadFile(tempFile, BYTES_LEN, BYTES_LEN);

    // 7 Test when bytes is from random to this random
    for (int i = 0; i < LOOP; i++) {
      int r = random.nextInt(BYTES_LEN - 2) + 1;
      testRandomReadFile(tempFile, r, r);
    }
  }

  private void testRandomReadMemory(ByteBuf byteBuf, int start, int end) throws IOException {
    SerInputStream input = SerInputStream.newInputStream(byteBuf, start, end);
    testRandomReadOneBytePerTime(input, start, end);
    input.close();

    input = SerInputStream.newInputStream(byteBuf, start, end);
    testRandomReadMultiBytesPerTime(input, start, end);
    input.close();
  }

  private void testRandomReadOneBytePerTime(SerInputStream input, long start, long end)
      throws IOException {
    // test read one byte per time
    long index = start;
    while (input.available() > 0) {
      int b = input.read();
      assertEquals(index & 0x7F, b);
      index++;
    }
    if (start == end) {
      assertEquals(0, input.available());
    }
    assertEquals(end, index);
    if (end == BYTES_LEN) {
      assertEquals(-1, input.read());
    }
  }

  private void testRandomReadMultiBytesPerTime(SerInputStream input, long start, long end)
      throws IOException {
    // test read multi bytes per times
    long index = start;
    Random random = new Random();
    while (input.available() > 0) {
      int wanna = Math.min(random.nextInt(100), input.available());
      byte[] buffer = new byte[wanna];
      int real = input.read(buffer, 0, wanna);
      assertNotEquals(-1, real);
      for (int i = 0; i < real; i++) {
        assertEquals((index + i) & 0x7F, buffer[i]);
      }
      index += real;
    }
    if (start == end) {
      assertEquals(0, input.available());
    }
    assertEquals(end, index);
    if (end == BYTES_LEN) {
      assertEquals(-1, input.read());
    }
  }

  private void testRandomReadFile(File file, long start, long end) throws IOException {
    SerInputStream input = SerInputStream.newInputStream(file, start, end);
    testRandomReadOneBytePerTime(input, start, end);
    input.close();

    input = SerInputStream.newInputStream(file, start, end);
    testRandomReadMultiBytesPerTime(input, start, end);
    input.close();
  }
}
