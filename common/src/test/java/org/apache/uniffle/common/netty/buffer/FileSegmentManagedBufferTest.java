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

package org.apache.uniffle.common.netty.buffer;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FileSegmentManagedBufferTest {
  @Test
  void testNioByteBuffer(@TempDir File tmpDir) {
    File dataFile = new File(tmpDir, "data_file_1");
    String str = "Hello";
    byte[] strToBytes = str.getBytes();
    try (FileOutputStream outputStream = new FileOutputStream(dataFile)) {
      outputStream.write(strToBytes);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    FileSegmentManagedBuffer fileSegmentManagedBuffer =
        new FileSegmentManagedBuffer(dataFile, 0, strToBytes.length);
    ByteBuffer byteBuffer1 = fileSegmentManagedBuffer.nioByteBuffer();
    assertEquals(new String(byteBuffer1.array()), str);

    ByteBuffer byteBuffer2 = fileSegmentManagedBuffer.nioByteBuffer();
    assertTrue(byteBuffer1 == byteBuffer2);
    fileSegmentManagedBuffer.release();

    fileSegmentManagedBuffer = new FileSegmentManagedBuffer(dataFile, 0, strToBytes.length);
    ByteBuffer byteBuffer3 = fileSegmentManagedBuffer.nioByteBuffer();
    assertFalse(byteBuffer3 == byteBuffer2);
    assertFalse(byteBuffer3 == byteBuffer1);

    fileSegmentManagedBuffer.release();
  }
}
