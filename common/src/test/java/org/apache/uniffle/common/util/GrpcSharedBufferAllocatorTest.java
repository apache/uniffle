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

import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class GrpcSharedBufferAllocatorTest {
  @Test
  void sharedPayloadHasIndependentProgressAndReferenceLifetimes() {
    GrpcSharedBufferAllocator allocator =
        new GrpcSharedBufferAllocator(UnpooledByteBufAllocator.DEFAULT);
    byte[] data = {10, 11, 12, 13};
    ByteBuf first;
    ByteBuf second;
    try (GrpcSharedBufferAllocator.Scope ignored = GrpcSharedBufferAllocator.share(data)) {
      first = allocator.buffer(8, 8);
      second = allocator.buffer(8, 8);
      first.writeByte(99).writeBytes(data, 0, 4);
      second.writeBytes(data, 0, 4).writeByte(88);
    }
    try {
      assertEquals(99, first.readByte());
      assertEquals(10, first.readByte());
      assertEquals(0, second.readerIndex());
      assertEquals(10, second.getByte(0));
      // Test-only mutation proves both views refer to the source instead of copied payloads.
      data[2] = 42;
      assertEquals(42, first.getByte(3));
      assertEquals(42, second.getByte(2));
      ByteBuf slice = first.retainedSlice(1, 4);
      first.release();
      assertEquals(42, slice.getByte(2));
      slice.release();
      assertEquals(42, second.getByte(2));
      assertEquals(8, allocator.getSharedBytes());
    } finally {
      if (first.refCnt() > 0) {
        first.release();
      }
      second.release();
    }
  }

  @Test
  void stagingBuffersAreCopiedAndScopeDoesNotLeak() {
    GrpcSharedBufferAllocator allocator =
        new GrpcSharedBufferAllocator(UnpooledByteBufAllocator.DEFAULT);
    byte[] shared = {1, 2, 3};
    byte[] staging = {4, 5};
    ByteBuf buffer;
    try (GrpcSharedBufferAllocator.Scope ignored = GrpcSharedBufferAllocator.share(shared)) {
      buffer = allocator.buffer(5, 5);
      buffer.writeBytes(staging, 0, 2).writeBytes(shared, 0, 3);
    }
    try {
      staging[0] = 99;
      assertEquals(4, buffer.getByte(0));
      assertEquals(0, buffer.writableBytes());
      assertEquals(5, buffer.capacity());
      byte[] actual = new byte[5];
      buffer.readBytes(actual);
      assertArrayEquals(new byte[] {4, 5, 1, 2, 3}, actual);
    } finally {
      buffer.release();
    }
    ByteBuf ordinary = allocator.buffer(3, 3);
    try {
      ordinary.writeBytes(shared);
      shared[0] = 77;
      assertEquals(1, ordinary.getByte(0));
      assertEquals(3, allocator.getSharedBytes());
    } finally {
      ordinary.release();
    }
  }
}
