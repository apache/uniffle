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

import java.util.concurrent.atomic.LongAdder;

import io.grpc.netty.shaded.io.netty.buffer.AbstractByteBufAllocator;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.ByteBufAllocator;
import io.grpc.netty.shaded.io.netty.buffer.CompositeByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.Unpooled;

/** Shares immutable protobuf byte arrays through gRPC's Netty send buffers. */
public final class GrpcSharedBufferAllocator extends AbstractByteBufAllocator {
  // gRPC 1.64 allocates message buffers of at most 1 MiB. Padding is never transmitted.
  private static final byte[] PADDING = new byte[1024 * 1024];
  private static final ThreadLocal<byte[]> SOURCE = new ThreadLocal<>();
  private final ByteBufAllocator delegate;
  private final LongAdder sharedBytes = new LongAdder();

  public GrpcSharedBufferAllocator(ByteBufAllocator delegate) {
    super(true);
    this.delegate = delegate;
  }

  /** The source must remain immutable, including after this scope ends. */
  public static Scope share(byte[] source) {
    return new Scope(source);
  }

  public static final class Scope implements AutoCloseable {
    private final byte[] previous = SOURCE.get();

    private Scope(byte[] source) {
      SOURCE.set(source);
    }

    @Override
    public void close() {
      if (previous == null) {
        SOURCE.remove();
      } else {
        SOURCE.set(previous);
      }
    }
  }

  /** Cumulative payload bytes referenced without copying, useful for transport verification. */
  public long getSharedBytes() {
    return sharedBytes.sum();
  }

  @Override
  public ByteBuf buffer(int initialCapacity, int maxCapacity) {
    byte[] source = SOURCE.get();
    if (source != null && initialCapacity > 0 && initialCapacity <= PADDING.length) {
      return new SharedBuffer(initialCapacity, maxCapacity, source);
    }
    return delegate.buffer(initialCapacity, maxCapacity);
  }

  @Override
  protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
    return delegate.heapBuffer(initialCapacity, maxCapacity);
  }

  @Override
  protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
    return delegate.directBuffer(initialCapacity, maxCapacity);
  }

  @Override
  public boolean isDirectBufferPooled() {
    return delegate.isDirectBufferPooled();
  }

  private final class SharedBuffer extends CompositeByteBuf {
    private final byte[] source;
    private final int size;

    private SharedBuffer(int size, int maximum, byte[] source) {
      super(delegate, false, Integer.MAX_VALUE);
      this.size = size;
      this.source = source;
      maxCapacity(maximum);
      addComponent(false, padding(size));
    }

    @Override
    public CompositeByteBuf writeBytes(byte[] bytes, int offset, int length) {
      if (length == 0) {
        return this;
      }
      if (offset < 0 || length < 0 || offset > bytes.length - length || length > writableBytes()) {
        throw new IndexOutOfBoundsException();
      }
      ByteBuf part;
      if (bytes == source) {
        part = Unpooled.wrappedBuffer(bytes, offset, length).asReadOnly();
        sharedBytes.add(length);
      } else {
        // Protobuf's staging buffer is mutable and must be copied; only the exact source is shared.
        part = delegate.directBuffer(length, length).writeBytes(bytes, offset, length);
      }
      int end = writerIndex() + length;
      removeComponent(numComponents() - 1);
      addComponent(false, part);
      if (end < size) {
        addComponent(false, padding(size - end));
      }
      writerIndex(end);
      return this;
    }

    @Override
    public CompositeByteBuf writeByte(int value) {
      return writeBytes(new byte[] {(byte) value}, 0, 1);
    }

    private ByteBuf padding(int length) {
      return Unpooled.wrappedBuffer(PADDING, 0, length).asReadOnly();
    }
  }
}
