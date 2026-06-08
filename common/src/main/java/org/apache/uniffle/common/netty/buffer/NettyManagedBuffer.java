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

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.uniffle.common.util.NettyUtils;

public class NettyManagedBuffer extends ManagedBuffer {

  public static final NettyManagedBuffer EMPTY_BUFFER =
      new NettyManagedBuffer(Unpooled.EMPTY_BUFFER);

  private ByteBuf buf;

  public NettyManagedBuffer(ByteBuf byteBuf) {
    this.buf = byteBuf;
  }

  @Override
  public int size() {
    return buf.readableBytes();
  }

  @Override
  public ByteBuf byteBuf() {
    return Unpooled.wrappedBuffer(this.nioByteBuffer());
  }

  @Override
  public ByteBuffer nioByteBuffer() {
    // CompositeByteBuf.nioBuffer will return a heap buffer if the composite buffer has more than
    // one component, even if all components are direct buffers. In native client scenarios
    // (like gluten), we prefer to use direct buffer to reduce data copying.
    if (buf instanceof CompositeByteBuf
        && buf.isDirect()
        && buf.nioBufferCount() > 1
        && NettyUtils.preferDirectForCompositeBuffer()) {
      int length = buf.readableBytes();
      ByteBuffer merged = ByteBuffer.allocateDirect(length).order(buf.order());
      for (ByteBuffer buf : buf.nioBuffers()) {
        merged.put(buf);
      }
      merged.flip();
      return merged;
    }

    return buf.nioBuffer();
  }

  @Override
  public ManagedBuffer retain() {
    buf.retain();
    return this;
  }

  @Override
  public ManagedBuffer release() {
    buf.release();
    return this;
  }

  @Override
  public Object convertToNetty() {
    return buf.duplicate().retain();
  }
}
