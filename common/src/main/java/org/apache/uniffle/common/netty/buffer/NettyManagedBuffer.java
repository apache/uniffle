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
import io.netty.buffer.Unpooled;

public class NettyManagedBuffer extends ManagedBuffer {

  public final static NettyManagedBuffer EMPTY_BUFFER = new NettyManagedBuffer(Unpooled.buffer(0, 0));

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
