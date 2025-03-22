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

import java.io.IOException;
import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class DynBufferSerOutputStream extends SerOutputStream {

  private WrappedByteArrayOutputStream buffer;

  public DynBufferSerOutputStream() {
    this.buffer = new WrappedByteArrayOutputStream();
  }

  public DynBufferSerOutputStream(int capacity) {
    this.buffer = new WrappedByteArrayOutputStream(capacity);
  }

  @Override
  public void write(ByteBuf from) throws IOException {
    // We copy the bytes, but it doesn't matter, only for test
    byte[] bytes = new byte[from.readableBytes()];
    from.readBytes(bytes);
    buffer.write(bytes);
  }

  @Override
  public void write(int b) throws IOException {
    this.buffer.write(b);
  }

  @Override
  public ByteBuf toByteBuf() {
    return Unpooled.wrappedBuffer(ByteBuffer.wrap(buffer.toByteArray()));
  }

  @Override
  public void flush() throws IOException {
    super.flush();
  }

  @Override
  public void close() throws IOException {
    super.close();
  }
}
