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

import io.netty.buffer.ByteBuf;

public class BufferSerInputStream extends SerInputStream {

  private ByteBuf buffer;
  private final int start;
  private final int end;

  private final int size;

  public BufferSerInputStream(ByteBuf byteBuf, int start, int end) {
    assert start >= 0;
    // TODO: the byteBuf should retain outside.
    this.buffer = byteBuf;
    this.start = start;
    this.end = end;
    this.buffer.readerIndex(start);
    this.size = end - start;
  }

  @Override
  public int available() {
    return end - this.buffer.readerIndex();
  }

  @Override
  public long getStart() {
    return start;
  }

  @Override
  public long getEnd() {
    return end;
  }

  @Override
  public void transferTo(ByteBuf to, int len) throws IOException {
    to.writeBytes(buffer, len);
  }

  @Override
  public int read() throws IOException {
    if (available() <= 0) {
      return -1;
    }
    return this.buffer.readByte() & 0xFF;
  }
}
