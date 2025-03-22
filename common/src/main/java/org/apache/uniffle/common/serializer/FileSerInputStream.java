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
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import io.netty.buffer.ByteBuf;

public class FileSerInputStream extends SerInputStream {

  private final long start; // the start of source input stream
  private final long end; // the end of source input stream
  private FileInputStream input; // the input stream of the source
  private FileChannel fileChannel;

  // In FileSerInputStream, buffer is not direct memory. This stream will read file
  // content to direct memory, then copy the direct memory to heap. But it doesn't
  // matter, because it is only used for testing.
  private ByteBuffer bb = null;
  private byte[] bs = null;
  private byte[] b1;
  private long pos;

  public FileSerInputStream(File file, long start, long end) throws IOException {
    if (start < 0) {
      throw new IOException("Negative position for channel!");
    }
    this.input = new FileInputStream(file);
    this.fileChannel = input.getChannel();
    if (this.fileChannel == null) {
      throw new IOException("channel is null!");
    }
    this.start = start;
    this.end = end;
    this.pos = start;
    this.fileChannel.position(start);
  }

  @Override
  public int available() {
    return (int) (end - pos);
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
    // We copy the bytes, but it doesn't matter, only for test
    byte[] bytes = new byte[len];
    while (len > 0) {
      int c = read(bytes, 0, bytes.length);
      len -= c;
    }
    to.writeBytes(bytes);
  }

  private int read(ByteBuffer bb) throws IOException {
    return fileChannel.read(bb);
  }

  @Override
  public synchronized int read() throws IOException {
    if (b1 == null) {
      b1 = new byte[1];
    }
    int n = read(b1);
    if (n == 1) {
      return b1[0] & 0xFF;
    }
    return -1;
  }

  @Override
  public synchronized int read(byte[] bs, int off, int len) throws IOException {
    if ((off < 0)
        || (off > bs.length)
        || (len < 0)
        || ((off + len) > bs.length)
        || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }
    ByteBuffer bb = (this.bs == bs) ? this.bb : ByteBuffer.wrap(bs);
    bb.limit(Math.min(off + len, bb.capacity()));
    bb.position(off);
    this.bb = bb;
    this.bs = bs;
    int ret = read(bb);
    if (ret >= 0) {
      pos += ret;
    }
    return ret;
  }

  @Override
  public void close() throws IOException {
    if (this.input != null) {
      this.input.close();
      this.input = null;
    }
  }
}
