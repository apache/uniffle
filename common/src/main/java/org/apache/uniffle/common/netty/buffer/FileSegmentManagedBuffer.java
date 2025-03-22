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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultFileRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.JavaUtils;

public class FileSegmentManagedBuffer extends ManagedBuffer {

  private static final Logger LOG = LoggerFactory.getLogger(FileSegmentManagedBuffer.class);
  private final File file;
  private final long offset;
  private final int length;
  private volatile boolean isFilled;
  private ByteBuffer cachedBuffer;

  public FileSegmentManagedBuffer(File file, long offset, int length) {
    this.file = file;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public int size() {
    return length;
  }

  @Override
  public ByteBuf byteBuf() {
    return Unpooled.wrappedBuffer(this.nioByteBuffer());
  }

  @Override
  public ByteBuffer nioByteBuffer() {
    if (isFilled) {
      return cachedBuffer;
    }
    FileChannel channel = null;
    try {
      channel = new RandomAccessFile(file, "r").getChannel();
      cachedBuffer = ByteBuffer.allocate(length);
      channel.position(offset);
      while (cachedBuffer.remaining() != 0) {
        if (channel.read(cachedBuffer) == -1) {
          throw new IOException(
              String.format(
                  "Reached EOF before filling buffer.offset=%s,file=%s,buf.remaining=%s",
                  offset, file.getAbsoluteFile(), cachedBuffer.remaining()));
        }
      }
      cachedBuffer.flip();
      isFilled = true;
      return cachedBuffer;
    } catch (IOException e) {
      String fileName = file.getAbsolutePath();
      String errorMessage =
          String.format(
              "Errors on reading localfile data with offset[%s] length[%s] from [%s]. ",
              offset, length, fileName);
      try {
        if (channel != null) {
          long size = channel.size();
          errorMessage += String.format("The actual file length: %s", size);
        }
      } catch (IOException ignored) {
        // ignore
      }

      LOG.error(errorMessage, e);
      return ByteBuffer.allocate(0);
    } finally {
      JavaUtils.closeQuietly(channel);
    }
  }

  @Override
  public ManagedBuffer retain() {
    return this;
  }

  @Override
  public ManagedBuffer release() {
    cachedBuffer = null;
    isFilled = false;
    return this;
  }

  @Override
  public Object convertToNetty() {
    FileChannel fileChannel;
    try {
      fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
    } catch (IOException e) {
      throw new RssException("Errors on reading " + file.getAbsolutePath(), e);
    }
    return new DefaultFileRegion(fileChannel, offset, length);
  }
}
