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

package org.apache.spark.shuffle.writer;

import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriterBuffer {

  private static final Logger LOG = LoggerFactory.getLogger(WriterBuffer.class);
  private long copyTime = 0;
  private byte[] buffer;
  private int bufferSize;
  private int nextOffset = 0;
  private List<WrappedBuffer> buffers = Lists.newArrayList();
  private int dataLength = 0;
  private int memoryUsed = 0;
  private long recordCount = 0;

  public WriterBuffer(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  public void addRecord(byte[] recordBuffer, int length) {
    if (askForMemory(length)) {
      // buffer has data already, add buffer to list
      if (nextOffset > 0) {
        buffers.add(new WrappedBuffer(buffer, nextOffset));
        nextOffset = 0;
      }
      int newBufferSize = Math.max(length, bufferSize);
      buffer = new byte[newBufferSize];
      memoryUsed += newBufferSize;
    }

    try {
      System.arraycopy(recordBuffer, 0, buffer, nextOffset, length);
    } catch (Exception e) {
      LOG.error(
          "Unexpected exception for System.arraycopy, length["
              + length
              + "], nextOffset["
              + nextOffset
              + "], bufferSize["
              + bufferSize
              + "]");
      throw e;
    }

    nextOffset += length;
    dataLength += length;
    recordCount++;
  }

  public boolean askForMemory(long length) {
    return buffer == null || nextOffset + length > bufferSize;
  }

  @VisibleForTesting
  public byte[] getData() {
    ByteBuf buf = getDataAsByteBuf();
    byte[] result = new byte[buf.readableBytes()];
    buf.getBytes(0, result);
    return result;
  }

  public ByteBuf getDataAsByteBuf() {
    if (buffers.isEmpty()) {
      if (buffer == null || nextOffset <= 0) {
        return Unpooled.EMPTY_BUFFER;
      }
      return Unpooled.wrappedBuffer(buffer, 0, nextOffset);
    }

    CompositeByteBuf composite = Unpooled.compositeBuffer(buffers.size() + 1);
    for (WrappedBuffer stagingBuffer : buffers) {
      if (stagingBuffer.getSize() > 0) {
        composite.addComponent(
            true, Unpooled.wrappedBuffer(stagingBuffer.getBuffer(), 0, stagingBuffer.getSize()));
      }
    }
    if (buffer != null && nextOffset > 0) {
      composite.addComponent(true, Unpooled.wrappedBuffer(buffer, 0, nextOffset));
    }
    return composite;
  }

  public int getDataLength() {
    return dataLength;
  }

  public long getCopyTime() {
    return copyTime;
  }

  public int getMemoryUsed() {
    return memoryUsed;
  }

  public long getRecordCount() {
    return recordCount;
  }

  private static final class WrappedBuffer {

    byte[] buffer;
    int size;

    WrappedBuffer(byte[] buffer, int size) {
      this.buffer = buffer;
      this.size = size;
    }

    public byte[] getBuffer() {
      return buffer;
    }

    public int getSize() {
      return size;
    }
  }
}
