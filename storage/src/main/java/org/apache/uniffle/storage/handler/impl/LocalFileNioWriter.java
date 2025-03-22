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

package org.apache.uniffle.storage.handler.impl;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.uniffle.storage.api.FileWriter;
import org.apache.uniffle.storage.common.FileBasedShuffleSegment;

public class LocalFileNioWriter implements FileWriter {

  private DataOutputStream dataOutputStream;
  private FileOutputStream fileOutputStream;
  private long nextOffset;

  @VisibleForTesting
  public LocalFileNioWriter(File file) throws IOException {
    this(file, 8 * 1024);
  }

  public LocalFileNioWriter(File file, int bufferSize) throws IOException {
    fileOutputStream = new FileOutputStream(file, true);
    // init fsDataOutputStream
    dataOutputStream = new DataOutputStream(new BufferedOutputStream(fileOutputStream, bufferSize));
    nextOffset = file.length();
  }

  @Override
  public void writeData(byte[] data) throws IOException {
    writeData(Unpooled.wrappedBuffer(data));
  }

  @Override
  public void writeData(ByteBuf buf) throws IOException {
    if (buf != null && buf.readableBytes() > 0) {
      int writtenSize = fileOutputStream.getChannel().write(buf.nioBuffer());
      if (writtenSize <= 0) {
        throw new IOException("Failed to write data to file");
      }
      nextOffset = nextOffset + buf.readableBytes();
    }
  }

  @Override
  public void writeIndex(FileBasedShuffleSegment segment) throws IOException {
    dataOutputStream.writeLong(segment.getOffset());
    dataOutputStream.writeInt(segment.getLength());
    dataOutputStream.writeInt(segment.getUncompressLength());
    dataOutputStream.writeLong(segment.getCrc());
    dataOutputStream.writeLong(segment.getBlockId());
    dataOutputStream.writeLong(segment.getTaskAttemptId());
  }

  @Override
  public long nextOffset() {
    return nextOffset;
  }

  @Override
  public synchronized void close() throws IOException {
    if (dataOutputStream != null) {
      dataOutputStream.close();
    }
  }
}
