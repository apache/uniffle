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

import org.apache.uniffle.common.util.ByteBufUtils;
import org.apache.uniffle.storage.api.FileWriter;
import org.apache.uniffle.storage.common.FileBasedShuffleSegment;

public class LocalFileWriter implements FileWriter {

  private DataOutputStream dataOutputStream;
  private FileOutputStream fileOutputStream;
  private long nextOffset;

  @VisibleForTesting
  public LocalFileWriter(File file) throws IOException {
    this(file, 8 * 1024);
  }

  public LocalFileWriter(File file, int bufferSize) throws IOException {
    fileOutputStream = new FileOutputStream(file, true);
    // init fsDataOutputStream
    dataOutputStream = new DataOutputStream(new BufferedOutputStream(fileOutputStream, bufferSize));
    nextOffset = file.length();
  }

  public void writeData(ByteBuf buf) throws IOException {
    byte[] data = ByteBufUtils.readBytes(buf);
    writeData(data);
  }

  public void writeData(byte[] data) throws IOException {
    if (data != null && data.length > 0) {
      dataOutputStream.write(data);
      nextOffset = nextOffset + data.length;
    }
  }

  public void writeIndex(FileBasedShuffleSegment segment) throws IOException {
    dataOutputStream.writeLong(segment.getOffset());
    dataOutputStream.writeInt(segment.getLength());
    dataOutputStream.writeInt(segment.getUncompressLength());
    dataOutputStream.writeLong(segment.getCrc());
    dataOutputStream.writeLong(segment.getBlockId());
    dataOutputStream.writeLong(segment.getTaskAttemptId());
  }

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
