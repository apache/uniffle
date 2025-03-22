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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.ByteBufUtils;
import org.apache.uniffle.storage.api.FileWriter;
import org.apache.uniffle.storage.common.FileBasedShuffleSegment;

public class HadoopFileWriter implements FileWriter {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopFileWriter.class);

  private final FileSystem fileSystem;

  private Path path;
  private Configuration hadoopConf;
  private FSDataOutputStream fsDataOutputStream;
  private long nextOffset;

  @VisibleForTesting
  public HadoopFileWriter(FileSystem fileSystem, Path path, Configuration hadoopConf)
      throws IOException {
    this(fileSystem, path, hadoopConf, 8 * 1024);
  }

  public HadoopFileWriter(
      FileSystem fileSystem, Path path, Configuration hadoopConf, int bufferSize)
      throws IOException {
    this.path = path;
    this.hadoopConf = hadoopConf;
    this.fileSystem = fileSystem;
    initStream(bufferSize);
  }

  private void initStream(int bufferSize) throws IOException, IllegalStateException {
    final FileSystem writerFs = fileSystem;
    if (writerFs.isFile(path)) {
      if (hadoopConf.getBoolean("dfs.support.append", true)) {
        fsDataOutputStream = writerFs.append(path, bufferSize);
        nextOffset = fsDataOutputStream.getPos();
      } else {
        String msg = path + " exists but append mode is not support!";
        LOG.error(msg);
        throw new IllegalStateException(msg);
      }
    } else if (writerFs.isDirectory(path)) {
      String msg = path + " is a directory!";
      LOG.error(msg);
      throw new IllegalStateException(msg);
    } else {
      fsDataOutputStream = writerFs.create(path, true, bufferSize);
      nextOffset = fsDataOutputStream.getPos();
    }
  }

  public void writeData(byte[] data) throws IOException {
    if (data != null && data.length > 0) {
      fsDataOutputStream.write(data);
      nextOffset = fsDataOutputStream.getPos();
    }
  }

  public void writeData(ByteBuf buf) throws IOException {
    byte[] data = ByteBufUtils.readBytes(buf);
    writeData(data);
  }

  public void writeData(ByteBuffer byteBuffer) throws IOException {
    if (byteBuffer.hasArray()) {
      fsDataOutputStream.write(
          byteBuffer.array(),
          byteBuffer.arrayOffset() + byteBuffer.position(),
          byteBuffer.remaining());
    } else {
      byte[] byteArray = new byte[byteBuffer.remaining()];
      byteBuffer.get(byteArray);
      fsDataOutputStream.write(byteArray);
    }
    nextOffset = fsDataOutputStream.getPos();
  }

  public void writeIndex(FileBasedShuffleSegment segment) throws IOException {
    fsDataOutputStream.writeLong(segment.getOffset());
    fsDataOutputStream.writeInt(segment.getLength());
    fsDataOutputStream.writeInt(segment.getUncompressLength());
    fsDataOutputStream.writeLong(segment.getCrc());
    fsDataOutputStream.writeLong(segment.getBlockId());
    fsDataOutputStream.writeLong(segment.getTaskAttemptId());
  }

  public long nextOffset() {
    return nextOffset;
  }

  public void flush() throws IOException {
    if (fsDataOutputStream != null) {
      fsDataOutputStream.flush();
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (fsDataOutputStream != null) {
      fsDataOutputStream.close();
    }
  }

  public long copy(FileInputStream inputStream, int bufferSize) throws IOException {
    long start = fsDataOutputStream.getPos();
    IOUtils.copyBytes(inputStream, fsDataOutputStream, bufferSize);
    return fsDataOutputStream.getPos() - start;
  }
}
