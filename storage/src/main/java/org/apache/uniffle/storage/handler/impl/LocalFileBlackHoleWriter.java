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

import java.io.File;
import java.io.IOException;

import io.netty.buffer.ByteBuf;

import org.apache.uniffle.storage.api.FileWriter;
import org.apache.uniffle.storage.common.FileBasedShuffleSegment;

/** A shuffle writer that write data into black hole for performance test purpose only. */
public class LocalFileBlackHoleWriter implements FileWriter {

  private long nextOffset;

  public LocalFileBlackHoleWriter(File file, int bufferSize) {}

  @Override
  public void writeData(byte[] data) throws IOException {
    nextOffset = nextOffset + data.length;
  }

  @Override
  public void writeData(ByteBuf buf) throws IOException {
    if (buf != null && buf.readableBytes() > 0) {
      nextOffset = nextOffset + buf.readableBytes();
    }
  }

  @Override
  public void writeIndex(FileBasedShuffleSegment segment) throws IOException {}

  @Override
  public long nextOffset() {
    return nextOffset;
  }

  @Override
  public synchronized void close() throws IOException {}
}
