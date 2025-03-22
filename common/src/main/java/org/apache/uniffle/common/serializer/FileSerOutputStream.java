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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import io.netty.buffer.ByteBuf;

public class FileSerOutputStream extends SerOutputStream {

  private OutputStream outputStream;

  public FileSerOutputStream(File file) throws IOException {
    this.outputStream = new FileOutputStream(file);
  }

  @Override
  public void write(ByteBuf from) throws IOException {
    // We copy the bytes, but it doesn't matter, only for test
    byte[] bytes = new byte[from.readableBytes()];
    from.readBytes(bytes);
    outputStream.write(bytes);
  }

  @Override
  public void write(int b) throws IOException {
    outputStream.write(b);
  }

  @Override
  public void flush() throws IOException {
    outputStream.flush();
  }

  @Override
  public void close() throws IOException {
    if (outputStream != null) {
      outputStream.close();
      outputStream = null;
    }
  }
}
