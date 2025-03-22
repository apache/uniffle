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

package org.apache.uniffle.common.serializer.writable;

import java.io.DataInputStream;
import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import org.apache.uniffle.common.serializer.DeserializationStream;
import org.apache.uniffle.common.serializer.SerInputStream;
import org.apache.uniffle.common.util.NettyUtils;

// Compare to RawWritableDeserializationStream, BufferedRawWritableDeserializationStream use shared
// buffer to store record. It means that after we use nextRecord, we store the record to shared
// buffer. It means we must use this before next nextRecord.
// Usually, BufferedRawWritableDeserializationStream is used on the server side and
// RawWritableDeserializationStream is used on the client side. Because the records obtained
// in BufferedRawWritableDeserializationStream are quickly used to form merged block, using
// shared buffer can avoid frequent memory requests. On the client side, the records obtained
// are generally used for subsequent data processing and must be independent copies, so
// RawWritableDeserializationStream is used in client side.
public class BufferedRawWritableDeserializationStream<K extends Writable, V extends Writable>
    extends DeserializationStream<ByteBuf, ByteBuf> {

  private static final int INIT_BUFFER_SIZE = 256;
  private static final int EOF_MARKER = -1; // End of File Marker

  private SerInputStream inputStream;
  private DataInputStream dataIn;

  private ByteBuf currentKeyBuffer;
  private ByteBuf currentValueBuffer;

  public BufferedRawWritableDeserializationStream(
      WritableSerializerInstance instance, SerInputStream inputStream) {
    this.inputStream = inputStream;
  }

  @Override
  public void init() {
    this.inputStream.init();
    this.dataIn = new DataInputStream(inputStream);
    // We will use key to compare, so use heap memory.Since we have a copy of the data,
    // the intermediate results can be placed either off-heap or heap. Using heap memory
    // here is also more secure.
    UnpooledByteBufAllocator allocator = NettyUtils.getSharedUnpooledByteBufAllocator(true);
    this.currentKeyBuffer = allocator.heapBuffer(INIT_BUFFER_SIZE);
    this.currentValueBuffer = allocator.heapBuffer(INIT_BUFFER_SIZE);
  }

  @Override
  public boolean nextRecord() throws IOException {
    if (inputStream.available() <= 0) {
      return false;
    }
    int currentKeyLength = WritableUtils.readVInt(dataIn);
    int currentValueLength = WritableUtils.readVInt(dataIn);
    if (currentKeyLength == EOF_MARKER && currentValueLength == EOF_MARKER) {
      return false;
    }
    currentKeyBuffer.clear();
    inputStream.transferTo(currentKeyBuffer, currentKeyLength);
    currentValueBuffer.clear();
    inputStream.transferTo(currentValueBuffer, currentValueLength);
    return true;
  }

  @Override
  public ByteBuf getCurrentKey() {
    return currentKeyBuffer;
  }

  @Override
  public ByteBuf getCurrentValue() {
    return currentValueBuffer;
  }

  @Override
  public void close() throws IOException {
    if (currentKeyBuffer != null) {
      currentKeyBuffer.release();
      currentKeyBuffer = null;
    }
    if (currentValueBuffer != null) {
      currentValueBuffer.release();
      currentValueBuffer = null;
    }
    if (inputStream != null) {
      inputStream.close();
      inputStream = null;
    }
    if (dataIn != null) {
      dataIn.close();
      dataIn = null;
    }
  }
}
