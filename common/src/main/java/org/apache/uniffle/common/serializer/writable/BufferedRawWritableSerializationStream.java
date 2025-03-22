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

import java.io.DataOutputStream;
import java.io.IOException;

import io.netty.buffer.ByteBuf;
import org.apache.hadoop.io.WritableUtils;

import org.apache.uniffle.common.serializer.SerOutputStream;
import org.apache.uniffle.common.serializer.SerializationStream;

public class BufferedRawWritableSerializationStream<K, V> extends SerializationStream {

  // DataOutputStream::size return int, can not support big file which is larger than
  // Integer.MAX_VALUE.
  // Here introduce totalBytesWritten to record the written bytes.
  private long totalBytesWritten = 0;
  private SerOutputStream output;
  private DataOutputStream dataOut;

  public BufferedRawWritableSerializationStream(
      WritableSerializerInstance instance, SerOutputStream output) {
    this.output = output;
  }

  @Override
  public void init() {
    this.dataOut = new DataOutputStream(this.output);
  }

  @Override
  public void writeRecord(Object key, Object value) throws IOException {
    ByteBuf keyBuffer = (ByteBuf) key;
    ByteBuf valueBuffer = (ByteBuf) value;
    int keyLength = keyBuffer.readableBytes();
    int valueLength = valueBuffer.readableBytes();
    int toWriteLength =
        WritableUtils.getVIntSize(keyLength)
            + WritableUtils.getVIntSize(valueLength)
            + keyLength
            + valueLength;
    this.output.preAllocate(toWriteLength);
    WritableUtils.writeVInt(dataOut, keyLength);
    WritableUtils.writeVInt(dataOut, valueLength);
    output.write(keyBuffer);
    output.write(valueBuffer);
    totalBytesWritten += toWriteLength;
  }

  @Override
  public void flush() throws IOException {
    dataOut.flush();
  }

  @Override
  public void close() throws IOException {
    if (dataOut != null) {
      dataOut.close();
      dataOut = null;
    }
  }

  @Override
  public long getTotalBytesWritten() {
    return totalBytesWritten;
  }
}
