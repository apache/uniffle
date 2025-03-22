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

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import org.apache.uniffle.common.serializer.SerOutputStream;
import org.apache.uniffle.common.serializer.SerializationStream;

public class WritableSerializationStream<K extends Writable, V extends Writable>
    extends SerializationStream {

  private SerOutputStream output;
  private DataOutputStream dataOut;
  // DataOutputStream::size return int, can not support big file which is larger than
  // Integer.MAX_VALUE.
  // Here introduce totalBytesWritten to record the written bytes.
  private long totalBytesWritten = 0;
  DataOutputBuffer buffer = new DataOutputBuffer();
  DataOutputBuffer sizebuffer = new DataOutputBuffer();

  public WritableSerializationStream(WritableSerializerInstance instance, SerOutputStream out) {
    this.output = out;
  }

  @Override
  public void init() {
    this.dataOut = new DataOutputStream(this.output);
  }

  @Override
  public void writeRecord(Object key, Object value) throws IOException {
    // write key and value to buffer
    buffer.reset();
    ((Writable) key).write(buffer);
    int keyLength = buffer.getLength();
    ((Writable) value).write(buffer);
    int valueLength = buffer.getLength() - keyLength;
    int toWriteLength =
        WritableUtils.getVIntSize(keyLength)
            + WritableUtils.getVIntSize(valueLength)
            + keyLength
            + valueLength;
    output.preAllocate(toWriteLength);

    // write size and buffer to output
    sizebuffer.reset();
    WritableUtils.writeVInt(sizebuffer, keyLength);
    WritableUtils.writeVInt(sizebuffer, valueLength);
    sizebuffer.writeTo(dataOut);
    buffer.writeTo(dataOut);
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
