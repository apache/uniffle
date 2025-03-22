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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import org.apache.uniffle.common.serializer.DeserializationStream;
import org.apache.uniffle.common.serializer.SerInputStream;

public class RawWritableDeserializationStream<K extends Writable, V extends Writable>
    extends DeserializationStream<ComparativeOutputBuffer, ComparativeOutputBuffer> {

  public static final int EOF_MARKER = -1; // End of File Marker

  private SerInputStream inputStream;
  private DataInputStream dataIn;
  private ComparativeOutputBuffer currentKeyBuffer;
  private ComparativeOutputBuffer currentValueBuffer;

  public RawWritableDeserializationStream(
      WritableSerializerInstance instance, SerInputStream inputStream) {
    this.inputStream = inputStream;
  }

  @Override
  public void init() {
    this.inputStream.init();
    this.dataIn = new DataInputStream(this.inputStream);
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
    currentKeyBuffer = new ComparativeOutputBuffer();
    currentValueBuffer = new ComparativeOutputBuffer();
    currentKeyBuffer.write(dataIn, currentKeyLength);
    currentValueBuffer.write(dataIn, currentValueLength);
    return true;
  }

  @Override
  public ComparativeOutputBuffer getCurrentKey() {
    return currentKeyBuffer;
  }

  @Override
  public ComparativeOutputBuffer getCurrentValue() {
    return currentValueBuffer;
  }

  @Override
  public void close() throws IOException {
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
