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
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.uniffle.common.serializer.DeserializationStream;
import org.apache.uniffle.common.serializer.SerInputStream;

public class WritableDeserializationStream<K extends Writable, V extends Writable>
    extends DeserializationStream<K, V> {

  public static final int EOF_MARKER = -1; // End of File Marker

  private SerInputStream inputStream;
  private DataInputStream dataIn;
  private Class<K> keyClass;
  private Class<V> valueClass;
  private K currentKey;
  private V currentValue;

  public WritableDeserializationStream(
      WritableSerializerInstance instance,
      SerInputStream inputStream,
      Class<K> keyClass,
      Class<V> valueClass) {
    this.inputStream = inputStream;
    this.keyClass = keyClass;
    this.valueClass = valueClass;
  }

  @Override
  public void init() {
    this.inputStream.init();
    this.dataIn = new DataInputStream(inputStream);
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
    this.currentKey = ReflectionUtils.newInstance(keyClass, null);
    this.currentKey.readFields(dataIn);
    this.currentValue = ReflectionUtils.newInstance(valueClass, null);
    this.currentValue.readFields(dataIn);
    return true;
  }

  @Override
  public K getCurrentKey() {
    return currentKey;
  }

  @Override
  public V getCurrentValue() {
    return currentValue;
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
