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

package org.apache.uniffle.common.serializer.kryo;

import java.io.EOFException;
import java.io.IOException;
import java.util.Locale;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.UnsafeInput;

import org.apache.uniffle.common.serializer.DeserializationStream;
import org.apache.uniffle.common.serializer.SerInputStream;

public class KryoDeserializationStream<K, V> extends DeserializationStream<K, V> {

  private SerInputStream inputStream;
  private Input input;

  private final KryoSerializerInstance instance;
  private Kryo kryo;
  private long start;
  private K currentKey;
  private V currentValue;

  public KryoDeserializationStream(
      KryoSerializerInstance instance,
      SerInputStream inputStream,
      Class<K> keyClass,
      Class<V> valueClass) {
    this.inputStream = inputStream;
    this.instance = instance;
    this.start = inputStream.getStart();
  }

  @Override
  public void init() {
    this.inputStream.init();
    this.input = new UnsafeInput(this.inputStream);
    this.kryo = instance.borrowKryo();
  }

  @Override
  public boolean nextRecord() throws IOException {
    boolean hasNext = available() > 0;
    if (hasNext) {
      try {
        this.currentKey = (K) kryo.readClassAndObject(input);
        this.currentValue = (V) kryo.readClassAndObject(input);
      } catch (KryoException e) {
        if (e.getMessage().toLowerCase(Locale.ROOT).contains("buffer underflow")) {
          throw new EOFException();
        }
        throw e;
      }
    }
    return hasNext;
  }

  @Override
  public K getCurrentKey() throws IOException {
    return this.currentKey;
  }

  @Override
  public V getCurrentValue() throws IOException {
    return this.currentValue;
  }

  private long available() throws IOException {
    // kryo use buffer to read inputStream, so we should use real read offset by input.total()
    long totalBytesRead = start + input.total();
    return inputStream.getEnd() - totalBytesRead;
  }

  @Override
  public void close() throws IOException {
    if (inputStream != null) {
      inputStream.close();
      inputStream = null;
    }
    if (input != null) {
      try {
        input.close();
      } finally {
        instance.releaseKryo(kryo);
        kryo = null;
        input = null;
      }
    }
  }
}
