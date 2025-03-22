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

import java.io.IOException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeOutput;

import org.apache.uniffle.common.serializer.SerOutputStream;
import org.apache.uniffle.common.serializer.SerializationStream;
import org.apache.uniffle.common.serializer.WrappedByteArrayOutputStream;

public class KryoSerializationStream<K, V> extends SerializationStream {

  private final KryoSerializerInstance instance;
  private SerOutputStream out;

  private WrappedByteArrayOutputStream byteStream = new WrappedByteArrayOutputStream(200);
  private Output output;
  private Kryo kryo;

  public KryoSerializationStream(KryoSerializerInstance instance, SerOutputStream out) {
    this.out = out;
    this.instance = instance;
  }

  @Override
  public void init() {
    this.output = new UnsafeOutput(byteStream);
    this.kryo = instance.borrowKryo();
  }

  @Override
  public void writeRecord(Object key, Object value) throws IOException {
    byteStream.reset();
    kryo.writeClassAndObject(output, key);
    kryo.writeClassAndObject(output, value);
    output.flush();
    int length = byteStream.size();
    out.preAllocate(length);
    out.write(byteStream.getBuf(), 0, length);
  }

  @Override
  public void flush() throws IOException {
    if (output == null) {
      throw new IOException("Stream is closed");
    }
    out.flush();
  }

  @Override
  public void close() throws IOException {
    if (output != null) {
      try {
        output.close();
      } finally {
        this.instance.releaseKryo(kryo);
        kryo = null;
        output = null;
      }
    }
  }

  @Override
  public long getTotalBytesWritten() {
    return output.total();
  }
}
