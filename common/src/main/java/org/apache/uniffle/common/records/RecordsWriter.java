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

package org.apache.uniffle.common.records;

import java.io.IOException;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.SerOutputStream;
import org.apache.uniffle.common.serializer.SerializationStream;
import org.apache.uniffle.common.serializer.Serializer;
import org.apache.uniffle.common.serializer.SerializerFactory;
import org.apache.uniffle.common.serializer.SerializerInstance;

public class RecordsWriter<K, V> {

  private SerializationStream stream;

  public RecordsWriter(
      RssConf rssConf,
      SerOutputStream out,
      Class keyClass,
      Class valueClass,
      boolean raw,
      boolean buffered) {
    SerializerFactory factory = new SerializerFactory(rssConf);
    Serializer serializer = factory.getSerializer(keyClass);
    assert factory.getSerializer(valueClass).getClass().equals(serializer.getClass());
    SerializerInstance instance = serializer.newInstance();
    stream = instance.serializeStream(out, raw, buffered);
  }

  public void init() {
    this.stream.init();
  }

  public void append(Object key, Object value) throws IOException {
    stream.writeRecord(key, value);
  }

  public void flush() throws IOException {
    stream.flush();
  }

  public void close() throws IOException {
    if (stream != null) {
      stream.close();
      stream = null;
    }
  }

  public long getTotalBytesWritten() {
    return stream.getTotalBytesWritten();
  }
}
