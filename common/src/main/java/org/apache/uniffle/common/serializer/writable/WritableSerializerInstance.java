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

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.DeserializationStream;
import org.apache.uniffle.common.serializer.SerInputStream;
import org.apache.uniffle.common.serializer.SerOutputStream;
import org.apache.uniffle.common.serializer.SerializationStream;
import org.apache.uniffle.common.serializer.SerializerInstance;

public class WritableSerializerInstance extends SerializerInstance {

  public WritableSerializerInstance(RssConf rssConf) {}

  public <T> void serialize(T t, DataOutputStream out) throws IOException {
    ((Writable) t).write(out);
  }

  @Override
  public <T> T deserialize(DataInputBuffer buffer, Class vClass) throws IOException {
    Writable writable = (Writable) ReflectionUtils.newInstance(vClass, null);
    writable.readFields(buffer);
    return (T) writable;
  }

  @Override
  public <K, V> SerializationStream serializeStream(
      SerOutputStream output, boolean raw, boolean buffered) {
    if (raw) {
      if (buffered) {
        return new BufferedRawWritableSerializationStream(this, output);
      } else {
        return new RawWritableSerializationStream(this, output);
      }
    } else {
      return new WritableSerializationStream(this, output);
    }
  }

  @Override
  public <K, V> DeserializationStream deserializeStream(
      SerInputStream input, Class<K> keyClass, Class<V> valueClass, boolean raw, boolean buffered) {
    if (raw) {
      if (buffered) {
        return new BufferedRawWritableDeserializationStream(this, input);
      } else {
        return new RawWritableDeserializationStream(this, input);
      }
    } else {
      return new WritableDeserializationStream(this, input, keyClass, valueClass);
    }
  }
}
