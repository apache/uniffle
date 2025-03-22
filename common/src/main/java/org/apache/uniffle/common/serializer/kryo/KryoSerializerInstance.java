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

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.DataInputBuffer;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.serializer.DeserializationStream;
import org.apache.uniffle.common.serializer.SerInputStream;
import org.apache.uniffle.common.serializer.SerOutputStream;
import org.apache.uniffle.common.serializer.SerializationStream;
import org.apache.uniffle.common.serializer.SerializerInstance;

import static org.apache.uniffle.common.config.RssBaseConf.RSS_KRYO_REGISTRATION_CLASSES;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_KRYO_SCALA_REGISTRATION_REQUIRED;

public class KryoSerializerInstance extends SerializerInstance {

  private final PoolWrapper pool;
  private final boolean scalaRegistrationRequired;
  private final RssConf rssConf;

  public KryoSerializerInstance(RssConf rssConf) {
    this.pool = new PoolWrapper(rssConf);
    this.scalaRegistrationRequired = rssConf.getBoolean(RSS_KRYO_SCALA_REGISTRATION_REQUIRED);
    this.rssConf = rssConf;
  }

  public Kryo borrowKryo() {
    Kryo kryo = pool.borrow();
    kryo.reset();
    try {
      Class cls = ClassUtils.getClass("com.twitter.chill.AllScalaRegistrar");
      Object obj = cls.newInstance();
      Method method = cls.getDeclaredMethod("apply", Kryo.class);
      for (String className : StringUtils.split(rssConf.get(RSS_KRYO_REGISTRATION_CLASSES), ",")) {
        kryo.register(
            ClassUtils.getClass(Thread.currentThread().getContextClassLoader(), className));
      }
      kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
      method.invoke(obj, kryo);
    } catch (ClassNotFoundException
        | InvocationTargetException
        | InstantiationException
        | IllegalAccessException
        | NoSuchMethodException e) {
      if (scalaRegistrationRequired) {
        throw new RssException(e);
      }
    }
    return kryo;
  }

  public void releaseKryo(Kryo kryo) {
    pool.release(kryo);
  }

  public <T> void serialize(T t, DataOutputStream out) throws IOException {
    Output output = new UnsafeOutput(out);
    Kryo kryo = this.borrowKryo();
    try {
      kryo.writeClassAndObject(output, t);
      output.flush();
    } finally {
      releaseKryo(kryo);
    }
  }

  @Override
  public <T> T deserialize(DataInputBuffer buffer, Class vClass) throws IOException {
    UnsafeInput input = new UnsafeInput(buffer);
    Kryo kryo = this.borrowKryo();
    try {
      return (T) kryo.readClassAndObject(input);
    } finally {
      releaseKryo(kryo);
    }
  }

  @Override
  public <K, V> SerializationStream serializeStream(
      SerOutputStream output, boolean raw, boolean shared) {
    return new KryoSerializationStream(this, output);
  }

  @Override
  public <K, V> DeserializationStream deserializeStream(
      SerInputStream input, Class<K> keyClass, Class<V> valueClass, boolean raw, boolean shared) {
    return new KryoDeserializationStream(this, input, keyClass, valueClass);
  }
}
