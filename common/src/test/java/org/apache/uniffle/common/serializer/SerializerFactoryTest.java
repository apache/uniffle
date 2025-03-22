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

package org.apache.uniffle.common.serializer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.kryo.KryoSerializer;
import org.apache.uniffle.common.serializer.writable.WritableSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SerializerFactoryTest {

  private static RssConf conf = new RssConf();

  @Test
  public void testGetSerializer() {
    SerializerFactory factory = new SerializerFactory(conf);
    // 1 Test whether it is null
    assertNotNull(factory.getSerializer(Writable.class));
    assertNotNull(factory.getSerializer(IntWritable.class));
    assertNotNull(factory.getSerializer(SerializerUtils.SomeClass.class));
    assertNotNull(factory.getSerializer(String.class));
    assertNotNull(factory.getSerializer(int.class));

    // 2 Check whether the type serializer is right
    assertInstanceOf(WritableSerializer.class, factory.getSerializer(Writable.class));
    assertInstanceOf(WritableSerializer.class, factory.getSerializer(IntWritable.class));
    assertInstanceOf(KryoSerializer.class, factory.getSerializer(SerializerUtils.SomeClass.class));
    assertInstanceOf(KryoSerializer.class, factory.getSerializer(String.class));
    assertInstanceOf(KryoSerializer.class, factory.getSerializer(int.class));

    // 3 Check whether the serializer is cached
    Serializer serializer = factory.getSerializer(Writable.class);
    assertEquals(serializer, factory.getSerializer(IntWritable.class));
    serializer = factory.getSerializer(SerializerUtils.SomeClass.class);
    assertEquals(serializer, factory.getSerializer(String.class));
    assertEquals(serializer, factory.getSerializer(int.class));
  }
}
