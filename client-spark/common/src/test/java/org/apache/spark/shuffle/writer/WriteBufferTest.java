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

package org.apache.spark.shuffle.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import scala.reflect.ClassTag$;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import io.netty.buffer.Unpooled;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.DeserializationStream;
import org.apache.uniffle.common.serializer.SerInputStream;
import org.apache.uniffle.common.serializer.SerializerUtils;
import org.apache.uniffle.common.serializer.kryo.KryoSerializerInstance;

import static org.apache.uniffle.common.serializer.SerializerUtils.genData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WriteBufferTest {

  private static final int RECORDS_NUM = 200;

  private SparkConf conf = new SparkConf(false);
  private Serializer kryoSerializer = new KryoSerializer(conf);
  private WrappedByteArrayOutputStream arrayOutputStream = new WrappedByteArrayOutputStream(32);
  private SerializationStream serializeStream =
      kryoSerializer.newInstance().serializeStream(arrayOutputStream);
  private byte[] serializedData;
  private int serializedDataLength;

  private int keyLength;
  private int valueLength;
  private byte[] sortSerializedData;

  @Test
  public void test() {
    WriterBuffer wb = new WriterBuffer(32);
    assertEquals(0, wb.getMemoryUsed());
    assertEquals(0, wb.getDataLength());

    serializeData("key", "value");
    // size of serialized kv is 12
    wb.addRecord(serializedData, serializedDataLength);
    assertEquals(32, wb.getMemoryUsed());
    assertEquals(12, wb.getDataLength());
    wb.addRecord(serializedData, serializedDataLength);
    assertEquals(32, wb.getMemoryUsed());
    // case: data size < output buffer size, when getData(), [] + buffer with 24b = 24b
    assertEquals(24, wb.getData().length);
    wb.addRecord(serializedData, serializedDataLength);
    // case: data size > output buffer size, when getData(), [1 buffer] + buffer with 12 = 36b
    assertEquals(36, wb.getData().length);
    assertEquals(64, wb.getMemoryUsed());
    wb.addRecord(serializedData, serializedDataLength);
    wb.addRecord(serializedData, serializedDataLength);
    // case: data size > output buffer size, when getData(), 2 buffer + output with 12b = 60b
    assertEquals(60, wb.getData().length);
    assertEquals(96, wb.getMemoryUsed());

    wb = new WriterBuffer(32);

    serializeData("key1111111111111111111111111111", "value222222222222222222222222222");
    wb.addRecord(serializedData, serializedDataLength);
    assertEquals(67, wb.getMemoryUsed());
    assertEquals(67, wb.getDataLength());

    serializeData("key", "value");
    wb.addRecord(serializedData, serializedDataLength);
    // 67 + 32
    assertEquals(99, wb.getMemoryUsed());
    // 67 + 12
    assertEquals(79, wb.getDataLength());
    assertEquals(79, wb.getData().length);

    wb.addRecord(serializedData, serializedDataLength);
    assertEquals(99, wb.getMemoryUsed());
    assertEquals(91, wb.getDataLength());
    assertEquals(91, wb.getData().length);
  }

  @Test
  public void testSortRecords() throws IOException {
    arrayOutputStream.reset();
    KryoSerializerInstance instance = new KryoSerializerInstance(new RssConf());
    Kryo serKryo = instance.borrowKryo();
    Output serOutput = new UnsafeOutput(this.arrayOutputStream);

    WriterBuffer wb = new WriterBuffer(1024);
    assertEquals(0, wb.getMemoryUsed());
    assertEquals(0, wb.getDataLength());

    List<Integer> arrays = new ArrayList<>();
    for (int i = 0; i < RECORDS_NUM; i++) {
      arrays.add(i);
    }
    Collections.shuffle(arrays);
    for (int i : arrays) {
      String key = (String) SerializerUtils.genData(String.class, i);
      int value = (int) SerializerUtils.genData(int.class, i);
      serializeData(key, value, serKryo, serOutput);
      wb.addRecord(sortSerializedData, keyLength, valueLength);
    }
    assertEquals(RECORDS_NUM, wb.getRecordCount());
    assertEquals(15 * RECORDS_NUM, wb.getDataLength());

    byte[] data = wb.getData(instance, SerializerUtils.getComparator(String.class));
    assertEquals(15 * RECORDS_NUM, data.length);
    // deserialized
    DeserializationStream dStream =
        instance.deserializeStream(
            SerInputStream.newInputStream(Unpooled.wrappedBuffer(data)),
            String.class,
            int.class,
            false,
            false);
    dStream.init();
    for (int i = 0; i < RECORDS_NUM; i++) {
      assertTrue(dStream.nextRecord());
      assertEquals(genData(String.class, i), dStream.getCurrentKey());
      assertEquals(i, dStream.getCurrentValue());
    }
    assertFalse(dStream.nextRecord());
  }

  private void serializeData(Object key, Object value) {
    arrayOutputStream.reset();
    serializeStream.writeKey(key, ClassTag$.MODULE$.apply(key.getClass()));
    serializeStream.writeValue(value, ClassTag$.MODULE$.apply(value.getClass()));
    serializeStream.flush();
    serializedData = arrayOutputStream.getBuf();
    serializedDataLength = arrayOutputStream.size();
  }

  private void serializeData(Object key, Object value, Kryo serKryo, Output serOutput) {
    arrayOutputStream.reset();
    serKryo.writeClassAndObject(serOutput, key);
    serOutput.flush();
    keyLength = arrayOutputStream.size();
    serKryo.writeClassAndObject(serOutput, value);
    serOutput.flush();
    valueLength = arrayOutputStream.size() - keyLength;
    sortSerializedData = arrayOutputStream.getBuf();
  }
}
