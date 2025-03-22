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

package org.apache.uniffle.client.record.writer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.uniffle.client.record.RecordBlob;
import org.apache.uniffle.client.record.RecordBuffer;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.records.RecordsReader;
import org.apache.uniffle.common.records.RecordsWriter;
import org.apache.uniffle.common.serializer.DynBufferSerOutputStream;
import org.apache.uniffle.common.serializer.SerInputStream;
import org.apache.uniffle.common.serializer.SerOutputStream;
import org.apache.uniffle.common.serializer.SerializerUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RecordCollectionTest {

  private static final int RECORDS = 1009;

  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable",
        "java.lang.String,java.lang.Integer",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer",
      })
  public void testSortAndSerializeRecords(String classes) throws Exception {
    // 1 Parse arguments
    String[] classArray = classes.split(",");
    Class keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class valueClass = SerializerUtils.getClassByName(classArray[1]);

    // 2 add record
    RecordBuffer recordBuffer = new RecordBuffer(0);
    List<Integer> indexes = new ArrayList<>();
    for (int i = 0; i < RECORDS; i++) {
      indexes.add(i);
    }
    Collections.shuffle(indexes);
    for (Integer index : indexes) {
      recordBuffer.addRecord(
          SerializerUtils.genData(keyClass, index), SerializerUtils.genData(valueClass, index));
    }

    // 3 sort
    recordBuffer.sort(SerializerUtils.getComparator(keyClass));

    // 4 serialize records
    RssConf rssConf = new RssConf();
    SerOutputStream outputStream = new DynBufferSerOutputStream();
    RecordsWriter writer =
        new RecordsWriter(rssConf, outputStream, keyClass, valueClass, false, false);
    writer.init();
    recordBuffer.serialize(writer);
    writer.close();

    // 5 check the serialized data
    ByteBuf byteBuf = outputStream.toByteBuf();
    RecordsReader reader =
        new RecordsReader<>(
            rssConf, SerInputStream.newInputStream(byteBuf), keyClass, valueClass, false, false);
    reader.init();
    int index = 0;
    while (reader.next()) {
      assertEquals(SerializerUtils.genData(keyClass, index), reader.getCurrentKey());
      assertEquals(SerializerUtils.genData(valueClass, index), reader.getCurrentValue());
      index++;
    }
    assertEquals(RECORDS, index);
    reader.close();
    byteBuf.release();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable",
        "java.lang.String,java.lang.Integer",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer",
      })
  public void testSortCombineAndSerializeRecords(String classes) throws Exception {
    // 1 Parse arguments
    String[] classArray = classes.split(",");
    Class keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class valueClass = SerializerUtils.getClassByName(classArray[1]);

    // 2 add record
    RecordBuffer recordBuffer = new RecordBuffer(0);
    List<Integer> indexes = new ArrayList<>();
    for (int i = 0; i < RECORDS; i++) {
      indexes.add(i);
    }
    Collections.shuffle(indexes);
    for (Integer index : indexes) {
      int times = index % 3 + 1;
      for (int j = 0; j < times; j++) {
        recordBuffer.addRecord(
            SerializerUtils.genData(keyClass, index),
            SerializerUtils.genData(valueClass, index + j));
      }
    }

    // 3 sort and combine
    recordBuffer.sort(SerializerUtils.getComparator(keyClass));
    RecordBlob recordBlob = new RecordBlob(0);
    recordBlob.addRecords(recordBuffer);
    recordBlob.combine(new SumByKeyCombiner(), false);

    // 4 serialize records
    RssConf rssConf = new RssConf();
    SerOutputStream outputStream = new DynBufferSerOutputStream();
    RecordsWriter writer =
        new RecordsWriter(rssConf, outputStream, keyClass, valueClass, false, false);
    writer.init();
    recordBlob.serialize(writer);
    writer.close();

    // 5 check the serialized data
    RecordsReader reader =
        new RecordsReader<>(
            rssConf,
            SerInputStream.newInputStream(outputStream.toByteBuf()),
            keyClass,
            valueClass,
            false,
            false);
    reader.init();
    int index = 0;
    while (reader.next()) {
      int aimValue = index;
      if (index % 3 == 1) {
        aimValue = 2 * aimValue + 1;
      }
      if (index % 3 == 2) {
        aimValue = 3 * aimValue + 3;
      }
      assertEquals(SerializerUtils.genData(keyClass, index), reader.getCurrentKey());
      assertEquals(SerializerUtils.genData(valueClass, aimValue), reader.getCurrentValue());
      index++;
    }
    reader.close();
    assertEquals(RECORDS, index);
  }
}
