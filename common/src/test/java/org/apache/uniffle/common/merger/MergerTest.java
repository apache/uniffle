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

package org.apache.uniffle.common.merger;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import io.netty.buffer.ByteBuf;
import org.apache.hadoop.io.DataInputBuffer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.records.RecordsReader;
import org.apache.uniffle.common.serializer.DynBufferSerOutputStream;
import org.apache.uniffle.common.serializer.SerInputStream;
import org.apache.uniffle.common.serializer.SerOutputStream;
import org.apache.uniffle.common.serializer.Serializer;
import org.apache.uniffle.common.serializer.SerializerFactory;
import org.apache.uniffle.common.serializer.SerializerInstance;
import org.apache.uniffle.common.serializer.SerializerUtils;

import static org.apache.uniffle.common.serializer.SerializerUtils.genData;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MergerTest {

  private static final int RECORDS = 1009;
  private static final int SEGMENTS = 4;

  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,true,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,true,false",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,false,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,false,false",
        "java.lang.String,java.lang.Integer",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer",
      })
  void testMergeSegmentToFile(String classes, @TempDir File tmpDir) throws Exception {
    // 1 Parse arguments
    String[] classArray = classes.split(",");
    Class<?> keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class<?> valueClass = SerializerUtils.getClassByName(classArray[1]);
    boolean raw = classArray.length > 2 && Boolean.parseBoolean(classArray[2]);
    boolean direct = classArray.length > 3 && Boolean.parseBoolean(classArray[3]);

    // 2 Construct segments, then merge
    RssConf rssConf = new RssConf();
    List<Segment> segments = new ArrayList<>();
    Comparator<?> comparator = SerializerUtils.getComparator(keyClass);
    for (int i = 0; i < SEGMENTS; i++) {
      Segment segment =
          i % 2 == 0
              ? SerializerUtils.genMemorySegment(
                  rssConf, keyClass, valueClass, i, i, SEGMENTS, RECORDS, raw, direct)
              : SerializerUtils.genFileSegment(
                  rssConf, keyClass, valueClass, i, i, SEGMENTS, RECORDS, tmpDir, raw);
      segment.init();
      segments.add(segment);
    }
    SerOutputStream outputStream = new DynBufferSerOutputStream();
    Merger.merge(rssConf, outputStream, segments, keyClass, valueClass, comparator, raw);
    outputStream.close();

    // 3 Check the merged
    ByteBuf byteBuf = outputStream.toByteBuf();
    RecordsReader<?, ?> reader =
        new RecordsReader<>(
            rssConf, SerInputStream.newInputStream(byteBuf), keyClass, valueClass, raw, true);
    reader.init();
    SerializerFactory factory = new SerializerFactory(rssConf);
    Serializer serializer = factory.getSerializer(keyClass);
    assert factory.getSerializer(valueClass).getClass().equals(serializer.getClass());
    SerializerInstance instance = serializer.newInstance();
    int index = 0;
    while (reader.next()) {
      if (raw) {
        ByteBuf keyByteBuffer = (ByteBuf) reader.getCurrentKey();
        ByteBuf valueByteBuffer = (ByteBuf) reader.getCurrentValue();
        byte[] keyBytes = new byte[keyByteBuffer.readableBytes()];
        byte[] valueBytes = new byte[valueByteBuffer.readableBytes()];
        keyByteBuffer.readBytes(keyBytes);
        valueByteBuffer.readBytes(valueBytes);
        DataInputBuffer keyInputBuffer = new DataInputBuffer();
        keyInputBuffer.reset(keyBytes, 0, keyBytes.length);
        assertEquals(genData(keyClass, index), instance.deserialize(keyInputBuffer, keyClass));
        DataInputBuffer valueInputBuffer = new DataInputBuffer();
        valueInputBuffer.reset(valueBytes, 0, valueBytes.length);
        assertEquals(
            genData(valueClass, index), instance.deserialize(valueInputBuffer, valueClass));
      } else {
        assertEquals(genData(keyClass, index), reader.getCurrentKey());
        assertEquals(genData(valueClass, index), reader.getCurrentValue());
      }
      index++;
    }
    byteBuf.release();
    assertEquals(RECORDS * SEGMENTS, index);
    reader.close();
  }
}
