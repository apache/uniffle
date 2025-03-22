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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.DynBufferSerOutputStream;
import org.apache.uniffle.common.serializer.FileSerOutputStream;
import org.apache.uniffle.common.serializer.SerInputStream;
import org.apache.uniffle.common.serializer.SerOutputStream;
import org.apache.uniffle.common.serializer.Serializer;
import org.apache.uniffle.common.serializer.SerializerFactory;
import org.apache.uniffle.common.serializer.SerializerInstance;
import org.apache.uniffle.common.serializer.SerializerUtils;

import static org.apache.uniffle.common.serializer.SerializerUtils.genData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class RecordsReaderWriterTest {

  private static final int RECORDS = 1009;
  private static final int LOOP = 5;

  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,mem,true,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,mem,true,false",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,mem,false,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,mem,false,false",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,file,true,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,file,true,false",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,file,false,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,file,false,false",
        "java.lang.String,java.lang.Integer,mem",
        "java.lang.String,java.lang.Integer,file",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer,mem",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer,file"
      })
  void testWriteAndReadRecordFile(String classes, @TempDir File tmpDir) throws Exception {
    RssConf rssConf = new RssConf();
    // 1 Parse arguments
    String[] classArray = classes.split(",");
    Class<?> keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class<?> valueClass = SerializerUtils.getClassByName(classArray[1]);
    boolean isFileMode = classArray[2].equals("file");
    final boolean serRaw = classArray.length > 3 && Boolean.parseBoolean(classArray[3]);
    final boolean derRaw = classArray.length > 4 && Boolean.parseBoolean(classArray[4]);
    File tmpFile = new File(tmpDir, "tmp.data");
    SerializerFactory factory = new SerializerFactory(rssConf);
    Serializer serializer = factory.getSerializer(keyClass);
    assert factory.getSerializer(valueClass).getClass().equals(serializer.getClass());
    SerializerInstance instance = serializer.newInstance();

    // 2 Write
    long[] offsets = new long[RECORDS];
    SerOutputStream outputStream =
        isFileMode ? new FileSerOutputStream(tmpFile) : new DynBufferSerOutputStream();
    RecordsWriter<?, ?> writer =
        new RecordsWriter<>(rssConf, outputStream, keyClass, valueClass, serRaw, false);
    writer.init();
    for (int i = 0; i < RECORDS; i++) {
      if (serRaw) {
        DataOutputBuffer keyBuffer = new DataOutputBuffer();
        DataOutputBuffer valueBuffer = new DataOutputBuffer();
        instance.serialize(genData(keyClass, i), keyBuffer);
        instance.serialize(genData(valueClass, i), valueBuffer);
        writer.append(keyBuffer, valueBuffer);
      } else {
        writer.append(SerializerUtils.genData(keyClass, i), SerializerUtils.genData(valueClass, i));
      }
      offsets[i] = writer.getTotalBytesWritten();
    }
    writer.close();

    // 3 Read
    // 3.1 read from start
    ByteBuf byteBuf = isFileMode ? null : outputStream.toByteBuf();
    SerInputStream inputStream =
        isFileMode
            ? SerInputStream.newInputStream(tmpFile)
            : SerInputStream.newInputStream(byteBuf);
    RecordsReader<?, ?> reader =
        new RecordsReader<>(rssConf, inputStream, keyClass, valueClass, derRaw, false);
    reader.init();
    int index = 0;
    while (reader.next()) {
      if (derRaw) {
        DataOutputBuffer keyBuffer = (DataOutputBuffer) reader.getCurrentKey();
        DataInputBuffer keyInputBuffer = new DataInputBuffer();
        keyInputBuffer.reset(keyBuffer.getData(), 0, keyBuffer.getLength());
        assertEquals(
            SerializerUtils.genData(keyClass, index),
            instance.deserialize(keyInputBuffer, keyClass));
        DataOutputBuffer valueBuffer = (DataOutputBuffer) reader.getCurrentValue();
        DataInputBuffer valueInputBuffer = new DataInputBuffer();
        valueInputBuffer.reset(valueBuffer.getData(), 0, valueBuffer.getLength());
        assertEquals(
            SerializerUtils.genData(valueClass, index),
            instance.deserialize(valueInputBuffer, valueClass));
      } else {
        assertEquals(SerializerUtils.genData(keyClass, index), reader.getCurrentKey());
        assertEquals(SerializerUtils.genData(valueClass, index), reader.getCurrentValue());
      }
      index++;
    }
    assertEquals(RECORDS, index);
    reader.close();

    // 3.2 read from end
    inputStream =
        isFileMode
            ? SerInputStream.newInputStream(tmpFile, offsets[RECORDS - 1])
            : SerInputStream.newInputStream(byteBuf, (int) offsets[RECORDS - 1]);
    reader = new RecordsReader<>(rssConf, inputStream, keyClass, valueClass, derRaw, false);
    reader.init();
    assertFalse(reader.next());
    reader.close();

    // 3.3 read from random position to end
    Random random = new Random();
    long[][] indexAndOffsets = new long[LOOP + 3][2];
    indexAndOffsets[0] = new long[] {0, 0};
    indexAndOffsets[1] = new long[] {RECORDS - 1, offsets[RECORDS - 2]}; // Last record
    indexAndOffsets[2] = new long[] {RECORDS, offsets[RECORDS - 1]}; // Records that don't exist
    for (int i = 0; i < LOOP; i++) {
      int off = random.nextInt(RECORDS - 2) + 1;
      indexAndOffsets[i + 3] = new long[] {off + 1, offsets[off]};
    }
    for (long[] indexAndOffset : indexAndOffsets) {
      index = (int) indexAndOffset[0];
      long offset = indexAndOffset[1];
      inputStream =
          isFileMode
              ? SerInputStream.newInputStream(tmpFile, offset)
              : SerInputStream.newInputStream(byteBuf, (int) offset);
      reader = new RecordsReader<>(rssConf, inputStream, keyClass, valueClass, derRaw, false);
      reader.init();
      while (reader.next()) {
        if (derRaw) {
          DataOutputBuffer keyBuffer = (DataOutputBuffer) reader.getCurrentKey();
          DataInputBuffer keyInputBuffer = new DataInputBuffer();
          keyInputBuffer.reset(keyBuffer.getData(), 0, keyBuffer.getLength());
          assertEquals(
              SerializerUtils.genData(keyClass, index),
              instance.deserialize(keyInputBuffer, keyClass));
          DataOutputBuffer valueBuffer = (DataOutputBuffer) reader.getCurrentValue();
          DataInputBuffer valueInputBuffer = new DataInputBuffer();
          valueInputBuffer.reset(valueBuffer.getData(), 0, valueBuffer.getLength());
          assertEquals(
              SerializerUtils.genData(valueClass, index),
              instance.deserialize(valueInputBuffer, valueClass));
        } else {
          assertEquals(SerializerUtils.genData(keyClass, index), reader.getCurrentKey());
          assertEquals(SerializerUtils.genData(valueClass, index), reader.getCurrentValue());
        }
        index++;
      }
      assertEquals(RECORDS, index);
    }
    if (!isFileMode) {
      byteBuf.release();
    }
    reader.close();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,mem",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,file",
      })
  void testWriteAndReadRecordFileUseDirect(String classes, @TempDir File tmpDir) throws Exception {
    RssConf rssConf = new RssConf();
    // 1 Parse arguments
    String[] classArray = classes.split(",");
    Class<?> keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class<?> valueClass = SerializerUtils.getClassByName(classArray[1]);
    boolean isFileMode = classArray[2].equals("file");
    File tmpFile = new File(tmpDir, "tmp.data");
    SerializerFactory factory = new SerializerFactory(rssConf);
    Serializer serializer = factory.getSerializer(keyClass);
    assert factory.getSerializer(valueClass).getClass().equals(serializer.getClass());
    SerializerInstance instance = serializer.newInstance();

    // 2 Write
    long[] offsets = new long[RECORDS];
    SerOutputStream outputStream =
        isFileMode ? new FileSerOutputStream(tmpFile) : new DynBufferSerOutputStream();
    RecordsWriter<?, ?> writer =
        new RecordsWriter<>(rssConf, outputStream, keyClass, valueClass, true, true);
    writer.init();
    for (int i = 0; i < RECORDS; i++) {
      DataOutputBuffer keyBuffer = new DataOutputBuffer();
      DataOutputBuffer valueBuffer = new DataOutputBuffer();
      instance.serialize(genData(keyClass, i), keyBuffer);
      instance.serialize(genData(valueClass, i), valueBuffer);
      ByteBuf kBuffer = Unpooled.buffer(keyBuffer.getLength());
      kBuffer.writeBytes(ByteBuffer.wrap(keyBuffer.getData(), 0, keyBuffer.getLength()));
      ByteBuf vBuffer = Unpooled.buffer(valueBuffer.getLength());
      vBuffer.writeBytes(ByteBuffer.wrap(valueBuffer.getData(), 0, valueBuffer.getLength()));
      writer.append(kBuffer, vBuffer);
      kBuffer.release();
      vBuffer.release();
      offsets[i] = writer.getTotalBytesWritten();
    }
    writer.close();

    // 3 Read
    // 3.1 read from start
    ByteBuf byteBuf = isFileMode ? null : outputStream.toByteBuf();
    SerInputStream inputStream =
        isFileMode
            ? SerInputStream.newInputStream(tmpFile)
            : SerInputStream.newInputStream(byteBuf);
    RecordsReader<?, ?> reader =
        new RecordsReader<>(rssConf, inputStream, keyClass, valueClass, true, true);
    reader.init();
    int index = 0;
    while (reader.next()) {
      ByteBuf keyByteBuf = (ByteBuf) reader.getCurrentKey();
      ByteBuf valueByteBuf = (ByteBuf) reader.getCurrentValue();
      byte[] keyBytes = new byte[keyByteBuf.readableBytes()];
      byte[] valueBytes = new byte[valueByteBuf.readableBytes()];
      keyByteBuf.readBytes(keyBytes);
      valueByteBuf.readBytes(valueBytes);
      DataInputBuffer keyInputBuffer = new DataInputBuffer();
      keyInputBuffer.reset(keyBytes, 0, keyBytes.length);
      assertEquals(genData(keyClass, index), instance.deserialize(keyInputBuffer, keyClass));
      DataInputBuffer valueInputBuffer = new DataInputBuffer();
      valueInputBuffer.reset(valueBytes, 0, valueBytes.length);
      assertEquals(genData(valueClass, index), instance.deserialize(valueInputBuffer, valueClass));
      index++;
    }
    assertEquals(RECORDS, index);
    reader.close();

    // 3.2 read from end
    inputStream =
        isFileMode
            ? SerInputStream.newInputStream(tmpFile, offsets[RECORDS - 1])
            : SerInputStream.newInputStream(byteBuf, (int) offsets[RECORDS - 1]);
    reader = new RecordsReader<>(rssConf, inputStream, keyClass, valueClass, true, true);
    reader.init();
    assertFalse(reader.next());
    reader.close();

    // 3.3 read from random position to end
    Random random = new Random();
    long[][] indexAndOffsets = new long[LOOP + 3][2];
    indexAndOffsets[0] = new long[] {0, 0};
    indexAndOffsets[1] = new long[] {RECORDS - 1, offsets[RECORDS - 2]}; // Last record
    indexAndOffsets[2] = new long[] {RECORDS, offsets[RECORDS - 1]}; // Records that don't exist
    for (int i = 0; i < LOOP; i++) {
      int off = random.nextInt(RECORDS - 2) + 1;
      indexAndOffsets[i + 3] = new long[] {off + 1, offsets[off]};
    }
    for (long[] indexAndOffset : indexAndOffsets) {
      index = (int) indexAndOffset[0];
      long offset = indexAndOffset[1];
      inputStream =
          isFileMode
              ? SerInputStream.newInputStream(tmpFile, offset)
              : SerInputStream.newInputStream(byteBuf, (int) offset);
      reader = new RecordsReader<>(rssConf, inputStream, keyClass, valueClass, true, true);
      reader.init();
      while (reader.next()) {
        ByteBuf keyByteBuf = (ByteBuf) reader.getCurrentKey();
        ByteBuf valueByteBuf = (ByteBuf) reader.getCurrentValue();
        byte[] keyBytes = new byte[keyByteBuf.readableBytes()];
        byte[] valueBytes = new byte[valueByteBuf.readableBytes()];
        keyByteBuf.readBytes(keyBytes);
        valueByteBuf.readBytes(valueBytes);
        DataInputBuffer keyInputBuffer = new DataInputBuffer();
        keyInputBuffer.reset(keyBytes, 0, keyBytes.length);
        assertEquals(genData(keyClass, index), instance.deserialize(keyInputBuffer, keyClass));
        DataInputBuffer valueInputBuffer = new DataInputBuffer();
        valueInputBuffer.reset(valueBytes, 0, valueBytes.length);
        assertEquals(
            genData(valueClass, index), instance.deserialize(valueInputBuffer, valueClass));
        index++;
      }
      assertEquals(RECORDS, index);
    }
    if (!isFileMode) {
      byteBuf.release();
    }
    reader.close();
  }
}
