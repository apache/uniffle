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

package org.apache.uniffle.client.record.reader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import org.apache.hadoop.io.IntWritable;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.record.Record;
import org.apache.uniffle.client.record.writer.Combiner;
import org.apache.uniffle.client.record.writer.SumByKeyCombiner;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.merger.Merger;
import org.apache.uniffle.common.merger.Segment;
import org.apache.uniffle.common.serializer.DynBufferSerOutputStream;
import org.apache.uniffle.common.serializer.SerOutputStream;
import org.apache.uniffle.common.serializer.SerializerFactory;
import org.apache.uniffle.common.serializer.SerializerInstance;
import org.apache.uniffle.common.serializer.SerializerUtils;

import static org.apache.uniffle.common.serializer.SerializerUtils.genSortedRecordBuffer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class RMRecordsReaderTest {

  private static final String APP_ID = "app1";
  private static final int SHUFFLE_ID = 0;
  private static final int RECORDS_NUM = 1009;

  @Timeout(30)
  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,false",
        "java.lang.String,java.lang.Integer",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer",
      })
  public void testNormalReadWithoutCombine(String classes) throws Exception {
    // 1 basic parameter
    final String[] classArray = classes.split(",");
    final String keyClassName = classArray[0];
    final String valueClassName = classArray[1];
    final boolean raw = classArray.length > 2 ? Boolean.parseBoolean(classArray[2]) : false;
    final Class keyClass = SerializerUtils.getClassByName(keyClassName);
    final Class valueClass = SerializerUtils.getClassByName(valueClassName);
    final Comparator comparator = SerializerUtils.getComparator(keyClass);
    final Combiner combiner = null;
    final int partitionId = 0;
    final RssConf rssConf = new RssConf();
    final List<ShuffleServerInfo> serverInfos = new ArrayList<>();
    serverInfos.add(new ShuffleServerInfo("dummy", -1));

    // 2 construct reader
    RMRecordsReader reader =
        new RMRecordsReader(
            APP_ID,
            SHUFFLE_ID,
            Sets.newHashSet(partitionId),
            ImmutableMap.of(partitionId, serverInfos),
            rssConf,
            keyClass,
            valueClass,
            comparator,
            raw,
            combiner,
            false,
            null);
    ByteBuf byteBuf = genSortedRecordBuffer(rssConf, keyClass, valueClass, 0, 1, RECORDS_NUM, 1);
    ShuffleServerClient serverClient =
        new MockedShuffleServerClient(new int[] {partitionId}, new ByteBuf[][] {{byteBuf}}, null);
    RMRecordsReader readerSpy = spy(reader);
    doReturn(serverClient).when(readerSpy).createShuffleServerClient(any());

    // 3 run reader and verify result
    readerSpy.start();
    int index = 0;
    KeyValueReader keyValueReader = readerSpy.keyValueReader();
    while (keyValueReader.hasNext()) {
      Record record = keyValueReader.next();
      assertEquals(SerializerUtils.genData(keyClass, index), record.getKey());
      assertEquals(SerializerUtils.genData(valueClass, index), record.getValue());
      index++;
    }
    assertEquals(RECORDS_NUM, index);
    byteBuf.release();
  }

  @Timeout(30)
  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,false",
        "java.lang.String,java.lang.Integer",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer",
      })
  public void testNormalReadWithCombine(String classes) throws Exception {
    // 1 basic parameter
    final String[] classArray = classes.split(",");
    final String keyClassName = classArray[0];
    final String valueClassName = classArray[1];
    final boolean raw = classArray.length > 2 ? Boolean.parseBoolean(classArray[2]) : false;
    final Class keyClass = SerializerUtils.getClassByName(keyClassName);
    final Class valueClass = SerializerUtils.getClassByName(valueClassName);
    final Comparator comparator = SerializerUtils.getComparator(keyClass);
    SerializerFactory factory = new SerializerFactory(new RssConf());
    org.apache.uniffle.common.serializer.Serializer serializer = factory.getSerializer(keyClass);
    SerializerInstance serializerInstance = serializer.newInstance();
    final Combiner combiner = new SumByKeyCombiner(raw, serializerInstance, keyClass, valueClass);
    final int partitionId = 0;
    final RssConf rssConf = new RssConf();
    final List<ShuffleServerInfo> serverInfos = new ArrayList<>();
    serverInfos.add(new ShuffleServerInfo("dummy", -1));

    // 2 construct reader
    List<Segment> segments = new ArrayList<>();
    segments.add(
        SerializerUtils.genMemorySegment(rssConf, keyClass, valueClass, 0L, 0, 2, RECORDS_NUM));
    segments.add(
        SerializerUtils.genMemorySegment(rssConf, keyClass, valueClass, 1L, 0, 2, RECORDS_NUM));
    segments.add(
        SerializerUtils.genMemorySegment(rssConf, keyClass, valueClass, 2L, 1, 2, RECORDS_NUM));
    segments.forEach(segment -> segment.init());
    SerOutputStream output = new DynBufferSerOutputStream();
    Merger.merge(rssConf, output, segments, keyClass, valueClass, comparator, false);
    output.close();
    ByteBuf byteBuf = output.toByteBuf();
    ShuffleServerClient serverClient =
        new MockedShuffleServerClient(new int[] {partitionId}, new ByteBuf[][] {{byteBuf}}, null);
    RMRecordsReader reader =
        new RMRecordsReader(
            APP_ID,
            SHUFFLE_ID,
            Sets.newHashSet(partitionId),
            ImmutableMap.of(partitionId, serverInfos),
            rssConf,
            keyClass,
            valueClass,
            comparator,
            raw,
            combiner,
            false,
            null);
    RMRecordsReader readerSpy = spy(reader);
    doReturn(serverClient).when(readerSpy).createShuffleServerClient(any());

    // 3 run reader and verify result
    readerSpy.start();
    int index = 0;
    KeyValueReader keyValueReader = readerSpy.keyValueReader();
    while (keyValueReader.hasNext()) {
      Record record = keyValueReader.next();
      assertEquals(SerializerUtils.genData(keyClass, index), record.getKey());
      Object value = SerializerUtils.genData(valueClass, index);
      Object newValue = value;
      if (index % 2 == 0) {
        if (value instanceof IntWritable) {
          newValue = new IntWritable(((IntWritable) value).get() * 2);
        } else {
          newValue = (int) value * 2;
        }
      }
      assertEquals(newValue, record.getValue());
      index++;
    }
    assertEquals(RECORDS_NUM * 2, index);
    byteBuf.release();
  }

  @Timeout(30)
  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,false",
        "java.lang.String,java.lang.Integer",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer",
      })
  public void testReadMulitPartitionWithoutCombine(String classes) throws Exception {
    // 1 basic parameter
    final String[] classArray = classes.split(",");
    final String keyClassName = classArray[0];
    final String valueClassName = classArray[1];
    final Class keyClass = SerializerUtils.getClassByName(keyClassName);
    final Class valueClass = SerializerUtils.getClassByName(valueClassName);
    final boolean raw = classArray.length > 2 ? Boolean.parseBoolean(classArray[2]) : false;
    final Comparator comparator = SerializerUtils.getComparator(keyClass);
    final Combiner combiner = null;
    final int partitionId = 0;
    final RssConf rssConf = new RssConf();
    final List<ShuffleServerInfo> serverInfos = new ArrayList<>();
    serverInfos.add(new ShuffleServerInfo("dummy", -1));

    // 2 construct reader
    RMRecordsReader reader =
        new RMRecordsReader(
            APP_ID,
            SHUFFLE_ID,
            Sets.newHashSet(partitionId, partitionId + 1, partitionId + 2),
            ImmutableMap.of(
                partitionId,
                serverInfos,
                partitionId + 1,
                serverInfos,
                partitionId + 2,
                serverInfos),
            rssConf,
            keyClass,
            valueClass,
            comparator,
            raw,
            combiner,
            false,
            null);
    RMRecordsReader readerSpy = spy(reader);
    ByteBuf[][] buffers = new ByteBuf[3][2];
    for (int i = 0; i < 3; i++) {
      buffers[i][0] = genSortedRecordBuffer(rssConf, keyClass, valueClass, i, 3, RECORDS_NUM, 1);
      buffers[i][1] =
          genSortedRecordBuffer(
              rssConf, keyClass, valueClass, i + RECORDS_NUM * 3, 3, RECORDS_NUM, 1);
    }
    ShuffleServerClient serverClient =
        new MockedShuffleServerClient(
            new int[] {partitionId, partitionId + 1, partitionId + 2}, buffers, null);
    doReturn(serverClient).when(readerSpy).createShuffleServerClient(any());

    // 3 run reader and verify result
    readerSpy.start();
    int index = 0;
    KeyValueReader keyValueReader = readerSpy.keyValueReader();
    while (keyValueReader.hasNext()) {
      Record record = keyValueReader.next();
      assertEquals(SerializerUtils.genData(keyClass, index), record.getKey());
      assertEquals(SerializerUtils.genData(valueClass, index), record.getValue());
      index++;
    }
    assertEquals(RECORDS_NUM * 6, index);
    Arrays.stream(buffers).forEach(bs -> Arrays.stream(bs).forEach(b -> b.release()));
  }

  @Timeout(30)
  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,false",
        "java.lang.String,java.lang.Integer",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer",
      })
  public void testReadMulitPartitionWithCombine(String classes) throws Exception {
    // 1 basic parameter
    final String[] classArray = classes.split(",");
    final String keyClassName = classArray[0];
    final String valueClassName = classArray[1];
    final Class keyClass = SerializerUtils.getClassByName(keyClassName);
    final Class valueClass = SerializerUtils.getClassByName(valueClassName);
    final boolean raw = classArray.length > 2 ? Boolean.parseBoolean(classArray[2]) : false;
    final Comparator comparator = SerializerUtils.getComparator(keyClass);
    SerializerFactory factory = new SerializerFactory(new RssConf());
    org.apache.uniffle.common.serializer.Serializer serializer = factory.getSerializer(keyClass);
    SerializerInstance serializerInstance = serializer.newInstance();
    final Combiner combiner = new SumByKeyCombiner(raw, serializerInstance, keyClass, valueClass);
    final int partitionId = 0;
    final RssConf rssConf = new RssConf();
    final List<ShuffleServerInfo> serverInfos = new ArrayList<>();
    serverInfos.add(new ShuffleServerInfo("dummy", -1));

    // 2 construct reader
    RMRecordsReader reader =
        new RMRecordsReader(
            APP_ID,
            SHUFFLE_ID,
            Sets.newHashSet(partitionId, partitionId + 1, partitionId + 2),
            ImmutableMap.of(
                partitionId,
                serverInfos,
                partitionId + 1,
                serverInfos,
                partitionId + 2,
                serverInfos),
            rssConf,
            keyClass,
            valueClass,
            comparator,
            raw,
            combiner,
            false,
            null);
    RMRecordsReader readerSpy = spy(reader);
    ByteBuf[][] buffers = new ByteBuf[3][2];
    for (int i = 0; i < 3; i++) {
      buffers[i][0] = genSortedRecordBuffer(rssConf, keyClass, valueClass, i, 3, RECORDS_NUM, 2);
      buffers[i][1] =
          genSortedRecordBuffer(
              rssConf, keyClass, valueClass, i + RECORDS_NUM * 3, 3, RECORDS_NUM, 2);
    }
    ShuffleServerClient serverClient =
        new MockedShuffleServerClient(
            new int[] {partitionId, partitionId + 1, partitionId + 2}, buffers, null);
    doReturn(serverClient).when(readerSpy).createShuffleServerClient(any());

    // 3 run reader and verify result
    readerSpy.start();
    int index = 0;
    KeyValueReader keyValueReader = readerSpy.keyValueReader();
    while (keyValueReader.hasNext()) {
      Record record = keyValueReader.next();
      assertEquals(SerializerUtils.genData(keyClass, index), record.getKey());
      assertEquals(SerializerUtils.genData(valueClass, index * 2), record.getValue());
      index++;
    }
    assertEquals(RECORDS_NUM * 6, index);
    Arrays.stream(buffers).forEach(bs -> Arrays.stream(bs).forEach(b -> b.release()));
  }
}
