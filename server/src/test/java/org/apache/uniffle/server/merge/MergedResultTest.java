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

package org.apache.uniffle.server.merge;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.DataInputBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.merger.Merger;
import org.apache.uniffle.common.merger.Segment;
import org.apache.uniffle.common.records.RecordsReader;
import org.apache.uniffle.common.serializer.SerInputStream;
import org.apache.uniffle.common.serializer.SerOutputStream;
import org.apache.uniffle.common.serializer.Serializer;
import org.apache.uniffle.common.serializer.SerializerFactory;
import org.apache.uniffle.common.serializer.SerializerInstance;
import org.apache.uniffle.common.serializer.SerializerUtils;

import static org.apache.uniffle.common.serializer.SerializerUtils.genData;
import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MERGE_DEFAULT_MERGED_BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class MergedResultTest {

  private static final int BYTES_LEN = 10240;
  private static final int RECORDS = 1009;
  private static final int SEGMENTS = 4;

  @Test
  void testMergedResult() throws IOException {
    // 1 Construct cache
    List<Pair<Integer, ByteBuf>> blocks = new ArrayList<>();
    MergedResult.CacheMergedBlockFuntion cache =
        (ByteBuf byteBuf, long blockId, int length) -> {
          byteBuf.retain();
          assertEquals(blockId - 1, blocks.size());
          blocks.add(Pair.of(length, byteBuf));
          return true;
        };

    // 2 Write to merged result
    RssConf rssConf = new RssConf();
    rssConf.set(SERVER_MERGE_DEFAULT_MERGED_BLOCK_SIZE, String.valueOf(BYTES_LEN / 10));
    Partition partition = mock(Partition.class);
    MergedResult result = new MergedResult(rssConf, cache, -1, partition);
    SerOutputStream output = result.getOutputStream(false, BYTES_LEN);
    for (int i = 0; i < BYTES_LEN; i++) {
      output.write((byte) (i & 0x7F));
    }
    output.flush();
    output.close();

    // 3 check blocks number
    //  Max merged block is 1024, every record have one byte, so will result to 10 block
    assertEquals(10, blocks.size());

    // 4 check the blocks
    int index = 0;
    for (int i = 0; i < blocks.size(); i++) {
      int length = blocks.get(i).getLeft();
      ByteBuf byteBuf = blocks.get(i).getRight();
      assertTrue(byteBuf.readableBytes() >= length);
      for (int j = 0; j < length; j++) {
        assertEquals(index & 0x7F, byteBuf.readByte());
        index++;
      }
    }
    assertEquals(BYTES_LEN, index);
    blocks.forEach(block -> block.getRight().release());
  }

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
  void testMergeSegmentToMergeResult(String classes, @TempDir File tmpDir) throws Exception {
    // 1 Parse arguments
    String[] classArray = classes.split(",");
    Class<?> keyClass = SerializerUtils.getClassByName(classArray[0]);
    Class<?> valueClass = SerializerUtils.getClassByName(classArray[1]);
    boolean raw = classArray.length > 2 && Boolean.parseBoolean(classArray[2]);
    boolean direct = classArray.length > 3 && Boolean.parseBoolean(classArray[3]);

    // 2 Construct cache
    List<Pair<Integer, ByteBuf>> blocks = new ArrayList<>();
    MergedResult.CacheMergedBlockFuntion cache =
        (ByteBuf byteBuf, long blockId, int length) -> {
          assertEquals(blockId - 1, blocks.size());
          byteBuf.retain();
          blocks.add(Pair.of(length, byteBuf));
          return true;
        };

    // 3 Construct segments, then merge
    RssConf rssConf = new RssConf();
    List<Segment> segments = new ArrayList<>();
    Comparator comparator = SerializerUtils.getComparator(keyClass);
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
    Partition partition = mock(Partition.class);
    MergedResult result = new MergedResult(rssConf, cache, -1, partition);
    long totalBytes = segments.stream().mapToLong(segment -> segment.getSize()).sum();
    SerOutputStream mergedOutputStream = result.getOutputStream(direct, totalBytes);
    Merger.merge(rssConf, mergedOutputStream, segments, keyClass, valueClass, comparator, raw);
    mergedOutputStream.close();

    // 4 check merged blocks
    int index = 0;
    SerializerFactory factory = new SerializerFactory(rssConf);
    Serializer serializer = factory.getSerializer(keyClass);
    assert factory.getSerializer(valueClass).getClass().equals(serializer.getClass());
    SerializerInstance instance = serializer.newInstance();
    for (int i = 0; i < blocks.size(); i++) {
      RecordsReader<?, ?> reader =
          new RecordsReader<>(
              rssConf,
              SerInputStream.newInputStream(blocks.get(i).getRight()),
              keyClass,
              valueClass,
              raw,
              true);
      reader.init();
      while (reader.next()) {
        if (raw) {
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
        } else {
          assertEquals(SerializerUtils.genData(keyClass, index), reader.getCurrentKey());
          assertEquals(SerializerUtils.genData(valueClass, index), reader.getCurrentValue());
        }
        index++;
      }
      reader.close();
    }
    assertEquals(RECORDS * SEGMENTS, index);
    blocks.forEach(block -> block.getRight().release());
  }
}
