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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.merger.Merger;
import org.apache.uniffle.common.merger.Segment;
import org.apache.uniffle.common.merger.StreamedSegment;
import org.apache.uniffle.common.records.RecordsReader;
import org.apache.uniffle.common.serializer.DynBufferSerOutputStream;
import org.apache.uniffle.common.serializer.SerInputStream;
import org.apache.uniffle.common.serializer.SerOutputStream;
import org.apache.uniffle.common.serializer.SerializerUtils;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.storage.handler.api.ShuffleWriteHandler;
import org.apache.uniffle.storage.handler.impl.LocalFileServerReadHandler;
import org.apache.uniffle.storage.handler.impl.LocalFileWriteHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BlockFlushFileReaderTest {

  private static AtomicInteger ATOMIC_INT = new AtomicInteger(0);

  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,2,true,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,2,true,false",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,2,false,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,2,false,false",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,8,true,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,8,true,false",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,8,false,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,8,false,false",
        "java.lang.String,java.lang.Integer,2",
        "java.lang.String,java.lang.Integer,8",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer,2",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer,8",
      })
  void writeTestWithMerge(String classes, @TempDir File tmpDir) throws Exception {
    final String[] classArray = classes.split(",");
    final Class<?> keyClass = SerializerUtils.getClassByName(classArray[0]);
    final Class<?> valueClass = SerializerUtils.getClassByName(classArray[1]);
    final Comparator comparator = SerializerUtils.getComparator(keyClass);
    final int ringBufferSize = Integer.parseInt(classArray[2]);
    boolean raw = classArray.length > 3 && Boolean.parseBoolean(classArray[3]);
    boolean direct = classArray.length > 4 && Boolean.parseBoolean(classArray[4]);

    final File dataDir = new File(tmpDir, "data");
    final String[] basePaths = new String[] {dataDir.getAbsolutePath()};
    final LocalFileWriteHandler writeHandler1 =
        new LocalFileWriteHandler("appId", 0, 1, 1, basePaths[0], "pre");

    RssBaseConf conf = new RssBaseConf();
    conf.setString("rss.storage.basePath", dataDir.getAbsolutePath());
    final Set<Long> expectedBlockIds = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      writeTestData(
          generateBlocks(conf, keyClass, valueClass, i, 10, 10090),
          writeHandler1,
          expectedBlockIds);
    }

    LocalFileServerReadHandler readHandler =
        new LocalFileServerReadHandler("appId", 0, 1, 1, 10, dataDir.getAbsolutePath());
    String dataFileName = readHandler.getDataFileName();
    String indexFileName = readHandler.getIndexFileName();

    BlockFlushFileReader blockFlushFileReader =
        new BlockFlushFileReader(dataFileName, indexFileName, ringBufferSize, direct);

    List<Segment> segments = new ArrayList<>();
    for (Long blockId : expectedBlockIds) {
      SerInputStream inputStream = blockFlushFileReader.registerBlockInputStream(blockId);
      Segment segment =
          new StreamedSegment(
              conf, inputStream, blockId, keyClass, valueClass, inputStream.available(), raw);
      segments.add(segment);
    }
    SerOutputStream outputStream = new DynBufferSerOutputStream();
    segments.forEach(segment -> segment.init());
    blockFlushFileReader.start();
    Merger.merge(conf, outputStream, segments, keyClass, valueClass, comparator, raw);
    blockFlushFileReader.close();
    outputStream.close();
    for (Segment segment : segments) {
      segment.close();
    }

    int index = 0;
    ByteBuf byteBuf = outputStream.toByteBuf();
    RecordsReader<?, ?> reader =
        new RecordsReader<>(
            conf, SerInputStream.newInputStream(byteBuf), keyClass, valueClass, false, false);
    reader.init();
    while (reader.next()) {
      assertEquals(SerializerUtils.genData(keyClass, index), reader.getCurrentKey());
      assertEquals(SerializerUtils.genData(valueClass, index), reader.getCurrentValue());
      index++;
    }
    assertEquals(100900, index);
    blockFlushFileReader.close();
    byteBuf.release();
  }

  public static void writeTestData(
      List<ShufflePartitionedBlock> blocks, ShuffleWriteHandler handler, Set<Long> expectedBlockIds)
      throws Exception {
    blocks.forEach(block -> block.getData().retain());
    handler.write(blocks);
    blocks.forEach(block -> expectedBlockIds.add(block.getBlockId()));
    blocks.forEach(block -> block.getData().release());
  }

  public static List<ShufflePartitionedBlock> generateBlocks(
      RssConf rssConf, Class keyClass, Class valueClass, int start, int interval, int length)
      throws IOException {
    BlockIdLayout layout = BlockIdLayout.DEFAULT;
    List<ShufflePartitionedBlock> blocks = Lists.newArrayList();
    ByteBuf byteBuf =
        SerializerUtils.genSortedRecordBuffer(
            rssConf, keyClass, valueClass, start, interval, length, 1);
    long blockId = layout.getBlockId(ATOMIC_INT.incrementAndGet(), 0, 100);
    blocks.add(
        new ShufflePartitionedBlock(
            byteBuf.readableBytes(), byteBuf.readableBytes(), 0, blockId, 100, byteBuf));
    return blocks;
  }

  @Timeout(20)
  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,2,true,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,2,true,false",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,2,false,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,2,false,false",
        "java.lang.String,java.lang.Integer,2",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer,2",
      })
  void writeTestWithMergeWhenInterrupted(String classes, @TempDir File tmpDir) throws Exception {
    String[] classArray = classes.split(",");
    final Class<?> keyClass = SerializerUtils.getClassByName(classArray[0]);
    final Class<?> valueClass = SerializerUtils.getClassByName(classArray[1]);
    final Comparator comparator = SerializerUtils.getComparator(keyClass);
    int ringBufferSize = Integer.parseInt(classArray[2]);
    boolean raw = classArray.length > 3 && Boolean.parseBoolean(classArray[3]);
    boolean direct = classArray.length > 4 && Boolean.parseBoolean(classArray[4]);

    File dataDir = new File(tmpDir, "data");
    String[] basePaths = new String[] {dataDir.getAbsolutePath()};
    final LocalFileWriteHandler writeHandler1 =
        new LocalFileWriteHandler("appId", 0, 1, 1, basePaths[0], "pre");

    RssBaseConf conf = new RssBaseConf();
    conf.setString("rss.storage.basePath", dataDir.getAbsolutePath());
    final Set<Long> expectedBlockIds = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      writeTestData(
          generateBlocks(conf, keyClass, valueClass, i, 10, 10090),
          writeHandler1,
          expectedBlockIds);
    }

    File dataOutput = new File(tmpDir, "dataOutput");
    LocalFileServerReadHandler readHandler =
        new LocalFileServerReadHandler("appId", 0, 1, 1, 10, dataDir.getAbsolutePath());
    String dataFileName = readHandler.getDataFileName();
    String indexFileName = readHandler.getIndexFileName();

    BlockFlushFileReader blockFlushFileReader =
        new BlockFlushFileReader(dataFileName, indexFileName, ringBufferSize, direct);
    blockFlushFileReader.start();

    List<Segment> segments = new ArrayList<>();
    for (Long blockId : expectedBlockIds) {
      SerInputStream inputStream = blockFlushFileReader.registerBlockInputStream(blockId);
      Segment segment =
          new MockedStreamedSegment(
              conf,
              inputStream,
              blockId,
              keyClass,
              valueClass,
              inputStream.available(),
              raw,
              blockFlushFileReader);
      segments.add(segment);
    }

    SerOutputStream outputStream = new DynBufferSerOutputStream();
    segments.forEach(segment -> segment.init());
    blockFlushFileReader.start();
    assertThrows(
        Exception.class,
        () -> Merger.merge(conf, outputStream, segments, keyClass, valueClass, comparator, raw));
    outputStream.close();
    blockFlushFileReader.close();
    outputStream.close();
    for (Segment segment : segments) {
      segment.close();
    }
  }

  class MockedStreamedSegment extends StreamedSegment {

    BlockFlushFileReader reader;
    int count;

    MockedStreamedSegment(
        RssConf rssConf,
        SerInputStream inputStream,
        long blockId,
        Class<?> keyClass,
        Class<?> valueClass,
        long size,
        boolean raw,
        BlockFlushFileReader reader) {
      super(rssConf, inputStream, blockId, keyClass, valueClass, size, raw);
      this.reader = reader;
    }

    public boolean next() throws IOException {
      boolean ret = super.next();
      if (this.count++ > 200) {
        this.reader.close();
      }
      return ret;
    }
  }
}
