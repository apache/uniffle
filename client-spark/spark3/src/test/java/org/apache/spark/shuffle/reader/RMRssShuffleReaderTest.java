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

package org.apache.spark.shuffle.reader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import scala.Option;
import scala.Product2;
import scala.collection.Iterator;
import scala.math.Ordering;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import org.apache.hadoop.io.IntWritable;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.record.reader.MockedShuffleServerClient;
import org.apache.uniffle.client.record.reader.RMRecordsReader;
import org.apache.uniffle.client.record.writer.Combiner;
import org.apache.uniffle.client.record.writer.SumByKeyCombiner;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.merger.Merger;
import org.apache.uniffle.common.merger.Segment;
import org.apache.uniffle.common.serializer.DynBufferSerOutputStream;
import org.apache.uniffle.common.serializer.SerOutputStream;
import org.apache.uniffle.common.serializer.SerializerUtils;
import org.apache.uniffle.common.util.BlockIdLayout;

import static org.apache.uniffle.common.serializer.SerializerUtils.genSortedRecordBuffer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class RMRssShuffleReaderTest {

  private static final int RECORDS_NUM = 1009;
  private static final String APP_ID = "app1";
  private static final int SHUFFLE_ID = 0;
  private static final int PARTITION_ID = 0;

  @Test
  public void testReadShuffleWithoutCombine() throws Exception {
    // 1 basic parameter
    final Class keyClass = String.class;
    final Class valueClass = Integer.class;
    final Comparator comparator = Ordering.String$.MODULE$;
    final List<ShuffleServerInfo> serverInfos =
        Lists.newArrayList(new ShuffleServerInfo("dummy", -1));
    final RssConf rssConf = new RssConf();
    final int taskAttemptId = 0;
    BlockIdLayout blockIdLayout = BlockIdLayout.from(rssConf);
    final long[] blockIds = new long[] {blockIdLayout.getBlockId(0, PARTITION_ID, taskAttemptId)};
    final Combiner combiner = null;

    // 2 mock TaskContext, ShuffleDependency, RssShuffleHandle
    // 2.1 mock TaskContext
    TaskContext context = mock(TaskContext.class);
    when(context.attemptNumber()).thenReturn(1);
    when(context.taskAttemptId()).thenReturn(1L);
    when(context.taskMetrics()).thenReturn(new TaskMetrics());
    doNothing().when(context).killTaskIfInterrupted();
    // 2.2 mock ShuffleDependency
    ShuffleDependency<String, Integer, Integer> dependency = mock(ShuffleDependency.class);
    when(dependency.mapSideCombine()).thenReturn(false);
    when(dependency.aggregator()).thenReturn(Option.empty());
    when(dependency.keyClassName()).thenReturn(keyClass.getName());
    when(dependency.valueClassName()).thenReturn(valueClass.getName());
    when(dependency.keyOrdering()).thenReturn(Option.empty());
    // 2.3 mock RssShuffleHandler
    RssShuffleHandle<String, Integer, Integer> handle = mock(RssShuffleHandle.class);
    when(handle.getAppId()).thenReturn(APP_ID);
    when(handle.getDependency()).thenReturn(dependency);
    when(handle.getShuffleId()).thenReturn(SHUFFLE_ID);
    when(handle.getNumMaps()).thenReturn(1);
    when(handle.getPartitionToServers()).thenReturn(ImmutableMap.of(PARTITION_ID, serverInfos));

    // 3 construct remote merge records reader
    ShuffleReadMetrics readMetrics = new ShuffleReadMetrics();
    RMRecordsReader recordsReader =
        new RMRecordsReader(
            APP_ID,
            SHUFFLE_ID,
            Sets.newHashSet(PARTITION_ID),
            ImmutableMap.of(PARTITION_ID, serverInfos),
            rssConf,
            keyClass,
            valueClass,
            comparator,
            false,
            combiner,
            false,
            inc -> readMetrics.incRecordsRead(inc));
    final RMRecordsReader recordsReaderSpy = spy(recordsReader);
    ByteBuf byteBuf = genSortedRecordBuffer(rssConf, keyClass, valueClass, 0, 1, RECORDS_NUM, 1);
    ByteBuf[][] buffers = new ByteBuf[][] {{byteBuf}};
    ShuffleServerClient serverClient =
        new MockedShuffleServerClient(new int[] {PARTITION_ID}, buffers, blockIds);
    doReturn(serverClient).when(recordsReaderSpy).createShuffleServerClient(any());

    // 4 construct spark shuffle reader
    ShuffleWriteClient writeClient = mock(ShuffleWriteClient.class);
    RMRssShuffleReader shuffleReader =
        new RMRssShuffleReader(
            PARTITION_ID,
            PARTITION_ID + 1,
            context,
            handle,
            writeClient,
            ImmutableMap.of(PARTITION_ID, serverInfos),
            ImmutableMap.of(PARTITION_ID, Roaring64NavigableMap.bitmapOf(blockIds)),
            Roaring64NavigableMap.bitmapOf(taskAttemptId),
            readMetrics,
            null,
            rssConf,
            ClientType.GRPC.name());
    RMRssShuffleReader shuffleReaderSpy = spy(shuffleReader);
    doReturn(recordsReaderSpy).when(shuffleReaderSpy).createRMRecordsReader(anySet(), anyMap());

    // 5 read and verify result
    Iterator<Product2<String, Integer>> iterator = shuffleReaderSpy.read();
    int index = 0;
    while (iterator.hasNext()) {
      Product2<String, Integer> record = iterator.next();
      assertEquals(SerializerUtils.genData(keyClass, index), record._1());
      assertEquals(SerializerUtils.genData(valueClass, index), record._2());
      index++;
    }
    assertEquals(RECORDS_NUM, index);
    assertEquals(RECORDS_NUM, readMetrics._recordsRead().value());
    byteBuf.release();
  }

  @Test
  public void testReadShuffleWithCombine() throws Exception {
    // 1 basic parameter
    final Class keyClass = String.class;
    final Class valueClass = Integer.class;
    final Comparator comparator = Ordering.String$.MODULE$;
    final List<ShuffleServerInfo> serverInfos =
        Lists.newArrayList(new ShuffleServerInfo("dummy", -1));
    final RssConf rssConf = new RssConf();
    final int taskAttemptId = 0;
    BlockIdLayout blockIdLayout = BlockIdLayout.from(rssConf);
    final long[] blockIds = new long[] {blockIdLayout.getBlockId(0, PARTITION_ID, taskAttemptId)};
    final Combiner combiner = new SumByKeyCombiner();

    // 2 mock TaskContext, ShuffleDependency, RssShuffleHandle
    // 2.1 mock TaskContext
    TaskContext context = mock(TaskContext.class);
    when(context.attemptNumber()).thenReturn(1);
    when(context.taskAttemptId()).thenReturn(1L);
    when(context.taskMetrics()).thenReturn(new TaskMetrics());
    doNothing().when(context).killTaskIfInterrupted();
    // 2.2 mock ShuffleDependency
    ShuffleDependency<String, Integer, Integer> dependency = mock(ShuffleDependency.class);
    when(dependency.mapSideCombine()).thenReturn(false);
    when(dependency.aggregator()).thenReturn(Option.empty());
    when(dependency.keyClassName()).thenReturn(keyClass.getName());
    when(dependency.valueClassName()).thenReturn(valueClass.getName());
    when(dependency.keyOrdering()).thenReturn(Option.empty());
    // 2.3 mock RssShuffleHandler
    RssShuffleHandle<String, Integer, Integer> handle = mock(RssShuffleHandle.class);
    when(handle.getAppId()).thenReturn(APP_ID);
    when(handle.getDependency()).thenReturn(dependency);
    when(handle.getShuffleId()).thenReturn(SHUFFLE_ID);
    when(handle.getNumMaps()).thenReturn(1);
    when(handle.getPartitionToServers()).thenReturn(ImmutableMap.of(PARTITION_ID, serverInfos));

    // 3 construct remote merge records reader
    ShuffleReadMetrics readMetrics = new ShuffleReadMetrics();
    RMRecordsReader recordsReader =
        new RMRecordsReader(
            APP_ID,
            SHUFFLE_ID,
            Sets.newHashSet(PARTITION_ID),
            ImmutableMap.of(PARTITION_ID, serverInfos),
            rssConf,
            keyClass,
            valueClass,
            comparator,
            false,
            combiner,
            false,
            inc -> readMetrics.incRecordsRead(inc));
    final RMRecordsReader recordsReaderSpy = spy(recordsReader);
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
    ByteBuf[][] buffers = new ByteBuf[][] {{byteBuf}};
    ShuffleServerClient serverClient =
        new MockedShuffleServerClient(new int[] {PARTITION_ID}, buffers, blockIds);
    doReturn(serverClient).when(recordsReaderSpy).createShuffleServerClient(any());

    // 4 construct spark shuffle reader
    ShuffleWriteClient writeClient = mock(ShuffleWriteClient.class);
    RMRssShuffleReader shuffleReader =
        new RMRssShuffleReader(
            PARTITION_ID,
            PARTITION_ID + 1,
            context,
            handle,
            writeClient,
            ImmutableMap.of(PARTITION_ID, serverInfos),
            ImmutableMap.of(PARTITION_ID, Roaring64NavigableMap.bitmapOf(blockIds)),
            Roaring64NavigableMap.bitmapOf(taskAttemptId),
            readMetrics,
            null,
            rssConf,
            ClientType.GRPC.name());
    RMRssShuffleReader shuffleReaderSpy = spy(shuffleReader);
    doReturn(recordsReaderSpy).when(shuffleReaderSpy).createRMRecordsReader(anySet(), anyMap());

    // 5 read and verify result
    Iterator<Product2<String, Integer>> iterator = shuffleReaderSpy.read();
    int index = 0;
    while (iterator.hasNext()) {
      Product2<String, Integer> record = iterator.next();
      assertEquals(SerializerUtils.genData(keyClass, index), record._1());
      Object value = SerializerUtils.genData(valueClass, index);
      Object newValue = value;
      if (index % 2 == 0) {
        if (value instanceof IntWritable) {
          newValue = new IntWritable(((IntWritable) value).get() * 2);
        } else {
          newValue = (int) value * 2;
        }
      }
      assertEquals(newValue, record._2());
      index++;
    }
    assertEquals(RECORDS_NUM * 2, index);
    assertEquals(RECORDS_NUM * 3, readMetrics._recordsRead().value());
    byteBuf.release();
  }

  @Test
  public void testReadMulitPartitionShuffleWithoutCombine() throws Exception {
    // 1 basic parameter
    final Class keyClass = String.class;
    final Class valueClass = Integer.class;
    final Comparator comparator = Ordering.String$.MODULE$;
    final List<ShuffleServerInfo> serverInfos =
        Lists.newArrayList(new ShuffleServerInfo("dummy", -1));
    final RssConf rssConf = new RssConf();
    final int taskAttemptId = 0;
    BlockIdLayout blockIdLayout = BlockIdLayout.from(rssConf);
    final long[] blockIds =
        new long[] {
          blockIdLayout.getBlockId(0, PARTITION_ID, taskAttemptId),
          blockIdLayout.getBlockId(1, PARTITION_ID, taskAttemptId),
          blockIdLayout.getBlockId(0, PARTITION_ID + 1, taskAttemptId),
          blockIdLayout.getBlockId(1, PARTITION_ID + 1, taskAttemptId),
          blockIdLayout.getBlockId(0, PARTITION_ID + 2, taskAttemptId),
          blockIdLayout.getBlockId(1, PARTITION_ID + 2, taskAttemptId)
        };
    final Combiner combiner = null;

    // 2 mock TaskContext, ShuffleDependency, RssShuffleHandle
    // 2.1 mock TaskContext
    TaskContext context = mock(TaskContext.class);
    when(context.attemptNumber()).thenReturn(1);
    when(context.taskAttemptId()).thenReturn(1L);
    when(context.taskMetrics()).thenReturn(new TaskMetrics());
    doNothing().when(context).killTaskIfInterrupted();
    // 2.2 mock ShuffleDependency
    ShuffleDependency<String, Integer, Integer> dependency = mock(ShuffleDependency.class);
    when(dependency.mapSideCombine()).thenReturn(false);
    when(dependency.aggregator()).thenReturn(Option.empty());
    when(dependency.keyClassName()).thenReturn(keyClass.getName());
    when(dependency.valueClassName()).thenReturn(valueClass.getName());
    when(dependency.keyOrdering()).thenReturn(Option.empty());
    // 2.3 mock RssShuffleHandler
    RssShuffleHandle<String, Integer, Integer> handle = mock(RssShuffleHandle.class);
    when(handle.getAppId()).thenReturn(APP_ID);
    when(handle.getDependency()).thenReturn(dependency);
    when(handle.getShuffleId()).thenReturn(SHUFFLE_ID);
    when(handle.getNumMaps()).thenReturn(1);
    when(handle.getPartitionToServers()).thenReturn(ImmutableMap.of(PARTITION_ID, serverInfos));

    // 3 construct remote merge records reader
    ShuffleReadMetrics readMetrics = new ShuffleReadMetrics();
    RMRecordsReader recordsReader =
        new RMRecordsReader(
            APP_ID,
            SHUFFLE_ID,
            Sets.newHashSet(PARTITION_ID, PARTITION_ID + 1, PARTITION_ID + 2),
            ImmutableMap.of(
                PARTITION_ID,
                serverInfos,
                PARTITION_ID + 1,
                serverInfos,
                PARTITION_ID + 2,
                serverInfos),
            rssConf,
            keyClass,
            valueClass,
            comparator,
            false,
            combiner,
            false,
            inc -> {
              synchronized (this) {
                readMetrics.incRecordsRead(inc);
              }
            });
    final RMRecordsReader recordsReaderSpy = spy(recordsReader);
    ByteBuf[][] buffers = new ByteBuf[3][2];
    for (int i = 0; i < 3; i++) {
      buffers[i][0] = genSortedRecordBuffer(rssConf, keyClass, valueClass, i, 3, RECORDS_NUM, 1);
      buffers[i][1] =
          genSortedRecordBuffer(
              rssConf, keyClass, valueClass, i + RECORDS_NUM * 3, 3, RECORDS_NUM, 1);
    }
    ShuffleServerClient serverClient =
        new MockedShuffleServerClient(
            new int[] {PARTITION_ID, PARTITION_ID + 1, PARTITION_ID + 2}, buffers, blockIds);
    doReturn(serverClient).when(recordsReaderSpy).createShuffleServerClient(any());

    // 4 construct spark shuffle reader
    ShuffleWriteClient writeClient = mock(ShuffleWriteClient.class);
    RMRssShuffleReader shuffleReader =
        new RMRssShuffleReader(
            PARTITION_ID,
            PARTITION_ID + 3,
            context,
            handle,
            writeClient,
            ImmutableMap.of(
                PARTITION_ID,
                serverInfos,
                PARTITION_ID + 1,
                serverInfos,
                PARTITION_ID + 2,
                serverInfos),
            ImmutableMap.of(
                PARTITION_ID,
                Roaring64NavigableMap.bitmapOf(blockIds[0], blockIds[1]),
                PARTITION_ID + 1,
                Roaring64NavigableMap.bitmapOf(blockIds[2], blockIds[3]),
                PARTITION_ID + 2,
                Roaring64NavigableMap.bitmapOf(blockIds[4], blockIds[5])),
            Roaring64NavigableMap.bitmapOf(taskAttemptId),
            readMetrics,
            null,
            rssConf,
            ClientType.GRPC.name());
    RMRssShuffleReader shuffleReaderSpy = spy(shuffleReader);
    doReturn(recordsReaderSpy).when(shuffleReaderSpy).createRMRecordsReader(anySet(), anyMap());

    // 5 read and verify result
    Iterator<Product2<String, Integer>> iterator = shuffleReaderSpy.read();
    int index = 0;
    while (iterator.hasNext()) {
      Product2<String, Integer> record = iterator.next();
      assertEquals(SerializerUtils.genData(keyClass, index), record._1());
      assertEquals(SerializerUtils.genData(valueClass, index), record._2());
      index++;
    }
    assertEquals(RECORDS_NUM * 6, index);
    assertEquals(RECORDS_NUM * 6, readMetrics._recordsRead().value());
    Arrays.stream(buffers).forEach(bs -> Arrays.stream(bs).forEach(b -> b.release()));
  }

  @Test
  public void testReadMulitPartitionShuffleWithCombine() throws Exception {
    // 1 basic parameter
    final Class keyClass = String.class;
    final Class valueClass = Integer.class;
    final Comparator comparator = Ordering.String$.MODULE$;
    final List<ShuffleServerInfo> serverInfos =
        Lists.newArrayList(new ShuffleServerInfo("dummy", -1));
    final RssConf rssConf = new RssConf();
    final int taskAttemptId = 0;
    BlockIdLayout blockIdLayout = BlockIdLayout.from(rssConf);
    final long[] blockIds =
        new long[] {
          blockIdLayout.getBlockId(0, PARTITION_ID, taskAttemptId),
          blockIdLayout.getBlockId(1, PARTITION_ID, taskAttemptId),
          blockIdLayout.getBlockId(0, PARTITION_ID + 1, taskAttemptId),
          blockIdLayout.getBlockId(1, PARTITION_ID + 1, taskAttemptId),
          blockIdLayout.getBlockId(0, PARTITION_ID + 2, taskAttemptId),
          blockIdLayout.getBlockId(1, PARTITION_ID + 2, taskAttemptId)
        };
    final Combiner combiner = new SumByKeyCombiner();

    // 2 mock TaskContext, ShuffleDependency, RssShuffleHandle
    // 2.1 mock TaskContext
    TaskContext context = mock(TaskContext.class);
    when(context.attemptNumber()).thenReturn(1);
    when(context.taskAttemptId()).thenReturn(1L);
    when(context.taskMetrics()).thenReturn(new TaskMetrics());
    doNothing().when(context).killTaskIfInterrupted();
    // 2.2 mock ShuffleDependency
    ShuffleDependency<String, Integer, Integer> dependency = mock(ShuffleDependency.class);
    when(dependency.mapSideCombine()).thenReturn(false);
    when(dependency.aggregator()).thenReturn(Option.empty());
    when(dependency.keyClassName()).thenReturn(keyClass.getName());
    when(dependency.valueClassName()).thenReturn(valueClass.getName());
    when(dependency.keyOrdering()).thenReturn(Option.empty());
    // 2.3 mock RssShuffleHandler
    RssShuffleHandle<String, Integer, Integer> handle = mock(RssShuffleHandle.class);
    when(handle.getAppId()).thenReturn(APP_ID);
    when(handle.getDependency()).thenReturn(dependency);
    when(handle.getShuffleId()).thenReturn(SHUFFLE_ID);
    when(handle.getNumMaps()).thenReturn(1);
    when(handle.getPartitionToServers()).thenReturn(ImmutableMap.of(PARTITION_ID, serverInfos));

    // 3 construct remote merge records reader
    ShuffleReadMetrics readMetrics = new ShuffleReadMetrics();
    RMRecordsReader recordsReader =
        new RMRecordsReader(
            APP_ID,
            SHUFFLE_ID,
            Sets.newHashSet(PARTITION_ID, PARTITION_ID + 1, PARTITION_ID + 2),
            ImmutableMap.of(
                PARTITION_ID,
                serverInfos,
                PARTITION_ID + 1,
                serverInfos,
                PARTITION_ID + 2,
                serverInfos),
            rssConf,
            keyClass,
            valueClass,
            comparator,
            false,
            combiner,
            false,
            inc -> {
              synchronized (this) {
                readMetrics.incRecordsRead(inc);
              }
            });
    final RMRecordsReader recordsReaderSpy = spy(recordsReader);
    ByteBuf[][] buffers = new ByteBuf[3][2];
    for (int i = 0; i < 3; i++) {
      buffers[i][0] = genSortedRecordBuffer(rssConf, keyClass, valueClass, i, 3, RECORDS_NUM, 2);
      buffers[i][1] =
          genSortedRecordBuffer(
              rssConf, keyClass, valueClass, i + RECORDS_NUM * 3, 3, RECORDS_NUM, 2);
    }
    ShuffleServerClient serverClient =
        new MockedShuffleServerClient(
            new int[] {PARTITION_ID, PARTITION_ID + 1, PARTITION_ID + 2}, buffers, blockIds);
    doReturn(serverClient).when(recordsReaderSpy).createShuffleServerClient(any());

    // 4 construct spark shuffle reader
    ShuffleWriteClient writeClient = mock(ShuffleWriteClient.class);
    RMRssShuffleReader shuffleReader =
        new RMRssShuffleReader(
            PARTITION_ID,
            PARTITION_ID + 3,
            context,
            handle,
            writeClient,
            ImmutableMap.of(
                PARTITION_ID,
                serverInfos,
                PARTITION_ID + 1,
                serverInfos,
                PARTITION_ID + 2,
                serverInfos),
            ImmutableMap.of(
                PARTITION_ID,
                Roaring64NavigableMap.bitmapOf(blockIds[0], blockIds[1]),
                PARTITION_ID + 1,
                Roaring64NavigableMap.bitmapOf(blockIds[2], blockIds[3]),
                PARTITION_ID + 2,
                Roaring64NavigableMap.bitmapOf(blockIds[4], blockIds[5])),
            Roaring64NavigableMap.bitmapOf(taskAttemptId),
            readMetrics,
            null,
            rssConf,
            ClientType.GRPC.name());
    RMRssShuffleReader shuffleReaderSpy = spy(shuffleReader);
    doReturn(recordsReaderSpy).when(shuffleReaderSpy).createRMRecordsReader(anySet(), anyMap());

    // 5 read and verify result
    Iterator<Product2<String, Integer>> iterator = shuffleReaderSpy.read();
    int index = 0;
    while (iterator.hasNext()) {
      Product2<String, Integer> record = iterator.next();
      assertEquals(SerializerUtils.genData(keyClass, index), record._1());
      assertEquals(SerializerUtils.genData(valueClass, index * 2), record._2());
      index++;
    }
    assertEquals(RECORDS_NUM * 6, index);
    assertEquals(RECORDS_NUM * 12, readMetrics._recordsRead().value());
    Arrays.stream(buffers).forEach(bs -> Arrays.stream(bs).forEach(b -> b.release()));
  }
}
