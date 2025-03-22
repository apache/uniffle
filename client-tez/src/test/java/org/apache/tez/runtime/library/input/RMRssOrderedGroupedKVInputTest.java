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

package org.apache.tez.runtime.library.input;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.UmbilicalUtils;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.MemoryUpdateCallback;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.RMRssShuffle;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.record.reader.MockedShuffleServerClient;
import org.apache.uniffle.client.record.reader.MockedShuffleWriteClient;
import org.apache.uniffle.client.record.reader.RMRecordsReader;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.merger.Merger;
import org.apache.uniffle.common.merger.Segment;
import org.apache.uniffle.common.serializer.DynBufferSerOutputStream;
import org.apache.uniffle.common.serializer.SerOutputStream;
import org.apache.uniffle.common.serializer.SerializerUtils;
import org.apache.uniffle.common.util.BlockIdLayout;

import static org.apache.tez.common.RssTezConfig.RSS_SHUFFLE_DESTINATION_VERTEX_ID;
import static org.apache.tez.common.RssTezConfig.RSS_SHUFFLE_SOURCE_VERTEX_ID;
import static org.apache.uniffle.common.serializer.SerializerUtils.genSortedRecordBuffer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class RMRssOrderedGroupedKVInputTest {

  private static final int RECORDS_NUM = 1009;
  private static final ApplicationAttemptId APPLICATION_ATTEMPT_ID =
      ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 0), 0);
  private static final int SHUFFLE_ID = 0;
  private static final int PARTITION_ID = 0;

  @Test
  public void testRMRssOrderedGroupedKVInput() throws Exception {
    // 1 basic parameter
    final Class keyClass = Text.class;
    final Class valueClass = IntWritable.class;
    final Comparator comparator = new Text.Comparator();
    final Configuration conf = new Configuration();
    conf.setInt(RSS_SHUFFLE_SOURCE_VERTEX_ID, 0);
    conf.setInt(RSS_SHUFFLE_DESTINATION_VERTEX_ID, 1);
    final RssConf rssConf = new RssConf();
    final List<ShuffleServerInfo> serverInfos =
        Lists.newArrayList(new ShuffleServerInfo("dummy", -1));
    final int taskAttemptId = 0;
    BlockIdLayout blockIdLayout = BlockIdLayout.from(rssConf);
    final long[] blockIds = new long[] {blockIdLayout.getBlockId(0, PARTITION_ID, taskAttemptId)};

    // 2 mock input context
    InputContext inputContext = mock(InputContext.class);
    when(inputContext.getSourceVertexName()).thenReturn("Map 0");
    TezCounters tezCounters = new TezCounters();
    when(inputContext.getCounters()).thenReturn(tezCounters);
    TezTaskAttemptID tezTaskAttemptID =
        TezTaskAttemptID.getInstance(
            TezTaskID.getInstance(
                TezVertexID.getInstance(
                    TezDAGID.getInstance(APPLICATION_ATTEMPT_ID.getApplicationId(), 0), 0),
                0),
            0);
    when(inputContext.getUniqueIdentifier())
        .thenReturn(String.format("%s_%05d", tezTaskAttemptID.toString(), 0));
    when(inputContext.getUserPayload()).thenReturn(TezUtils.createUserPayloadFromConf(conf));
    when(inputContext.getWorkDirs()).thenReturn(new String[] {"/dummy"});
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocation) throws Throwable {
                long requestedSize = (Long) invocation.getArguments()[0];
                MemoryUpdateCallbackHandler callback =
                    (MemoryUpdateCallbackHandler) invocation.getArguments()[1];
                callback.memoryAssigned(requestedSize);
                return null;
              }
            })
        .when(inputContext)
        .requestInitialMemory(anyLong(), any(MemoryUpdateCallback.class));
    when(inputContext.getDagIdentifier()).thenReturn(0);
    when(inputContext.getApplicationId()).thenReturn(APPLICATION_ATTEMPT_ID.getApplicationId());
    ExecutionContext executionContext = mock(ExecutionContext.class);
    when(executionContext.getHostName()).thenReturn("hostname");
    when(inputContext.getExecutionContext()).thenReturn(executionContext);
    DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
    dataOutputBuffer.writeInt(-1);
    when(inputContext.getServiceProviderMetaData(anyString()))
        .thenReturn(ByteBuffer.wrap(dataOutputBuffer.getData(), 0, dataOutputBuffer.getLength()));
    Token<JobTokenIdentifier> sessionToken =
        new Token<JobTokenIdentifier>(
            new JobTokenIdentifier(new Text("text")), new JobTokenSecretManager());
    ByteBuffer tokenBuffer = TezCommonUtils.serializeServiceData(sessionToken);
    doReturn(tokenBuffer).when(inputContext).getServiceConsumerMetaData(anyString());

    // 3 mock recordsReader
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
    ByteBuf[][] buffers = new ByteBuf[][] {{output.toByteBuf()}};
    ShuffleServerClient serverClient =
        new MockedShuffleServerClient(new int[] {PARTITION_ID}, buffers, blockIds);
    RMRecordsReader recordsReader =
        new RMRecordsReader(
            APPLICATION_ATTEMPT_ID.toString(),
            SHUFFLE_ID,
            Sets.newHashSet(PARTITION_ID),
            ImmutableMap.of(PARTITION_ID, serverInfos),
            rssConf,
            keyClass,
            valueClass,
            comparator,
            true,
            null,
            false,
            null);
    RMRecordsReader recordsReaderSpy = spy(recordsReader);
    doReturn(serverClient).when(recordsReaderSpy).createShuffleServerClient(any());

    // 4 mock shuffle
    RMRssShuffle rssShuffle = new RMRssShuffle(inputContext, conf, 5, 0, APPLICATION_ATTEMPT_ID);
    RMRssShuffle rssShuffleSpy = spy(rssShuffle);
    doReturn(recordsReaderSpy).when(rssShuffleSpy).createRMRecordsReader(anySet());
    // rssShuffleSpy.setEventHandler(new RMShuffleEventHandler(rssShuffleSpy));

    try (MockedStatic<UmbilicalUtils> umbilicalUtils = Mockito.mockStatic(UmbilicalUtils.class);
        MockedStatic<RssTezUtils> tezUtils = Mockito.mockStatic(RssTezUtils.class)) {
      umbilicalUtils
          .when(() -> UmbilicalUtils.requestShuffleServer(any(), any(), any(), anyInt()))
          .thenReturn(ImmutableMap.of(PARTITION_ID, serverInfos));
      ShuffleWriteClient writeClient = new MockedShuffleWriteClient();

      Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds =
          ImmutableMap.of(
              serverInfos.get(0), ImmutableMap.of(PARTITION_ID, Sets.newHashSet(blockIds[0])));
      writeClient.reportShuffleResult(
          serverToPartitionToBlockIds,
          APPLICATION_ATTEMPT_ID.toString(),
          SHUFFLE_ID,
          taskAttemptId,
          0);
      tezUtils.when(() -> RssTezUtils.createShuffleClient(any())).thenReturn(writeClient);
      tezUtils
          .when(() -> RssTezUtils.fetchAllRssTaskIds(anySet(), anyInt(), anyInt(), anyInt()))
          .thenReturn(Roaring64NavigableMap.bitmapOf(taskAttemptId));
      tezUtils
          .when(() -> RssTezUtils.uniqueIdentifierToAttemptId(anyString()))
          .thenReturn(tezTaskAttemptID.toString());

      // 5 init and start kv input
      RMRssOrderedGroupedKVInput input = new RMRssOrderedGroupedKVInput(inputContext, 5);
      RMRssOrderedGroupedKVInput inputSpy = spy(input);
      doReturn(rssShuffleSpy).when(inputSpy).createRssShuffle();
      inputSpy.initialize();
      List<Event> events = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        TezTaskAttemptID taskAttemptID =
            TezTaskAttemptID.getInstance(
                TezTaskID.getInstance(
                    TezVertexID.getInstance(
                        TezDAGID.getInstance(APPLICATION_ATTEMPT_ID.getApplicationId(), 0), 0),
                    i),
                0);
        events.add(
            createDataMovementEvent(
                PARTITION_ID, String.format("%s_%05d", taskAttemptID.toString(), 1)));
      }
      inputSpy.handleEvents(events);
      inputSpy.start();
      events.clear();
      for (int i = 2; i < 5; i++) {
        TezTaskAttemptID taskAttemptID =
            TezTaskAttemptID.getInstance(
                TezTaskID.getInstance(
                    TezVertexID.getInstance(
                        TezDAGID.getInstance(APPLICATION_ATTEMPT_ID.getApplicationId(), 0), 0),
                    i),
                0);
        events.add(
            createDataMovementEvent(
                PARTITION_ID, String.format("%s_%05d", taskAttemptID.toString(), 1)));
      }
      inputSpy.handleEvents(events);

      // 6 verify result
      KeyValuesReader reader = inputSpy.getReader();
      int index = 0;
      while (reader.next()) {
        assertEquals(SerializerUtils.genData(keyClass, index), reader.getCurrentKey());
        Iterator iterator = reader.getCurrentValues().iterator();
        int i = 0;
        while (iterator.hasNext()) {
          assertEquals(SerializerUtils.genData(valueClass, index), iterator.next());
          i++;
        }
        assertEquals(index % 2 == 0 ? 2 : 1, i);
        index++;
      }
      assertEquals(RECORDS_NUM * 2, index);
    }
  }

  @Test
  public void testRMRssOrderedGroupedKVInputMulitPartition() throws Exception {
    // 1 basic parameter
    final Class keyClass = Text.class;
    final Class valueClass = IntWritable.class;
    final Comparator comparator = new Text.Comparator();
    final Configuration conf = new Configuration();
    conf.setInt(RSS_SHUFFLE_SOURCE_VERTEX_ID, 0);
    conf.setInt(RSS_SHUFFLE_DESTINATION_VERTEX_ID, 1);
    final RssConf rssConf = new RssConf();
    final List<ShuffleServerInfo> serverInfos =
        Lists.newArrayList(new ShuffleServerInfo("dummy", -1));
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

    // 2 mock input context
    InputContext inputContext = mock(InputContext.class);
    when(inputContext.getSourceVertexName()).thenReturn("Map 0");
    TezCounters tezCounters = new TezCounters();
    when(inputContext.getCounters()).thenReturn(tezCounters);
    TezTaskAttemptID tezTaskAttemptID =
        TezTaskAttemptID.getInstance(
            TezTaskID.getInstance(
                TezVertexID.getInstance(
                    TezDAGID.getInstance(APPLICATION_ATTEMPT_ID.getApplicationId(), 0), 0),
                0),
            0);
    when(inputContext.getUniqueIdentifier())
        .thenReturn(String.format("%s_%05d", tezTaskAttemptID.toString(), 0));
    when(inputContext.getUserPayload()).thenReturn(TezUtils.createUserPayloadFromConf(conf));
    when(inputContext.getWorkDirs()).thenReturn(new String[] {"/dummy"});
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocation) throws Throwable {
                long requestedSize = (Long) invocation.getArguments()[0];
                MemoryUpdateCallbackHandler callback =
                    (MemoryUpdateCallbackHandler) invocation.getArguments()[1];
                callback.memoryAssigned(requestedSize);
                return null;
              }
            })
        .when(inputContext)
        .requestInitialMemory(anyLong(), any(MemoryUpdateCallback.class));
    when(inputContext.getDagIdentifier()).thenReturn(0);
    when(inputContext.getApplicationId()).thenReturn(APPLICATION_ATTEMPT_ID.getApplicationId());
    ExecutionContext executionContext = mock(ExecutionContext.class);
    when(executionContext.getHostName()).thenReturn("hostname");
    when(inputContext.getExecutionContext()).thenReturn(executionContext);
    DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
    dataOutputBuffer.writeInt(-1);
    when(inputContext.getServiceProviderMetaData(anyString()))
        .thenReturn(ByteBuffer.wrap(dataOutputBuffer.getData(), 0, dataOutputBuffer.getLength()));
    Token<JobTokenIdentifier> sessionToken =
        new Token<JobTokenIdentifier>(
            new JobTokenIdentifier(new Text("text")), new JobTokenSecretManager());
    ByteBuffer tokenBuffer = TezCommonUtils.serializeServiceData(sessionToken);
    doReturn(tokenBuffer).when(inputContext).getServiceConsumerMetaData(anyString());

    // 3 mock recordsReader
    RMRecordsReader recordsReader =
        new RMRecordsReader(
            APPLICATION_ATTEMPT_ID.toString(),
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
            true,
            null,
            false,
            null);
    RMRecordsReader recordsReaderSpy = spy(recordsReader);
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

    // 4 mock shuffle
    RMRssShuffle rssShuffle = new RMRssShuffle(inputContext, conf, 5, 0, APPLICATION_ATTEMPT_ID);
    RMRssShuffle rssShuffleSpy = spy(rssShuffle);
    doReturn(recordsReaderSpy).when(rssShuffleSpy).createRMRecordsReader(anySet());
    // rssShuffleSpy.setEventHandler(new RMShuffleEventHandler(rssShuffleSpy));

    try (MockedStatic<UmbilicalUtils> umbilicalUtils = Mockito.mockStatic(UmbilicalUtils.class);
        MockedStatic<RssTezUtils> tezUtils = Mockito.mockStatic(RssTezUtils.class)) {
      umbilicalUtils
          .when(() -> UmbilicalUtils.requestShuffleServer(any(), any(), any(), anyInt()))
          .thenReturn(
              ImmutableMap.of(
                  PARTITION_ID,
                  serverInfos,
                  PARTITION_ID + 1,
                  serverInfos,
                  PARTITION_ID + 2,
                  serverInfos));
      ShuffleWriteClient writeClient = new MockedShuffleWriteClient();
      Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds =
          ImmutableMap.of(
              serverInfos.get(0),
              ImmutableMap.of(
                  PARTITION_ID,
                  Sets.newHashSet(blockIds[0], blockIds[1]),
                  PARTITION_ID + 1,
                  Sets.newHashSet(blockIds[2], blockIds[3]),
                  PARTITION_ID + 2,
                  Sets.newHashSet(blockIds[4], blockIds[5])));
      writeClient.reportShuffleResult(
          serverToPartitionToBlockIds,
          APPLICATION_ATTEMPT_ID.toString(),
          SHUFFLE_ID,
          taskAttemptId,
          0);
      tezUtils.when(() -> RssTezUtils.createShuffleClient(any())).thenReturn(writeClient);
      tezUtils
          .when(() -> RssTezUtils.fetchAllRssTaskIds(anySet(), anyInt(), anyInt(), anyInt()))
          .thenReturn(Roaring64NavigableMap.bitmapOf(taskAttemptId));
      tezUtils
          .when(() -> RssTezUtils.uniqueIdentifierToAttemptId(anyString()))
          .thenReturn(tezTaskAttemptID.toString());

      // 5 init and start kv input
      RMRssOrderedGroupedKVInput input = new RMRssOrderedGroupedKVInput(inputContext, 5);
      RMRssOrderedGroupedKVInput inputSpy = spy(input);
      doReturn(rssShuffleSpy).when(inputSpy).createRssShuffle();
      inputSpy.initialize();
      List<Event> events = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        TezTaskAttemptID taskAttemptID =
            TezTaskAttemptID.getInstance(
                TezTaskID.getInstance(
                    TezVertexID.getInstance(
                        TezDAGID.getInstance(APPLICATION_ATTEMPT_ID.getApplicationId(), 0), 0),
                    i),
                0);
        events.add(
            createDataMovementEvent(i / 2, String.format("%s_%05d", taskAttemptID.toString(), 1)));
      }
      inputSpy.handleEvents(events);
      inputSpy.start();
      events.clear();
      for (int i = 2; i < 5; i++) {
        TezTaskAttemptID taskAttemptID =
            TezTaskAttemptID.getInstance(
                TezTaskID.getInstance(
                    TezVertexID.getInstance(
                        TezDAGID.getInstance(APPLICATION_ATTEMPT_ID.getApplicationId(), 0), 0),
                    i),
                0);
        events.add(
            createDataMovementEvent(i / 2, String.format("%s_%05d", taskAttemptID.toString(), 1)));
      }
      inputSpy.handleEvents(events);

      // 6 verify result
      KeyValuesReader reader = inputSpy.getReader();
      int index = 0;
      while (reader.next()) {
        assertEquals(SerializerUtils.genData(keyClass, index), reader.getCurrentKey());
        Iterator iterator = reader.getCurrentValues().iterator();
        int i = 0;
        while (iterator.hasNext()) {
          assertEquals(SerializerUtils.genData(valueClass, index), iterator.next());
          i++;
        }
        assertEquals(1, i);
        index++;
      }
      assertEquals(RECORDS_NUM * 6, index);
    }
  }

  private static DataMovementEvent createDataMovementEvent(int partition, String path) {
    ShuffleUserPayloads.DataMovementEventPayloadProto proto =
        ShuffleUserPayloads.DataMovementEventPayloadProto.newBuilder()
            .setPathComponent(path)
            .build();
    return DataMovementEvent.create(partition, proto.toByteString().asReadOnlyByteBuffer());
  }
}
