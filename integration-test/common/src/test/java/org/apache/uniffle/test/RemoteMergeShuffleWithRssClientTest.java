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

package org.apache.uniffle.test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import org.apache.hadoop.io.IntWritable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;
import org.apache.uniffle.client.record.Record;
import org.apache.uniffle.client.record.reader.KeyValueReader;
import org.apache.uniffle.client.record.reader.RMRecordsReader;
import org.apache.uniffle.client.record.writer.Combiner;
import org.apache.uniffle.client.record.writer.SumByKeyCombiner;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.serializer.Serializer;
import org.apache.uniffle.common.serializer.SerializerFactory;
import org.apache.uniffle.common.serializer.SerializerInstance;
import org.apache.uniffle.common.serializer.SerializerUtils;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.ChecksumUtils;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.buffer.ShuffleBufferType;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.coordinator.CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RemoteMergeShuffleWithRssClientTest extends ShuffleReadWriteBase {

  private static final int SHUFFLE_ID = 0;
  private static final int PARTITION_ID = 0;
  private static final int RECORD_NUMBER = 1009;

  private static ShuffleServerInfo shuffleServerInfo;
  private ShuffleWriteClientImpl shuffleWriteClientImpl;

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    coordinatorConf.setBoolean(COORDINATOR_DYNAMIC_CLIENT_CONF_ENABLED, false);
    storeCoordinatorConf(coordinatorConf);

    ShuffleServerConf shuffleServerConf =
        shuffleServerConfWithoutPort(0, tmpDir, ServerType.GRPC_NETTY);
    shuffleServerConf.set(ShuffleServerConf.SERVER_MERGE_ENABLE, true);
    shuffleServerConf.set(ShuffleServerConf.SERVER_MERGE_DEFAULT_MERGED_BLOCK_SIZE, "1k");
    shuffleServerConf.set(
        ShuffleServerConf.SERVER_SHUFFLE_BUFFER_TYPE, ShuffleBufferType.SKIP_LIST);
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 10000000);
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    storeShuffleServerConf(shuffleServerConf);

    startServersWithRandomPorts();
    shuffleServerInfo =
        new ShuffleServerInfo(
            LOCALHOST,
            nettyShuffleServers.get(0).getGrpcPort(),
            nettyShuffleServers.get(0).getNettyPort());
  }

  public void createClient(String clientType) {
    shuffleWriteClientImpl =
        new ShuffleWriteClientImpl(
            ShuffleClientFactory.newWriteBuilder()
                .clientType(clientType)
                .retryMax(3)
                .retryIntervalMax(1000)
                .heartBeatThreadNum(1)
                .replica(1)
                .replicaWrite(1)
                .replicaRead(1)
                .replicaSkipEnabled(true)
                .dataTransferPoolSize(1)
                .dataCommitPoolSize(1)
                .unregisterThreadPoolSize(10)
                .unregisterRequestTimeSec(10));
  }

  @AfterEach
  public void closeClient() {
    shuffleWriteClientImpl.close();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,GRPC,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,GRPC_NETTY,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,GRPC,false",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,GRPC_NETTY,false",
        "java.lang.String,java.lang.Integer,GRPC",
        "java.lang.String,java.lang.Integer,GRPC_NETTY",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer,GRPC",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer,GRPC_NETTY",
      })
  @Timeout(10)
  public void remoteMergeWriteReadTest(String classes) throws Exception {
    // 1 basic parameter
    final String[] classArray = classes.split(",");
    final String keyClassName = classArray[0];
    final String valueClassName = classArray[1];
    final Class keyClass = SerializerUtils.getClassByName(keyClassName);
    final Class valueClass = SerializerUtils.getClassByName(valueClassName);
    final String clientType = classArray[2];
    final boolean raw = classArray.length > 3 ? Boolean.parseBoolean(classArray[3]) : false;
    final Comparator comparator = SerializerUtils.getComparator(keyClass);
    final RssConf rssConf = new RssConf();

    // 2 register shuffle
    createClient(clientType);
    String testAppId = "remoteMergeWriteReadTest" + classes;
    shuffleWriteClientImpl.registerShuffle(
        shuffleServerInfo,
        testAppId,
        SHUFFLE_ID,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(""),
        ShuffleDataDistributionType.NORMAL,
        0,
        0,
        RssProtos.MergeContext.newBuilder()
            .setKeyClass(keyClass.getName())
            .setValueClass(valueClass.getName())
            .setComparatorClass(comparator.getClass().getName())
            .setMergedBlockSize(-1)
            .setMergeClassLoader("")
            .build());

    // 3 report shuffle result
    // task 0 attempt 0 generate three blocks
    BlockIdLayout layout = BlockIdLayout.from(rssConf);
    List<ShuffleBlockInfo> blocks1 = new ArrayList<>();
    blocks1.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            0,
            PARTITION_ID,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            0,
            5,
            RECORD_NUMBER,
            1));
    blocks1.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            0,
            PARTITION_ID,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            2,
            5,
            RECORD_NUMBER,
            1));
    blocks1.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            0,
            PARTITION_ID,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            4,
            5,
            RECORD_NUMBER,
            1));
    shuffleWriteClientImpl.sendShuffleData(testAppId, blocks1, () -> false);
    // task 1 attempt 0 generate two blocks
    List<ShuffleBlockInfo> blocks2 = new ArrayList<>();
    blocks2.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            1,
            PARTITION_ID,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            1,
            5,
            RECORD_NUMBER,
            1));
    blocks2.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            1,
            PARTITION_ID,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            3,
            5,
            RECORD_NUMBER,
            1));
    shuffleWriteClientImpl.sendShuffleData(testAppId, blocks2, () -> false);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers =
        ImmutableMap.of(PARTITION_ID, Lists.newArrayList(shuffleServerInfo));

    // 4 report shuffle result
    Map<Integer, Set<Long>> ptb = ImmutableMap.of(PARTITION_ID, new HashSet());
    ptb.get(PARTITION_ID)
        .addAll(blocks1.stream().map(s -> s.getBlockId()).collect(Collectors.toList()));
    ptb.get(PARTITION_ID)
        .addAll(blocks2.stream().map(s -> s.getBlockId()).collect(Collectors.toList()));
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds = new HashMap();
    serverToPartitionToBlockIds.put(shuffleServerInfo, ptb);
    shuffleWriteClientImpl.reportShuffleResult(
        serverToPartitionToBlockIds, testAppId, SHUFFLE_ID, 0, 1);
    shuffleWriteClientImpl.reportShuffleResult(
        serverToPartitionToBlockIds, testAppId, SHUFFLE_ID, 1, 1);

    // 5 report unique blocks
    Roaring64NavigableMap uniqueBlockIds = Roaring64NavigableMap.bitmapOf();
    ptb.get(PARTITION_ID).stream().forEach(block -> uniqueBlockIds.add(block));
    shuffleWriteClientImpl.startSortMerge(
        Sets.newHashSet(shuffleServerInfo), testAppId, SHUFFLE_ID, PARTITION_ID, uniqueBlockIds);

    // 6 read result
    Map<Integer, List<ShuffleServerInfo>> serverInfoMap =
        ImmutableMap.of(PARTITION_ID, Lists.newArrayList(shuffleServerInfo));
    RMRecordsReader reader =
        new RMRecordsReader(
            testAppId,
            SHUFFLE_ID,
            Sets.newHashSet(PARTITION_ID),
            serverInfoMap,
            rssConf,
            keyClass,
            valueClass,
            comparator,
            raw,
            null,
            false,
            null,
            clientType);
    reader.start();
    int index = 0;
    KeyValueReader keyValueReader = reader.keyValueReader();
    while (keyValueReader.hasNext()) {
      Record record = keyValueReader.next();
      assertEquals(SerializerUtils.genData(keyClass, index), record.getKey());
      assertEquals(SerializerUtils.genData(valueClass, index), record.getValue());
      index++;
    }
    assertEquals(5 * RECORD_NUMBER, index);
    shuffleWriteClientImpl.unregisterShuffle(testAppId);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,GRPC,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,GRPC_NETTY,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,GRPC,false",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,GRPC_NETTY,false",
        "java.lang.String,java.lang.Integer,GRPC",
        "java.lang.String,java.lang.Integer,GRPC_NETTY",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer,GRPC",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer,GRPC_NETTY",
      })
  @Timeout(10)
  public void remoteMergeWriteReadTestWithCombine(String classes) throws Exception {
    // 1 basic parameter
    final String[] classArray = classes.split(",");
    final String keyClassName = classArray[0];
    final String valueClassName = classArray[1];
    final Class keyClass = SerializerUtils.getClassByName(keyClassName);
    final Class valueClass = SerializerUtils.getClassByName(valueClassName);
    final String clientType = classArray[2];
    final boolean raw = classArray.length > 3 ? Boolean.parseBoolean(classArray[3]) : false;
    final Comparator comparator = SerializerUtils.getComparator(keyClass);
    final RssConf rssConf = new RssConf();
    SerializerFactory factory = new SerializerFactory(rssConf);
    Serializer serializer = factory.getSerializer(keyClass);
    SerializerInstance serializerInstance = serializer.newInstance();
    final Combiner combiner = new SumByKeyCombiner(raw, serializerInstance, keyClass, valueClass);

    // 2 register shuffle
    createClient(clientType);
    String testAppId = "remoteMergeWriteReadTestWithCombine" + classes;
    shuffleWriteClientImpl.registerShuffle(
        shuffleServerInfo,
        testAppId,
        SHUFFLE_ID,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(""),
        ShuffleDataDistributionType.NORMAL,
        0,
        0,
        RssProtos.MergeContext.newBuilder()
            .setKeyClass(keyClass.getName())
            .setValueClass(valueClass.getName())
            .setComparatorClass(comparator.getClass().getName())
            .setMergedBlockSize(-1)
            .setMergeClassLoader("")
            .build());

    // 3 report shuffle result
    // task 0 attempt 0 generate three blocks
    BlockIdLayout layout = BlockIdLayout.from(rssConf);
    List<ShuffleBlockInfo> blocks1 = new ArrayList<>();
    blocks1.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            0,
            PARTITION_ID,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            0,
            3,
            RECORD_NUMBER,
            1));
    blocks1.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            0,
            PARTITION_ID,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            1,
            3,
            RECORD_NUMBER,
            1));
    blocks1.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            0,
            PARTITION_ID,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            2,
            3,
            RECORD_NUMBER,
            1));
    shuffleWriteClientImpl.sendShuffleData(testAppId, blocks1, () -> false);
    // task 1 attempt 0 generate two blocks
    List<ShuffleBlockInfo> blocks2 = new ArrayList<>();
    blocks2.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            1,
            PARTITION_ID,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            0,
            3,
            RECORD_NUMBER,
            1));
    blocks2.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            1,
            PARTITION_ID,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            2,
            3,
            RECORD_NUMBER,
            1));
    shuffleWriteClientImpl.sendShuffleData(testAppId, blocks2, () -> false);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers =
        ImmutableMap.of(PARTITION_ID, Lists.newArrayList(shuffleServerInfo));

    // 4 report shuffle result
    Map<Integer, Set<Long>> ptb = ImmutableMap.of(PARTITION_ID, new HashSet());
    ptb.get(PARTITION_ID)
        .addAll(blocks1.stream().map(s -> s.getBlockId()).collect(Collectors.toList()));
    ptb.get(PARTITION_ID)
        .addAll(blocks2.stream().map(s -> s.getBlockId()).collect(Collectors.toList()));
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds = new HashMap();
    serverToPartitionToBlockIds.put(shuffleServerInfo, ptb);
    shuffleWriteClientImpl.reportShuffleResult(
        serverToPartitionToBlockIds, testAppId, SHUFFLE_ID, 0, 1);
    shuffleWriteClientImpl.reportShuffleResult(
        serverToPartitionToBlockIds, testAppId, SHUFFLE_ID, 1, 1);

    // 5 report unique blocks
    Roaring64NavigableMap uniqueBlockIds = Roaring64NavigableMap.bitmapOf();
    ptb.get(PARTITION_ID).stream().forEach(block -> uniqueBlockIds.add(block));
    shuffleWriteClientImpl.startSortMerge(
        Sets.newHashSet(shuffleServerInfo), testAppId, SHUFFLE_ID, PARTITION_ID, uniqueBlockIds);

    // 6 read result
    Map<Integer, List<ShuffleServerInfo>> serverInfoMap =
        ImmutableMap.of(PARTITION_ID, Lists.newArrayList(shuffleServerInfo));
    RMRecordsReader reader =
        new RMRecordsReader(
            testAppId,
            SHUFFLE_ID,
            Sets.newHashSet(PARTITION_ID),
            serverInfoMap,
            rssConf,
            keyClass,
            valueClass,
            comparator,
            raw,
            combiner,
            false,
            null,
            clientType);
    reader.start();
    int index = 0;
    KeyValueReader keyValueReader = reader.keyValueReader();
    while (keyValueReader.hasNext()) {
      Record record = keyValueReader.next();
      assertEquals(SerializerUtils.genData(keyClass, index), record.getKey());
      Object value = SerializerUtils.genData(valueClass, index);
      Object newValue = value;
      if (index % 3 != 1) {
        if (value instanceof IntWritable) {
          newValue = new IntWritable(((IntWritable) value).get() * 2);
        } else {
          newValue = (int) value * 2;
        }
      }
      assertEquals(newValue, record.getValue());
      index++;
    }
    assertEquals(3 * RECORD_NUMBER, index);
    shuffleWriteClientImpl.unregisterShuffle(testAppId);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,GRPC,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,GRPC_NETTY,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,GRPC,false",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,GRPC_NETTY,false",
        "java.lang.String,java.lang.Integer,GRPC",
        "java.lang.String,java.lang.Integer,GRPC_NETTY",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer,GRPC",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer,GRPC_NETTY",
      })
  @Timeout(10)
  public void remoteMergeWriteReadTestMultiPartition(String classes) throws Exception {
    // 1 basic parameter
    final String[] classArray = classes.split(",");
    final String keyClassName = classArray[0];
    final String valueClassName = classArray[1];
    final Class keyClass = SerializerUtils.getClassByName(keyClassName);
    final Class valueClass = SerializerUtils.getClassByName(valueClassName);
    final String clientType = classArray[2];
    final boolean raw = classArray.length > 3 ? Boolean.parseBoolean(classArray[3]) : false;
    final Comparator comparator = SerializerUtils.getComparator(keyClass);
    final RssConf rssConf = new RssConf();

    // 2 register shuffle
    createClient(clientType);
    String testAppId = "remoteMergeWriteReadTestMultiPartition" + classes;
    shuffleWriteClientImpl.registerShuffle(
        shuffleServerInfo,
        testAppId,
        SHUFFLE_ID,
        Lists.newArrayList(
            new PartitionRange(PARTITION_ID, PARTITION_ID),
            new PartitionRange(PARTITION_ID + 1, PARTITION_ID + 1),
            new PartitionRange(PARTITION_ID + 2, PARTITION_ID + 2)),
        new RemoteStorageInfo(""),
        ShuffleDataDistributionType.NORMAL,
        0,
        0,
        RssProtos.MergeContext.newBuilder()
            .setKeyClass(keyClass.getName())
            .setValueClass(valueClass.getName())
            .setComparatorClass(comparator.getClass().getName())
            .setMergedBlockSize(-1)
            .setMergeClassLoader("")
            .build());

    // 3 report shuffle result
    // this shuffle have three partition, which is hash by key index mode 3
    // task 0 attempt 0 generate three blocks
    BlockIdLayout layout = BlockIdLayout.from(rssConf);
    List<ShuffleBlockInfo> blocks1 = new ArrayList<>();
    blocks1.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            0,
            0,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            0,
            6,
            RECORD_NUMBER,
            1));
    blocks1.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            0,
            2,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            2,
            6,
            RECORD_NUMBER,
            1));
    blocks1.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            0,
            1,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            4,
            6,
            RECORD_NUMBER,
            1));
    shuffleWriteClientImpl.sendShuffleData(testAppId, blocks1, () -> false);
    // task 1 attempt 0 generate two blocks
    List<ShuffleBlockInfo> blocks2 = new ArrayList<>();
    blocks2.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            1,
            1,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            1,
            6,
            RECORD_NUMBER,
            1));
    blocks2.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            1,
            0,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            3,
            6,
            RECORD_NUMBER,
            1));
    blocks2.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            1,
            2,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            5,
            6,
            RECORD_NUMBER,
            1));
    shuffleWriteClientImpl.sendShuffleData(testAppId, blocks2, () -> false);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers =
        ImmutableMap.of(
            PARTITION_ID,
            Lists.newArrayList(shuffleServerInfo),
            PARTITION_ID + 1,
            Lists.newArrayList(shuffleServerInfo),
            PARTITION_ID + 2,
            Lists.newArrayList(shuffleServerInfo));

    // 4 report shuffle result
    Map<Integer, Set<Long>> ptb = new HashMap<>();
    for (int i = PARTITION_ID; i < PARTITION_ID + 3; i++) {
      final int partitionId = i;
      ptb.put(partitionId, new HashSet<>());
      ptb.get(partitionId)
          .addAll(
              blocks1.stream()
                  .filter(s -> s.getPartitionId() == partitionId)
                  .map(s -> s.getBlockId())
                  .collect(Collectors.toList()));
      ptb.get(partitionId)
          .addAll(
              blocks2.stream()
                  .filter(s -> s.getPartitionId() == partitionId)
                  .map(s -> s.getBlockId())
                  .collect(Collectors.toList()));
    }
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds = new HashMap();
    serverToPartitionToBlockIds.put(shuffleServerInfo, ptb);
    shuffleWriteClientImpl.reportShuffleResult(
        serverToPartitionToBlockIds, testAppId, SHUFFLE_ID, 0, 1);
    shuffleWriteClientImpl.reportShuffleResult(
        serverToPartitionToBlockIds, testAppId, SHUFFLE_ID, 1, 1);

    // 5 report unique blocks
    for (int i = PARTITION_ID; i < PARTITION_ID + 3; i++) {
      Roaring64NavigableMap uniqueBlockIds = Roaring64NavigableMap.bitmapOf();
      ptb.get(i).stream().forEach(block -> uniqueBlockIds.add(block));
      shuffleWriteClientImpl.startSortMerge(
          Sets.newHashSet(shuffleServerInfo), testAppId, SHUFFLE_ID, i, uniqueBlockIds);
    }

    // 6 read result
    Map<Integer, List<ShuffleServerInfo>> serverInfoMap =
        ImmutableMap.of(
            PARTITION_ID,
            Lists.newArrayList(shuffleServerInfo),
            PARTITION_ID + 1,
            Lists.newArrayList(shuffleServerInfo),
            PARTITION_ID + 2,
            Lists.newArrayList(shuffleServerInfo));
    RMRecordsReader reader =
        new RMRecordsReader(
            testAppId,
            SHUFFLE_ID,
            Sets.newHashSet(PARTITION_ID, PARTITION_ID + 1, PARTITION_ID + 2),
            serverInfoMap,
            rssConf,
            keyClass,
            valueClass,
            comparator,
            raw,
            null,
            false,
            null,
            clientType);
    reader.start();
    int index = 0;
    KeyValueReader keyValueReader = reader.keyValueReader();
    while (keyValueReader.hasNext()) {
      Record record = keyValueReader.next();
      assertEquals(SerializerUtils.genData(keyClass, index), record.getKey());
      assertEquals(SerializerUtils.genData(valueClass, index), record.getValue());
      index++;
    }
    assertEquals(6 * RECORD_NUMBER, index);
    shuffleWriteClientImpl.unregisterShuffle(testAppId);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,GRPC,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,GRPC_NETTY,true",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,GRPC,false",
        "org.apache.hadoop.io.Text,org.apache.hadoop.io.IntWritable,GRPC_NETTY,false",
        "java.lang.String,java.lang.Integer,GRPC",
        "java.lang.String,java.lang.Integer,GRPC_NETTY",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer,GRPC",
        "org.apache.uniffle.common.serializer.SerializerUtils$SomeClass,java.lang.Integer,GRPC_NETTY",
      })
  @Timeout(10)
  public void remoteMergeWriteReadTestMultiPartitionWithCombine(String classes) throws Exception {
    // 1 basic parameter
    final String[] classArray = classes.split(",");
    final String keyClassName = classArray[0];
    final String valueClassName = classArray[1];
    final Class keyClass = SerializerUtils.getClassByName(keyClassName);
    final Class valueClass = SerializerUtils.getClassByName(valueClassName);
    final String clientType = classArray[2];
    final boolean raw = classArray.length > 3 ? Boolean.parseBoolean(classArray[3]) : false;
    final Comparator comparator = SerializerUtils.getComparator(keyClass);
    final RssConf rssConf = new RssConf();
    SerializerFactory factory = new SerializerFactory(rssConf);
    Serializer serializer = factory.getSerializer(keyClass);
    SerializerInstance serializerInstance = serializer.newInstance();
    final Combiner combiner = new SumByKeyCombiner(raw, serializerInstance, keyClass, valueClass);

    // 2 register shuffle
    createClient(clientType);
    String testAppId = "remoteMergeWriteReadTestMultiPartitionWithCombine" + classes;
    shuffleWriteClientImpl.registerShuffle(
        shuffleServerInfo,
        testAppId,
        SHUFFLE_ID,
        Lists.newArrayList(
            new PartitionRange(PARTITION_ID, PARTITION_ID),
            new PartitionRange(PARTITION_ID + 1, PARTITION_ID + 1),
            new PartitionRange(PARTITION_ID + 2, PARTITION_ID + 2)),
        new RemoteStorageInfo(""),
        ShuffleDataDistributionType.NORMAL,
        0,
        0,
        RssProtos.MergeContext.newBuilder()
            .setKeyClass(keyClass.getName())
            .setValueClass(valueClass.getName())
            .setComparatorClass(comparator.getClass().getName())
            .setMergedBlockSize(-1)
            .setMergeClassLoader("")
            .build());

    // 3 report shuffle result
    // this shuffle have three partition, which is hash by key index mode 3
    // task 0 attempt 0 generate three blocks
    BlockIdLayout layout = BlockIdLayout.from(rssConf);
    List<ShuffleBlockInfo> blocks1 = new ArrayList<>();
    blocks1.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            0,
            0,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            0,
            6,
            RECORD_NUMBER,
            2));
    blocks1.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            0,
            2,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            2,
            6,
            RECORD_NUMBER,
            2));
    blocks1.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            0,
            1,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            4,
            6,
            RECORD_NUMBER,
            2));
    shuffleWriteClientImpl.sendShuffleData(testAppId, blocks1, () -> false);
    // task 1 attempt 0 generate two blocks
    List<ShuffleBlockInfo> blocks2 = new ArrayList<>();
    blocks2.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            1,
            1,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            1,
            6,
            RECORD_NUMBER,
            2));
    blocks2.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            1,
            0,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            3,
            6,
            RECORD_NUMBER,
            2));
    blocks2.add(
        createShuffleBlockForRemoteMerge(
            rssConf,
            layout,
            1,
            2,
            Lists.newArrayList(shuffleServerInfo),
            keyClass,
            valueClass,
            5,
            6,
            RECORD_NUMBER,
            2));
    shuffleWriteClientImpl.sendShuffleData(testAppId, blocks2, () -> false);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers =
        ImmutableMap.of(
            PARTITION_ID,
            Lists.newArrayList(shuffleServerInfo),
            PARTITION_ID + 1,
            Lists.newArrayList(shuffleServerInfo),
            PARTITION_ID + 2,
            Lists.newArrayList(shuffleServerInfo));

    // 4 report shuffle result
    Map<Integer, Set<Long>> ptb = new HashMap<>();
    for (int i = PARTITION_ID; i < PARTITION_ID + 3; i++) {
      final int partitionId = i;
      ptb.put(partitionId, new HashSet<>());
      ptb.get(partitionId)
          .addAll(
              blocks1.stream()
                  .filter(s -> s.getPartitionId() == partitionId)
                  .map(s -> s.getBlockId())
                  .collect(Collectors.toList()));
      ptb.get(partitionId)
          .addAll(
              blocks2.stream()
                  .filter(s -> s.getPartitionId() == partitionId)
                  .map(s -> s.getBlockId())
                  .collect(Collectors.toList()));
    }
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds = new HashMap();
    serverToPartitionToBlockIds.put(shuffleServerInfo, ptb);
    shuffleWriteClientImpl.reportShuffleResult(
        serverToPartitionToBlockIds, testAppId, SHUFFLE_ID, 0, 1);
    shuffleWriteClientImpl.reportShuffleResult(
        serverToPartitionToBlockIds, testAppId, SHUFFLE_ID, 1, 1);

    // 5 report unique blocks
    for (int i = PARTITION_ID; i < PARTITION_ID + 3; i++) {
      Roaring64NavigableMap uniqueBlockIds = Roaring64NavigableMap.bitmapOf();
      ptb.get(i).stream().forEach(block -> uniqueBlockIds.add(block));
      shuffleWriteClientImpl.startSortMerge(
          new HashSet<>(partitionToServers.get(i)), testAppId, SHUFFLE_ID, i, uniqueBlockIds);
    }

    // 6 read result
    Map<Integer, List<ShuffleServerInfo>> serverInfoMap =
        ImmutableMap.of(
            PARTITION_ID,
            Lists.newArrayList(shuffleServerInfo),
            PARTITION_ID + 1,
            Lists.newArrayList(shuffleServerInfo),
            PARTITION_ID + 2,
            Lists.newArrayList(shuffleServerInfo));
    RMRecordsReader reader =
        new RMRecordsReader(
            testAppId,
            SHUFFLE_ID,
            Sets.newHashSet(PARTITION_ID, PARTITION_ID + 1, PARTITION_ID + 2),
            serverInfoMap,
            rssConf,
            keyClass,
            valueClass,
            comparator,
            raw,
            combiner,
            false,
            null,
            clientType);
    reader.start();
    int index = 0;
    KeyValueReader keyValueReader = reader.keyValueReader();
    while (keyValueReader.hasNext()) {
      Record record = keyValueReader.next();
      assertEquals(SerializerUtils.genData(keyClass, index), record.getKey());
      assertEquals(SerializerUtils.genData(valueClass, index * 2), record.getValue());
      index++;
    }
    assertEquals(6 * RECORD_NUMBER, index);
    shuffleWriteClientImpl.unregisterShuffle(testAppId);
  }

  private static final AtomicInteger ATOMIC_INT_SORTED = new AtomicInteger(0);

  public static ShuffleBlockInfo createShuffleBlockForRemoteMerge(
      RssConf rssConf,
      BlockIdLayout blockIdLayout,
      int taskAttemptId,
      int partitionId,
      List<ShuffleServerInfo> shuffleServerInfoList,
      Class keyClass,
      Class valueClass,
      int start,
      int interval,
      int samples,
      int duplicated)
      throws IOException {
    long blockId =
        blockIdLayout.getBlockId(ATOMIC_INT_SORTED.getAndIncrement(), PARTITION_ID, taskAttemptId);
    ByteBuf byteBuf =
        SerializerUtils.genSortedRecordBuffer(
            rssConf, keyClass, valueClass, start, interval, samples, duplicated);
    ByteBuffer byteBuffer = byteBuf.nioBuffer();
    return new ShuffleBlockInfo(
        SHUFFLE_ID,
        partitionId,
        blockId,
        byteBuf.readableBytes(),
        ChecksumUtils.getCrc32(byteBuffer),
        byteBuffer.array(),
        shuffleServerInfoList,
        byteBuf.readableBytes(),
        0,
        taskAttemptId);
  }
}
