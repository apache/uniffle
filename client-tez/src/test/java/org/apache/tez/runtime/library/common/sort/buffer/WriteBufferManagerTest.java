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

package org.apache.tez.runtime.library.common.sort.buffer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.output.OutputTestHelpers;
import org.apache.tez.runtime.library.output.RssOrderedPartitionedKVOutputTest;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.impl.FailedBlockSendTracker;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.records.RecordsReader;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.serializer.SerInputStream;
import org.apache.uniffle.common.serializer.SerializerFactory;
import org.apache.uniffle.common.serializer.SerializerInstance;
import org.apache.uniffle.common.serializer.SerializerUtils;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class WriteBufferManagerTest {

  private static final int RECORDS = 1009;

  @Test
  public void testWriteException(@TempDir File tmpDir) throws IOException, InterruptedException {
    TezTaskAttemptID tezTaskAttemptID =
        TezTaskAttemptID.fromString("attempt_1681717153064_3770270_1_00_000000_0");
    final long maxMemSize = 10240;
    final String appId = "application_1681717153064_3770270";
    final long taskAttemptId = 0;
    final Set<Long> successBlockIds = Sets.newConcurrentHashSet();
    final Set<Long> failedBlockIds = Sets.newConcurrentHashSet();
    MockShuffleWriteClient writeClient = new MockShuffleWriteClient();
    RawComparator comparator = WritableComparator.get(BytesWritable.class);
    long maxSegmentSize = 3 * 1024;
    SerializationFactory serializationFactory = new SerializationFactory(new JobConf());
    Serializer<BytesWritable> keySerializer =
        serializationFactory.getSerializer(BytesWritable.class);
    Serializer<BytesWritable> valSerializer =
        serializationFactory.getSerializer(BytesWritable.class);
    long maxBufferSize = 14 * 1024 * 1024;
    double memoryThreshold = 0.8f;
    int sendThreadNum = 1;
    double sendThreshold = 0.2f;
    int batch = 50;
    int numMaps = 1;
    String storageType = "MEMORY";
    RssConf rssConf = new RssConf();
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    long sendCheckInterval = 500L;
    long sendCheckTimeout = 5;
    int bitmapSplitNum = 1;
    int shuffleId =
        RssTezUtils.computeShuffleId(
            tezTaskAttemptID.getTaskID().getVertexID().getDAGId().getId(), 1, 2);

    Configuration conf = new Configuration();
    FileSystem localFs = FileSystem.getLocal(conf);
    Path workingDir =
        new Path(
                System.getProperty(
                    "test.build.data", System.getProperty("java.io.tmpdir", tmpDir.toString())),
                RssOrderedPartitionedKVOutputTest.class.getName())
            .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());
    conf.set(
        TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, HashPartitioner.class.getName());
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, workingDir.toString());
    OutputContext outputContext = OutputTestHelpers.createOutputContext(conf, workingDir);
    TezCounter mapOutputByteCounter =
        outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES);
    TezCounter mapOutputRecordCounter =
        outputContext.getCounters().findCounter(TaskCounter.OUTPUT_RECORDS);

    WriteBufferManager<BytesWritable, BytesWritable> bufferManager =
        new WriteBufferManager(
            tezTaskAttemptID,
            maxMemSize,
            appId,
            taskAttemptId,
            successBlockIds,
            failedBlockIds,
            writeClient,
            comparator,
            maxSegmentSize,
            keySerializer,
            valSerializer,
            maxBufferSize,
            memoryThreshold,
            sendThreadNum,
            sendThreshold,
            batch,
            rssConf,
            partitionToServers,
            numMaps,
            StorageType.withMemory(StorageType.valueOf(storageType)),
            sendCheckInterval,
            sendCheckTimeout,
            bitmapSplitNum,
            shuffleId,
            true,
            mapOutputByteCounter,
            mapOutputRecordCounter,
            false,
            null,
            null);
    partitionToServers.put(1, Lists.newArrayList(mock(ShuffleServerInfo.class)));
    Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      byte[] key = new byte[20];
      byte[] value = new byte[1024];
      random.nextBytes(key);
      random.nextBytes(value);
      bufferManager.addRecord(1, new BytesWritable(key), new BytesWritable(value));
    }

    assertEquals(1000, mapOutputRecordCounter.getValue());
    assertEquals(1052000, mapOutputByteCounter.getValue());

    boolean isException = false;
    try {
      bufferManager.waitSendFinished();
    } catch (RssException re) {
      isException = true;
    }
    assertTrue(isException);
  }

  @Test
  public void testWriteNormal(@TempDir File tmpDir) throws IOException, InterruptedException {
    TezTaskAttemptID tezTaskAttemptID =
        TezTaskAttemptID.fromString("attempt_1681717153064_3770270_1_00_000000_0");
    final long maxMemSize = 10240;
    final String appId = "appattempt_1681717153064_3770270_000001";
    final long taskAttemptId = 0;
    final Set<Long> successBlockIds = Sets.newConcurrentHashSet();
    final Set<Long> failedBlockIds = Sets.newConcurrentHashSet();
    MockShuffleWriteClient writeClient = new MockShuffleWriteClient();
    writeClient.setMode(2);
    RawComparator comparator = WritableComparator.get(BytesWritable.class);
    long maxSegmentSize = 3 * 1024;
    SerializationFactory serializationFactory = new SerializationFactory(new JobConf());
    Serializer<BytesWritable> keySerializer =
        serializationFactory.getSerializer(BytesWritable.class);
    Serializer<BytesWritable> valSerializer =
        serializationFactory.getSerializer(BytesWritable.class);
    long maxBufferSize = 14 * 1024 * 1024;
    double memoryThreshold = 0.8f;
    int sendThreadNum = 1;
    double sendThreshold = 0.2f;
    int batch = 50;
    int numMaps = 1;
    String storageType = "MEMORY";
    RssConf rssConf = new RssConf();
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    long sendCheckInterval = 500L;
    long sendCheckTimeout = 60 * 1000 * 10L;
    int bitmapSplitNum = 1;
    int shuffleId =
        RssTezUtils.computeShuffleId(
            tezTaskAttemptID.getTaskID().getVertexID().getDAGId().getId(), 1, 2);

    Configuration conf = new Configuration();
    FileSystem localFs = FileSystem.getLocal(conf);
    Path workingDir =
        new Path(
                System.getProperty(
                    "test.build.data", System.getProperty("java.io.tmpdir", tmpDir.toString())),
                RssOrderedPartitionedKVOutputTest.class.getName())
            .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());
    conf.set(
        TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, HashPartitioner.class.getName());
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, workingDir.toString());
    OutputContext outputContext = OutputTestHelpers.createOutputContext(conf, workingDir);
    TezCounter mapOutputByteCounter =
        outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES);
    TezCounter mapOutputRecordCounter =
        outputContext.getCounters().findCounter(TaskCounter.OUTPUT_RECORDS);

    WriteBufferManager<BytesWritable, BytesWritable> bufferManager =
        new WriteBufferManager(
            tezTaskAttemptID,
            maxMemSize,
            appId,
            taskAttemptId,
            successBlockIds,
            failedBlockIds,
            writeClient,
            comparator,
            maxSegmentSize,
            keySerializer,
            valSerializer,
            maxBufferSize,
            memoryThreshold,
            sendThreadNum,
            sendThreshold,
            batch,
            rssConf,
            partitionToServers,
            numMaps,
            StorageType.withMemory(StorageType.valueOf(storageType)),
            sendCheckInterval,
            sendCheckTimeout,
            bitmapSplitNum,
            shuffleId,
            true,
            mapOutputByteCounter,
            mapOutputRecordCounter,
            false,
            null,
            null);

    Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      byte[] key = new byte[20];
      byte[] value = new byte[1024];
      random.nextBytes(key);
      random.nextBytes(value);
      int partitionId = random.nextInt(50);
      partitionToServers.put(partitionId, Lists.newArrayList(mock(ShuffleServerInfo.class)));
      bufferManager.addRecord(partitionId, new BytesWritable(key), new BytesWritable(value));
    }

    assertEquals(1000, mapOutputRecordCounter.getValue());
    assertEquals(1052000, mapOutputByteCounter.getValue());
    bufferManager.waitSendFinished();
    assertTrue(bufferManager.getWaitSendBuffers().isEmpty());

    for (int i = 0; i < 50; i++) {
      byte[] key = new byte[20];
      byte[] value = new byte[i * 100];
      random.nextBytes(key);
      random.nextBytes(value);
      bufferManager.addRecord(i, new BytesWritable(key), new BytesWritable(value));
    }

    assertEquals(1175900, mapOutputByteCounter.getValue());

    bufferManager.waitSendFinished();
    assertTrue(bufferManager.getWaitSendBuffers().isEmpty());
  }

  @Test
  public void testCommitBlocksWhenMemoryShuffleDisabled(@TempDir File tmpDir)
      throws IOException, InterruptedException {
    TezTaskAttemptID tezTaskAttemptID =
        TezTaskAttemptID.fromString("attempt_1681717153064_3770270_1_00_000000_0");
    final long maxMemSize = 10240;
    final String appId = "application_1681717153064_3770270";
    final long taskAttemptId = 0;
    final Set<Long> successBlockIds = Sets.newConcurrentHashSet();
    final Set<Long> failedBlockIds = Sets.newConcurrentHashSet();
    MockShuffleWriteClient writeClient = new MockShuffleWriteClient();
    writeClient.setMode(3);
    RawComparator comparator = WritableComparator.get(BytesWritable.class);
    long maxSegmentSize = 3 * 1024;
    SerializationFactory serializationFactory = new SerializationFactory(new JobConf());
    Serializer<BytesWritable> keySerializer =
        serializationFactory.getSerializer(BytesWritable.class);
    Serializer<BytesWritable> valSerializer =
        serializationFactory.getSerializer(BytesWritable.class);
    long maxBufferSize = 14 * 1024 * 1024;
    double memoryThreshold = 0.8f;
    int sendThreadNum = 1;
    double sendThreshold = 0.2f;
    int batch = 50;
    int numMaps = 1;
    RssConf rssConf = new RssConf();
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    long sendCheckInterval = 500L;
    long sendCheckTimeout = 60 * 1000 * 10L;
    int bitmapSplitNum = 1;
    int shuffleId =
        RssTezUtils.computeShuffleId(
            tezTaskAttemptID.getTaskID().getVertexID().getDAGId().getId(), 1, 2);

    Configuration conf = new Configuration();
    FileSystem localFs = FileSystem.getLocal(conf);
    Path workingDir =
        new Path(
                System.getProperty(
                    "test.build.data", System.getProperty("java.io.tmpdir", tmpDir.toString())),
                RssOrderedPartitionedKVOutputTest.class.getName())
            .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());
    conf.set(
        TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, HashPartitioner.class.getName());
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, workingDir.toString());
    OutputContext outputContext = OutputTestHelpers.createOutputContext(conf, workingDir);
    TezCounter mapOutputByteCounter =
        outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES);
    TezCounter mapOutputRecordCounter =
        outputContext.getCounters().findCounter(TaskCounter.OUTPUT_RECORDS);

    WriteBufferManager<BytesWritable, BytesWritable> bufferManager =
        new WriteBufferManager(
            tezTaskAttemptID,
            maxMemSize,
            appId,
            taskAttemptId,
            successBlockIds,
            failedBlockIds,
            writeClient,
            comparator,
            maxSegmentSize,
            keySerializer,
            valSerializer,
            maxBufferSize,
            memoryThreshold,
            sendThreadNum,
            sendThreshold,
            batch,
            rssConf,
            partitionToServers,
            numMaps,
            false,
            sendCheckInterval,
            sendCheckTimeout,
            bitmapSplitNum,
            shuffleId,
            true,
            mapOutputByteCounter,
            mapOutputRecordCounter,
            false,
            null,
            null);

    Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      byte[] key = new byte[20];
      byte[] value = new byte[1024];
      random.nextBytes(key);
      random.nextBytes(value);
      int partitionId = random.nextInt(50);
      partitionToServers.put(partitionId, Lists.newArrayList(mock(ShuffleServerInfo.class)));
      bufferManager.addRecord(partitionId, new BytesWritable(key), new BytesWritable(value));
    }
    bufferManager.waitSendFinished();

    assertEquals(1000, mapOutputRecordCounter.getValue());
    assertEquals(1052000, mapOutputByteCounter.getValue());
    assertTrue(bufferManager.getWaitSendBuffers().isEmpty());
    assertEquals(
        writeClient.mockedShuffleServer.getFinishBlockSize(),
        writeClient.mockedShuffleServer.getFlushBlockSize());
  }

  @Test
  public void testFailFastWhenFailedToSendBlocks(@TempDir File tmpDir) throws IOException {
    TezTaskAttemptID tezTaskAttemptID =
        TezTaskAttemptID.fromString("attempt_1681717153064_3770270_1_00_000000_0");
    final long maxMemSize = 10240;
    final String appId = "application_1681717153064_3770270";
    final long taskAttemptId = 0;
    final Set<Long> successBlockIds = Sets.newConcurrentHashSet();
    final Set<Long> failedBlockIds = Sets.newConcurrentHashSet();
    MockShuffleWriteClient writeClient = new MockShuffleWriteClient();
    // set mode = 1 to fake sending shuffle data failed.
    writeClient.setMode(1);
    RawComparator comparator = WritableComparator.get(BytesWritable.class);
    long maxSegmentSize = 3 * 1024;
    SerializationFactory serializationFactory = new SerializationFactory(new JobConf());
    Serializer<BytesWritable> keySerializer =
        serializationFactory.getSerializer(BytesWritable.class);
    Serializer<BytesWritable> valSerializer =
        serializationFactory.getSerializer(BytesWritable.class);
    // note: max buffer size is tiny.
    long maxBufferSize = 14 * 1024;
    double memoryThreshold = 0.8f;
    int sendThreadNum = 1;
    double sendThreshold = 0.2f;
    int batch = 50;
    int numMaps = 1;
    RssConf rssConf = new RssConf();
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    long sendCheckInterval = 500L;
    long sendCheckTimeout = 60 * 1000 * 10L;
    int bitmapSplitNum = 1;
    int shuffleId =
        RssTezUtils.computeShuffleId(
            tezTaskAttemptID.getTaskID().getVertexID().getDAGId().getId(), 1, 2);

    Configuration conf = new Configuration();
    FileSystem localFs = FileSystem.getLocal(conf);
    Path workingDir =
        new Path(
                System.getProperty(
                    "test.build.data", System.getProperty("java.io.tmpdir", tmpDir.toString())),
                RssOrderedPartitionedKVOutputTest.class.getName())
            .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());
    conf.set(
        TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, HashPartitioner.class.getName());
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, workingDir.toString());
    OutputContext outputContext = OutputTestHelpers.createOutputContext(conf, workingDir);
    TezCounter mapOutputByteCounter =
        outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES);
    TezCounter mapOutputRecordCounter =
        outputContext.getCounters().findCounter(TaskCounter.OUTPUT_RECORDS);

    WriteBufferManager<BytesWritable, BytesWritable> bufferManager =
        new WriteBufferManager(
            tezTaskAttemptID,
            maxMemSize,
            appId,
            taskAttemptId,
            successBlockIds,
            failedBlockIds,
            writeClient,
            comparator,
            maxSegmentSize,
            keySerializer,
            valSerializer,
            maxBufferSize,
            memoryThreshold,
            sendThreadNum,
            sendThreshold,
            batch,
            rssConf,
            partitionToServers,
            numMaps,
            false,
            sendCheckInterval,
            sendCheckTimeout,
            bitmapSplitNum,
            shuffleId,
            true,
            mapOutputByteCounter,
            mapOutputRecordCounter,
            false,
            null,
            null);

    Random random = new Random();
    RssException rssException =
        assertThrows(
            RssException.class,
            () -> {
              for (int i = 0; i < 10000; i++) {
                byte[] key = new byte[20];
                byte[] value = new byte[1024];
                random.nextBytes(key);
                random.nextBytes(value);
                int partitionId = random.nextInt(50);
                partitionToServers.put(
                    partitionId, Lists.newArrayList(mock(ShuffleServerInfo.class)));
                bufferManager.addRecord(
                    partitionId, new BytesWritable(key), new BytesWritable(value));
              }
            });
    assertTrue(rssException.getMessage().contains("Send failed"));

    rssException = assertThrows(RssException.class, bufferManager::waitSendFinished);
    assertTrue(rssException.getMessage().contains("Send failed"));

    assertTrue(mapOutputRecordCounter.getValue() < 10000);
    assertTrue(mapOutputByteCounter.getValue() < 10520000);
  }

  @Test
  public void testWriteWithRemoteMerge() throws Exception {
    MockShuffleWriteClient client = new MockShuffleWriteClient();
    client.setMode(3);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = JavaUtils.newConcurrentMap();
    partitionToServers.put(0, new ArrayList());
    partitionToServers.get(0).add(new ShuffleServerInfo("host", 39998));
    Set<Long> successBlockIds = Sets.newConcurrentHashSet();
    Set<Long> failedBlockIds = Sets.newConcurrentHashSet();
    RssConf rssConf = new RssConf();
    TezCounters counter = new TezCounters();
    TezCounter mapOutputByteCounter = counter.findCounter("group", "bytes");
    TezCounter mapOutputRecordCounter = counter.findCounter("group", "records");
    TezTaskAttemptID tezTaskAttemptID =
        TezTaskAttemptID.fromString("attempt_1681717153064_3770270_1_00_000000_0");
    final long maxMemSize = 102400L;
    final String appId = "application_1681717153064_3770270";
    final int taskAttemptId = 0;
    long maxSegmentSize = 3 * 1024;
    long maxBufferSize = 14 * 1024 * 1024;
    int shuffleId =
        RssTezUtils.computeShuffleId(
            tezTaskAttemptID.getTaskID().getVertexID().getDAGId().getId(), 1, 2);

    WriteBufferManager<Text, Text> manager =
        new WriteBufferManager<Text, Text>(
            tezTaskAttemptID,
            maxMemSize,
            appId,
            taskAttemptId,
            successBlockIds,
            failedBlockIds,
            client,
            new Text.Comparator(),
            maxSegmentSize,
            null,
            null,
            maxBufferSize,
            0.8f,
            1,
            0.2f,
            50,
            rssConf,
            partitionToServers,
            1,
            false,
            500L,
            5 * 1000,
            1,
            shuffleId,
            true,
            mapOutputByteCounter,
            mapOutputRecordCounter,
            true,
            Text.class,
            Text.class);

    List<Integer> indexes = new ArrayList<>();
    for (int i = 0; i < RECORDS; i++) {
      indexes.add(i);
    }
    Collections.shuffle(indexes);
    for (Integer index : indexes) {
      manager.addRecord(
          0,
          (Text) SerializerUtils.genData(Text.class, index),
          (Text) SerializerUtils.genData(Text.class, index + 1));
    }
    manager.waitSendFinished();
    assertEquals(RECORDS, mapOutputRecordCounter.getValue());
    SerializerFactory factory = new SerializerFactory(rssConf);
    org.apache.uniffle.common.serializer.Serializer serializer = factory.getSerializer(Text.class);
    SerializerInstance instance = serializer.newInstance();
    DataOutputBuffer keyBuffer = new DataOutputBuffer();
    instance.serialize(SerializerUtils.genData(Text.class, 0), keyBuffer);
    assertEquals(RECORDS * keyBuffer.getLength() * 2, mapOutputByteCounter.getValue());
    assertTrue(manager.getWaitSendBuffers().isEmpty());

    // check blocks
    List<ShuffleBlockInfo> blockInfos = client.getCachedBlockInfos();
    assertEquals(1, blockInfos.size());
    ByteBuf buf = blockInfos.get(0).getData();
    byte[] bytes = new byte[blockInfos.get(0).getLength()];
    buf.readBytes(bytes);
    RecordsReader<Text, Text> reader =
        new RecordsReader<>(
            rssConf, SerInputStream.newInputStream(buf), Text.class, Text.class, false, false);
    reader.init();
    int index = 0;
    while (reader.next()) {
      assertEquals(SerializerUtils.genData(Text.class, index), reader.getCurrentKey());
      assertEquals(SerializerUtils.genData(Text.class, index + 1), reader.getCurrentValue());
      index++;
    }
    reader.close();
    assertEquals(RECORDS, index);
  }

  class MockShuffleServer {
    private List<ShuffleBlockInfo> cachedBlockInfos = new ArrayList<>();
    private List<ShuffleBlockInfo> flushBlockInfos = new ArrayList<>();
    private List<Long> finishedBlockInfos = new ArrayList<>();

    public synchronized void finishShuffle() {
      flushBlockInfos.addAll(cachedBlockInfos);
    }

    public synchronized void addCachedBlockInfos(List<ShuffleBlockInfo> shuffleBlockInfoList) {
      cachedBlockInfos.addAll(shuffleBlockInfoList);
    }

    public synchronized void addFinishedBlockInfos(List<Long> shuffleBlockInfoList) {
      finishedBlockInfos.addAll(shuffleBlockInfoList);
    }

    public synchronized int getFlushBlockSize() {
      return flushBlockInfos.size();
    }

    public synchronized int getFinishBlockSize() {
      return finishedBlockInfos.size();
    }

    public List<ShuffleBlockInfo> getCachedBlockInfos() {
      return cachedBlockInfos;
    }
  }

  class MockShuffleWriteClient implements ShuffleWriteClient {

    int mode = 0;
    MockShuffleServer mockedShuffleServer = new MockShuffleServer();
    int committedMaps = 0;

    public void setMode(int mode) {
      this.mode = mode;
    }

    @Override
    public SendShuffleDataResult sendShuffleData(
        String appId,
        List<ShuffleBlockInfo> shuffleBlockInfoList,
        Supplier<Boolean> needCancelRequest) {
      if (mode == 0) {
        throw new RssException("send data failed.");
      } else if (mode == 1) {
        FailedBlockSendTracker failedBlockSendTracker = new FailedBlockSendTracker();
        ShuffleBlockInfo failedBlock =
            new ShuffleBlockInfo(1, 1, 3, 1, 1, new byte[1], null, 1, 100, 1);
        failedBlockSendTracker.add(
            failedBlock, new ShuffleServerInfo("host", 39998), StatusCode.NO_BUFFER);
        return new SendShuffleDataResult(Sets.newHashSet(2L), failedBlockSendTracker);
      } else {
        if (mode == 3) {
          try {
            Thread.sleep(10);
            mockedShuffleServer.addCachedBlockInfos(shuffleBlockInfoList);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RssException(e.toString());
          }
        }
        Set<Long> successBlockIds = Sets.newHashSet();
        for (ShuffleBlockInfo blockInfo : shuffleBlockInfoList) {
          successBlockIds.add(blockInfo.getBlockId());
        }
        return new SendShuffleDataResult(successBlockIds, new FailedBlockSendTracker());
      }
    }

    @Override
    public void sendAppHeartbeat(String appId, long timeoutMs) {}

    @Override
    public void registerApplicationInfo(String appId, long timeoutMs, String user) {}

    @Override
    public void registerShuffle(
        ShuffleServerInfo shuffleServerInfo,
        String appId,
        int shuffleId,
        List<PartitionRange> partitionRanges,
        RemoteStorageInfo remoteStorage,
        ShuffleDataDistributionType dataDistributionType,
        int maxConcurrencyPerPartitionToWrite,
        int stageAttemptNumber,
        RssProtos.MergeContext mergeContext,
        Map<String, String> properties) {}

    @Override
    public boolean sendCommit(
        Set<ShuffleServerInfo> shuffleServerInfoSet, String appId, int shuffleId, int numMaps) {
      if (mode == 3) {
        committedMaps++;
        if (committedMaps >= numMaps) {
          mockedShuffleServer.finishShuffle();
        }
        return true;
      }
      return false;
    }

    @Override
    public void registerCoordinators(String coordinators, long retryIntervalMs, int retryTimes) {}

    @Override
    public Map<String, String> fetchClientConf(int timeoutMs) {
      return null;
    }

    @Override
    public RemoteStorageInfo fetchRemoteStorage(String appId) {
      return null;
    }

    @Override
    public void reportShuffleResult(
        Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds,
        String appId,
        int shuffleId,
        long taskAttemptId,
        int bitmapNum) {
      if (mode == 3) {
        serverToPartitionToBlockIds
            .values()
            .forEach(
                partitionToBlockIds -> {
                  mockedShuffleServer.addFinishedBlockInfos(
                      partitionToBlockIds.values().stream()
                          .flatMap(it -> it.stream())
                          .collect(Collectors.toList()));
                });
      }
    }

    @Override
    public ShuffleAssignmentsInfo getShuffleAssignments(
        String appId,
        int shuffleId,
        int partitionNum,
        int partitionNumPerRange,
        Set<String> requiredTags,
        int assignmentShuffleServerNumber,
        int estimateTaskConcurrency,
        Set<String> faultyServerIds,
        int stageId,
        int stageAttemptNumber,
        boolean reassign,
        long retryIntervalMs,
        int retryTimes) {
      return null;
    }

    @Override
    public Roaring64NavigableMap getShuffleResult(
        String clientType,
        Set<ShuffleServerInfo> shuffleServerInfoSet,
        String appId,
        int shuffleId,
        int partitionId) {
      return null;
    }

    @Override
    public Roaring64NavigableMap getShuffleResultForMultiPart(
        String clientType,
        Map<ShuffleServerInfo, Set<Integer>> serverToPartitions,
        String appId,
        int shuffleId,
        Set<Integer> failedPartitions,
        PartitionDataReplicaRequirementTracking tracking) {
      return null;
    }

    @Override
    public void close() {}

    @Override
    public void unregisterShuffle(String appId, int shuffleId) {}

    @Override
    public void unregisterShuffle(String appId) {}

    @Override
    public void startSortMerge(
        Set<ShuffleServerInfo> serverInfos,
        String appId,
        int shuffleId,
        int partitionId,
        Roaring64NavigableMap expectedTaskIds) {}

    public List<ShuffleBlockInfo> getCachedBlockInfos() {
      return mockedShuffleServer.getCachedBlockInfos();
    }
  }
}
