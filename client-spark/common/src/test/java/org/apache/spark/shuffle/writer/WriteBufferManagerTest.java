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

package org.apache.spark.shuffle.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryManager;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.RssSparkConfig;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.records.RecordsReader;
import org.apache.uniffle.common.serializer.SerInputStream;
import org.apache.uniffle.common.serializer.SerializerUtils;
import org.apache.uniffle.common.util.BlockIdLayout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class WriteBufferManagerTest {

  private static final int RECORD_NUM = 1009;

  private WriteBufferManager createManager(SparkConf conf) {
    Serializer kryoSerializer = new KryoSerializer(conf);
    TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);

    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);
    WriteBufferManager wbm =
        new WriteBufferManager(
            0,
            0,
            bufferOptions,
            kryoSerializer,
            Maps.newHashMap(),
            mockTaskMemoryManager,
            new ShuffleWriteMetrics(),
            RssSparkConfig.toRssConf(conf));
    WriteBufferManager spyManager = spy(wbm);
    doReturn(512L).when(spyManager).acquireMemory(anyLong());
    return spyManager;
  }

  private SparkConf getConf() {
    SparkConf conf = new SparkConf(false);
    conf.set(RssSparkConfig.RSS_WRITER_BUFFER_SIZE.key(), "64")
        .set(RssSparkConfig.RSS_WRITER_BUFFER_SEGMENT_SIZE.key(), "32")
        .set(RssSparkConfig.RSS_WRITER_SERIALIZER_BUFFER_SIZE.key(), "128")
        .set(RssSparkConfig.RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE.key(), "512")
        .set(RssSparkConfig.RSS_WRITER_BUFFER_SPILL_SIZE.key(), "190");
    return conf;
  }

  public static Stream<Arguments> testBlockIdLayouts() {
    return Stream.of(
        Arguments.of(BlockIdLayout.DEFAULT), Arguments.of(BlockIdLayout.from(20, 21, 22)));
  }

  @ParameterizedTest
  @MethodSource("testBlockIdLayouts")
  public void addRecordCompressedTest(BlockIdLayout layout) throws Exception {
    addRecord(true, layout);
  }

  @ParameterizedTest
  @MethodSource("testBlockIdLayouts")
  public void addRecordUnCompressedTest(BlockIdLayout layout) throws Exception {
    addRecord(false, layout);
  }

  private void addRecord(boolean compress, BlockIdLayout layout) throws IllegalAccessException {
    SparkConf conf = getConf();
    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_SEQUENCE_NO_BITS.key(),
        String.valueOf(layout.sequenceNoBits));
    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_PARTITION_ID_BITS.key(),
        String.valueOf(layout.partitionIdBits));
    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS.key(),
        String.valueOf(layout.taskAttemptIdBits));
    if (!compress) {
      conf.set(RssSparkConfig.SPARK_SHUFFLE_COMPRESS_KEY, String.valueOf(false));
    }
    WriteBufferManager wbm = createManager(conf);
    Optional<Codec> codec = (Optional<Codec>) FieldUtils.readField(wbm, "codec", true);
    if (compress) {
      Assertions.assertTrue(codec.isPresent());
    } else {
      Assertions.assertFalse(codec.isPresent());
    }
    wbm.setShuffleWriteMetrics(new ShuffleWriteMetrics());
    String testKey = "Key";
    String testValue = "Value";
    List<ShuffleBlockInfo> result = wbm.addRecord(0, testKey, testValue);
    // single buffer is not full, there is no data return
    assertEquals(0, result.size());
    assertEquals(512, wbm.getAllocatedBytes());
    assertEquals(32, wbm.getUsedBytes());
    assertEquals(0, wbm.getInSendListBytes());
    assertEquals(1, wbm.getBuffers().size());
    wbm.addRecord(0, testKey, testValue);
    wbm.addRecord(0, testKey, testValue);
    wbm.addRecord(0, testKey, testValue);
    result = wbm.addRecord(0, testKey, testValue);
    // single buffer is full
    assertEquals(1, result.size());
    assertEquals(layout.asBlockId(0, 0, 0), layout.asBlockId(result.get(0).getBlockId()));
    assertEquals(512, wbm.getAllocatedBytes());
    assertEquals(96, wbm.getUsedBytes());
    assertEquals(96, wbm.getInSendListBytes());
    assertEquals(0, wbm.getBuffers().size());
    wbm.addRecord(0, testKey, testValue);
    wbm.addRecord(1, testKey, testValue);
    wbm.addRecord(2, testKey, testValue);
    // single buffer is not full, and less than spill size
    assertEquals(512, wbm.getAllocatedBytes());
    assertEquals(192, wbm.getUsedBytes());
    assertEquals(96, wbm.getInSendListBytes());
    assertEquals(3, wbm.getBuffers().size());
    // all buffer size > spill size
    wbm.addRecord(3, testKey, testValue);
    wbm.addRecord(4, testKey, testValue);
    result = wbm.addRecord(5, testKey, testValue);
    assertEquals(6, result.size());
    assertEquals(layout.asBlockId(1, 0, 0), layout.asBlockId(result.get(0).getBlockId()));
    assertEquals(layout.asBlockId(0, 1, 0), layout.asBlockId(result.get(1).getBlockId()));
    assertEquals(layout.asBlockId(0, 2, 0), layout.asBlockId(result.get(2).getBlockId()));
    assertEquals(layout.asBlockId(0, 3, 0), layout.asBlockId(result.get(3).getBlockId()));
    assertEquals(layout.asBlockId(0, 4, 0), layout.asBlockId(result.get(4).getBlockId()));
    assertEquals(layout.asBlockId(0, 5, 0), layout.asBlockId(result.get(5).getBlockId()));
    assertEquals(512, wbm.getAllocatedBytes());
    assertEquals(288, wbm.getUsedBytes());
    assertEquals(288, wbm.getInSendListBytes());
    assertEquals(0, wbm.getBuffers().size());
    // free memory
    wbm.freeAllocatedMemory(96);
    assertEquals(416, wbm.getAllocatedBytes());
    assertEquals(192, wbm.getUsedBytes());
    assertEquals(192, wbm.getInSendListBytes());

    assertEquals(11, wbm.getShuffleWriteMetrics().recordsWritten());
    assertTrue(wbm.getShuffleWriteMetrics().bytesWritten() > 0);

    wbm.freeAllocatedMemory(192);
    wbm.addRecord(0, testKey, testValue);
    wbm.addRecord(1, testKey, testValue);
    wbm.addRecord(2, testKey, testValue);
    result = wbm.clear(1.0);
    assertEquals(3, result.size());
    assertEquals(224, wbm.getAllocatedBytes());
    assertEquals(96, wbm.getUsedBytes());
    assertEquals(96, wbm.getInSendListBytes());
  }

  @Test
  public void addHugeRecordTest() {
    SparkConf conf = getConf();
    WriteBufferManager wbm = createManager(conf);
    String testKey = "len_more_than_32";
    String testValue = "len_more_than_32";
    List<ShuffleBlockInfo> result = wbm.addRecord(0, testKey, testValue);
    assertEquals(0, result.size());
    assertEquals(512, wbm.getAllocatedBytes());
    assertEquals(36, wbm.getUsedBytes());
    assertEquals(0, wbm.getInSendListBytes());
    assertEquals(1, wbm.getBuffers().size());
  }

  @Test
  public void addNullValueRecordTest() {
    SparkConf conf = getConf();
    WriteBufferManager wbm = createManager(conf);
    String testKey = "key";
    String testValue = null;
    List<ShuffleBlockInfo> result = wbm.addRecord(0, testKey, testValue);
    assertEquals(0, result.size());
    assertEquals(512, wbm.getAllocatedBytes());
    assertEquals(32, wbm.getUsedBytes());
    assertEquals(0, wbm.getInSendListBytes());
    assertEquals(1, wbm.getBuffers().size());
  }

  @Test
  public void addPartitionDataTest() {
    SparkConf conf = getConf();
    TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);
    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);
    RssConf rssConf = RssSparkConfig.toRssConf(conf);
    rssConf.set(RssSparkConfig.RSS_ROW_BASED, false);
    WriteBufferManager wbm =
        new WriteBufferManager(
            0,
            0,
            bufferOptions,
            null,
            Maps.newHashMap(),
            mockTaskMemoryManager,
            new ShuffleWriteMetrics(),
            rssConf);
    WriteBufferManager spyManager = spy(wbm);
    doReturn(512L).when(spyManager).acquireMemory(anyLong());

    List<ShuffleBlockInfo> shuffleBlockInfos = spyManager.addPartitionData(0, new byte[64]);
    assertEquals(1, spyManager.getBuffers().size());
    assertEquals(0, shuffleBlockInfos.size());
    shuffleBlockInfos = spyManager.addPartitionData(0, new byte[64]);
    assertEquals(0, spyManager.getBuffers().size());
    assertEquals(1, shuffleBlockInfos.size());
    assertEquals(128, shuffleBlockInfos.get(0).getUncompressLength());
    assertEquals(0, spyManager.getShuffleWriteMetrics().recordsWritten());
  }

  @ParameterizedTest
  @MethodSource("testBlockIdLayouts")
  public void createBlockIdTest(BlockIdLayout layout) {
    SparkConf conf = getConf();
    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_SEQUENCE_NO_BITS.key(),
        String.valueOf(layout.sequenceNoBits));
    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_PARTITION_ID_BITS.key(),
        String.valueOf(layout.partitionIdBits));
    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS.key(),
        String.valueOf(layout.taskAttemptIdBits));

    WriteBufferManager wbm = createManager(conf);
    WriterBuffer mockWriterBuffer = mock(WriterBuffer.class);
    when(mockWriterBuffer.getData()).thenReturn(new byte[] {});
    when(mockWriterBuffer.getMemoryUsed()).thenReturn(0);
    ShuffleBlockInfo sbi = wbm.createShuffleBlock(0, mockWriterBuffer);

    // seqNo = 0, partitionId = 0, taskId = 0
    assertEquals(layout.asBlockId(0, 0, 0), layout.asBlockId(sbi.getBlockId()));

    // seqNo = 1, partitionId = 0, taskId = 0
    sbi = wbm.createShuffleBlock(0, mockWriterBuffer);
    assertEquals(layout.asBlockId(1, 0, 0), layout.asBlockId(sbi.getBlockId()));

    // seqNo = 0, partitionId = 1, taskId = 0
    sbi = wbm.createShuffleBlock(1, mockWriterBuffer);
    assertEquals(layout.asBlockId(0, 1, 0), layout.asBlockId(sbi.getBlockId()));

    // seqNo = 1, partitionId = 1, taskId = 0
    sbi = wbm.createShuffleBlock(1, mockWriterBuffer);
    assertEquals(layout.asBlockId(1, 1, 0), layout.asBlockId(sbi.getBlockId()));
  }

  @Test
  public void buildBlockEventsTest() {
    SparkConf conf = getConf();
    conf.set("spark.rss.client.send.size.limit", "30");

    TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);

    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);
    WriteBufferManager wbm =
        new WriteBufferManager(
            0,
            0,
            bufferOptions,
            new KryoSerializer(conf),
            Maps.newHashMap(),
            mockTaskMemoryManager,
            new ShuffleWriteMetrics(),
            RssSparkConfig.toRssConf(conf));

    // every block: length=4, memoryUsed=12
    ShuffleBlockInfo info1 = new ShuffleBlockInfo(1, 1, 1, 4, 1, new byte[1], null, 1, 12, 1);
    ShuffleBlockInfo info2 = new ShuffleBlockInfo(1, 1, 1, 4, 1, new byte[1], null, 1, 12, 1);
    ShuffleBlockInfo info3 = new ShuffleBlockInfo(1, 1, 1, 4, 1, new byte[1], null, 1, 12, 1);
    List<AddBlockEvent> events = wbm.buildBlockEvents(Arrays.asList(info1, info2, info3));
    assertEquals(3, events.size());
  }

  @Test
  public void spillByOthersTest() {
    SparkConf conf = getConf();
    conf.set("spark.rss.client.memory.spill.enabled", "true");
    TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);
    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);

    WriteBufferManager wbm =
        new WriteBufferManager(
            0,
            "taskId_spillByOthersTest",
            0,
            bufferOptions,
            new KryoSerializer(conf),
            Maps.newHashMap(),
            mockTaskMemoryManager,
            new ShuffleWriteMetrics(),
            RssSparkConfig.toRssConf(conf),
            null,
            0);

    WriteBufferManager spyManager = spy(wbm);
    doReturn(512L).when(spyManager).acquireMemory(anyLong());

    String testKey = "Key";
    String testValue = "Value";
    spyManager.addRecord(0, testKey, testValue);
    spyManager.addRecord(1, testKey, testValue);

    // case1. if one thread wants to spill other consumers data, it will return 0
    assertEquals(0, spyManager.spill(1000, mock(WriteBufferManager.class)));
  }

  @Test
  public void spillByOwnTest() {
    SparkConf conf = getConf();
    conf.set("spark.rss.client.send.size.limit", "1000");
    conf.set("spark.rss.client.memory.spill.enabled", "true");
    TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);
    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);

    WriteBufferManager wbm =
        new WriteBufferManager(
            0,
            "taskId_spillTest",
            0,
            bufferOptions,
            new KryoSerializer(conf),
            Maps.newHashMap(),
            mockTaskMemoryManager,
            new ShuffleWriteMetrics(),
            RssSparkConfig.toRssConf(conf),
            null,
            0);

    Function<List<ShuffleBlockInfo>, List<CompletableFuture<Long>>> spillFunc =
        blocks -> {
          long sum = 0L;
          List<AddBlockEvent> events = wbm.buildBlockEvents(blocks);
          for (AddBlockEvent event : events) {
            for (ShuffleBlockInfo block : event.getShuffleDataInfoList()) {
              block.executeCompletionCallback(true);
            }
            event.getProcessedCallbackChain().stream().forEach(x -> x.run());
            sum += event.getShuffleDataInfoList().stream().mapToLong(x -> x.getFreeMemory()).sum();
          }
          return Arrays.asList(CompletableFuture.completedFuture(sum));
        };
    wbm.setSpillFunc(spillFunc);

    WriteBufferManager spyManager = spy(wbm);
    doReturn(512L).when(spyManager).acquireMemory(anyLong());

    String testKey = "Key";
    String testValue = "Value";
    spyManager.addRecord(0, testKey, testValue);
    spyManager.addRecord(1, testKey, testValue);

    // case1. all events are flushed within normal time.
    long releasedSize = spyManager.spill(1000, spyManager);
    assertEquals(64, releasedSize);
    assertEquals(0, spyManager.getUsedBytes());

    // case2. partial events are not flushed within normal time.
    // when calling spill func, 2 events should be spilled. But
    // only event will be finished in the expected time.
    spyManager.setSendSizeLimit(30);
    spyManager.addRecord(0, testKey, testValue);
    spyManager.addRecord(1, testKey, testValue);
    spillFunc =
        shuffleBlockInfos ->
            Arrays.asList(
                CompletableFuture.supplyAsync(
                    () -> {
                      List<AddBlockEvent> events = spyManager.buildBlockEvents(shuffleBlockInfos);
                      long sum = 0L;
                      for (AddBlockEvent event : events) {
                        int partitionId = event.getShuffleDataInfoList().get(0).getPartitionId();
                        if (partitionId == 1) {
                          try {
                            Thread.sleep(2000);
                          } catch (InterruptedException interruptedException) {
                            // ignore.
                          }
                        }
                        for (ShuffleBlockInfo block : event.getShuffleDataInfoList()) {
                          block.executeCompletionCallback(true);
                        }
                        event.getProcessedCallbackChain().stream().forEach(x -> x.run());
                        sum +=
                            event.getShuffleDataInfoList().stream()
                                .mapToLong(x -> x.getFreeMemory())
                                .sum();
                      }
                      return sum;
                    }));
    spyManager.setSpillFunc(spillFunc);
    releasedSize = spyManager.spill(1000, spyManager);
    assertEquals(0, releasedSize);
    Awaitility.await().timeout(5, TimeUnit.SECONDS).until(() -> spyManager.getUsedBytes() == 0);
  }

  @Test
  public void testClearWithSpillRatio() {
    SparkConf conf = getConf();
    conf.set("spark.rss.client.send.size.limit", String.valueOf(Integer.MAX_VALUE));
    WriteBufferManager wbm = createManager(conf);
    assertEquals(0, wbm.getUsedBytes());

    String testKey = "Key";
    String testValue = "Value";
    wbm.addRecord(0, testKey, testValue);
    wbm.addRecord(1, testKey, testValue);

    assertEquals(64, wbm.getUsedBytes());
    assertEquals(0, wbm.getInSendListBytes());
    // Although the usedBytes-inSendListBytes don't meet requirements of buffer spill ratio,
    // But for the case of 1.0 ratio, it will ignore all.
    when(wbm.getUsedBytes()).thenReturn(0L);
    List<ShuffleBlockInfo> blocks = wbm.clear(1.0);
    assertEquals(2, blocks.size());
  }

  @Test
  public void spillPartial() {
    SparkConf conf = getConf();
    conf.set("spark.rss.client.send.size.limit", "1000");
    conf.set("spark.rss.client.memory.spill.ratio", "0.5");
    conf.set("spark.rss.client.memory.spill.enabled", "true");
    TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);
    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);

    WriteBufferManager wbm =
        new WriteBufferManager(
            0,
            "taskId_spillPartialTest",
            0,
            bufferOptions,
            new KryoSerializer(conf),
            Maps.newHashMap(),
            mockTaskMemoryManager,
            new ShuffleWriteMetrics(),
            RssSparkConfig.toRssConf(conf),
            null,
            0);

    Function<List<ShuffleBlockInfo>, List<CompletableFuture<Long>>> spillFunc =
        blocks -> {
          long sum = 0L;
          List<AddBlockEvent> events = wbm.buildBlockEvents(blocks);
          for (AddBlockEvent event : events) {
            event.getProcessedCallbackChain().stream().forEach(x -> x.run());
            sum += event.getShuffleDataInfoList().stream().mapToLong(x -> x.getFreeMemory()).sum();
          }
          return Arrays.asList(CompletableFuture.completedFuture(sum));
        };
    wbm.setSpillFunc(spillFunc);

    when(wbm.acquireMemory(512)).thenReturn(512L);

    String testKey = "Key";
    String testValue = "Value";
    wbm.addRecord(0, testKey, testValue);
    wbm.addRecord(1, testKey, testValue);
    wbm.addRecord(1, testKey, testValue);
    wbm.addRecord(1, testKey, testValue);
    wbm.addRecord(1, testKey, testValue);

    long releasedSize = wbm.spill(1000, wbm);
    assertEquals(64, releasedSize);
    assertEquals(96, wbm.getUsedBytes());
    assertEquals(0, wbm.getBuffers().keySet().toArray()[0]);
  }

  public static class FakedTaskMemoryManager extends TaskMemoryManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(FakedTaskMemoryManager.class);
    private int invokedCnt = 0;
    private int spilledCnt = 0;
    private int bytesReturnFirstTime = 32;

    public FakedTaskMemoryManager() {
      super(mock(MemoryManager.class), 1);
    }

    public FakedTaskMemoryManager(int bytesReturnFirstTime) {
      this();
      this.bytesReturnFirstTime = bytesReturnFirstTime;
    }

    public long acquireExecutionMemory(long required, MemoryConsumer consumer) {
      if (invokedCnt++ == 0) {
        LOGGER.info("Return existing memory: {}", bytesReturnFirstTime);
        return bytesReturnFirstTime;
      }
      try {
        spilledCnt++;
        long size = consumer.spill(required, consumer);
        LOGGER.info("Return spilled memory: {}", size);
        return size;
      } catch (IOException e) {
        return 0L;
      }
    }

    public int getInvokedCnt() {
      return invokedCnt;
    }

    public int getSpilledCnt() {
      return spilledCnt;
    }
  }

  @Test
  public void spillByOwnWithSparkTaskMemoryManagerTest() {
    SparkConf conf = getConf();
    conf.set(RssSparkConfig.RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE.key(), "32");
    conf.set("spark.rss.client.send.size.limit", "1000");
    conf.set("spark.rss.client.memory.spill.enabled", "true");
    FakedTaskMemoryManager fakedTaskMemoryManager = new FakedTaskMemoryManager();
    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);

    WriteBufferManager wbm =
        new WriteBufferManager(
            0,
            "taskId_spillTest",
            0,
            bufferOptions,
            new KryoSerializer(conf),
            Maps.newHashMap(),
            fakedTaskMemoryManager,
            new ShuffleWriteMetrics(),
            RssSparkConfig.toRssConf(conf),
            null,
            0);

    List<ShuffleBlockInfo> blockList = new ArrayList<>();

    Function<List<ShuffleBlockInfo>, List<CompletableFuture<Long>>> spillFunc =
        blocks -> {
          blockList.addAll(blocks);
          long sum = 0L;
          List<AddBlockEvent> events = wbm.buildBlockEvents(blocks);
          for (AddBlockEvent event : events) {
            event.getProcessedCallbackChain().stream().forEach(x -> x.run());
            sum += event.getShuffleDataInfoList().stream().mapToLong(x -> x.getFreeMemory()).sum();
          }
          return Arrays.asList(CompletableFuture.completedFuture(sum));
        };
    wbm.setSpillFunc(spillFunc);

    WriteBufferManager spyManager = spy(wbm);

    String testKey = "Key";
    String testValue = "Value";

    // First time, it request 32 bytes and then insert the record. It will not flush buffer.
    spyManager.addRecord(0, testKey, testValue);
    assertEquals(0, blockList.size());

    // Second time, the memory manager trigger the spill, so it will flush buffer and then insert
    // the record
    spyManager.addRecord(1, testKey, testValue);
    assertEquals(1, blockList.size());
    assertEquals(32, blockList.stream().mapToLong(x -> x.getFreeMemory()).sum());

    // Third time, it will still do above.
    spyManager.addRecord(2, testKey, testValue);
    assertEquals(2, blockList.size());
    assertEquals(64, blockList.stream().mapToLong(x -> x.getFreeMemory()).sum());

    assertEquals(3, fakedTaskMemoryManager.getInvokedCnt());
    assertEquals(2, fakedTaskMemoryManager.getSpilledCnt());
  }

  @Test
  public void addFirstRecordWithLargeSizeTest() {
    SparkConf conf = getConf();
    WriteBufferManager wbm = createManager(conf);
    String testKey = "key";
    String testValue = "~~~~~~~~~~~~~~~~~~~~This is a long text~~~~~~~~~~~~~~~~~~~~";
    List<ShuffleBlockInfo> shuffleBlockInfos = wbm.addRecord(0, testKey, testValue);
    assertEquals(1, shuffleBlockInfos.size());
    String testValue2 = "This is a short text";
    List<ShuffleBlockInfo> shuffleBlockInfos2 = wbm.addRecord(1, testKey, testValue2);
    assertEquals(0, shuffleBlockInfos2.size());
  }

  @Test
  public void testWriteRemoteMerge() throws Exception {
    final SparkConf conf = new SparkConf();
    final RssConf rssConf = RssSparkConfig.toRssConf(conf);
    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);
    TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);
    doReturn(16L * 1024L * 1024L)
        .when(mockTaskMemoryManager)
        .acquireExecutionMemory(anyLong(), any());

    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    WriteBufferManager manager =
        new RMWriteBufferManager(
            0,
            "",
            0,
            bufferOptions,
            new KryoSerializer(conf),
            mockTaskMemoryManager,
            new ShuffleWriteMetrics(),
            RssSparkConfig.toRssConf(conf),
            null,
            pid -> partitionToServers.get(pid),
            0,
            SerializerUtils.getComparator(String.class));
    List<ShuffleBlockInfo> shuffleBlockInfos;
    List<Integer> indexes = new ArrayList<>();
    for (int i = 0; i < RECORD_NUM; i++) {
      indexes.add(i);
    }
    Collections.shuffle(indexes);
    for (Integer index : indexes) {
      shuffleBlockInfos =
          manager.addRecord(
              0,
              SerializerUtils.genData(String.class, index),
              SerializerUtils.genData(Integer.class, index));
      assertTrue(CollectionUtils.isEmpty(shuffleBlockInfos));
    }

    shuffleBlockInfos = manager.clear();
    assertFalse(CollectionUtils.isEmpty(shuffleBlockInfos));

    // check blocks
    List<AddBlockEvent> events = manager.buildBlockEvents(shuffleBlockInfos);
    assertEquals(1, events.size());
    List<ShuffleBlockInfo> blocks = events.get(0).getShuffleDataInfoList();
    assertEquals(1, blocks.size());

    ByteBuf buf = blocks.get(0).getData();
    RecordsReader<String, Integer> reader =
        new RecordsReader<>(
            rssConf, SerInputStream.newInputStream(buf), String.class, Integer.class, false, false);
    reader.init();
    int index = 0;
    while (reader.next()) {
      assertEquals(SerializerUtils.genData(String.class, index), reader.getCurrentKey());
      assertEquals(SerializerUtils.genData(Integer.class, index), reader.getCurrentValue());
      index++;
    }
    reader.close();
    assertEquals(RECORD_NUM, index);
  }
}
