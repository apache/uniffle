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

package org.apache.uniffle.server.buffer;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.RangeMap;
import com.google.common.util.concurrent.Uninterruptibles;
import io.prometheus.client.Collector;
import org.apache.commons.lang3.tuple.Pair;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.config.ConfigUtils;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.ByteBufUtils;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.server.DefaultFlushEventHandler;
import org.apache.uniffle.server.HugePartitionUtils;
import org.apache.uniffle.server.ShuffleFlushManager;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.server.ShuffleTaskManager;
import org.apache.uniffle.server.storage.StorageManager;
import org.apache.uniffle.server.storage.StorageManagerFactory;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShuffleBufferManagerTest extends BufferTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleBufferManagerTest.class);

  private ShuffleBufferManager shuffleBufferManager;
  private ShuffleFlushManager mockShuffleFlushManager;
  private ShuffleServer mockShuffleServer;
  private ShuffleTaskManager mockShuffleTaskManager;
  private ShuffleServerConf conf;

  @BeforeEach
  public void setUp(@TempDir File tmpDir) {
    conf = new ShuffleServerConf();
    File dataDir = new File(tmpDir, "data");
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE.name());
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(dataDir.getAbsolutePath()));
    conf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 500L);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 20.0);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 80.0);
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L * 1024L * 1024L);
    conf.setBoolean(ShuffleServerConf.APP_LEVEL_SHUFFLE_BLOCK_SIZE_METRIC_ENABLED, true);

    mockShuffleFlushManager = mock(ShuffleFlushManager.class);
    mockShuffleServer = mock(ShuffleServer.class);
    mockShuffleTaskManager = mock(ShuffleTaskManager.class);
    when(mockShuffleServer.getShuffleTaskManager()).thenReturn(mockShuffleTaskManager);
    shuffleBufferManager = new ShuffleBufferManager(conf, mockShuffleFlushManager, false);
    shuffleBufferManager.setShuffleTaskManager(mockShuffleTaskManager);
  }

  @Test
  public void registerBufferTest() {
    String appId = "registerBufferTest";
    int shuffleId = 1;

    StatusCode sc = shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    assertEquals(StatusCode.SUCCESS, sc);
    sc = shuffleBufferManager.registerBuffer(appId, shuffleId, 2, 3);
    assertEquals(StatusCode.SUCCESS, sc);

    Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> bufferPool =
        shuffleBufferManager.getBufferPool();

    assertNotNull(bufferPool.get(appId).get(shuffleId).get(0));
    ShuffleBuffer buffer = bufferPool.get(appId).get(shuffleId).get(0);
    assertEquals(buffer, bufferPool.get(appId).get(shuffleId).get(1));
    assertNotNull(bufferPool.get(appId).get(shuffleId).get(2));
    assertEquals(
        bufferPool.get(appId).get(shuffleId).get(2), bufferPool.get(appId).get(shuffleId).get(3));

    // register again
    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    assertEquals(buffer, bufferPool.get(appId).get(shuffleId).get(0));
  }

  @Test
  public void getShuffleDataWithExpectedTaskIdsTest() {
    String appId = "getShuffleDataWithExpectedTaskIdsTest";
    shuffleBufferManager.setShuffleTaskManager(mockShuffleTaskManager);
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    when(mockShuffleTaskManager.getAppReadLock(appId)).thenReturn(rwLock.readLock());
    when(mockShuffleServer.getShuffleTaskManager()).thenReturn(mockShuffleTaskManager);
    shuffleBufferManager.registerBuffer(appId, 1, 0, 1);
    ShufflePartitionedData spd1 = createData(0, 1, 68);
    ShufflePartitionedData spd2 = createData(0, 2, 68);
    ShufflePartitionedData spd3 = createData(0, 1, 68);
    ShufflePartitionedData spd4 = createData(0, 3, 68);
    shuffleBufferManager.cacheShuffleData(appId, 1, false, spd1);
    shuffleBufferManager.cacheShuffleData(appId, 1, false, spd2);
    shuffleBufferManager.cacheShuffleData(appId, 1, false, spd3);
    shuffleBufferManager.cacheShuffleData(appId, 1, false, spd4);

    /** case1: all blocks in cached and read multiple times */
    ShuffleDataResult result =
        shuffleBufferManager.getShuffleData(
            appId, 1, 0, Constants.INVALID_BLOCK_ID, 60, Roaring64NavigableMap.bitmapOf(1));
    assertEquals(1, result.getBufferSegments().size());
    assertEquals(0, result.getBufferSegments().get(0).getOffset());
    assertEquals(68, result.getBufferSegments().get(0).getLength());

    // 2nd read
    long lastBlockId = result.getBufferSegments().get(0).getBlockId();
    result =
        shuffleBufferManager.getShuffleData(
            appId, 1, 0, lastBlockId, 60, Roaring64NavigableMap.bitmapOf(1));
    assertEquals(1, result.getBufferSegments().size());
    assertEquals(0, result.getBufferSegments().get(0).getOffset());
    assertEquals(68, result.getBufferSegments().get(0).getLength());
  }

  @Test
  public void getShuffleDataTest() {
    String appId = "getShuffleDataTest";
    shuffleBufferManager.setShuffleTaskManager(mockShuffleTaskManager);
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    when(mockShuffleTaskManager.getAppReadLock(appId)).thenReturn(rwLock.readLock());
    when(mockShuffleServer.getShuffleTaskManager()).thenReturn(mockShuffleTaskManager);
    shuffleBufferManager.registerBuffer(appId, 1, 0, 1);
    shuffleBufferManager.registerBuffer(appId, 2, 0, 1);
    shuffleBufferManager.registerBuffer(appId, 3, 0, 1);
    shuffleBufferManager.registerBuffer(appId, 4, 0, 1);
    ShufflePartitionedData spd1 = createData(0, 68);
    ShufflePartitionedData spd2 = createData(0, 68);
    ShufflePartitionedData spd3 = createData(0, 68);
    ShufflePartitionedData spd4 = createData(0, 68);
    shuffleBufferManager.cacheShuffleData(appId, 1, false, spd1);
    shuffleBufferManager.cacheShuffleData(appId, 2, false, spd2);
    shuffleBufferManager.cacheShuffleData(appId, 2, false, spd3);
    shuffleBufferManager.cacheShuffleData(appId, 3, false, spd4);
    // validate buffer, no flush happened
    Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> bufferPool =
        shuffleBufferManager.getBufferPool();
    assertEquals(100, bufferPool.get(appId).get(1).get(0).getEncodedLength());
    assertEquals(200, bufferPool.get(appId).get(2).get(0).getEncodedLength());
    assertEquals(100, bufferPool.get(appId).get(3).get(0).getEncodedLength());
    // validate get shuffle data
    ShuffleDataResult sdr =
        shuffleBufferManager.getShuffleData(appId, 2, 0, Constants.INVALID_BLOCK_ID, 60);
    assertArrayEquals(ByteBufUtils.readBytes(spd2.getBlockList()[0].getData()), sdr.getData());
    long lastBlockId = spd2.getBlockList()[0].getBlockId();
    sdr = shuffleBufferManager.getShuffleData(appId, 2, 0, lastBlockId, 100);
    assertArrayEquals(ByteBufUtils.readBytes(spd3.getBlockList()[0].getData()), sdr.getData());
    // flush happen
    ShufflePartitionedData spd5 = createData(0, 10);
    shuffleBufferManager.cacheShuffleData(appId, 4, false, spd5);
    // according to flush strategy, some buffers should be moved to inFlushMap
    assertEquals(0, bufferPool.get(appId).get(1).get(0).getBlocks().size());
    assertEquals(1, bufferPool.get(appId).get(1).get(0).getInFlushBlockMap().size());
    assertEquals(0, bufferPool.get(appId).get(2).get(0).getBlocks().size());
    assertEquals(1, bufferPool.get(appId).get(2).get(0).getInFlushBlockMap().size());
    assertEquals(0, bufferPool.get(appId).get(3).get(0).getBlocks().size());
    assertEquals(1, bufferPool.get(appId).get(3).get(0).getInFlushBlockMap().size());
    // keep buffer whose size < low watermark
    assertEquals(1, bufferPool.get(appId).get(4).get(0).getBlocks().size());
    // data in flush buffer now, it also can be got before flush finish
    sdr = shuffleBufferManager.getShuffleData(appId, 2, 0, Constants.INVALID_BLOCK_ID, 60);
    assertArrayEquals(ByteBufUtils.readBytes(spd2.getBlockList()[0].getData()), sdr.getData());
    lastBlockId = spd2.getBlockList()[0].getBlockId();
    sdr = shuffleBufferManager.getShuffleData(appId, 2, 0, lastBlockId, 100);
    assertArrayEquals(ByteBufUtils.readBytes(spd3.getBlockList()[0].getData()), sdr.getData());
    // cache data again, it should cause flush
    spd1 = createData(0, 10);
    shuffleBufferManager.cacheShuffleData(appId, 1, false, spd1);
    assertEquals(1, bufferPool.get(appId).get(1).get(0).getBlocks().size());
    // finish flush
    bufferPool.get(appId).get(1).get(0).getInFlushBlockMap().clear();
    bufferPool.get(appId).get(2).get(0).getInFlushBlockMap().clear();
    bufferPool.get(appId).get(3).get(0).getInFlushBlockMap().clear();
    // empty data return
    sdr = shuffleBufferManager.getShuffleData(appId, 2, 0, Constants.INVALID_BLOCK_ID, 60);
    assertEquals(0, sdr.getDataLength());
    lastBlockId = spd2.getBlockList()[0].getBlockId();
    sdr = shuffleBufferManager.getShuffleData(appId, 2, 0, lastBlockId, 100);
    assertEquals(0, sdr.getDataLength());
  }

  @Test
  public void shuffleIdToSizeTest() {
    String appId1 = "shuffleIdToSizeTest1";
    String appId2 = "shuffleIdToSizeTest2";
    ReentrantReadWriteLock rwLock1 = new ReentrantReadWriteLock();
    when(mockShuffleTaskManager.getAppReadLock(appId1)).thenReturn(rwLock1.readLock());
    ReentrantReadWriteLock rwLock2 = new ReentrantReadWriteLock();
    when(mockShuffleTaskManager.getAppReadLock(appId2)).thenReturn(rwLock2.readLock());
    shuffleBufferManager.registerBuffer(appId1, 1, 0, 0);
    shuffleBufferManager.registerBuffer(appId1, 2, 0, 0);
    shuffleBufferManager.registerBuffer(appId2, 1, 0, 0);
    shuffleBufferManager.registerBuffer(appId2, 2, 0, 0);
    ShufflePartitionedData spd1 = createData(0, 67);
    ShufflePartitionedData spd2 = createData(0, 68);
    ShufflePartitionedData spd3 = createData(0, 68);
    ShufflePartitionedData spd4 = createData(0, 68);
    shuffleBufferManager.cacheShuffleData(appId1, 1, false, spd1);
    shuffleBufferManager.cacheShuffleData(appId1, 2, false, spd2);
    shuffleBufferManager.cacheShuffleData(appId1, 2, false, spd3);
    shuffleBufferManager.cacheShuffleData(appId2, 1, false, spd4);

    // validate metadata of shuffle size
    Map<String, Map<Integer, AtomicLong>> shuffleSizeMap = shuffleBufferManager.getShuffleSizeMap();
    assertEquals(99, shuffleSizeMap.get(appId1).get(1).get());
    assertEquals(200, shuffleSizeMap.get(appId1).get(2).get());
    assertEquals(100, shuffleSizeMap.get(appId2).get(1).get());

    ShufflePartitionedData spd5 = createData(0, 68);
    shuffleBufferManager.cacheShuffleData(appId2, 2, false, spd5);
    // flush happen
    assertEquals(99, shuffleSizeMap.get(appId1).get(1).get());
    assertEquals(0, shuffleSizeMap.get(appId1).get(2).get());
    assertEquals(0, shuffleSizeMap.get(appId2).get(1).get());
    assertEquals(0, shuffleSizeMap.get(appId2).get(1).get());
    shuffleBufferManager.releaseMemory(400, true, false);

    ShufflePartitionedData spd6 = createData(0, 300);
    shuffleBufferManager.cacheShuffleData(appId1, 1, false, spd6);
    // flush happen
    assertEquals(0, shuffleSizeMap.get(appId1).get(1).get());
    shuffleBufferManager.releaseMemory(463, true, false);

    shuffleBufferManager.cacheShuffleData(appId1, 1, false, spd1);
    shuffleBufferManager.cacheShuffleData(appId1, 2, false, spd2);
    shuffleBufferManager.cacheShuffleData(appId2, 1, false, spd4);
    shuffleBufferManager.removeBuffer(appId1);
    assertNull(shuffleSizeMap.get(appId1));
    assertEquals(100, shuffleSizeMap.get(appId2).get(1).get());
  }

  @Test
  public void cacheShuffleDataTest() {
    String appId = "cacheShuffleDataTest";
    shuffleBufferManager.setShuffleTaskManager(mockShuffleTaskManager);
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    when(mockShuffleTaskManager.getAppReadLock(appId)).thenReturn(rwLock.readLock());
    when(mockShuffleServer.getShuffleTaskManager()).thenReturn(mockShuffleTaskManager);
    int startPartitionNum = (int) ShuffleServerMetrics.gaugeTotalPartitionNum.get();
    int shuffleId = 1;
    StatusCode sc =
        shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(StatusCode.NO_REGISTER, sc);
    shuffleBufferManager.registerBuffer(appId, shuffleId + 1, 0, 1);
    assertEquals(startPartitionNum + 1, (int) ShuffleServerMetrics.gaugeTotalPartitionNum.get());
    sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(StatusCode.NO_REGISTER, sc);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 100, 101);
    assertEquals(startPartitionNum + 2, (int) ShuffleServerMetrics.gaugeTotalPartitionNum.get());
    sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(StatusCode.NO_REGISTER, sc);

    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    assertEquals(startPartitionNum + 3, (int) ShuffleServerMetrics.gaugeTotalPartitionNum.get());
    sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(StatusCode.SUCCESS, sc);

    Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> bufferPool =
        shuffleBufferManager.getBufferPool();
    ShuffleBuffer buffer = bufferPool.get(appId).get(shuffleId).get(0);
    assertEquals(48, buffer.getEncodedLength());
    assertEquals(48, shuffleBufferManager.getUsedMemory());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(96, buffer.getEncodedLength());
    assertEquals(96, shuffleBufferManager.getUsedMemory());

    // reach high water lever, flush
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 273));
    assertEquals(0, buffer.getEncodedLength());
    assertEquals(401, shuffleBufferManager.getUsedMemory());
    assertEquals(401, shuffleBufferManager.getInFlushSize());
    verify(mockShuffleFlushManager, times(1)).addToFlushQueue(any());

    // now buffer should be full
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 100));
    verify(mockShuffleFlushManager, times(1)).addToFlushQueue(any());
    sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 1));
    assertEquals(StatusCode.NO_BUFFER, sc);

    // size won't be reduced which should be processed by flushManager, reset buffer size to 0
    shuffleBufferManager.resetSize();
    shuffleBufferManager.removeBuffer(appId);
    assertEquals(startPartitionNum, (int) ShuffleServerMetrics.gaugeTotalPartitionNum.get());
    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 0);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 1, 1);
    shuffleBufferManager.registerBuffer(appId, 2, 0, 0);

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 200));
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(1, 200));
    shuffleBufferManager.cacheShuffleData(appId, 2, false, createData(0, 32));
    ShuffleBuffer buffer0 = bufferPool.get(appId).get(shuffleId).get(0);
    ShuffleBuffer buffer1 = bufferPool.get(appId).get(shuffleId).get(1);
    ShuffleBuffer buffer2 = bufferPool.get(appId).get(2).get(0);
    assertEquals(0, buffer0.getEncodedLength());
    assertEquals(0, buffer1.getEncodedLength());
    assertEquals(64, buffer2.getEncodedLength());
    assertEquals(528, shuffleBufferManager.getUsedMemory());
    assertEquals(464, shuffleBufferManager.getInFlushSize());
    verify(mockShuffleFlushManager, times(3)).addToFlushQueue(any());
  }

  @Test
  public void cacheShuffleDataWithPreAllocationTest() {
    String appId = "cacheShuffleDataWithPreAllocationTest";
    shuffleBufferManager.setShuffleTaskManager(mockShuffleTaskManager);
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    when(mockShuffleTaskManager.getAppReadLock(appId)).thenReturn(rwLock.readLock());
    when(mockShuffleServer.getShuffleTaskManager()).thenReturn(mockShuffleTaskManager);
    int shuffleId = 1;
    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    // pre allocate memory
    shuffleBufferManager.requireMemory(48, true);
    assertEquals(48, shuffleBufferManager.getUsedMemory());
    assertEquals(48, shuffleBufferManager.getPreAllocatedSize());
    // receive data with preAllocation
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, true, createData(0, 16));
    assertEquals(48, shuffleBufferManager.getUsedMemory());
    assertEquals(48, shuffleBufferManager.getPreAllocatedSize());
    // release memory
    shuffleBufferManager.releaseMemory(48, false, true);
    assertEquals(0, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getPreAllocatedSize());
    // receive data without preAllocation
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 17));
    assertEquals(49, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getPreAllocatedSize());
    // release memory
    shuffleBufferManager.releaseMemory(49, false, false);
    assertEquals(0, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getPreAllocatedSize());

    // release memory with preAllocation
    shuffleBufferManager.requireMemory(16, true);
    shuffleBufferManager.releaseMemory(16, false, true);
    assertEquals(0, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getPreAllocatedSize());

    // pre allocate all memory
    shuffleBufferManager.requireMemory(500, true);
    assertEquals(500, shuffleBufferManager.getUsedMemory());
    assertEquals(500, shuffleBufferManager.getPreAllocatedSize());

    // no buffer if data without pre allocation
    StatusCode sc =
        shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(1, 16));
    assertEquals(StatusCode.NO_BUFFER, sc);

    // actual data size < spillThreshold, won't flush
    sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, true, createData(1, 16));
    shuffleBufferManager.releasePreAllocatedSize(48);
    assertEquals(StatusCode.SUCCESS, sc);
    assertEquals(500, shuffleBufferManager.getUsedMemory());
    assertEquals(452, shuffleBufferManager.getPreAllocatedSize());

    // actual data size > highWaterMark, flush
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, true, createData(0, 400));
    shuffleBufferManager.releasePreAllocatedSize(432);
    // trigger flush manually
    shuffleBufferManager.flushIfNecessary();
    assertEquals(StatusCode.SUCCESS, sc);
    assertEquals(500, shuffleBufferManager.getUsedMemory());
    assertEquals(20, shuffleBufferManager.getPreAllocatedSize());
    verify(mockShuffleFlushManager, times(1)).addToFlushQueue(any());
  }

  @Test
  public void bufferSizeTest() throws Exception {
    ShuffleServer mockShuffleServer = mock(ShuffleServer.class);
    StorageManager storageManager = StorageManagerFactory.getInstance().createStorageManager(conf);
    ShuffleFlushManager shuffleFlushManager =
        new ShuffleFlushManager(conf, mockShuffleServer, storageManager);
    shuffleBufferManager = new ShuffleBufferManager(conf, shuffleFlushManager, false);

    when(mockShuffleServer.getShuffleFlushManager()).thenReturn(shuffleFlushManager);
    when(mockShuffleServer.getShuffleBufferManager()).thenReturn(shuffleBufferManager);
    when(mockShuffleServer.getShuffleTaskManager()).thenReturn(mock(ShuffleTaskManager.class));

    String appId = "bufferSizeTest";
    when(mockShuffleServer.getShuffleTaskManager()).thenReturn(mockShuffleTaskManager);
    shuffleBufferManager.setShuffleTaskManager(mockShuffleTaskManager);
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    when(mockShuffleTaskManager.getAppReadLock(appId)).thenReturn(rwLock.readLock());
    int shuffleId = 1;
    when(mockShuffleServer.getShuffleTaskManager()).thenReturn(mockShuffleTaskManager);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 2, 3);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 4, 5);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 6, 7);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 8, 9);
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(48, shuffleBufferManager.getUsedMemory());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(96, shuffleBufferManager.getUsedMemory());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 300));
    waitForFlush(shuffleFlushManager, appId, shuffleId, 3);
    assertEquals(0, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getInFlushSize());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 64));
    assertEquals(96, shuffleBufferManager.getUsedMemory());
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(2, 64));
    assertEquals(192, shuffleBufferManager.getUsedMemory());
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(4, 64));
    assertEquals(288, shuffleBufferManager.getUsedMemory());
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(6, 64));
    assertEquals(384, shuffleBufferManager.getUsedMemory());
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(8, 64));
    waitForFlush(shuffleFlushManager, appId, shuffleId, 4, 96);
    assertEquals(0, shuffleBufferManager.getInFlushSize());

    shuffleBufferManager.removeBuffer(appId);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    shuffleBufferManager.registerBuffer("bufferSizeTest1", shuffleId, 0, 1);
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 32));
    assertEquals(64, shuffleBufferManager.getUsedMemory());
    shuffleBufferManager.cacheShuffleData("bufferSizeTest1", shuffleId, false, createData(0, 32));
    assertEquals(128, shuffleBufferManager.getUsedMemory());
    assertEquals(2, shuffleBufferManager.getBufferPool().keySet().size());
    shuffleBufferManager.removeBuffer(appId);
    assertEquals(64, shuffleBufferManager.getUsedMemory());
    assertEquals(1, shuffleBufferManager.getBufferPool().keySet().size());
  }

  @Test
  public void flushSingleBufferForHugePartitionTest(@TempDir File tmpDir) throws Exception {
    ShuffleServerConf shuffleConf = new ShuffleServerConf();
    File dataDir = new File(tmpDir, "data");
    shuffleConf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE.name());
    shuffleConf.set(
        ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(dataDir.getAbsolutePath()));
    shuffleConf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 20.0);
    shuffleConf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 80.0);
    shuffleConf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L * 1024L * 1024L);
    shuffleConf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 200L);
    shuffleConf.set(ShuffleServerConf.HUGE_PARTITION_MEMORY_USAGE_LIMITATION_RATIO, 0.1);
    shuffleConf.set(ShuffleServerConf.HUGE_PARTITION_SIZE_THRESHOLD, 100L);
    shuffleConf.set(ShuffleServerConf.SINGLE_BUFFER_FLUSH_ENABLED, false);
    shuffleConf.setSizeAsBytes(ShuffleServerConf.SINGLE_BUFFER_FLUSH_SIZE_THRESHOLD, 64L);

    ShuffleServer mockShuffleServer = mock(ShuffleServer.class);
    StorageManager storageManager =
        StorageManagerFactory.getInstance().createStorageManager(shuffleConf);
    ShuffleFlushManager shuffleFlushManager =
        new ShuffleFlushManager(shuffleConf, mockShuffleServer, storageManager);
    shuffleBufferManager = new ShuffleBufferManager(shuffleConf, shuffleFlushManager, false);
    ShuffleTaskManager shuffleTaskManager =
        new ShuffleTaskManager(
            shuffleConf, shuffleFlushManager, shuffleBufferManager, storageManager);

    when(mockShuffleServer.getShuffleFlushManager()).thenReturn(shuffleFlushManager);
    when(mockShuffleServer.getShuffleBufferManager()).thenReturn(shuffleBufferManager);
    when(mockShuffleServer.getShuffleTaskManager()).thenReturn(shuffleTaskManager);

    String appId = "flushSingleBufferForHugePartitionTest_appId";
    int shuffleId = 1;

    shuffleTaskManager.registerShuffle(
        appId, shuffleId, Arrays.asList(new PartitionRange(0, 0)), new RemoteStorageInfo(""), "");

    // case1: its partition is not huge partition
    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 0);
    ShufflePartitionedData partitionedData = createData(0, 1);
    shuffleTaskManager.cacheShuffleData(appId, shuffleId, false, partitionedData);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, 0, partitionedData.getBlockList());
    assertEquals(1 + 32, shuffleBufferManager.getUsedMemory());
    long usedSize = shuffleTaskManager.getPartitionDataSize(appId, shuffleId, 0);
    assertEquals(1 + 32, usedSize);
    assertFalse(
        HugePartitionUtils.limitHugePartition(
            shuffleBufferManager,
            appId,
            shuffleId,
            0,
            shuffleTaskManager.getPartitionDataSize(appId, shuffleId, 0)));

    // case2: its partition is huge partition, its buffer will be flushed to DISK directly
    partitionedData = createData(0, 36);
    shuffleTaskManager.cacheShuffleData(appId, shuffleId, false, partitionedData);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, 0, partitionedData.getBlockList());
    assertEquals(33 + 36 + 32, shuffleBufferManager.getUsedMemory());
    assertTrue(
        HugePartitionUtils.limitHugePartition(
            shuffleBufferManager,
            appId,
            shuffleId,
            0,
            shuffleTaskManager.getPartitionDataSize(appId, shuffleId, 0)));
    partitionedData = createData(0, 1);
    shuffleTaskManager.cacheShuffleData(appId, shuffleId, false, partitionedData);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, 0, partitionedData.getBlockList());
    waitForFlush(shuffleFlushManager, appId, shuffleId, 3);
  }

  @Test
  public void flushSingleBufferTest(@TempDir File tmpDir) throws Exception {
    ShuffleServerConf shuffleConf = new ShuffleServerConf();
    File dataDir = new File(tmpDir, "data");
    shuffleConf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE.name());
    shuffleConf.set(
        ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(dataDir.getAbsolutePath()));
    shuffleConf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 200L);
    shuffleConf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 20.0);
    shuffleConf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 80.0);
    shuffleConf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L * 1024L * 1024L);
    shuffleConf.setBoolean(ShuffleServerConf.SINGLE_BUFFER_FLUSH_ENABLED, true);
    shuffleConf.setSizeAsBytes(ShuffleServerConf.SINGLE_BUFFER_FLUSH_SIZE_THRESHOLD, 128L);

    ShuffleServer mockShuffleServer = mock(ShuffleServer.class);
    StorageManager storageManager =
        StorageManagerFactory.getInstance().createStorageManager(shuffleConf);
    ShuffleFlushManager shuffleFlushManager =
        new ShuffleFlushManager(shuffleConf, mockShuffleServer, storageManager);
    shuffleBufferManager = new ShuffleBufferManager(shuffleConf, shuffleFlushManager, false);

    when(mockShuffleServer.getShuffleFlushManager()).thenReturn(shuffleFlushManager);
    when(mockShuffleServer.getShuffleBufferManager()).thenReturn(shuffleBufferManager);
    when(mockShuffleServer.getShuffleTaskManager()).thenReturn(mock(ShuffleTaskManager.class));

    String appId = "bufferSizeTest";
    shuffleBufferManager.setShuffleTaskManager(mockShuffleTaskManager);
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    when(mockShuffleTaskManager.getAppReadLock(appId)).thenReturn(rwLock.readLock());
    when(mockShuffleServer.getShuffleTaskManager()).thenReturn(mockShuffleTaskManager);
    int shuffleId = 1;
    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 2, 3);
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 64));
    assertEquals(96, shuffleBufferManager.getUsedMemory());
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(2, 64));
    waitForFlush(shuffleFlushManager, appId, shuffleId, 2);
    assertEquals(0, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getInFlushSize());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 32));
    assertEquals(64, shuffleBufferManager.getUsedMemory());
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(1, 48));
    waitForFlush(shuffleFlushManager, appId, shuffleId, 4);
    assertEquals(0, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getInFlushSize());
  }

  @Test
  public void shuffleFlushThreshold() throws Exception {
    ShuffleServerConf serverConf = new ShuffleServerConf();
    serverConf.addAll(conf);
    serverConf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 180L);
    serverConf.set(ShuffleServerConf.SERVER_SHUFFLE_FLUSH_THRESHOLD, 64L);

    StorageManager storageManager = StorageManagerFactory.getInstance().createStorageManager(conf);
    ShuffleFlushManager shuffleFlushManager =
        new ShuffleFlushManager(conf, mockShuffleServer, storageManager);
    shuffleBufferManager = new ShuffleBufferManager(serverConf, shuffleFlushManager, false);

    String appId = "shuffleFlushTest";
    shuffleBufferManager.setShuffleTaskManager(mockShuffleTaskManager);
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    when(mockShuffleTaskManager.getAppReadLock(appId)).thenReturn(rwLock.readLock());
    when(mockShuffleServer.getShuffleTaskManager()).thenReturn(mockShuffleTaskManager);

    int shuffleId = 0;
    int smallShuffleId = 1;
    int smallShuffleIdTwo = 2;
    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 2, 3);
    shuffleBufferManager.registerBuffer(appId, smallShuffleId, 0, 1);
    shuffleBufferManager.registerBuffer(appId, smallShuffleIdTwo, 0, 1);
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 64));
    assertEquals(96, shuffleBufferManager.getUsedMemory());
    shuffleBufferManager.cacheShuffleData(appId, smallShuffleId, false, createData(0, 31));
    assertEquals(96 + 63, shuffleBufferManager.getUsedMemory());
    Thread.sleep(100);
    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    // small shuffle id is kept in memory
    assertEquals(63, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getInFlushSize());

    // more data will trigger the flush
    shuffleBufferManager.cacheShuffleData(appId, smallShuffleId, false, createData(0, 31));
    shuffleBufferManager.cacheShuffleData(appId, smallShuffleId, false, createData(0, 31));
    assertEquals(63 * 3, shuffleBufferManager.getUsedMemory());
    Thread.sleep(100);
    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    assertEquals(0, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getInFlushSize());

    // all the small data in shuffle server, which could be extremely rare
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 22));
    shuffleBufferManager.cacheShuffleData(appId, smallShuffleId, false, createData(0, 21));
    shuffleBufferManager.cacheShuffleData(appId, smallShuffleIdTwo, false, createData(0, 20));
    assertEquals(54 + 53 + 52, shuffleBufferManager.getUsedMemory());
    Thread.sleep(100);
    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    assertEquals(52, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getInFlushSize());
  }

  private void waitForFlush(
      ShuffleFlushManager shuffleFlushManager, String appId, int shuffleId, int expectedBlockNum)
      throws Exception {
    waitForFlush(shuffleFlushManager, appId, shuffleId, expectedBlockNum, 0);
  }

  private void waitForFlush(
      ShuffleFlushManager shuffleFlushManager,
      String appId,
      int shuffleId,
      int expectedBlockNum,
      long expectedUsedMemory)
      throws Exception {
    int retry = 0;
    long committedCount = 0;
    do {
      try {
        committedCount =
            shuffleFlushManager.getCommittedBlockIds(appId, shuffleId).getLongCardinality();
      } catch (Throwable e) {
        // ignore ArrayIndexOutOfBoundsException and ConcurrentModificationException
        LOG.error("Ignored exception.", e);
      }
      if (committedCount < expectedBlockNum) {
        Thread.sleep(500);
      }
      retry++;
      if (retry > 10) {
        fail("Flush data time out");
      }
    } while (committedCount < expectedBlockNum);

    // Need to wait for `event.doCleanup` to be executed
    // to ensure the correctness of subsequent checks of
    // `shuffleBufferManager.getUsedMemory()` and `shuffleBufferManager.getInFlushSize()`.
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> shuffleBufferManager.getUsedMemory() == expectedUsedMemory);
  }

  @Test
  public void bufferManagerInitTest() {
    ShuffleServerConf serverConf = new ShuffleServerConf();
    shuffleBufferManager = new ShuffleBufferManager(serverConf, mockShuffleFlushManager, false);
    double ratio = ShuffleServerConf.SERVER_BUFFER_CAPACITY_RATIO.defaultValue();
    double readRatio = ShuffleServerConf.SERVER_READ_BUFFER_CAPACITY_RATIO.defaultValue();
    assertEquals(
        (long) (Runtime.getRuntime().maxMemory() * ratio), shuffleBufferManager.getCapacity());
    assertEquals(
        (long) (Runtime.getRuntime().maxMemory() * readRatio),
        shuffleBufferManager.getReadCapacity());
    ratio = 0.6;
    readRatio = 0.1;
    serverConf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY_RATIO, ratio);
    serverConf.set(ShuffleServerConf.SERVER_READ_BUFFER_CAPACITY_RATIO, readRatio);
    shuffleBufferManager = new ShuffleBufferManager(serverConf, mockShuffleFlushManager, false);
    assertEquals(
        (long) (Runtime.getRuntime().maxMemory() * ratio), shuffleBufferManager.getCapacity());
    assertEquals(
        (long) (Runtime.getRuntime().maxMemory() * readRatio),
        shuffleBufferManager.getReadCapacity());
  }

  @Test
  public void flushBufferTestWhenNotSelectedStorage(@TempDir File tmpDir) throws Exception {
    // In this test, rss.server.single.buffer.flush.threshold and
    // rss.server.flush.cold.storage.threshold.size are 16.
    // When cacheShuffleData with 64 bytes, will flush to HDFS storage, but we do not register
    // remote storage.
    // Then storageManager.selectStorage will return null, we should make sure that when we can not
    // select a storage,
    // the resources will not leak.
    ShuffleServerConf shuffleConf = new ShuffleServerConf();
    File dataDir = new File(tmpDir, "data");
    shuffleConf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE.name());
    shuffleConf.set(
        ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(dataDir.getAbsolutePath()));
    shuffleConf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 200L);
    shuffleConf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 20.0);
    shuffleConf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 80.0);
    shuffleConf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L * 1024L * 1024L);
    shuffleConf.setBoolean(ShuffleServerConf.SINGLE_BUFFER_FLUSH_ENABLED, true);
    shuffleConf.setSizeAsBytes(ShuffleServerConf.SINGLE_BUFFER_FLUSH_SIZE_THRESHOLD, 16L);
    shuffleConf.setSizeAsBytes(ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, 16L);
    shuffleConf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE_HDFS.name());
    ShuffleServer mockShuffleServer = mock(ShuffleServer.class);
    StorageManager storageManager =
        StorageManagerFactory.getInstance().createStorageManager(shuffleConf);
    ShuffleFlushManager shuffleFlushManager =
        new ShuffleFlushManager(shuffleConf, mockShuffleServer, storageManager);
    shuffleBufferManager = new ShuffleBufferManager(shuffleConf, shuffleFlushManager, false);

    when(mockShuffleServer.getShuffleFlushManager()).thenReturn(shuffleFlushManager);
    when(mockShuffleServer.getShuffleBufferManager()).thenReturn(shuffleBufferManager);
    when(mockShuffleServer.getShuffleTaskManager()).thenReturn(mock(ShuffleTaskManager.class));

    String appId = "bufferSizeTest";
    shuffleBufferManager.setShuffleTaskManager(mockShuffleTaskManager);
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    when(mockShuffleTaskManager.getAppReadLock(appId)).thenReturn(rwLock.readLock());
    when(mockShuffleServer.getShuffleTaskManager()).thenReturn(mockShuffleTaskManager);
    int shuffleId = 1;
    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 2, 3);
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 64));
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(2, 64));
    // wait flush event drained
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> shuffleFlushManager.getEventNumInFlush() == 0);
    // make sure all cleanup tasks are done.
    DefaultFlushEventHandler flushEventHandler =
        (DefaultFlushEventHandler) shuffleFlushManager.getEventHandler();
    ThreadPoolExecutor executor =
        ((ThreadPoolExecutor) flushEventHandler.getFallbackThreadPoolExecutor());
    executor.shutdown();
    assertEquals(0, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getInFlushSize());
  }

  @Test
  public void blockSizeMetricsTest() {
    String appId = "blockSizeMetricsTest";
    shuffleBufferManager.setShuffleTaskManager(mockShuffleTaskManager);
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    when(mockShuffleTaskManager.getAppReadLock(appId)).thenReturn(rwLock.readLock());
    when(mockShuffleServer.getShuffleTaskManager()).thenReturn(mockShuffleTaskManager);
    int shuffleId = 1;
    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);

    // cache shuffle block data, and record metrics
    double[] buckets =
        ConfigUtils.convertBytesStringToDoubleArray(
            new ShuffleServerConf()
                .get(ShuffleServerConf.APP_LEVEL_SHUFFLE_BLOCK_SIZE_METRIC_BUCKETS));
    Arrays.stream(buckets)
        .sorted()
        .forEach(
            bucket -> {
              StatusCode sc =
                  shuffleBufferManager.cacheShuffleData(
                      appId, shuffleId, true, createData(0, (int) bucket));
              assertEquals(StatusCode.SUCCESS, sc);
            });
    // check metrics values
    List<Collector.MetricFamilySamples> samples =
        ShuffleServerMetrics.appHistogramWriteBlockSize.collect();
    assertEquals(samples.size(), 1);
    int index = 1;
    Arrays.stream(buckets)
        .sorted()
        .forEach(
            bucket -> {
              for (Collector.MetricFamilySamples.Sample s : samples.get(0).samples) {
                if (s.labelValues.contains(bucket)) {
                  assertEquals(s.value, index);
                }
              }
            });
  }

  @Test
  public void splitPartitionTest(@TempDir File tmpDir) throws Exception {
    ShuffleServerConf shuffleConf = new ShuffleServerConf();
    File dataDir = new File(tmpDir, "data");
    shuffleConf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE.name());
    shuffleConf.set(
        ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(dataDir.getAbsolutePath()));
    shuffleConf.set(ShuffleServerConf.HUGE_PARTITION_SPLIT_LIMIT, 200L);

    ShuffleServer mockShuffleServer = mock(ShuffleServer.class);
    StorageManager storageManager =
        StorageManagerFactory.getInstance().createStorageManager(shuffleConf);
    ShuffleFlushManager shuffleFlushManager =
        new ShuffleFlushManager(shuffleConf, mockShuffleServer, storageManager);
    shuffleBufferManager = new ShuffleBufferManager(shuffleConf, shuffleFlushManager, false);
    ShuffleTaskManager shuffleTaskManager =
        new ShuffleTaskManager(
            shuffleConf, shuffleFlushManager, shuffleBufferManager, storageManager);

    when(mockShuffleServer.getShuffleFlushManager()).thenReturn(shuffleFlushManager);
    when(mockShuffleServer.getShuffleBufferManager()).thenReturn(shuffleBufferManager);
    when(mockShuffleServer.getShuffleTaskManager()).thenReturn(shuffleTaskManager);

    String appId = "flushSingleBufferForHugePartitionTest_appId";
    int shuffleId = 1;

    shuffleTaskManager.registerShuffle(
        appId, shuffleId, Arrays.asList(new PartitionRange(0, 0)), new RemoteStorageInfo(""), "");

    // case1: its partition size does not exceed the split limit
    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 0);
    ShufflePartitionedData partitionedData = createData(0, 1);
    shuffleTaskManager.cacheShuffleData(appId, shuffleId, false, partitionedData);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, 0, partitionedData.getBlockList());
    long usedSize = shuffleTaskManager.getPartitionDataSize(appId, shuffleId, 0);
    assertEquals(1 + 32, usedSize);
    assertFalse(
        HugePartitionUtils.hasExceedPartitionSplitLimit(
            shuffleBufferManager, shuffleTaskManager.getPartitionDataSize(appId, shuffleId, 0)));

    // case2: its partition exceed the split limit
    partitionedData = createData(0, 200);
    shuffleTaskManager.cacheShuffleData(appId, shuffleId, false, partitionedData);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, 0, partitionedData.getBlockList());
    usedSize = shuffleTaskManager.getPartitionDataSize(appId, shuffleId, 0);
    assertEquals(1 + 32 + 200 + 32, usedSize);
    assertTrue(
        HugePartitionUtils.hasExceedPartitionSplitLimit(
            shuffleBufferManager, shuffleTaskManager.getPartitionDataSize(appId, shuffleId, 0)));

    // check returned need split partitions
    Pair<Long, List<Integer>> pair =
        shuffleTaskManager.requireBufferReturnPair(
            appId, shuffleId, Arrays.asList(0, 1), Arrays.asList(10, 10), 20);
    assertEquals(1, pair.getRight().size());
    assertEquals(0, pair.getRight().get(0));
  }
}
