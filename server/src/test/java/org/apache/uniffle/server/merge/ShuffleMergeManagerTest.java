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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import org.apache.hadoop.io.DataInputBuffer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.merger.MergeState;
import org.apache.uniffle.common.records.RecordsReader;
import org.apache.uniffle.common.serializer.SerInputStream;
import org.apache.uniffle.common.serializer.SerializerInstance;
import org.apache.uniffle.common.serializer.SerializerUtils;
import org.apache.uniffle.common.serializer.writable.WritableSerializer;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.server.ShuffleTaskManager;
import org.apache.uniffle.server.buffer.ShuffleBufferType;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.common.serializer.SerializerUtils.genData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ShuffleMergeManagerTest {

  private static final String APP_ID = "app1";
  private static final int SHUFFLE_ID = 1;
  private static final int PARTITION_ID = 2;
  private static final int RECORDS_NUMBER = 1009;
  private static final String USER = "testUser";

  private ShuffleServer shuffleServer;
  ShuffleServerConf serverConf;

  @TempDir File tempDir1;
  @TempDir File tempDir2;

  @BeforeEach
  public void beforeEach() {
    String confFile = ClassLoader.getSystemResource("server.conf").getFile();
    serverConf = new ShuffleServerConf(confFile);
    serverConf.setString(
        ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE.name());
    serverConf.setString(
        ShuffleServerConf.RSS_STORAGE_BASE_PATH.key(),
        tempDir1.getAbsolutePath() + "," + tempDir2.getAbsolutePath());
    serverConf.setLong(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 60L * 1000L * 60L);
    serverConf.set(ShuffleServerConf.SERVER_MERGE_ENABLE, true);
    serverConf.set(ShuffleServerConf.SERVER_SHUFFLE_BUFFER_TYPE, ShuffleBufferType.SKIP_LIST);
    serverConf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 100 * 1024 * 1024L);
    ShuffleServerMetrics.clear();
    ShuffleServerMetrics.register();
    assertTrue(this.tempDir1.isDirectory());
    assertTrue(this.tempDir2.isDirectory());
  }

  @AfterEach
  public void afterEach() throws Exception {
    serverConf = null;
    if (shuffleServer != null) {
      shuffleServer.stopServer();
      shuffleServer = null;
    }
  }

  @Timeout(10)
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
  void testMergerManager(String classes) throws Exception {
    // 1 Construct serializer and comparator
    final String[] classArray = classes.split(",");
    final String keyClassName = classArray[0];
    final String valueClassName = classArray[1];
    final Class<?> keyClass = SerializerUtils.getClassByName(keyClassName);
    final Class<?> valueClass = SerializerUtils.getClassByName(valueClassName);
    boolean raw = classArray.length > 2 && Boolean.parseBoolean(classArray[2]);
    boolean direct = classArray.length > 3 && Boolean.parseBoolean(classArray[3]);
    final Comparator comparator = SerializerUtils.getComparator(keyClass);
    final String comparatorClassName = comparator.getClass().getName();
    final WritableSerializer serializer = new WritableSerializer(new RssConf());
    final SerializerInstance instance = serializer.newInstance();

    // 2 Construct shuffle task manager and merge manager
    shuffleServer = new ShuffleServer(serverConf);
    final ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    final ShuffleMergeManager mergeManager = shuffleServer.getShuffleMergeManager();

    // 3 register shuffle
    List<PartitionRange> partitionRanges = new ArrayList<>();
    partitionRanges.add(new PartitionRange(PARTITION_ID, PARTITION_ID));
    shuffleTaskManager.registerShuffle(
        APP_ID, SHUFFLE_ID, partitionRanges, new RemoteStorageInfo(""), USER);
    shuffleTaskManager.registerShuffle(
        APP_ID + ShuffleMergeManager.MERGE_APP_SUFFIX,
        SHUFFLE_ID,
        partitionRanges,
        new RemoteStorageInfo(""),
        USER);
    mergeManager.registerShuffle(
        APP_ID,
        SHUFFLE_ID,
        RssProtos.MergeContext.newBuilder()
            .setKeyClass(keyClassName)
            .setValueClass(valueClassName)
            .setComparatorClass(comparatorClassName)
            .setMergedBlockSize(-1)
            .setMergeClassLoader("")
            .build());

    // 4 report blocks
    // 4.1 send shuffle data
    // Upstream have 2 task, each task generate 2 blocks
    BlockIdLayout blockIdLayout = BlockIdLayout.from(serverConf);
    long[] blocks = new long[4];
    blocks[0] = blockIdLayout.getBlockId(0, PARTITION_ID, 0);
    blocks[1] = blockIdLayout.getBlockId(1, PARTITION_ID, 0);
    blocks[2] = blockIdLayout.getBlockId(0, PARTITION_ID, 1);
    blocks[3] = blockIdLayout.getBlockId(1, PARTITION_ID, 1);
    ShufflePartitionedBlock[] shufflePartitionedBlocks = new ShufflePartitionedBlock[4];
    for (int i = 0; i < 4; i++) {
      ByteBuf byteBuf =
          SerializerUtils.genSortedRecordBuffer(
              serverConf, keyClass, valueClass, i, 4, RECORDS_NUMBER, 1, direct);
      shufflePartitionedBlocks[i] =
          new ShufflePartitionedBlock(
              byteBuf.readableBytes(),
              byteBuf.readableBytes(),
              0,
              blocks[i],
              blockIdLayout.getTaskAttemptId(blocks[i]),
              byteBuf);
    }
    ShufflePartitionedData spd = new ShufflePartitionedData(PARTITION_ID, shufflePartitionedBlocks);
    shuffleTaskManager.cacheShuffleData(APP_ID, SHUFFLE_ID, false, spd);
    mergeManager.setDirect(APP_ID, SHUFFLE_ID, direct);
    // 4.2 report shuffle result
    shuffleTaskManager.addFinishedBlockIds(
        APP_ID, SHUFFLE_ID, ImmutableMap.of(PARTITION_ID, blocks), 1);
    // 4.3 report unique blockIds
    Roaring64NavigableMap blockIdMap = Roaring64NavigableMap.bitmapOf();
    blockIdMap.add(blocks);
    mergeManager.startSortMerge(APP_ID, SHUFFLE_ID, PARTITION_ID, blockIdMap);

    // 4 wait for drain event
    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .until(() -> mergeManager.getEventHandler().getEventNumInMerge() == 0);
    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .until(
            () ->
                mergeManager.getPartition(APP_ID, SHUFFLE_ID, PARTITION_ID).getState()
                    == MergeState.DONE);

    // 5 read and check result
    int blockId = 1;
    int index = 0;
    boolean finish = false;
    while (!finish) {
      MergeStatus mergeStatus = mergeManager.tryGetBlock(APP_ID, SHUFFLE_ID, PARTITION_ID, blockId);
      MergeState mergeState = mergeStatus.getState();
      long blockSize = mergeStatus.getSize();
      switch (mergeState) {
        case INITED:
        case MERGING:
        case INTERNAL_ERROR:
          fail("Find wrong merge state!");
          break;
        case DONE:
          if (blockSize != -1) {
            ShuffleDataResult shuffleDataResult =
                mergeManager.getShuffleData(APP_ID, SHUFFLE_ID, PARTITION_ID, blockId);
            SerInputStream inputStream =
                SerInputStream.newInputStream(shuffleDataResult.getDataBuf());
            RecordsReader<?, ?> reader =
                new RecordsReader<>(serverConf, inputStream, keyClass, valueClass, raw, true);
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
                assertEquals(
                    genData(keyClass, index), instance.deserialize(keyInputBuffer, keyClass));
                DataInputBuffer valueInputBuffer = new DataInputBuffer();
                valueInputBuffer.reset(valueBytes, 0, valueBytes.length);
                assertEquals(
                    genData(valueClass, index), instance.deserialize(valueInputBuffer, valueClass));
              } else {
                assertEquals(genData(keyClass, index), reader.getCurrentKey());
                assertEquals(genData(valueClass, index), reader.getCurrentValue());
              }
              index++;
            }
            shuffleDataResult.release();
            blockId++;
          } else {
            finish = true;
          }
          break;
        default:
          fail("Find invalid merge state!");
      }
    }
    assertEquals(RECORDS_NUMBER * 4, index);

    // 8 cleanup
    mergeManager.removeBuffer(APP_ID, SHUFFLE_ID);
  }
}
