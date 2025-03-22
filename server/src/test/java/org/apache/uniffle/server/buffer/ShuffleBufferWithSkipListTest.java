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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.server.ShuffleDataFlushEvent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShuffleBufferWithSkipListTest extends BufferTestBase {
  private static AtomicInteger atomSequenceNo = new AtomicInteger(0);

  @Test
  public void appendTest() {
    ShuffleBuffer shuffleBuffer = new ShuffleBufferWithSkipList();
    shuffleBuffer.append(createData(10));
    // ShufflePartitionedBlock has constant 32 bytes overhead
    assertEquals(42, shuffleBuffer.getEncodedLength());

    shuffleBuffer.append(createData(26));
    assertEquals(100, shuffleBuffer.getEncodedLength());

    shuffleBuffer.append(createData(1));
    assertEquals(133, shuffleBuffer.getEncodedLength());
  }

  @Test
  public void appendMultiBlocksTest() {
    ShuffleBuffer shuffleBuffer = new ShuffleBufferWithSkipList();
    ShufflePartitionedData data1 = createData(10);
    ShufflePartitionedData data2 = createData(10);
    ShufflePartitionedBlock[] dataCombine = new ShufflePartitionedBlock[2];
    dataCombine[0] = data1.getBlockList()[0];
    dataCombine[1] = data2.getBlockList()[0];
    shuffleBuffer.append(new ShufflePartitionedData(1, dataCombine));
    assertEquals(84, shuffleBuffer.getEncodedLength());
  }

  @Test
  public void toFlushEventTest() {
    ShuffleBuffer shuffleBuffer = new ShuffleBufferWithSkipList();
    ShuffleDataFlushEvent event = shuffleBuffer.toFlushEvent("appId", 0, 0, 1, null);
    assertNull(event);
    shuffleBuffer.append(createData(10));
    assertEquals(42, shuffleBuffer.getEncodedLength());
    event = shuffleBuffer.toFlushEvent("appId", 0, 0, 1, null);
    assertEquals(42, event.getEncodedLength());
    assertEquals(0, shuffleBuffer.getEncodedLength());
    assertEquals(0, shuffleBuffer.getBlocks().size());
  }

  @Test
  public void getShuffleDataWithExpectedTaskIdsFilterTest() {
    /** case1: all blocks in cached(or in flushed map) and size < readBufferSize */
    ShuffleBuffer shuffleBuffer = new ShuffleBufferWithSkipList();
    ShufflePartitionedData spd1 = createData(1, 1, 15);
    ShufflePartitionedData spd2 = createData(1, 0, 15);
    ShufflePartitionedData spd3 = createData(1, 2, 55);
    ShufflePartitionedData spd4 = createData(1, 1, 45);
    shuffleBuffer.append(spd1);
    shuffleBuffer.append(spd2);
    shuffleBuffer.append(spd3);
    shuffleBuffer.append(spd4);

    Roaring64NavigableMap expectedTasks = Roaring64NavigableMap.bitmapOf(1, 2);
    ShuffleDataResult result =
        shuffleBuffer.getShuffleData(Constants.INVALID_BLOCK_ID, 1000, expectedTasks);
    assertEquals(3, result.getBufferSegments().size());
    for (BufferSegment segment : result.getBufferSegments()) {
      assertTrue(expectedTasks.contains(segment.getTaskAttemptId()));
    }
    assertEquals(0, result.getBufferSegments().get(0).getOffset());
    // Currently, if we use skip_list, we can't guarantee that the reading order is same as
    // writing order. So only check the total segment size of taskAttempt 1.
    assertEquals(
        60,
        result.getBufferSegments().get(0).getLength()
            + result.getBufferSegments().get(1).getLength());
    assertEquals(60, result.getBufferSegments().get(2).getOffset());
    assertEquals(55, result.getBufferSegments().get(2).getLength());

    expectedTasks = Roaring64NavigableMap.bitmapOf(0);
    result = shuffleBuffer.getShuffleData(Constants.INVALID_BLOCK_ID, 1000, expectedTasks);
    assertEquals(1, result.getBufferSegments().size());
    assertEquals(15, result.getBufferSegments().get(0).getLength());

    /**
     * case2: all blocks in cached(or in flushed map) and size > readBufferSize, so it will read
     * multiple times.
     *
     * <p>required blocks size list: 15+45, 55
     */
    expectedTasks = Roaring64NavigableMap.bitmapOf(1, 2);
    result = shuffleBuffer.getShuffleData(Constants.INVALID_BLOCK_ID, 60, expectedTasks);
    assertEquals(2, result.getBufferSegments().size());
    assertEquals(0, result.getBufferSegments().get(0).getOffset());
    assertEquals(
        60,
        result.getBufferSegments().get(0).getLength()
            + result.getBufferSegments().get(1).getLength());

    // 2nd read
    long lastBlockId = result.getBufferSegments().get(1).getBlockId();
    result = shuffleBuffer.getShuffleData(lastBlockId, 60, expectedTasks);
    assertEquals(1, result.getBufferSegments().size());
    assertEquals(0, result.getBufferSegments().get(0).getOffset());
    assertEquals(55, result.getBufferSegments().get(0).getLength());

    /** case3: all blocks in flushed map and size < readBufferSize */
    expectedTasks = Roaring64NavigableMap.bitmapOf(1, 2);
    ShuffleDataFlushEvent event1 =
        shuffleBuffer.toFlushEvent("appId", 0, 0, 1, null, ShuffleDataDistributionType.LOCAL_ORDER);
    result = shuffleBuffer.getShuffleData(Constants.INVALID_BLOCK_ID, 1000, expectedTasks);
    assertEquals(3, result.getBufferSegments().size());
    for (BufferSegment segment : result.getBufferSegments()) {
      assertTrue(expectedTasks.contains(segment.getTaskAttemptId()));
    }
    assertEquals(0, result.getBufferSegments().get(0).getOffset());
    // Currently, if we use skip_list, we can't guarantee that the reading order is same as
    // writing order. So only check the total segment size of taskAttempt 1.
    assertEquals(
        60,
        result.getBufferSegments().get(0).getLength()
            + result.getBufferSegments().get(1).getLength());
    assertEquals(60, result.getBufferSegments().get(2).getOffset());
    assertEquals(55, result.getBufferSegments().get(2).getLength());

    /** case4: all blocks in flushed map and size > readBufferSize, it will read multiple times */
    expectedTasks = Roaring64NavigableMap.bitmapOf(1, 2);
    result = shuffleBuffer.getShuffleData(Constants.INVALID_BLOCK_ID, 60, expectedTasks);
    assertEquals(2, result.getBufferSegments().size());
    assertEquals(0, result.getBufferSegments().get(0).getOffset());
    assertEquals(
        60,
        result.getBufferSegments().get(0).getLength()
            + result.getBufferSegments().get(1).getLength());

    // 2nd read
    lastBlockId = result.getBufferSegments().get(1).getBlockId();
    result = shuffleBuffer.getShuffleData(lastBlockId, 60, expectedTasks);
    assertEquals(1, result.getBufferSegments().size());
    assertEquals(0, result.getBufferSegments().get(0).getOffset());
    assertEquals(55, result.getBufferSegments().get(0).getLength());

    /**
     * case5: partial blocks in cache and another in flushedMap, and it will read multiple times.
     *
     * <p>required size: 15, 55, 45 (in flushed map) 55, 45, 5, 25(in cached)
     */
    ShufflePartitionedData spd5 = createData(1, 2, 55);
    ShufflePartitionedData spd6 = createData(1, 1, 45);
    ShufflePartitionedData spd7 = createData(1, 1, 5);
    ShufflePartitionedData spd8 = createData(1, 1, 25);
    shuffleBuffer.append(spd5);
    shuffleBuffer.append(spd6);
    shuffleBuffer.append(spd7);
    shuffleBuffer.append(spd8);

    expectedTasks = Roaring64NavigableMap.bitmapOf(1, 2);
    result = shuffleBuffer.getShuffleData(Constants.INVALID_BLOCK_ID, 60, expectedTasks);
    assertEquals(2, result.getBufferSegments().size());

    // 2nd read
    lastBlockId = result.getBufferSegments().get(1).getBlockId();
    result = shuffleBuffer.getShuffleData(lastBlockId, 60, expectedTasks);
    assertEquals(2, result.getBufferSegments().size());
    // 3rd read
    lastBlockId = result.getBufferSegments().get(1).getBlockId();
    result = shuffleBuffer.getShuffleData(lastBlockId, 60, expectedTasks);
    assertEquals(3, result.getBufferSegments().size());
  }

  @Test
  public void appendRepeatBlockTest() {
    ShuffleBuffer shuffleBuffer = new ShuffleBufferWithSkipList();
    ShufflePartitionedData block = createData(10);
    shuffleBuffer.append(block);
    // ShufflePartitionedBlock has constant 32 bytes overhead
    assertEquals(42, shuffleBuffer.getEncodedLength());

    shuffleBuffer.append(block);
    // The repeat block should not append to shuffleBuffer
    assertEquals(42, shuffleBuffer.getEncodedLength());
  }

  @Override
  protected AtomicInteger getAtomSequenceNo() {
    return atomSequenceNo;
  }
}
