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

package org.apache.uniffle.common.segment;

import java.nio.ByteBuffer;
import java.util.List;

import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.uniffle.common.ShuffleDataSegment;
import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.common.netty.buffer.NettyManagedBuffer;

import static org.apache.uniffle.common.segment.LocalOrderSegmentSplitterTest.generateData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class FixedSizeSegmentSplitterTest {

  @ParameterizedTest
  @ValueSource(ints = {48, 49, 57})
  public void testAvoidEOFException(int dataLength) {
    SegmentSplitter splitter = new FixedSizeSegmentSplitter(1000);
    byte[] data = generateData(Pair.of(32, 0), Pair.of(16, 0), Pair.of(10, 0));

    List<ShuffleDataSegment> shuffleDataSegments =
        splitter.split(new ShuffleIndexResult(ByteBuffer.wrap(data), dataLength));
    assertEquals(1, shuffleDataSegments.size());
    assertEquals(0, shuffleDataSegments.get(0).getOffset());
    assertEquals(48, shuffleDataSegments.get(0).getLength());
  }

  @Test
  public void testSplit() {
    SegmentSplitter splitter = new FixedSizeSegmentSplitter(100);
    ShuffleIndexResult shuffleIndexResult = new ShuffleIndexResult();
    List<ShuffleDataSegment> shuffleDataSegments = splitter.split(shuffleIndexResult);
    assertTrue(shuffleDataSegments.isEmpty());

    int readBufferSize = 32;
    splitter = new FixedSizeSegmentSplitter(32);

    // those 5 segment's data length are [32, 16, 10, 32, 6] so the index should be
    // split into 3 ShuffleDataSegment, which are [32, 16 + 10 + 32, 6]
    byte[] data =
        generateData(Pair.of(32, 0), Pair.of(16, 0), Pair.of(10, 0), Pair.of(32, 6), Pair.of(6, 0));
    shuffleDataSegments = splitter.split(new ShuffleIndexResult(ByteBuffer.wrap(data), -1));
    assertEquals(3, shuffleDataSegments.size());

    assertEquals(0, shuffleDataSegments.get(0).getOffset());
    assertEquals(32, shuffleDataSegments.get(0).getLength());
    assertEquals(1, shuffleDataSegments.get(0).getBufferSegments().size());

    assertEquals(32, shuffleDataSegments.get(1).getOffset());
    assertEquals(58, shuffleDataSegments.get(1).getLength());
    assertEquals(3, shuffleDataSegments.get(1).getBufferSegments().size());

    assertEquals(90, shuffleDataSegments.get(2).getOffset());
    assertEquals(6, shuffleDataSegments.get(2).getLength());
    assertEquals(1, shuffleDataSegments.get(2).getBufferSegments().size());

    ByteBuffer incompleteByteBuffer = ByteBuffer.allocate(12);
    incompleteByteBuffer.putLong(1L);
    incompleteByteBuffer.putInt(2);
    data = incompleteByteBuffer.array();
    // It should throw exception
    try {
      splitter.split(new ShuffleIndexResult(ByteBuffer.wrap(data), -1));
      fail();
    } catch (Exception e) {
      // ignore
      assertTrue(e.getMessage().contains("Read index data under flow"));
    }
  }

  @Test
  @DisplayName("Test splitting with storage ID changes")
  void testSplitContainsStorageId() {
    SegmentSplitter splitter = new FixedSizeSegmentSplitter(50);
    int[] storageIds = new int[] {1, 2, 3};
    byte[] data0 =
        generateData(Pair.of(32, 0), Pair.of(16, 0), Pair.of(10, 0), Pair.of(32, 6), Pair.of(6, 0));
    byte[] data1 =
        generateData(Pair.of(32, 1), Pair.of(16, 0), Pair.of(10, 0), Pair.of(32, 6), Pair.of(6, 0));
    byte[] data2 =
        generateData(Pair.of(32, 1), Pair.of(16, 0), Pair.of(10, 0), Pair.of(32, 6), Pair.of(6, 0));

    ByteBuffer dataCombined =
        ByteBuffer.allocate(data0.length + data1.length + data2.length)
            .put(data0)
            .put(data1)
            .put(data2);
    dataCombined.flip();
    List<ShuffleDataSegment> shuffleDataSegments =
        splitter.split(
            new ShuffleIndexResult(
                new NettyManagedBuffer(Unpooled.wrappedBuffer(dataCombined)), -1L, "", storageIds));
    assertEquals(6, shuffleDataSegments.size(), "Expected 6 segments");
    assertSegment(shuffleDataSegments.get(0), 0, 58, 3, storageIds[0]);
    // split while previous data segments over read buffer size
    assertSegment(shuffleDataSegments.get(1), 58, 38, 2, storageIds[0]);
    // split while storage id changed, which offset less than previous offset
    assertSegment(shuffleDataSegments.get(2), 0, 58, 3, storageIds[1]);
    // split while previous data segments over read buffer size
    assertSegment(shuffleDataSegments.get(3), 58, 38, 2, storageIds[1]);
    // split while storage id changed, which offset less than previous offset
    assertSegment(shuffleDataSegments.get(4), 0, 58, 3, storageIds[2]);
    // split while previous data segments over read buffer size
    assertSegment(shuffleDataSegments.get(5), 58, 38, 2, storageIds[2]);
  }

  private void assertSegment(
      ShuffleDataSegment segment,
      int expectedOffset,
      int expectedLength,
      int expectedSize,
      int expectedStorageId) {
    assertEquals(expectedOffset, segment.getOffset(), "Incorrect offset");
    assertEquals(expectedLength, segment.getLength(), "Incorrect length");
    assertEquals(expectedSize, segment.getBufferSegments().size(), "Incorrect buffer segment size");
    assertEquals(expectedStorageId, segment.getStorageId(), "Incorrect storage ID");
  }
}
