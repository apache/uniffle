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

package org.apache.uniffle.client.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.response.DecompressedShuffleBlock;
import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.config.RssConf;

import static org.apache.uniffle.common.config.RssClientConf.COMPRESSION_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class DecompressionWorkerTest {

  @Test
  public void testEmptyGet() throws Exception {
    DecompressionWorker worker = new DecompressionWorker(Codec.newInstance(new RssConf()).get(), 1);
    assertNull(worker.get(1, 1));
  }

  private ByteBuffer createByteBuffer(int size) {
    ByteBuffer buffer = ByteBuffer.allocate(size);
    Random random = new Random();
    for (int i = 0; i < buffer.capacity(); i++) {
      buffer.put((byte) random.nextInt(256));
    }
    buffer.flip();
    return buffer;
  }

  private ShuffleDataResult createShuffleDataResult(
      int segmentSize, Codec codec, int segmentLength) {
    List<ByteBuffer> buffers = new ArrayList<>();
    List<BufferSegment> segments = new ArrayList<>();
    int offset = 0;
    for (int i = 0; i < segmentSize; i++) {
      ByteBuffer buffer = createByteBuffer(segmentLength);
      ByteBuffer dest = ByteBuffer.wrap(codec.compress(buffer.array()));
      buffers.add(buffer);
      segments.add(new BufferSegment(i, offset, dest.remaining(), buffer.remaining(), 1, i));
      offset += dest.remaining();
    }
    ByteBuffer merged = ByteBuffer.allocate(offset);
    for (ByteBuffer b : buffers) {
      merged.put(b.duplicate());
    }
    merged.flip();
    return new ShuffleDataResult(merged, segments);
  }

  @Test
  public void test() throws Exception {
    RssConf rssConf = new RssConf();
    rssConf.set(COMPRESSION_TYPE, Codec.Type.NOOP);
    Codec codec = Codec.newInstance(rssConf).get();
    DecompressionWorker worker = new DecompressionWorker(codec, 1);

    // create some data
    ShuffleDataResult shuffleDataResult = createShuffleDataResult(10, codec, 100);
    worker.add(0, shuffleDataResult);

    DecompressedShuffleBlock block1 = worker.get(0, 1);
    assertEquals(100, block1.getByteBuffer().remaining());
  }
}
