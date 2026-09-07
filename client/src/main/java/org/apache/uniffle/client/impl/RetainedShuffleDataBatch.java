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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataResult;

class RetainedShuffleDataBatch {
  private final ShuffleDataResult result;
  private final ByteBuffer dataBuffer;
  private final AtomicInteger refCount = new AtomicInteger(1);

  RetainedShuffleDataBatch(ShuffleDataResult result) {
    this.result = result;
    this.dataBuffer = result.getDataBuffer();
  }

  ByteBuffer getDataBuffer() {
    return dataBuffer;
  }

  List<BufferSegment> getBufferSegments() {
    return result.getBufferSegments();
  }

  ByteBuffer retainedSlice(BufferSegment segment) {
    retain();
    ByteBuffer slice = dataBuffer.duplicate();
    slice.position(segment.getOffset());
    slice.limit(segment.getOffset() + segment.getLength());
    return slice;
  }

  void release() {
    int refs = refCount.decrementAndGet();
    if (refs == 0) {
      result.release();
    } else if (refs < 0) {
      throw new IllegalStateException("Shuffle data batch released too many times");
    }
  }

  private void retain() {
    while (true) {
      int refs = refCount.get();
      if (refs <= 0) {
        throw new IllegalStateException("Shuffle data batch has already been released");
      }
      if (refCount.compareAndSet(refs, refs + 1)) {
        return;
      }
    }
  }
}
