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

package org.apache.uniffle.client.response;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.uniffle.common.exception.RssException;

public class DecompressedShuffleBlock extends ShuffleBlock {
  private CompletableFuture<ByteBuffer> f;
  private Consumer<Long> waitMillisCallback;
  private final int fetchSecondsThreshold;

  public DecompressedShuffleBlock(
      CompletableFuture<ByteBuffer> f,
      Consumer<Long> consumer,
      long taskAttemptId,
      int fetchSecondsThreshold) {
    super(taskAttemptId);
    this.f = f;
    this.waitMillisCallback = consumer;
    this.fetchSecondsThreshold = fetchSecondsThreshold;
  }

  @Override
  public int getUncompressLength() {
    ByteBuffer buffer = getByteBuffer();
    return buffer.limit() - buffer.position();
  }

  @Override
  public ByteBuffer getByteBuffer() {
    try {
      long startTime = System.currentTimeMillis();
      ByteBuffer buffer =
          fetchSecondsThreshold > 0 ? f.get(fetchSecondsThreshold, TimeUnit.SECONDS) : f.get();
      if (waitMillisCallback != null) {
        waitMillisCallback.accept(System.currentTimeMillis() - startTime);
      }
      return buffer;
    } catch (Exception e) {
      throw new RssException(e);
    }
  }
}
