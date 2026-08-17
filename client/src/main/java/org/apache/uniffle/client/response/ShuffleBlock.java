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
import java.util.concurrent.atomic.AtomicBoolean;

public class ShuffleBlock implements AutoCloseable {
  private final ByteBuffer byteBuffer;
  private final int uncompressedLength;
  private final int compressedLength;
  private final long taskAttemptId;
  private final Runnable releaseCallback;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public ShuffleBlock(ByteBuffer byteBuffer, int uncompressedLength, int compressedLength) {
    this(byteBuffer, uncompressedLength, -1, compressedLength);
  }

  public ShuffleBlock(
      ByteBuffer byteBuffer, int uncompressedLength, long taskAttemptId, int compressedLength) {
    this(byteBuffer, uncompressedLength, taskAttemptId, compressedLength, null);
  }

  public ShuffleBlock(
      ByteBuffer byteBuffer,
      int uncompressedLength,
      long taskAttemptId,
      int compressedLength,
      Runnable releaseCallback) {
    this.byteBuffer = byteBuffer;
    this.uncompressedLength = uncompressedLength;
    this.compressedLength = compressedLength;
    this.taskAttemptId = taskAttemptId;
    this.releaseCallback = releaseCallback;
  }

  public int getCompressedLength() {
    return compressedLength;
  }

  public int getUncompressLength() {
    return uncompressedLength;
  }

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  @Override
  public void close() {
    if (releaseCallback != null && closed.compareAndSet(false, true)) {
      releaseCallback.run();
    }
  }
}
