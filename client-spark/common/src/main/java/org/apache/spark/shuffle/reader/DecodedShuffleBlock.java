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

package org.apache.spark.shuffle.reader;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

class DecodedShuffleBlock implements AutoCloseable {
  private final ByteBuffer data;
  private final long taskAttemptId;
  private final int compressedLength;
  private final int uncompressedLength;
  private final long readMillis;
  private long decodeWaitMillis;
  private final Runnable closeCallback;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  DecodedShuffleBlock(
      ByteBuffer data,
      long taskAttemptId,
      int compressedLength,
      int uncompressedLength,
      long readMillis,
      long decodeWaitMillis,
      Runnable closeCallback) {
    this.data = data;
    this.taskAttemptId = taskAttemptId;
    this.compressedLength = compressedLength;
    this.uncompressedLength = uncompressedLength;
    this.readMillis = readMillis;
    this.decodeWaitMillis = decodeWaitMillis;
    this.closeCallback = closeCallback;
  }

  ByteBuffer data() {
    return data;
  }

  long taskAttemptId() {
    return taskAttemptId;
  }

  int compressedLength() {
    return compressedLength;
  }

  int uncompressedLength() {
    return uncompressedLength;
  }

  long readMillis() {
    return readMillis;
  }

  long decodeWaitMillis() {
    return decodeWaitMillis;
  }

  void addDecodeWaitMillis(long waitMillis) {
    this.decodeWaitMillis += waitMillis;
  }

  @Override
  public void close() {
    if (closeCallback != null && closed.compareAndSet(false, true)) {
      closeCallback.run();
    }
  }
}
