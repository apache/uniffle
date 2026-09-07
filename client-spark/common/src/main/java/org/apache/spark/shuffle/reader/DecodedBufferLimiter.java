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

class DecodedBufferLimiter {
  private final long budgetBytes;
  private long nextSequence;
  private long usedBytes;
  private boolean closed;

  DecodedBufferLimiter(long budgetBytes) {
    this.budgetBytes = Math.max(1L, budgetBytes);
  }

  Permit acquire(long sequence, long bytes) {
    long normalizedBytes = Math.max(0L, bytes);
    synchronized (this) {
      while (!closed && (sequence != nextSequence || !canAcquire(normalizedBytes))) {
        try {
          wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
      if (closed) {
        throw new RuntimeException("Decoded buffer limiter is closed");
      }
      usedBytes += normalizedBytes;
      nextSequence++;
      notifyAll();
    }
    return new Permit(normalizedBytes);
  }

  void close() {
    synchronized (this) {
      closed = true;
      notifyAll();
    }
  }

  private boolean canAcquire(long bytes) {
    if (bytes <= budgetBytes) {
      return usedBytes + bytes <= budgetBytes;
    }
    return usedBytes == 0;
  }

  class Permit {
    private final long bytes;
    private boolean released;

    private Permit(long bytes) {
      this.bytes = bytes;
    }

    void release() {
      synchronized (DecodedBufferLimiter.this) {
        if (released) {
          return;
        }
        released = true;
        usedBytes -= bytes;
        DecodedBufferLimiter.this.notifyAll();
      }
    }
  }
}
