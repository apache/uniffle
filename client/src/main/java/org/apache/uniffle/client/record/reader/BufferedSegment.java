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

package org.apache.uniffle.client.record.reader;

import java.io.IOException;

import org.apache.uniffle.client.record.RecordBuffer;
import org.apache.uniffle.common.merger.Segment;

public class BufferedSegment<K, V> extends Segment {

  private RecordBuffer<K, V> recordBuffer;
  private int index = -1;

  public BufferedSegment(RecordBuffer recordBuffer) {
    super(recordBuffer.getPartitionId());
    this.recordBuffer = recordBuffer;
  }

  @Override
  public boolean next() throws IOException {
    boolean hasNext = index < this.recordBuffer.size() - 1;
    if (hasNext) {
      index++;
    }
    return hasNext;
  }

  @Override
  public Object getCurrentKey() {
    return this.recordBuffer.getKey(index);
  }

  @Override
  public Object getCurrentValue() {
    return this.recordBuffer.getValue(index);
  }

  @Override
  public void close() throws IOException {
    if (recordBuffer != null) {
      this.recordBuffer.clear();
      this.recordBuffer = null;
    }
  }

  @Override
  public long getSize() {
    // Should never use
    return -1;
  }
}
