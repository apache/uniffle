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

import java.util.List;

import io.netty.buffer.ByteBuf;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.netty.buffer.NettyManagedBuffer;

public class MemoryShuffleDataResult extends ShuffleDataResult {
  private final boolean isEnd;

  public MemoryShuffleDataResult() {
    super();
    // empty
    this.isEnd = true;
  }

  public MemoryShuffleDataResult(ByteBuf data, List<BufferSegment> bufferSegments, boolean isEnd) {
    super(new NettyManagedBuffer(data), bufferSegments);
    this.isEnd = isEnd;
  }

  public boolean isEnd() {
    return isEnd;
  }
}
