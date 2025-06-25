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

package org.apache.uniffle.common;

import java.util.List;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * This class is to deferred compress the block data to avoid blocking the main thread progress. And
 * so, the below override methods should be invoked in the background thread. Because it will
 * trigger the underlying the compression initialization for the data.
 */
public class DeferredCompressedBlock extends ShuffleBlockInfo {
  private final Function<DeferredCompressedBlock, DeferredCompressedBlock> rebuildFunction;
  private int estimatedCompressedSize;
  private boolean isInitialized = false;

  public DeferredCompressedBlock(
      int shuffleId,
      int partitionId,
      long blockId,
      List<ShuffleServerInfo> shuffleServerInfos,
      int uncompressLength,
      long freeMemory,
      long taskAttemptId,
      Function<Integer, List<ShuffleServerInfo>> partitionAssignmentRetrieveFunc,
      Function<DeferredCompressedBlock, DeferredCompressedBlock> rebuildFunction,
      int estimatedCompressedSize) {
    super(
        shuffleId,
        partitionId,
        blockId,
        shuffleServerInfos,
        uncompressLength,
        freeMemory,
        taskAttemptId,
        partitionAssignmentRetrieveFunc);
    this.rebuildFunction = rebuildFunction;
    this.estimatedCompressedSize = estimatedCompressedSize;
  }

  public void reset(byte[] data, int length, long crc) {
    super.length = length;
    super.crc = crc;
    super.data = Unpooled.wrappedBuffer(data);
  }

  private void initialize() {
    if (!isInitialized) {
      rebuildFunction.apply(this);
      isInitialized = true;
    }
  }

  public int getEstimatedLayoutSize() {
    return estimatedCompressedSize + 3 * 8 + 2 * 4;
  }

  @Override
  public int getLength() {
    initialize();
    return super.getLength();
  }

  @Override
  public int getSize() {
    initialize();
    return super.getSize();
  }

  @Override
  public long getCrc() {
    initialize();
    return super.getCrc();
  }

  @Override
  public ByteBuf getData() {
    initialize();
    return super.getData();
  }

  @Override
  public synchronized void copyDataTo(ByteBuf to) {
    initialize();
    super.copyDataTo(to);
  }
}
