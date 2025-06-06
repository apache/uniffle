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

package org.apache.uniffle.server.buffer.lab;

import java.util.function.Supplier;

import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.buffer.ShuffleBufferWithSkipList;

public class LABShuffleBufferWithSkipList extends ShuffleBufferWithSkipList implements SupportsLAB {
  private LAB lab;

  public LABShuffleBufferWithSkipList() {
    super();
    this.lab = new LAB();
  }

  @Override
  protected void addBlock(ShufflePartitionedBlock block) {
    ShufflePartitionedBlock newBlock = lab.tryCopyBlockToChunk(block);
    if (newBlock.isOnLAB()) {
      super.releaseBlock(block);
    }
    super.addBlock(newBlock);
  }

  @Override
  protected void releaseBlock(ShufflePartitionedBlock block) {
    if (!block.isOnLAB()) {
      super.releaseBlock(block);
    }
  }

  @Override
  public synchronized ShuffleDataFlushEvent toFlushEvent(
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      Supplier<Boolean> isValid,
      ShuffleDataDistributionType dataDistributionType) {
    ShuffleDataFlushEvent event =
        super.toFlushEvent(
            appId, shuffleId, startPartition, endPartition, isValid, dataDistributionType);
    lab = new LAB();
    return event;
  }

  @Override
  protected Runnable createCallbackForFlush(ShuffleDataFlushEvent event) {
    Runnable runnable = super.createCallbackForFlush(event);
    final LAB labRef = lab;
    return () -> {
      runnable.run();
      labRef.close();
    };
  }

  @Override
  public synchronized long release() {
    long size = super.release();
    lab.close();
    return size;
  }
}
