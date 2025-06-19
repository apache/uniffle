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

public class UncompressedShuffleBlockInfo extends ShuffleBlockInfo {
  private final Function<UncompressedShuffleBlockInfo, ShuffleBlockInfo> compressedFunc;
  private ShuffleBlockInfo compressedBlock;

  public UncompressedShuffleBlockInfo(
      Function<UncompressedShuffleBlockInfo, ShuffleBlockInfo> compressedFunc) {
    super();
    this.compressedFunc = compressedFunc;
  }

  private ShuffleBlockInfo getOrCreate() {
    if (compressedBlock == null) {
      compressedBlock = compressedFunc.apply(this);
    }
    return compressedBlock;
  }

  public long getBlockId() {
    return getOrCreate().getBlockId();
  }

  public int getLength() {
    return getOrCreate().getLength();
  }

  public int getSize() {
    return getOrCreate().getSize();
  }

  public long getCrc() {
    return getOrCreate().getCrc();
  }

  public ByteBuf getData() {
    return getOrCreate().getData();
  }

  public int getShuffleId() {
    return getOrCreate().getShuffleId();
  }

  public int getPartitionId() {
    return getOrCreate().getPartitionId();
  }

  public List<ShuffleServerInfo> getShuffleServerInfos() {
    return getOrCreate().getShuffleServerInfos();
  }

  public int getUncompressLength() {
    return getOrCreate().getUncompressLength();
  }

  public long getFreeMemory() {
    return getOrCreate().getFreeMemory();
  }

  public long getTaskAttemptId() {
    return getOrCreate().getTaskAttemptId();
  }

  @Override
  public String toString() {
    return getOrCreate().toString();
  }

  public void incrRetryCnt() {
    getOrCreate().incrRetryCnt();
  }

  public int getRetryCnt() {
    return getOrCreate().getRetryCnt();
  }

  public void reassignShuffleServers(List<ShuffleServerInfo> replacements) {
    getOrCreate().reassignShuffleServers(replacements);
  }

  public synchronized void copyDataTo(ByteBuf to) {
    getOrCreate().copyDataTo(to);
  }

  public void withCompletionCallback(BlockCompletionCallback callback) {
    getOrCreate().withCompletionCallback(callback);
  }

  public void executeCompletionCallback(boolean isSuccessful) {
    getOrCreate().executeCompletionCallback(isSuccessful);
  }
}
