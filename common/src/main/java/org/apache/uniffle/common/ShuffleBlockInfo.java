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

import org.apache.uniffle.common.util.ByteBufUtils;

public class ShuffleBlockInfo {
  private int partitionId;
  private long blockId;
  private int shuffleId;
  private long taskAttemptId;
  private List<ShuffleServerInfo> shuffleServerInfos;
  private int uncompressLength;
  private long freeMemory;
  private int retryCnt = 0;
  private transient BlockCompletionCallback completionCallback;
  private Function<Integer, List<ShuffleServerInfo>> partitionAssignmentRetrieveFunc;

  protected int length;
  protected long crc;
  protected ByteBuf data;

  public ShuffleBlockInfo(
      int shuffleId,
      int partitionId,
      long blockId,
      int length,
      long crc,
      byte[] data,
      List<ShuffleServerInfo> shuffleServerInfos,
      int uncompressLength,
      long freeMemory,
      long taskAttemptId,
      Function<Integer, List<ShuffleServerInfo>> partitionAssignmentRetrieveFunc) {
    this(
        shuffleId,
        partitionId,
        blockId,
        length,
        crc,
        data,
        shuffleServerInfos,
        uncompressLength,
        freeMemory,
        taskAttemptId);
    this.partitionAssignmentRetrieveFunc = partitionAssignmentRetrieveFunc;
  }

  protected ShuffleBlockInfo(
      int shuffleId,
      int partitionId,
      long blockId,
      List<ShuffleServerInfo> shuffleServerInfos,
      int uncompressLength,
      long freeMemory,
      long taskAttemptId,
      Function<Integer, List<ShuffleServerInfo>> partitionAssignmentRetrieveFunc) {
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.blockId = blockId;
    this.shuffleServerInfos = shuffleServerInfos;
    this.uncompressLength = uncompressLength;
    this.freeMemory = freeMemory;
    this.taskAttemptId = taskAttemptId;
    this.partitionAssignmentRetrieveFunc = partitionAssignmentRetrieveFunc;
  }

  public ShuffleBlockInfo(
      int shuffleId,
      int partitionId,
      long blockId,
      int length,
      long crc,
      byte[] data,
      List<ShuffleServerInfo> shuffleServerInfos,
      int uncompressLength,
      long freeMemory,
      long taskAttemptId) {
    this(
        shuffleId,
        partitionId,
        blockId,
        length,
        crc,
        Unpooled.wrappedBuffer(data),
        shuffleServerInfos,
        uncompressLength,
        freeMemory,
        taskAttemptId);
  }

  public ShuffleBlockInfo(
      int shuffleId,
      int partitionId,
      long blockId,
      int length,
      long crc,
      ByteBuf data,
      List<ShuffleServerInfo> shuffleServerInfos,
      int uncompressLength,
      long freeMemory,
      long taskAttemptId) {
    this.partitionId = partitionId;
    this.blockId = blockId;
    this.length = length;
    this.crc = crc;
    this.data = data;
    this.shuffleId = shuffleId;
    this.shuffleServerInfos = shuffleServerInfos;
    this.uncompressLength = uncompressLength;
    this.freeMemory = freeMemory;
    this.taskAttemptId = taskAttemptId;
  }

  public long getBlockId() {
    return blockId;
  }

  public int getLength() {
    return length;
  }

  // calculate the data size for this block in memory including metadata which are
  // partitionId, blockId, crc, taskAttemptId, length, uncompressLength
  public int getSize() {
    return length + 3 * 8 + 2 * 4;
  }

  public long getCrc() {
    return crc;
  }

  public ByteBuf getData() {
    return data;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public List<ShuffleServerInfo> getShuffleServerInfos() {
    return shuffleServerInfos;
  }

  public int getUncompressLength() {
    return uncompressLength;
  }

  public long getFreeMemory() {
    return freeMemory;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ShuffleBlockInfo:");
    sb.append("shuffleId[" + shuffleId + "],");
    sb.append("partitionId[" + partitionId + "],");
    sb.append("blockId[" + blockId + "],");
    sb.append("length[" + length + "],");
    sb.append("uncompressLength[" + uncompressLength + "],");
    sb.append("crc[" + crc + "],");
    if (shuffleServerInfos != null) {
      sb.append("shuffleServer[");
      for (ShuffleServerInfo ssi : shuffleServerInfos) {
        sb.append(ssi.getId() + ",");
      }
      sb.append("]");
    } else {
      sb.append("shuffleServer is empty");
    }

    return sb.toString();
  }

  public void incrRetryCnt() {
    this.retryCnt += 1;
  }

  public int getRetryCnt() {
    return retryCnt;
  }

  public void reassignShuffleServers(List<ShuffleServerInfo> replacements) {
    this.shuffleServerInfos = replacements;
  }

  public synchronized void copyDataTo(ByteBuf to) {
    ByteBufUtils.copyByteBuf(data, to);
  }

  public void withCompletionCallback(BlockCompletionCallback callback) {
    this.completionCallback = callback;
  }

  public void executeCompletionCallback(boolean isSuccessful) {
    if (completionCallback == null) {
      return;
    }
    completionCallback.onBlockCompletion(this, isSuccessful);
  }

  public boolean isStaleAssignment() {
    if (partitionAssignmentRetrieveFunc == null) {
      return false;
    }
    List<ShuffleServerInfo> latestAssignment = partitionAssignmentRetrieveFunc.apply(partitionId);
    if (latestAssignment == null || shuffleServerInfos == null) {
      return false;
    }
    if (latestAssignment.size() != shuffleServerInfos.size()) {
      return true;
    }
    for (int i = 0; i < latestAssignment.size(); i++) {
      if (!latestAssignment.get(i).getId().equals(shuffleServerInfos.get(i).getId())) {
        return true;
      }
    }
    return false;
  }
}
