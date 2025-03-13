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

package org.apache.spark.shuffle.handle;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import org.apache.spark.shuffle.handle.split.PartitionSplitInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.proto.RssProtos;

public class StageAttemptShuffleHandleInfo extends ShuffleHandleInfoBase {
  private static final long serialVersionUID = 0L;
  private static final Logger LOGGER = LoggerFactory.getLogger(StageAttemptShuffleHandleInfo.class);

  private ShuffleHandleInfo current;
  /** When Stage retry occurs, record the Shuffle Server of the previous Stage. */
  private LinkedList<ShuffleHandleInfo> historyHandles;

  public StageAttemptShuffleHandleInfo(
      int shuffleId, RemoteStorageInfo remoteStorage, ShuffleHandleInfo shuffleServerInfo) {
    super(shuffleId, remoteStorage);
    this.current = shuffleServerInfo;
    this.historyHandles = Lists.newLinkedList();
  }

  public StageAttemptShuffleHandleInfo(
      int shuffleId,
      RemoteStorageInfo remoteStorage,
      ShuffleHandleInfo currentShuffleServerInfo,
      LinkedList<ShuffleHandleInfo> historyHandles) {
    super(shuffleId, remoteStorage);
    this.current = currentShuffleServerInfo;
    this.historyHandles = historyHandles;
  }

  @Override
  public Set<ShuffleServerInfo> getServers() {
    return current.getServers();
  }

  @Override
  public Map<Integer, List<ShuffleServerInfo>> getAvailablePartitionServersForWriter() {
    return current.getAvailablePartitionServersForWriter();
  }

  @Override
  public Map<Integer, List<ShuffleServerInfo>> getAllPartitionServersForReader() {
    return current.getAllPartitionServersForReader();
  }

  @Override
  public PartitionDataReplicaRequirementTracking createPartitionReplicaTracking() {
    return current.createPartitionReplicaTracking();
  }

  @Override
  public PartitionSplitInfo getPartitionSplitInfo(int partitionId) {
    return current.getPartitionSplitInfo(partitionId);
  }

  /**
   * When a Stage retry occurs, replace the current shuffleHandleInfo and record the historical
   * shuffleHandleInfo.
   */
  public void replaceCurrentShuffleHandleInfo(ShuffleHandleInfo shuffleHandleInfo) {
    this.historyHandles.add(current);
    this.current = shuffleHandleInfo;
  }

  public ShuffleHandleInfo getCurrent() {
    return current;
  }

  public LinkedList<ShuffleHandleInfo> getHistoryHandles() {
    return historyHandles;
  }

  public static RssProtos.StageAttemptShuffleHandleInfo toProto(
      StageAttemptShuffleHandleInfo handleInfo) {
    LinkedList<RssProtos.MutableShuffleHandleInfo> mutableShuffleHandleInfoLinkedList =
        Lists.newLinkedList();
    RssProtos.MutableShuffleHandleInfo currentMutableShuffleHandleInfo =
        MutableShuffleHandleInfo.toProto((MutableShuffleHandleInfo) handleInfo.getCurrent());
    for (ShuffleHandleInfo historyHandle : handleInfo.getHistoryHandles()) {
      mutableShuffleHandleInfoLinkedList.add(
          MutableShuffleHandleInfo.toProto((MutableShuffleHandleInfo) historyHandle));
    }
    RssProtos.StageAttemptShuffleHandleInfo handleProto =
        RssProtos.StageAttemptShuffleHandleInfo.newBuilder()
            .setCurrentMutableShuffleHandleInfo(currentMutableShuffleHandleInfo)
            .addAllHistoryMutableShuffleHandleInfo(mutableShuffleHandleInfoLinkedList)
            .build();
    return handleProto;
  }

  public static StageAttemptShuffleHandleInfo fromProto(
      RssProtos.StageAttemptShuffleHandleInfo handleProto) {
    if (handleProto == null) {
      return null;
    }

    MutableShuffleHandleInfo mutableShuffleHandleInfo =
        MutableShuffleHandleInfo.fromProto(handleProto.getCurrentMutableShuffleHandleInfo());
    List<RssProtos.MutableShuffleHandleInfo> historyMutableShuffleHandleInfoList =
        handleProto.getHistoryMutableShuffleHandleInfoList();
    LinkedList<ShuffleHandleInfo> historyHandles = Lists.newLinkedList();
    for (RssProtos.MutableShuffleHandleInfo shuffleHandleInfo :
        historyMutableShuffleHandleInfoList) {
      historyHandles.add(MutableShuffleHandleInfo.fromProto(shuffleHandleInfo));
    }

    StageAttemptShuffleHandleInfo stageAttemptShuffleHandleInfo =
        new StageAttemptShuffleHandleInfo(
            mutableShuffleHandleInfo.shuffleId,
            mutableShuffleHandleInfo.remoteStorage,
            mutableShuffleHandleInfo,
            historyHandles);
    return stageAttemptShuffleHandleInfo;
  }
}
