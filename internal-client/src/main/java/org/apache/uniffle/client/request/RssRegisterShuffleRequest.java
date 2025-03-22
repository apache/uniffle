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

package org.apache.uniffle.client.request;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;

import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.proto.RssProtos.MergeContext;

public class RssRegisterShuffleRequest {

  private String appId;
  private int shuffleId;
  private List<PartitionRange> partitionRanges;
  private RemoteStorageInfo remoteStorageInfo;
  private String user;
  private ShuffleDataDistributionType dataDistributionType;
  private int maxConcurrencyPerPartitionToWrite;
  private int stageAttemptNumber;

  private final MergeContext mergeContext;
  private Map<String, String> properties;

  @VisibleForTesting
  public RssRegisterShuffleRequest(
      String appId,
      int shuffleId,
      List<PartitionRange> partitionRanges,
      RemoteStorageInfo remoteStorageInfo,
      String user,
      ShuffleDataDistributionType dataDistributionType,
      int maxConcurrencyPerPartitionToWrite) {
    this(
        appId,
        shuffleId,
        partitionRanges,
        remoteStorageInfo,
        user,
        dataDistributionType,
        maxConcurrencyPerPartitionToWrite,
        0,
        null,
        Collections.emptyMap());
  }

  public RssRegisterShuffleRequest(
      String appId,
      int shuffleId,
      List<PartitionRange> partitionRanges,
      RemoteStorageInfo remoteStorageInfo,
      String user,
      ShuffleDataDistributionType dataDistributionType,
      int maxConcurrencyPerPartitionToWrite,
      int stageAttemptNumber,
      MergeContext mergeContext,
      Map<String, String> properties) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionRanges = partitionRanges;
    this.remoteStorageInfo = remoteStorageInfo;
    this.user = user;
    this.dataDistributionType = dataDistributionType;
    this.maxConcurrencyPerPartitionToWrite = maxConcurrencyPerPartitionToWrite;
    this.stageAttemptNumber = stageAttemptNumber;
    this.mergeContext = mergeContext;
    this.properties = properties;
  }

  @VisibleForTesting
  public RssRegisterShuffleRequest(
      String appId,
      int shuffleId,
      List<PartitionRange> partitionRanges,
      RemoteStorageInfo remoteStorageInfo,
      String user,
      ShuffleDataDistributionType dataDistributionType) {
    this(
        appId,
        shuffleId,
        partitionRanges,
        remoteStorageInfo,
        user,
        dataDistributionType,
        RssClientConf.MAX_CONCURRENCY_PER_PARTITION_TO_WRITE.defaultValue(),
        0,
        null,
        Collections.emptyMap());
  }

  public RssRegisterShuffleRequest(
      String appId, int shuffleId, List<PartitionRange> partitionRanges, String remoteStoragePath) {
    this(
        appId,
        shuffleId,
        partitionRanges,
        new RemoteStorageInfo(remoteStoragePath),
        StringUtils.EMPTY,
        ShuffleDataDistributionType.NORMAL,
        RssClientConf.MAX_CONCURRENCY_PER_PARTITION_TO_WRITE.defaultValue(),
        0,
        null,
        Collections.emptyMap());
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public List<PartitionRange> getPartitionRanges() {
    return partitionRanges;
  }

  public RemoteStorageInfo getRemoteStorageInfo() {
    return remoteStorageInfo;
  }

  public String getUser() {
    return user;
  }

  public ShuffleDataDistributionType getDataDistributionType() {
    return dataDistributionType;
  }

  public int getMaxConcurrencyPerPartitionToWrite() {
    return maxConcurrencyPerPartitionToWrite;
  }

  public int getStageAttemptNumber() {
    return stageAttemptNumber;
  }

  public MergeContext getMergeContext() {
    return mergeContext;
  }

  public Map<String, String> getProperties() {
    return properties;
  }
}
