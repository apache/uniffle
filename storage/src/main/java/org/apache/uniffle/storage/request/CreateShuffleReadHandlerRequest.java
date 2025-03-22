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

package org.apache.uniffle.storage.request;

import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.IdHelper;
import org.apache.uniffle.storage.handler.impl.PrefetchableClientReadHandler;

public class CreateShuffleReadHandlerRequest {

  private String storageType;
  private String appId;
  private int shuffleId;
  private int partitionId;
  private int indexReadLimit;
  private int partitionNumPerRange;
  private int partitionNum;
  private int readBufferSize;
  private int retryMax;
  private long retryIntervalMax;
  private String storageBasePath;
  private RssBaseConf rssBaseConf;
  private Configuration hadoopConf;
  private List<ShuffleServerInfo> shuffleServerInfoList;
  private Roaring64NavigableMap expectBlockIds;
  private Roaring64NavigableMap processBlockIds;
  private ShuffleDataDistributionType distributionType;
  private Roaring64NavigableMap expectTaskIds;
  private boolean expectedTaskIdsBitmapFilterEnable;
  private boolean offHeapEnabled;
  private RssConf clientConf;

  private IdHelper idHelper;

  private ClientType clientType;

  public CreateShuffleReadHandlerRequest() {}

  public RssBaseConf getRssBaseConf() {
    return rssBaseConf;
  }

  public void setRssBaseConf(RssBaseConf rssBaseConf) {
    this.rssBaseConf = rssBaseConf;
  }

  public String getStorageType() {
    return storageType;
  }

  public void setStorageType(String storageType) {
    this.storageType = storageType;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public void setShuffleId(int shuffleId) {
    this.shuffleId = shuffleId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public int getIndexReadLimit() {
    return indexReadLimit;
  }

  public void setIndexReadLimit(int indexReadLimit) {
    this.indexReadLimit = indexReadLimit;
  }

  public int getPartitionNumPerRange() {
    return partitionNumPerRange;
  }

  public void setPartitionNumPerRange(int partitionNumPerRange) {
    this.partitionNumPerRange = partitionNumPerRange;
  }

  public int getPartitionNum() {
    return partitionNum;
  }

  public void setPartitionNum(int partitionNum) {
    this.partitionNum = partitionNum;
  }

  public int getReadBufferSize() {
    return readBufferSize;
  }

  public void setReadBufferSize(int readBufferSize) {
    this.readBufferSize = readBufferSize;
  }

  public int getRetryMax() {
    return retryMax;
  }

  public void setRetryMax(int retryMax) {
    this.retryMax = retryMax;
  }

  public long getRetryIntervalMax() {
    return retryIntervalMax;
  }

  public void setRetryIntervalMax(long retryIntervalMax) {
    this.retryIntervalMax = retryIntervalMax;
  }

  public String getStorageBasePath() {
    return storageBasePath;
  }

  public void setStorageBasePath(String storageBasePath) {
    this.storageBasePath = storageBasePath;
  }

  public List<ShuffleServerInfo> getShuffleServerInfoList() {
    return shuffleServerInfoList;
  }

  public void setShuffleServerInfoList(List<ShuffleServerInfo> shuffleServerInfoList) {
    this.shuffleServerInfoList = shuffleServerInfoList;
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public void setHadoopConf(Configuration hadoopConf) {
    this.hadoopConf = hadoopConf;
  }

  public void setExpectBlockIds(Roaring64NavigableMap expectBlockIds) {
    this.expectBlockIds = expectBlockIds;
  }

  public Roaring64NavigableMap getExpectBlockIds() {
    return expectBlockIds;
  }

  public void setProcessBlockIds(Roaring64NavigableMap processBlockIds) {
    this.processBlockIds = processBlockIds;
  }

  public Roaring64NavigableMap getProcessBlockIds() {
    return processBlockIds;
  }

  public ShuffleDataDistributionType getDistributionType() {
    return distributionType;
  }

  public void setDistributionType(ShuffleDataDistributionType distributionType) {
    this.distributionType = distributionType;
  }

  public Roaring64NavigableMap getExpectTaskIds() {
    return expectTaskIds;
  }

  public void setExpectTaskIds(Roaring64NavigableMap expectTaskIds) {
    this.expectTaskIds = expectTaskIds;
  }

  public boolean isExpectedTaskIdsBitmapFilterEnable() {
    return expectedTaskIdsBitmapFilterEnable;
  }

  public void useExpectedTaskIdsBitmapFilter() {
    this.expectedTaskIdsBitmapFilterEnable = true;
  }

  public IdHelper getIdHelper() {
    return idHelper;
  }

  public void setIdHelper(IdHelper idHelper) {
    this.idHelper = idHelper;
  }

  public void enableOffHeap() {
    this.offHeapEnabled = true;
  }

  public boolean isOffHeapEnabled() {
    return offHeapEnabled;
  }

  public RssConf getClientConf() {
    return clientConf;
  }

  public void setClientConf(RssConf clientConf) {
    this.clientConf = clientConf;
  }

  public ClientType getClientType() {
    return clientType;
  }

  public void setClientType(ClientType clientType) {
    this.clientType = clientType;
  }

  public Optional<PrefetchableClientReadHandler.PrefetchOption> getPrefetchOption() {
    if (clientConf.get(RssClientConf.RSS_CLIENT_PREFETCH_ENABLED)) {
      return Optional.of(
          new PrefetchableClientReadHandler.PrefetchOption(
              clientConf.get(RssClientConf.RSS_CLIENT_PREFETCH_CAPACITY),
              clientConf.get(RssClientConf.READ_CLIENT_PREFETCH_TIMEOUT_SEC)));
    } else {
      return Optional.empty();
    }
  }
}
