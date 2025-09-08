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

package org.apache.uniffle.client.factory;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.impl.ShuffleReadClientImpl;
import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.IdHelper;
import org.apache.uniffle.storage.handler.impl.ShuffleServerReadCostTracker;

public class ShuffleClientFactory {

  private static final ShuffleClientFactory INSTANCE = new ShuffleClientFactory();

  public static ShuffleClientFactory getInstance() {
    return INSTANCE;
  }

  public ShuffleWriteClient createShuffleWriteClient(WriteClientBuilder builder) {
    if (builder.isReplicaSkipEnabled() && builder.getReplica() > builder.getReplicaWrite()) {
      builder.retryMax(builder.getRetryMax() / 2);
    }
    return builder.build();
  }

  public ShuffleReadClient createShuffleReadClient(ReadClientBuilder builder) {
    return builder.build();
  }

  public static class WriteClientBuilder<T extends WriteClientBuilder> {
    private String clientType;
    private int retryMax;
    private long retryIntervalMax;
    private int heartBeatThreadNum;
    private int replica;
    private int replicaWrite;
    private int replicaRead;
    private boolean replicaSkipEnabled;
    private int dataTransferPoolSize;
    private int dataCommitPoolSize;
    private int unregisterThreadPoolSize;
    private int unregisterTimeSec;
    private int unregisterRequestTimeSec;
    private RssConf rssConf;

    public String getClientType() {
      return clientType;
    }

    public int getRetryMax() {
      return retryMax;
    }

    public long getRetryIntervalMax() {
      return retryIntervalMax;
    }

    public int getHeartBeatThreadNum() {
      return heartBeatThreadNum;
    }

    public int getReplica() {
      return replica;
    }

    public int getReplicaWrite() {
      return replicaWrite;
    }

    public int getReplicaRead() {
      return replicaRead;
    }

    public boolean isReplicaSkipEnabled() {
      return replicaSkipEnabled;
    }

    public int getDataTransferPoolSize() {
      return dataTransferPoolSize;
    }

    public int getDataCommitPoolSize() {
      return dataCommitPoolSize;
    }

    public int getUnregisterThreadPoolSize() {
      return unregisterThreadPoolSize;
    }

    public int getUnregisterTimeSec() {
      return unregisterTimeSec;
    }

    public int getUnregisterRequestTimeSec() {
      return unregisterRequestTimeSec;
    }

    public RssConf getRssConf() {
      return rssConf;
    }

    protected T self() {
      return (T) this;
    }

    public T clientType(String clientType) {
      this.clientType = clientType;
      return self();
    }

    public T retryMax(int retryMax) {
      this.retryMax = retryMax;
      return self();
    }

    public T retryIntervalMax(long retryIntervalMax) {
      this.retryIntervalMax = retryIntervalMax;
      return self();
    }

    public T heartBeatThreadNum(int heartBeatThreadNum) {
      this.heartBeatThreadNum = heartBeatThreadNum;
      return self();
    }

    public T replica(int replica) {
      this.replica = replica;
      return self();
    }

    public T replicaWrite(int replicaWrite) {
      this.replicaWrite = replicaWrite;
      return self();
    }

    public T replicaRead(int replicaRead) {
      this.replicaRead = replicaRead;
      return self();
    }

    public T replicaSkipEnabled(boolean replicaSkipEnabled) {
      this.replicaSkipEnabled = replicaSkipEnabled;
      return self();
    }

    public T dataTransferPoolSize(int dataTransferPoolSize) {
      this.dataTransferPoolSize = dataTransferPoolSize;
      return self();
    }

    public T dataCommitPoolSize(int dataCommitPoolSize) {
      this.dataCommitPoolSize = dataCommitPoolSize;
      return self();
    }

    public T unregisterThreadPoolSize(int unregisterThreadPoolSize) {
      this.unregisterThreadPoolSize = unregisterThreadPoolSize;
      return self();
    }

    public T unregisterTimeSec(int unregisterTimeSec) {
      this.unregisterTimeSec = unregisterTimeSec;
      return self();
    }

    public T unregisterRequestTimeSec(int unregisterRequestTimeSec) {
      this.unregisterRequestTimeSec = unregisterRequestTimeSec;
      return self();
    }

    public T rssConf(RssConf rssConf) {
      this.rssConf = rssConf;
      return self();
    }

    public ShuffleWriteClientImpl build() {
      return new ShuffleWriteClientImpl(this);
    }
  }

  public static class ReadClientBuilder {
    private String appId;
    private int shuffleId;
    private int partitionId;
    private String basePath;
    private int partitionNumPerRange;
    private int partitionNum;
    private Roaring64NavigableMap blockIdBitmap;
    private Roaring64NavigableMap taskIdBitmap;
    private List<ShuffleServerInfo> shuffleServerInfoList;
    private Configuration hadoopConf;
    private IdHelper idHelper;
    private ShuffleDataDistributionType shuffleDataDistributionType;
    private boolean expectedTaskIdsBitmapFilterEnable;
    private RssConf rssConf;
    private boolean offHeapEnable;
    private String storageType;
    private int indexReadLimit;
    private long readBufferSize;
    private ClientType clientType;
    private int retryMax;
    private long retryIntervalMax;
    private ShuffleServerReadCostTracker readCostTracker;
    private long taskAttemptId;

    public ReadClientBuilder taskAttemptId(long taskAttemptId) {
      this.taskAttemptId = taskAttemptId;
      return this;
    }

    private boolean overlappingDecompressionEnabled;
    private int overlappingDecompressionThreadNum;
    private Codec codec;

    public ReadClientBuilder overlappingDecompressionEnabled(
        boolean overlappingDecompressionEnabled) {
      this.overlappingDecompressionEnabled = overlappingDecompressionEnabled;
      return this;
    }

    public ReadClientBuilder overlappingDecompressionThreadNum(
        int overlappingDecompressionThreadNum) {
      this.overlappingDecompressionThreadNum = overlappingDecompressionThreadNum;
      return this;
    }

    public ReadClientBuilder codec(Codec codec) {
      this.codec = codec;
      return this;
    }

    public ReadClientBuilder readCostTracker(ShuffleServerReadCostTracker tracker) {
      this.readCostTracker = tracker;
      return this;
    }

    public ReadClientBuilder appId(String appId) {
      this.appId = appId;
      return this;
    }

    public ReadClientBuilder shuffleId(int shuffleId) {
      this.shuffleId = shuffleId;
      return this;
    }

    public ReadClientBuilder partitionId(int partitionId) {
      this.partitionId = partitionId;
      return this;
    }

    public ReadClientBuilder basePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    public ReadClientBuilder partitionNumPerRange(int partitionNumPerRange) {
      this.partitionNumPerRange = partitionNumPerRange;
      return this;
    }

    public ReadClientBuilder partitionNum(int partitionNum) {
      this.partitionNum = partitionNum;
      return this;
    }

    public ReadClientBuilder blockIdBitmap(Roaring64NavigableMap blockIdBitmap) {
      this.blockIdBitmap = blockIdBitmap;
      return this;
    }

    public ReadClientBuilder taskIdBitmap(Roaring64NavigableMap taskIdBitmap) {
      this.taskIdBitmap = taskIdBitmap;
      return this;
    }

    public ReadClientBuilder shuffleServerInfoList(List<ShuffleServerInfo> shuffleServerInfoList) {
      this.shuffleServerInfoList = shuffleServerInfoList;
      return this;
    }

    public ReadClientBuilder hadoopConf(Configuration hadoopConf) {
      this.hadoopConf = hadoopConf;
      return this;
    }

    public ReadClientBuilder idHelper(IdHelper idHelper) {
      this.idHelper = idHelper;
      return this;
    }

    public ReadClientBuilder shuffleDataDistributionType(
        ShuffleDataDistributionType shuffleDataDistributionType) {
      this.shuffleDataDistributionType = shuffleDataDistributionType;
      return this;
    }

    public ReadClientBuilder expectedTaskIdsBitmapFilterEnable(
        boolean expectedTaskIdsBitmapFilterEnable) {
      this.expectedTaskIdsBitmapFilterEnable = expectedTaskIdsBitmapFilterEnable;
      return this;
    }

    public ReadClientBuilder rssConf(RssConf rssConf) {
      this.rssConf = rssConf;
      return this;
    }

    public ReadClientBuilder offHeapEnable(boolean offHeapEnable) {
      this.offHeapEnable = offHeapEnable;
      return this;
    }

    public ReadClientBuilder storageType(String storageType) {
      this.storageType = storageType;
      return this;
    }

    public ReadClientBuilder indexReadLimit(int indexReadLimit) {
      this.indexReadLimit = indexReadLimit;
      return this;
    }

    public ReadClientBuilder readBufferSize(long readBufferSize) {
      this.readBufferSize = readBufferSize;
      return this;
    }

    public ReadClientBuilder clientType(ClientType clientType) {
      this.clientType = clientType;
      return this;
    }

    public ReadClientBuilder retryMax(int retryMax) {
      this.retryMax = retryMax;
      return this;
    }

    public ReadClientBuilder retryIntervalMax(long retryIntervalMax) {
      this.retryIntervalMax = retryIntervalMax;
      return this;
    }

    public ReadClientBuilder() {}

    public ShuffleServerReadCostTracker getReadCostTracker() {
      return readCostTracker;
    }

    public String getAppId() {
      return appId;
    }

    public int getShuffleId() {
      return shuffleId;
    }

    public int getPartitionId() {
      return partitionId;
    }

    public int getPartitionNumPerRange() {
      return partitionNumPerRange;
    }

    public int getPartitionNum() {
      return partitionNum;
    }

    public String getBasePath() {
      return basePath;
    }

    public Roaring64NavigableMap getBlockIdBitmap() {
      return blockIdBitmap;
    }

    public Roaring64NavigableMap getTaskIdBitmap() {
      return taskIdBitmap;
    }

    public List<ShuffleServerInfo> getShuffleServerInfoList() {
      return shuffleServerInfoList;
    }

    public Configuration getHadoopConf() {
      return hadoopConf;
    }

    public IdHelper getIdHelper() {
      return idHelper;
    }

    public ShuffleDataDistributionType getShuffleDataDistributionType() {
      return shuffleDataDistributionType;
    }

    public boolean isExpectedTaskIdsBitmapFilterEnable() {
      return expectedTaskIdsBitmapFilterEnable;
    }

    public RssConf getRssConf() {
      return rssConf;
    }

    public boolean isOffHeapEnable() {
      return offHeapEnable;
    }

    public String getStorageType() {
      return storageType;
    }

    public int getIndexReadLimit() {
      return indexReadLimit;
    }

    public long getReadBufferSize() {
      return readBufferSize;
    }

    public ClientType getClientType() {
      return clientType;
    }

    public int getRetryMax() {
      return retryMax;
    }

    public long getRetryIntervalMax() {
      return retryIntervalMax;
    }

    public boolean isOverlappingDecompressionEnabled() {
      return overlappingDecompressionEnabled;
    }

    public int getOverlappingDecompressionThreadNum() {
      return overlappingDecompressionThreadNum;
    }

    public Codec getCodec() {
      return codec;
    }

    public long getTaskAttemptId() {
      return taskAttemptId;
    }

    public ShuffleReadClientImpl build() {
      return new ShuffleReadClientImpl(this);
    }
  }

  public static WriteClientBuilder newWriteBuilder() {
    return new WriteClientBuilder();
  }

  public static ReadClientBuilder newReadBuilder() {
    return new ReadClientBuilder();
  }
}
