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

package org.apache.tez.runtime.library.common.sort.buffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.serializer.SerializerFactory;
import org.apache.uniffle.common.serializer.SerializerInstance;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.ChecksumUtils;
import org.apache.uniffle.common.util.ThreadUtils;

public class WriteBufferManager<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(WriteBufferManager.class);
  private long copyTime = 0;
  private long sortTime = 0;
  private long compressTime = 0;
  private final Map<Integer, Integer> partitionToSeqNo = Maps.newHashMap();
  private long uncompressedDataLen = 0;
  private final long maxMemSize;
  private final ExecutorService sendExecutorService;
  private final ShuffleWriteClient shuffleWriteClient;
  private final String appId;
  private final Set<Long> successBlockIds;
  private final Set<Long> failedBlockIds;
  private final ReentrantLock memoryLock = new ReentrantLock();
  private final AtomicLong memoryUsedSize = new AtomicLong(0);
  private final AtomicLong inSendListBytes = new AtomicLong(0);
  private final Condition full = memoryLock.newCondition();
  private final RawComparator<K> comparator;
  private final long maxSegmentSize;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valSerializer;
  private final List<WriteBuffer<K, V>> waitSendBuffers = Lists.newLinkedList();
  private final Map<Integer, WriteBuffer<K, V>> buffers = Maps.newConcurrentMap();
  private final long maxBufferSize;
  private final double memoryThreshold;
  private final double sendThreshold;
  private final int batch;
  private final Optional<Codec> codec;
  private final Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private final Set<Long> allBlockIds = Sets.newConcurrentHashSet();
  // server -> partitionId -> blockIds
  private Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds =
      Maps.newConcurrentMap();
  private final int numMaps;
  private final boolean isMemoryShuffleEnabled;
  private final long sendCheckInterval;
  private final long sendCheckTimeout;
  private final int bitmapSplitNum;
  private final long taskAttemptId;
  private final BlockIdLayout blockIdLayout;
  private TezTaskAttemptID tezTaskAttemptID;
  private final RssConf rssConf;
  private final int shuffleId;
  private final boolean isNeedSorted;
  private final TezCounter mapOutputByteCounter;
  private final TezCounter mapOutputRecordCounter;

  private final boolean useUniffleSerializer;
  private SerializerInstance serializerInstance;

  /** WriteBufferManager */
  public WriteBufferManager(
      TezTaskAttemptID tezTaskAttemptID,
      long maxMemSize,
      String appId,
      long taskAttemptId,
      Set<Long> successBlockIds,
      Set<Long> failedBlockIds,
      ShuffleWriteClient shuffleWriteClient,
      RawComparator<K> comparator,
      long maxSegmentSize,
      Serializer<K> keySerializer,
      Serializer<V> valSerializer,
      long maxBufferSize,
      double memoryThreshold,
      int sendThreadNum,
      double sendThreshold,
      int batch,
      RssConf rssConf,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      int numMaps,
      boolean isMemoryShuffleEnabled,
      long sendCheckInterval,
      long sendCheckTimeout,
      int bitmapSplitNum,
      int shuffleId,
      boolean isNeedSorted,
      TezCounter mapOutputByteCounter,
      TezCounter mapOutputRecordCounter,
      boolean useUniffleSerializer,
      Class<K> keyClass,
      Class<V> valClass) {
    this.tezTaskAttemptID = tezTaskAttemptID;
    this.maxMemSize = maxMemSize;
    this.appId = appId;
    this.taskAttemptId = taskAttemptId;
    this.blockIdLayout = BlockIdLayout.from(rssConf);
    this.successBlockIds = successBlockIds;
    this.failedBlockIds = failedBlockIds;
    this.shuffleWriteClient = shuffleWriteClient;
    // For hive on tez, we use separate serializer and comparator, namely
    // TezBytesWritableSerialization and TezBytesComparator. But in remote
    // merge mode, we use separate serializers, so we should also use
    // separate comparators.
    this.comparator =
        useUniffleSerializer
            ? WritableComparator.get((Class<? extends WritableComparable>) keyClass)
            : comparator;
    this.maxSegmentSize = maxSegmentSize;
    this.keySerializer = keySerializer;
    this.valSerializer = valSerializer;
    this.maxBufferSize = maxBufferSize;
    this.memoryThreshold = memoryThreshold;
    this.sendThreshold = sendThreshold;
    this.batch = batch;
    this.codec = Codec.newInstance(rssConf);
    this.partitionToServers = partitionToServers;
    this.numMaps = numMaps;
    this.isMemoryShuffleEnabled = isMemoryShuffleEnabled;
    this.sendCheckInterval = sendCheckInterval;
    this.sendCheckTimeout = sendCheckTimeout;
    this.bitmapSplitNum = bitmapSplitNum;
    this.rssConf = rssConf;
    this.shuffleId = shuffleId;
    this.isNeedSorted = isNeedSorted;
    this.mapOutputByteCounter = mapOutputByteCounter;
    this.mapOutputRecordCounter = mapOutputRecordCounter;
    this.useUniffleSerializer = useUniffleSerializer;
    if (useUniffleSerializer) {
      SerializerFactory factory = new SerializerFactory(rssConf);
      org.apache.uniffle.common.serializer.Serializer serializer = factory.getSerializer(keyClass);
      this.serializerInstance = serializer.newInstance();
      assert factory.getSerializer(valClass).getClass().equals(serializer.getClass());
    }
    this.sendExecutorService =
        Executors.newFixedThreadPool(sendThreadNum, ThreadUtils.getThreadFactory("send-thread"));
  }

  /** add record */
  public void addRecord(int partitionId, K key, V value) throws InterruptedException, IOException {
    memoryLock.lock();
    try {
      while (memoryUsedSize.get() > maxMemSize) {
        LOG.warn(
            "memoryUsedSize {} is more than {}, inSendListBytes {}",
            memoryUsedSize,
            maxMemSize,
            inSendListBytes);
        full.await();
      }
    } finally {
      memoryLock.unlock();
    }

    // Fail fast if there are some failed blocks.
    checkFailedBlocks();

    if (!buffers.containsKey(partitionId)) {
      WriteBuffer<K, V> sortWriterBuffer =
          new WriteBuffer(
              isNeedSorted,
              partitionId,
              comparator,
              maxSegmentSize,
              useUniffleSerializer,
              keySerializer,
              valSerializer,
              serializerInstance);
      buffers.putIfAbsent(partitionId, sortWriterBuffer);
      waitSendBuffers.add(sortWriterBuffer);
    }
    WriteBuffer<K, V> buffer = buffers.get(partitionId);
    int length = buffer.addRecord(key, value);
    if (length > maxMemSize) {
      throw new RssException("record is too big");
    }

    memoryUsedSize.addAndGet(length);
    if (buffer.getDataLength() > maxBufferSize) {
      if (waitSendBuffers.remove(buffer)) {
        sendBufferToServers(buffer);
      } else {
        LOG.error("waitSendBuffers don't contain buffer {}", buffer);
      }
    }
    if (memoryUsedSize.get() > maxMemSize * memoryThreshold
        && inSendListBytes.get() <= maxMemSize * sendThreshold) {
      sendBuffersToServers();
    }
    mapOutputRecordCounter.increment(1);
    mapOutputByteCounter.increment(length);
  }

  private void sendBufferToServers(WriteBuffer<K, V> buffer) {
    List<ShuffleBlockInfo> shuffleBlocks = new ArrayList<>(1);
    prepareBufferForSend(shuffleBlocks, buffer);
    sendShuffleBlocks(shuffleBlocks);
  }

  void sendBuffersToServers() {
    waitSendBuffers.sort(
        new Comparator<WriteBuffer<K, V>>() {
          @Override
          public int compare(WriteBuffer<K, V> o1, WriteBuffer<K, V> o2) {
            return o2.getDataLength() - o1.getDataLength();
          }
        });

    int sendSize = batch;
    if (batch > waitSendBuffers.size()) {
      sendSize = waitSendBuffers.size();
    }

    Iterator<WriteBuffer<K, V>> iterator = waitSendBuffers.iterator();
    int index = 0;
    List<ShuffleBlockInfo> shuffleBlocks = Lists.newArrayList();
    while (iterator.hasNext() && index < sendSize) {
      WriteBuffer<K, V> buffer = iterator.next();
      prepareBufferForSend(shuffleBlocks, buffer);
      iterator.remove();
      index++;
    }
    sendShuffleBlocks(shuffleBlocks);
  }

  private void prepareBufferForSend(List<ShuffleBlockInfo> shuffleBlocks, WriteBuffer buffer) {
    buffers.remove(buffer.getPartitionId());
    ShuffleBlockInfo block = createShuffleBlock(buffer);
    buffer.clear();
    shuffleBlocks.add(block);
    allBlockIds.add(block.getBlockId());
    block
        .getShuffleServerInfos()
        .forEach(
            shuffleServerInfo -> {
              Map<Integer, Set<Long>> pToBlockIds =
                  serverToPartitionToBlockIds.computeIfAbsent(
                      shuffleServerInfo, k -> Maps.newHashMap());
              pToBlockIds
                  .computeIfAbsent(block.getPartitionId(), v -> Sets.newHashSet())
                  .add(block.getBlockId());
            });
  }

  private void sendShuffleBlocks(List<ShuffleBlockInfo> shuffleBlocks) {
    sendExecutorService.submit(
        new Runnable() {
          @Override
          public void run() {
            long size = 0;
            try {
              for (ShuffleBlockInfo block : shuffleBlocks) {
                size += block.getFreeMemory();
              }
              SendShuffleDataResult result =
                  shuffleWriteClient.sendShuffleData(appId, shuffleBlocks, () -> false);
              successBlockIds.addAll(result.getSuccessBlockIds());
              failedBlockIds.addAll(result.getFailedBlockIds());
            } catch (Throwable t) {
              LOG.warn("send shuffle data exception ", t);
            } finally {
              memoryLock.lock();
              try {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("memoryUsedSize {} decrease {}", memoryUsedSize, size);
                }
                memoryUsedSize.addAndGet(-size);
                inSendListBytes.addAndGet(-size);
                full.signalAll();
              } finally {
                memoryLock.unlock();
              }
            }
          }
        });
  }

  /** wait send finished */
  public void waitSendFinished() {
    while (!waitSendBuffers.isEmpty()) {
      sendBuffersToServers();
    }
    long start = System.currentTimeMillis();
    while (true) {
      checkFailedBlocks();
      allBlockIds.removeAll(successBlockIds);
      if (allBlockIds.isEmpty()) {
        break;
      }
      LOG.info("Wait " + allBlockIds.size() + " blocks sent to shuffle server");
      Uninterruptibles.sleepUninterruptibly(sendCheckInterval, TimeUnit.MILLISECONDS);
      if (System.currentTimeMillis() - start > sendCheckTimeout) {
        String errorMsg =
            "Timeout: failed because "
                + allBlockIds.size()
                + " blocks can't be sent to shuffle server in "
                + sendCheckTimeout
                + " ms.";
        LOG.error(errorMsg);
        throw new RssException(errorMsg);
      }
    }
    long commitDuration = 0;
    if (!isMemoryShuffleEnabled) {
      long s = System.currentTimeMillis();
      sendCommit();
      commitDuration = System.currentTimeMillis() - s;
    }
    start = System.currentTimeMillis();
    TezVertexID tezVertexID = tezTaskAttemptID.getTaskID().getVertexID();
    TezDAGID tezDAGID = tezVertexID.getDAGId();
    LOG.info(
        "tezVertexID is {}, tezDAGID is {}, shuffleId is {}", tezVertexID, tezDAGID, shuffleId);
    shuffleWriteClient.reportShuffleResult(
        serverToPartitionToBlockIds, appId, shuffleId, taskAttemptId, bitmapSplitNum);
    LOG.info(
        "Report shuffle result for task[{}] with bitmapNum[{}] cost {} ms",
        taskAttemptId,
        bitmapSplitNum,
        (System.currentTimeMillis() - start));
    LOG.info(
        "Task uncompressed data length {} compress time cost {} ms, commit time cost {} ms,"
            + " copy time cost {} ms, sort time cost {} ms",
        uncompressedDataLen,
        compressTime,
        commitDuration,
        copyTime,
        sortTime);
  }

  // Check if there are some failed blocks, if true then throw Exception.
  private void checkFailedBlocks() {
    if (failedBlockIds.size() > 0) {
      String errorMsg =
          "Send failed: failed because "
              + failedBlockIds.size()
              + " blocks can't be sent to shuffle server.";
      LOG.error(errorMsg);
      throw new RssException(errorMsg);
    }
  }

  ShuffleBlockInfo createShuffleBlock(WriteBuffer wb) {
    byte[] data = wb.getData();
    copyTime += wb.getCopyTime();
    sortTime += wb.getSortTime();
    int partitionId = wb.getPartitionId();
    final int uncompressLength = data.length;
    long start = System.currentTimeMillis();

    final byte[] compressed =
        useUniffleSerializer ? data : codec.map(c -> c.compress(data)).orElse(data);
    final long crc32 = ChecksumUtils.getCrc32(compressed);
    compressTime += System.currentTimeMillis() - start;
    final long blockId =
        RssTezUtils.getBlockId(partitionId, taskAttemptId, getNextSeqNo(partitionId));
    LOG.info("blockId is {}", blockIdLayout.asBlockId(blockId));
    uncompressedDataLen += data.length;
    // add memory to indicate bytes which will be sent to shuffle server
    inSendListBytes.addAndGet(wb.getDataLength());

    TezVertexID tezVertexID = tezTaskAttemptID.getTaskID().getVertexID();
    TezDAGID tezDAGID = tezVertexID.getDAGId();
    LOG.info(
        "tezVertexID is {}, tezDAGID is {}, shuffleId is {}", tezVertexID, tezDAGID, shuffleId);
    return new ShuffleBlockInfo(
        shuffleId,
        partitionId,
        blockId,
        compressed.length,
        crc32,
        compressed,
        partitionToServers.get(partitionId),
        uncompressLength,
        wb.getDataLength(),
        taskAttemptId);
  }

  protected void sendCommit() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Set<ShuffleServerInfo> serverInfos = Sets.newHashSet();
    for (List<ShuffleServerInfo> serverInfoList : partitionToServers.values()) {
      for (ShuffleServerInfo serverInfo : serverInfoList) {
        serverInfos.add(serverInfo);
      }
    }
    LOG.info("sendCommit  shuffle id is {}", shuffleId);
    Future<Boolean> future =
        executor.submit(
            () -> shuffleWriteClient.sendCommit(serverInfos, appId, shuffleId, numMaps));
    long start = System.currentTimeMillis();
    int currentWait = 200;
    int maxWait = 5000;
    while (!future.isDone()) {
      LOG.info(
          "Wait commit to shuffle server for task["
              + taskAttemptId
              + "] cost "
              + (System.currentTimeMillis() - start)
              + " ms");
      Uninterruptibles.sleepUninterruptibly(currentWait, TimeUnit.MILLISECONDS);
      currentWait = Math.min(currentWait * 2, maxWait);
    }
    try {
      if (!future.get()) {
        throw new RssException("Failed to commit task to shuffle server");
      }
    } catch (InterruptedException ie) {
      LOG.warn("Ignore the InterruptedException which should be caused by internal killed");
    } catch (Exception e) {
      throw new RssException("Exception happened when get commit status", e);
    } finally {
      executor.shutdown();
    }
  }

  List<WriteBuffer<K, V>> getWaitSendBuffers() {
    return waitSendBuffers;
  }

  private int getNextSeqNo(int partitionId) {
    partitionToSeqNo.putIfAbsent(partitionId, 0);
    int seqNo = partitionToSeqNo.get(partitionId);
    partitionToSeqNo.put(partitionId, seqNo + 1);
    return seqNo;
  }

  public void freeAllResources() {
    sendExecutorService.shutdownNow();
    shuffleWriteClient.close();
  }
}
