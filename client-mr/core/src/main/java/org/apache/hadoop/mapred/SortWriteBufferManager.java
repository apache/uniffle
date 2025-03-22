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

package org.apache.hadoop.mapred;

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
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.RssMRUtils;
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
import org.apache.uniffle.common.util.ChecksumUtils;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.ThreadUtils;

public class SortWriteBufferManager<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(SortWriteBufferManager.class);

  private final long maxMemSize;
  private final Map<Integer, SortWriteBuffer<K, V>> buffers = JavaUtils.newConcurrentMap();
  private final Map<Integer, Integer> partitionToSeqNo = Maps.newHashMap();
  private final Counters.Counter mapOutputByteCounter;
  private final Counters.Counter mapOutputRecordCounter;
  private long uncompressedDataLen = 0;
  private long compressTime = 0;
  private final long taskAttemptId;
  private final AtomicLong memoryUsedSize = new AtomicLong(0);
  private final int batch;
  private final AtomicLong inSendListBytes = new AtomicLong(0);
  private final Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private final double memoryThreshold;
  private final double sendThreshold;
  private final ReentrantLock memoryLock = new ReentrantLock();
  private final Condition full = memoryLock.newCondition();
  private Serializer<K> keySerializer;
  private Serializer<V> valSerializer;
  private final RawComparator<K> comparator;
  private final Set<Long> successBlockIds;
  private final Set<Long> failedBlockIds;
  private final List<SortWriteBuffer<K, V>> waitSendBuffers = Lists.newLinkedList();
  private final String appId;
  private final ShuffleWriteClient shuffleWriteClient;
  private final long sendCheckTimeout;
  private final long sendCheckInterval;
  private final Set<Long> allBlockIds = Sets.newConcurrentHashSet();
  private final int bitmapSplitNum;
  // server -> partitionId -> blockIds
  private Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds =
      Maps.newHashMap();
  private final long maxSegmentSize;
  private final boolean isMemoryShuffleEnabled;
  private final int numMaps;
  private long copyTime = 0;
  private long sortTime = 0;
  private final long maxBufferSize;
  private final ExecutorService sendExecutorService;
  private final RssConf rssConf;
  private final Optional<Codec> codec;
  private final Task.CombinerRunner<K, V> combinerRunner;
  private final boolean useUniffleSerializer;
  private SerializerInstance serializerInstance;

  public SortWriteBufferManager(
      long maxMemSize,
      long taskAttemptId,
      int batch,
      Class<K> keyClass,
      Class<V> valClass,
      JobConf mrJobConf,
      RawComparator<K> comparator,
      double memoryThreshold,
      String appId,
      ShuffleWriteClient shuffleWriteClient,
      long sendCheckInterval,
      long sendCheckTimeout,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      Set<Long> successBlockIds,
      Set<Long> failedBlockIds,
      Counters.Counter mapOutputByteCounter,
      Counters.Counter mapOutputRecordCounter,
      int bitmapSplitNum,
      long maxSegmentSize,
      int numMaps,
      boolean isMemoryShuffleEnabled,
      int sendThreadNum,
      double sendThreshold,
      long maxBufferSize,
      RssConf rssConf,
      Task.CombinerRunner<K, V> combinerRunner,
      boolean useUniffleSerializer) {
    this.maxMemSize = maxMemSize;
    this.taskAttemptId = taskAttemptId;
    this.batch = batch;
    this.comparator = comparator;
    this.memoryThreshold = memoryThreshold;
    this.appId = appId;
    this.shuffleWriteClient = shuffleWriteClient;
    this.sendCheckInterval = sendCheckInterval;
    this.sendCheckTimeout = sendCheckTimeout;
    this.partitionToServers = partitionToServers;
    this.successBlockIds = successBlockIds;
    this.failedBlockIds = failedBlockIds;
    this.mapOutputByteCounter = mapOutputByteCounter;
    this.mapOutputRecordCounter = mapOutputRecordCounter;
    this.bitmapSplitNum = bitmapSplitNum;
    this.maxSegmentSize = maxSegmentSize;
    this.numMaps = numMaps;
    this.isMemoryShuffleEnabled = isMemoryShuffleEnabled;
    this.sendThreshold = sendThreshold;
    this.maxBufferSize = maxBufferSize;
    this.sendExecutorService = ThreadUtils.getDaemonFixedThreadPool(sendThreadNum, "send-thread");
    this.rssConf = rssConf;
    this.codec = Codec.newInstance(rssConf);
    this.combinerRunner = combinerRunner;
    this.useUniffleSerializer = useUniffleSerializer;
    if (useUniffleSerializer) {
      SerializerFactory factory = new SerializerFactory(rssConf);
      org.apache.uniffle.common.serializer.Serializer serializer = factory.getSerializer(keyClass);
      this.serializerInstance = serializer.newInstance();
      assert factory.getSerializer(valClass).getClass().equals(serializer.getClass());
    } else {
      SerializationFactory serializationFactory = new SerializationFactory(mrJobConf);
      this.keySerializer = serializationFactory.getSerializer(keyClass);
      this.valSerializer = serializationFactory.getSerializer(valClass);
    }
  }

  // todo: Single Buffer should also have its size limit
  public void addRecord(int partitionId, K key, V value) throws IOException, InterruptedException {
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

    buffers.computeIfAbsent(
        partitionId,
        k -> {
          SortWriteBuffer<K, V> sortWriterBuffer =
              new SortWriteBuffer(
                  partitionId,
                  comparator,
                  maxSegmentSize,
                  useUniffleSerializer,
                  keySerializer,
                  valSerializer,
                  serializerInstance);
          waitSendBuffers.add(sortWriterBuffer);
          return sortWriterBuffer;
        });

    SortWriteBuffer<K, V> buffer = buffers.get(partitionId);
    long length = buffer.addRecord(key, value);
    if (length > maxMemSize) {
      throw new RssException("record is too big");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("memoryUsedSize {} increase {}", memoryUsedSize, length);
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

  private void sendBufferToServers(SortWriteBuffer<K, V> buffer) {
    List<ShuffleBlockInfo> shuffleBlocks = new ArrayList<>(1);
    prepareBufferForSend(shuffleBlocks, buffer);
    sendShuffleBlocks(shuffleBlocks);
  }

  // Only for test
  void sendBuffersToServers() {
    waitSendBuffers.sort(
        new Comparator<SortWriteBuffer<K, V>>() {
          @Override
          public int compare(SortWriteBuffer<K, V> o1, SortWriteBuffer<K, V> o2) {
            return o2.getDataLength() - o1.getDataLength();
          }
        });
    int sendSize = Math.min(batch, waitSendBuffers.size());
    Iterator<SortWriteBuffer<K, V>> iterator = waitSendBuffers.iterator();
    int index = 0;
    List<ShuffleBlockInfo> shuffleBlocks = Lists.newArrayList();
    while (iterator.hasNext() && index < sendSize) {
      SortWriteBuffer buffer = iterator.next();
      prepareBufferForSend(shuffleBlocks, buffer);
      iterator.remove();
      index++;
    }
    sendShuffleBlocks(shuffleBlocks);
  }

  private void prepareBufferForSend(List<ShuffleBlockInfo> shuffleBlocks, SortWriteBuffer buffer) {
    buffers.remove(buffer.getPartitionId());
    buffer.sort();
    ShuffleBlockInfo block;
    if (combinerRunner != null) {
      try {
        buffer = combineBuffer(buffer);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Successfully finished combining.");
        }
      } catch (Exception e) {
        LOG.error("Error occurred while combining in Map:", e);
      }
    }
    block = createShuffleBlock(buffer);
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

  public SortWriteBuffer<K, V> combineBuffer(SortWriteBuffer<K, V> buffer)
      throws IOException, InterruptedException, ClassNotFoundException {
    RawKeyValueIterator kvIterator = new SortWriteBuffer.SortBufferIterator<>(buffer);

    RssCombineOutputCollector<K, V> combineCollector = new RssCombineOutputCollector<>();

    SortWriteBuffer<K, V> newBuffer =
        new SortWriteBuffer<>(
            buffer.getPartitionId(),
            comparator,
            maxSegmentSize,
            useUniffleSerializer,
            keySerializer,
            valSerializer,
            serializerInstance);

    combineCollector.setWriter(newBuffer);
    combinerRunner.combine(kvIterator, combineCollector);
    return newBuffer;
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

  public void waitSendFinished() {
    while (!waitSendBuffers.isEmpty()) {
      sendBuffersToServers();
    }
    long start = System.currentTimeMillis();
    while (true) {
      checkFailedBlocks();
      // remove blockIds which was sent successfully, if there has none left, all data are sent
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
    shuffleWriteClient.reportShuffleResult(
        serverToPartitionToBlockIds, appId, 0, taskAttemptId, bitmapSplitNum);
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

  // if failed when send data to shuffle server, mark task as failed and throw exception.
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

  // transform records to shuffleBlock
  ShuffleBlockInfo createShuffleBlock(SortWriteBuffer wb) {
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
        RssMRUtils.getBlockId(partitionId, taskAttemptId, getNextSeqNo(partitionId));
    uncompressedDataLen += data.length;
    // add memory to indicate bytes which will be sent to shuffle server
    inSendListBytes.addAndGet(wb.getDataLength());
    return new ShuffleBlockInfo(
        0,
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
    for (List<ShuffleServerInfo> serverInfoLists : partitionToServers.values()) {
      for (ShuffleServerInfo serverInfo : serverInfoLists) {
        serverInfos.add(serverInfo);
      }
    }
    Future<Boolean> future =
        executor.submit(() -> shuffleWriteClient.sendCommit(serverInfos, appId, 0, numMaps));
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
      // check if commit/finish rpc is successful
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

  // Only for test
  List<SortWriteBuffer<K, V>> getWaitSendBuffers() {
    return waitSendBuffers;
  }

  // it's run in single thread, and is not thread safe
  private int getNextSeqNo(int partitionId) {
    partitionToSeqNo.computeIfAbsent(partitionId, key -> 0);
    int seqNo = partitionToSeqNo.get(partitionId);
    partitionToSeqNo.put(partitionId, seqNo + 1);
    return seqNo;
  }

  public void freeAllResources() {
    sendExecutorService.shutdownNow();
  }
}
