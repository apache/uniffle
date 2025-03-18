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

package org.apache.spark.shuffle.writer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import scala.reflect.ClassTag$;
import scala.reflect.ManifestFactory$;

import com.clearspring.analytics.util.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.RssSparkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.ChecksumUtils;

public class WriteBufferManager extends MemoryConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(WriteBufferManager.class);
  protected int bufferSize;
  protected long spillSize;
  // allocated bytes from executor memory
  private AtomicLong allocatedBytes = new AtomicLong(0);
  // bytes of shuffle data in memory
  protected final AtomicLong usedBytes = new AtomicLong(0);
  // bytes of shuffle data which is in send list
  protected final AtomicLong inSendListBytes = new AtomicLong(0);
  /** An atomic counter used to keep track of the number of records */
  protected final AtomicLong recordCounter = new AtomicLong(0);
  /** An atomic counter used to keep track of the number of blocks */
  protected AtomicLong blockCounter = new AtomicLong(0);
  // it's part of blockId
  private Map<Integer, AtomicInteger> partitionToSeqNo = Maps.newHashMap();
  private long askExecutorMemory;
  protected final int shuffleId;
  private String taskId;
  protected final long taskAttemptId;
  private SerializerInstance instance;
  protected ShuffleWriteMetrics shuffleWriteMetrics;
  protected final RssConf rssConf;
  // cache partition -> records
  protected final Map<Integer, WriterBuffer> buffers;
  private int serializerBufferSize;
  protected final int bufferSegmentSize;
  protected long copyTime = 0;
  protected long sortRecordTime = 0;
  protected long serializeTime = 0;
  private long compressTime = 0;
  private long sortTime = 0;
  protected long writeTime = 0;
  private long estimateTime = 0;
  private long requireMemoryTime = 0;
  private SerializationStream serializeStream;
  protected final WrappedByteArrayOutputStream arrayOutputStream;
  protected long uncompressedDataLen = 0;
  private long compressedDataLen = 0;
  private long requireMemoryInterval;
  private int requireMemoryRetryMax;
  private Optional<Codec> codec;
  private Function<List<ShuffleBlockInfo>, List<CompletableFuture<Long>>> spillFunc;
  private long sendSizeLimit;
  private boolean memorySpillEnabled;
  private int memorySpillTimeoutSec;
  protected boolean isRowBased;
  protected BlockIdLayout blockIdLayout;
  protected double bufferSpillRatio;
  protected Function<Integer, List<ShuffleServerInfo>> partitionAssignmentRetrieveFunc;
  private int stageAttemptNumber;

  public WriteBufferManager(
      int shuffleId,
      long taskAttemptId,
      BufferManagerOptions bufferManagerOptions,
      Serializer serializer,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      TaskMemoryManager taskMemoryManager,
      ShuffleWriteMetrics shuffleWriteMetrics,
      RssConf rssConf) {
    this(
        shuffleId,
        null,
        taskAttemptId,
        bufferManagerOptions,
        serializer,
        partitionToServers,
        taskMemoryManager,
        shuffleWriteMetrics,
        rssConf,
        null,
        0);
  }

  public WriteBufferManager(
      int shuffleId,
      String taskId,
      long taskAttemptId,
      BufferManagerOptions bufferManagerOptions,
      Serializer serializer,
      TaskMemoryManager taskMemoryManager,
      ShuffleWriteMetrics shuffleWriteMetrics,
      RssConf rssConf,
      Function<List<ShuffleBlockInfo>, List<CompletableFuture<Long>>> spillFunc,
      Function<Integer, List<ShuffleServerInfo>> partitionAssignmentRetrieveFunc) {
    this(
        shuffleId,
        taskId,
        taskAttemptId,
        bufferManagerOptions,
        serializer,
        taskMemoryManager,
        shuffleWriteMetrics,
        rssConf,
        spillFunc,
        partitionAssignmentRetrieveFunc,
        0);
  }

  public WriteBufferManager(
      int shuffleId,
      String taskId,
      long taskAttemptId,
      BufferManagerOptions bufferManagerOptions,
      Serializer serializer,
      TaskMemoryManager taskMemoryManager,
      ShuffleWriteMetrics shuffleWriteMetrics,
      RssConf rssConf,
      Function<List<ShuffleBlockInfo>, List<CompletableFuture<Long>>> spillFunc,
      Function<Integer, List<ShuffleServerInfo>> partitionAssignmentRetrieveFunc,
      int stageAttemptNumber) {
    super(taskMemoryManager, taskMemoryManager.pageSizeBytes(), MemoryMode.ON_HEAP);
    this.bufferSize = bufferManagerOptions.getBufferSize();
    this.spillSize = bufferManagerOptions.getBufferSpillThreshold();
    this.buffers = Maps.newHashMap();
    this.shuffleId = shuffleId;
    this.taskId = taskId;
    this.taskAttemptId = taskAttemptId;
    this.shuffleWriteMetrics = shuffleWriteMetrics;
    this.rssConf = rssConf;
    this.serializerBufferSize = bufferManagerOptions.getSerializerBufferSize();
    this.bufferSegmentSize = bufferManagerOptions.getBufferSegmentSize();
    this.askExecutorMemory = bufferManagerOptions.getPreAllocatedBufferSize();
    this.requireMemoryInterval = bufferManagerOptions.getRequireMemoryInterval();
    this.requireMemoryRetryMax = bufferManagerOptions.getRequireMemoryRetryMax();
    this.arrayOutputStream = new WrappedByteArrayOutputStream(serializerBufferSize);
    // in columnar shuffle, the serializer here is never used
    this.isRowBased = rssConf.getBoolean(RssSparkConfig.RSS_ROW_BASED);
    if (isRowBased) {
      this.instance = serializer.newInstance();
      this.serializeStream = instance.serializeStream(arrayOutputStream);
    }
    boolean compress =
        rssConf.getBoolean(
            RssSparkConfig.SPARK_SHUFFLE_COMPRESS_KEY.substring(
                RssSparkConfig.SPARK_RSS_CONFIG_PREFIX.length()),
            RssSparkConfig.SPARK_SHUFFLE_COMPRESS_DEFAULT);
    this.codec = compress ? Codec.newInstance(rssConf) : Optional.empty();
    this.spillFunc = spillFunc;
    this.sendSizeLimit = rssConf.get(RssSparkConfig.RSS_CLIENT_SEND_SIZE_LIMITATION);
    this.memorySpillTimeoutSec = rssConf.get(RssSparkConfig.RSS_MEMORY_SPILL_TIMEOUT);
    this.memorySpillEnabled = rssConf.get(RssSparkConfig.RSS_MEMORY_SPILL_ENABLED);
    this.bufferSpillRatio = rssConf.get(RssSparkConfig.RSS_MEMORY_SPILL_RATIO);
    this.blockIdLayout = BlockIdLayout.from(rssConf);
    this.partitionAssignmentRetrieveFunc = partitionAssignmentRetrieveFunc;
    this.stageAttemptNumber = stageAttemptNumber;
  }

  public WriteBufferManager(
      int shuffleId,
      String taskId,
      long taskAttemptId,
      BufferManagerOptions bufferManagerOptions,
      Serializer serializer,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      TaskMemoryManager taskMemoryManager,
      ShuffleWriteMetrics shuffleWriteMetrics,
      RssConf rssConf,
      Function<List<ShuffleBlockInfo>, List<CompletableFuture<Long>>> spillFunc,
      int stageAttemptNumber) {
    this(
        shuffleId,
        taskId,
        taskAttemptId,
        bufferManagerOptions,
        serializer,
        taskMemoryManager,
        shuffleWriteMetrics,
        rssConf,
        spillFunc,
        partitionId -> partitionToServers.get(partitionId),
        stageAttemptNumber);
  }

  /** add serialized columnar data directly when integrate with gluten */
  public List<ShuffleBlockInfo> addPartitionData(int partitionId, byte[] serializedData) {
    return addPartitionData(
        partitionId, serializedData, serializedData.length, System.currentTimeMillis());
  }

  public List<ShuffleBlockInfo> addPartitionData(
      int partitionId, byte[] serializedData, int serializedDataLength, long start) {
    List<ShuffleBlockInfo> singleOrEmptySendingBlocks =
        insertIntoBuffer(partitionId, serializedData, serializedDataLength);

    // check buffer size > spill threshold
    if (usedBytes.get() - inSendListBytes.get() > spillSize) {
      LOG.info(
          "ShuffleBufferManager spill for buffer size exceeding spill threshold, "
              + "usedBytes[{}], inSendListBytes[{}], spill size threshold[{}]",
          usedBytes.get(),
          inSendListBytes.get(),
          spillSize);
      List<ShuffleBlockInfo> multiSendingBlocks = clear(bufferSpillRatio);
      multiSendingBlocks.addAll(singleOrEmptySendingBlocks);
      writeTime += System.currentTimeMillis() - start;
      return multiSendingBlocks;
    }
    writeTime += System.currentTimeMillis() - start;
    return singleOrEmptySendingBlocks;
  }

  /**
   * Before inserting a record into its corresponding buffer, the system should check if there is
   * sufficient buffer memory available. If there isn't enough memory, it will request additional
   * memory from the {@link TaskMemoryManager}. In the event that the JVM is low on memory, a spill
   * operation will be triggered. If any memory consumer managed by the {@link TaskMemoryManager}
   * fails to meet its memory requirements, it will also be triggered one by one.
   *
   * <p>If the current buffer manager requests memory and triggers a spill operation, the buffer
   * that is currently being held should be dropped, and then re-inserted.
   */
  private List<ShuffleBlockInfo> insertIntoBuffer(
      int partitionId, byte[] serializedData, int serializedDataLength) {
    long required = Math.max(bufferSegmentSize, serializedDataLength);
    // Asking memory from task memory manager for the existing writer buffer,
    // this may trigger current WriteBufferManager spill method, which will
    // make the current write buffer discard. So we have to recheck the buffer existence.
    boolean hasRequested = false;
    WriterBuffer wb = buffers.get(partitionId);
    if (wb != null) {
      if (wb.askForMemory(serializedDataLength)) {
        requestMemory(required);
        hasRequested = true;
      }
    }

    // hasRequested is not true means spill method was not trigger,
    // and we don't have to recheck the buffer existence in this case.
    if (hasRequested) {
      wb = buffers.get(partitionId);
    }

    if (wb != null) {
      if (hasRequested) {
        usedBytes.addAndGet(required);
      }
      wb.addRecord(serializedData, serializedDataLength);
    } else {
      // The true of hasRequested means the former partitioned buffer has been flushed, that is
      // triggered by the spill operation caused by asking for memory. So it needn't to re-request
      // the memory.
      if (!hasRequested) {
        requestMemory(required);
      }
      usedBytes.addAndGet(required);
      wb = new WriterBuffer(bufferSegmentSize);
      wb.addRecord(serializedData, serializedDataLength);
      buffers.put(partitionId, wb);
    }

    if (wb.getMemoryUsed() > bufferSize) {
      List<ShuffleBlockInfo> sentBlocks = new ArrayList<>(1);
      sentBlocks.add(createShuffleBlock(partitionId, wb));
      recordCounter.addAndGet(wb.getRecordCount());
      copyTime += wb.getCopyTime();
      sortRecordTime += wb.getSortTime();
      buffers.remove(partitionId);
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Single buffer is full for shuffleId["
                + shuffleId
                + "] partition["
                + partitionId
                + "] with memoryUsed["
                + wb.getMemoryUsed()
                + "], dataLength["
                + wb.getDataLength()
                + "]");
      }
      return sentBlocks;
    }
    return Collections.emptyList();
  }

  public List<ShuffleBlockInfo> addRecord(int partitionId, Object key, Object value) {
    final long start = System.currentTimeMillis();
    arrayOutputStream.reset();
    if (key != null) {
      serializeStream.writeKey(key, ClassTag$.MODULE$.apply(key.getClass()));
    } else {
      serializeStream.writeKey(null, ManifestFactory$.MODULE$.Null());
    }
    if (value != null) {
      serializeStream.writeValue(value, ClassTag$.MODULE$.apply(value.getClass()));
    } else {
      serializeStream.writeValue(null, ManifestFactory$.MODULE$.Null());
    }
    serializeStream.flush();
    serializeTime += System.currentTimeMillis() - start;
    byte[] serializedData = arrayOutputStream.getBuf();
    int serializedDataLength = arrayOutputStream.size();
    if (serializedDataLength == 0) {
      return null;
    }
    List<ShuffleBlockInfo> shuffleBlockInfos =
        addPartitionData(partitionId, serializedData, serializedDataLength, start);
    // records is a row based semantic, when in columnar shuffle records num should be taken from
    // ColumnarBatch
    // that is handled by rss shuffle writer implementation
    if (isRowBased) {
      shuffleWriteMetrics.incRecordsWritten(1L);
    }
    return shuffleBlockInfos;
  }

  // Gluten needs this method.
  public synchronized List<ShuffleBlockInfo> clear() {
    return clear(bufferSpillRatio);
  }

  // transform all [partition, records] to [partition, ShuffleBlockInfo] and clear cache
  public synchronized List<ShuffleBlockInfo> clear(double bufferSpillRatio) {
    List<ShuffleBlockInfo> result = Lists.newArrayList();
    long dataSize = 0;
    long memoryUsed = 0;

    long targetSpillSize = Long.MAX_VALUE;
    bufferSpillRatio = Math.max(0.1, Math.min(1.0, bufferSpillRatio));
    List<Integer> partitionList = new ArrayList(buffers.keySet());
    if (Double.compare(bufferSpillRatio, 1.0) < 0) {
      long start = System.currentTimeMillis();
      partitionList.sort(
          Comparator.comparingInt(o -> buffers.get(o) == null ? 0 : buffers.get(o).getMemoryUsed())
              .reversed());
      sortTime += start;
      targetSpillSize = (long) ((getUsedBytes() - getInSendListBytes()) * bufferSpillRatio);
    }

    for (int partitionId : partitionList) {
      WriterBuffer wb = buffers.get(partitionId);
      if (wb == null) {
        LOG.warn("get partition buffer failed,this should not happen!");
        continue;
      }
      dataSize += wb.getDataLength();
      memoryUsed += wb.getMemoryUsed();
      result.add(createShuffleBlock(partitionId, wb));
      recordCounter.addAndGet(wb.getRecordCount());
      copyTime += wb.getCopyTime();
      buffers.remove(partitionId);
      // got enough buffer to spill
      if (memoryUsed >= targetSpillSize) {
        break;
      }
    }
    LOG.info(
        "Flush total buffer for shuffleId["
            + shuffleId
            + "] with allocated["
            + allocatedBytes
            + "], dataSize["
            + dataSize
            + "], memoryUsed["
            + memoryUsed
            + "], number of blocks["
            + result.size()
            + "], flush ratio["
            + bufferSpillRatio
            + "]");
    return result;
  }

  // transform records to shuffleBlock
  protected ShuffleBlockInfo createShuffleBlock(int partitionId, WriterBuffer wb) {
    byte[] data = wb.getData();
    final int uncompressLength = data.length;
    byte[] compressed = data;
    if (codec.isPresent()) {
      long start = System.currentTimeMillis();
      compressed = codec.get().compress(data);
      compressTime += System.currentTimeMillis() - start;
    }
    final long crc32 = ChecksumUtils.getCrc32(compressed);
    final long blockId =
        blockIdLayout.getBlockId(getNextSeqNo(partitionId), partitionId, taskAttemptId);
    blockCounter.incrementAndGet();
    uncompressedDataLen += data.length;
    compressedDataLen += compressed.length;
    shuffleWriteMetrics.incBytesWritten(compressed.length);
    // add memory to indicate bytes which will be sent to shuffle server
    inSendListBytes.addAndGet(wb.getMemoryUsed());
    return new ShuffleBlockInfo(
        shuffleId,
        partitionId,
        blockId,
        compressed.length,
        crc32,
        compressed,
        partitionAssignmentRetrieveFunc.apply(partitionId),
        uncompressLength,
        wb.getMemoryUsed(),
        taskAttemptId);
  }

  // it's run in single thread, and is not thread safe
  protected int getNextSeqNo(int partitionId) {
    return partitionToSeqNo
        .computeIfAbsent(partitionId, k -> new AtomicInteger(0))
        .getAndIncrement();
  }

  protected void requestMemory(long requiredMem) {
    final long start = System.currentTimeMillis();
    if (allocatedBytes.get() - usedBytes.get() < requiredMem) {
      requestExecutorMemory(requiredMem);
    }
    requireMemoryTime += System.currentTimeMillis() - start;
  }

  private void requestExecutorMemory(long leastMem) {
    long gotMem = acquireMemory(askExecutorMemory);
    allocatedBytes.addAndGet(gotMem);
    int retry = 0;
    while (allocatedBytes.get() - usedBytes.get() < leastMem) {
      LOG.info(
          "Can't get memory for now, sleep and try["
              + retry
              + "] again, request["
              + askExecutorMemory
              + "], got["
              + gotMem
              + "] less than "
              + leastMem);
      try {
        Thread.sleep(requireMemoryInterval);
      } catch (InterruptedException ie) {
        throw new RssException("Interrupted when waiting for memory.", ie);
      }
      gotMem = acquireMemory(askExecutorMemory);
      allocatedBytes.addAndGet(gotMem);
      retry++;
      if (retry > requireMemoryRetryMax) {
        String message =
            "Can't get memory to cache shuffle data, request["
                + askExecutorMemory
                + "], got["
                + gotMem
                + "],"
                + " WriteBufferManager allocated["
                + allocatedBytes
                + "] task used["
                + used
                + "]. It may be caused by shuffle server is full of data"
                + " or consider to optimize 'spark.executor.memory',"
                + " 'spark.rss.writer.buffer.spill.size'.";
        LOG.error(message);
        throw new RssException(message);
      }
    }
  }

  public void releaseBlockResource(ShuffleBlockInfo block) {
    this.freeAllocatedMemory(block.getFreeMemory());
    block.getData().release();
  }

  public List<AddBlockEvent> buildBlockEvents(List<ShuffleBlockInfo> shuffleBlockInfoList) {
    long totalSize = 0;
    List<AddBlockEvent> events = new ArrayList<>();
    List<ShuffleBlockInfo> shuffleBlockInfosPerEvent = Lists.newArrayList();
    for (ShuffleBlockInfo sbi : shuffleBlockInfoList) {
      sbi.withCompletionCallback((block, isSuccessful) -> this.releaseBlockResource(block));
      totalSize += sbi.getSize();
      shuffleBlockInfosPerEvent.add(sbi);
      // split shuffle data according to the size
      if (totalSize > sendSizeLimit) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Build event with "
                  + shuffleBlockInfosPerEvent.size()
                  + " blocks and "
                  + totalSize
                  + " bytes");
        }
        events.add(new AddBlockEvent(taskId, stageAttemptNumber, shuffleBlockInfosPerEvent));
        shuffleBlockInfosPerEvent = Lists.newArrayList();
        totalSize = 0;
      }
    }
    if (!shuffleBlockInfosPerEvent.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Build event with "
                + shuffleBlockInfosPerEvent.size()
                + " blocks and "
                + totalSize
                + " bytes");
      }
      // Use final temporary variables for closures
      events.add(new AddBlockEvent(taskId, stageAttemptNumber, shuffleBlockInfosPerEvent));
    }
    return events;
  }

  @Override
  public long spill(long size, MemoryConsumer trigger) {
    // Only for the MemoryConsumer of this instance, it will flush buffer
    if (!memorySpillEnabled || trigger != this) {
      return 0L;
    }

    List<CompletableFuture<Long>> futures = spillFunc.apply(clear(bufferSpillRatio));
    CompletableFuture<Void> allOfFutures =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    try {
      allOfFutures.get(memorySpillTimeoutSec, TimeUnit.SECONDS);
    } catch (TimeoutException timeoutException) {
      // A best effort strategy to wait.
      // If timeout exception occurs, the underlying tasks won't be cancelled.
      LOG.warn("[taskId: {}] Spill tasks timeout after {} seconds", taskId, memorySpillTimeoutSec);
    } catch (Exception e) {
      LOG.warn("[taskId: {}] Failed to spill buffers due to ", taskId, e);
    } finally {
      long releasedSize =
          futures.stream()
              .filter(x -> x.isDone())
              .mapToLong(
                  x -> {
                    try {
                      return x.get();
                    } catch (Exception e) {
                      return 0;
                    }
                  })
              .sum();
      LOG.info(
          "[taskId: {}] Spill triggered by own, released memory size: {}", taskId, releasedSize);
      return releasedSize;
    }
  }

  @VisibleForTesting
  protected long getAllocatedBytes() {
    return allocatedBytes.get();
  }

  @VisibleForTesting
  protected long getUsedBytes() {
    return usedBytes.get();
  }

  @VisibleForTesting
  protected long getInSendListBytes() {
    return inSendListBytes.get();
  }

  protected long getRecordCount() {
    return recordCounter.get();
  }

  public long getBlockCount() {
    return blockCounter.get();
  }

  public void freeAllocatedMemory(long freeMemory) {
    freeMemory(freeMemory);
    allocatedBytes.addAndGet(-freeMemory);
    usedBytes.addAndGet(-freeMemory);
    inSendListBytes.addAndGet(-freeMemory);
  }

  public void freeAllMemory() {
    long memory = allocatedBytes.get();
    if (memory > 0) {
      freeMemory(memory);
    }
  }

  @VisibleForTesting
  protected Map<Integer, WriterBuffer> getBuffers() {
    return buffers;
  }

  @VisibleForTesting
  protected ShuffleWriteMetrics getShuffleWriteMetrics() {
    return shuffleWriteMetrics;
  }

  @VisibleForTesting
  protected void setShuffleWriteMetrics(ShuffleWriteMetrics shuffleWriteMetrics) {
    this.shuffleWriteMetrics = shuffleWriteMetrics;
  }

  public long getWriteTime() {
    return writeTime;
  }

  public String getManagerCostInfo() {
    return "WriteBufferManager cost copyTime["
        + copyTime
        + "], sortRecordTime["
        + sortRecordTime
        + "], writeTime["
        + writeTime
        + "], serializeTime["
        + serializeTime
        + "], sortTime["
        + sortTime
        + "], estimateTime["
        + estimateTime
        + "], requireMemoryTime["
        + requireMemoryTime
        + "], uncompressedDataLen["
        + uncompressedDataLen
        + "], compressedDataLen["
        + compressedDataLen
        + "], compressTime["
        + compressTime
        + "], compressRatio["
        + (compressedDataLen == 0 ? 0 : (float) uncompressedDataLen / compressedDataLen)
        + "]";
  }

  @VisibleForTesting
  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  @VisibleForTesting
  public void setSpillFunc(
      Function<List<ShuffleBlockInfo>, List<CompletableFuture<Long>>> spillFunc) {
    this.spillFunc = spillFunc;
  }

  @VisibleForTesting
  public void setSendSizeLimit(long sendSizeLimit) {
    this.sendSizeLimit = sendSizeLimit;
  }

  public void setPartitionAssignmentRetrieveFunc(
      Function<Integer, List<ShuffleServerInfo>> partitionAssignmentRetrieveFunc) {
    this.partitionAssignmentRetrieveFunc = partitionAssignmentRetrieveFunc;
  }
}
