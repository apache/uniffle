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
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.serializer.kryo.KryoSerializerInstance;
import org.apache.uniffle.common.util.ChecksumUtils;

public class RMWriteBufferManager extends WriteBufferManager {

  private static final Logger LOG = LoggerFactory.getLogger(RMWriteBufferManager.class);

  private Comparator comparator;
  private KryoSerializerInstance instance;
  private Kryo serKryo;
  private Output serOutput;

  public RMWriteBufferManager(
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
      int stageAttemptNumber,
      Comparator comparator) {
    super(
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
        stageAttemptNumber);
    this.comparator = comparator;
    this.instance = new KryoSerializerInstance(this.rssConf);
    this.serKryo = this.instance.borrowKryo();
    this.serOutput = new UnsafeOutput(this.arrayOutputStream);
  }

  @Override
  public List<ShuffleBlockInfo> addRecord(int partitionId, Object key, Object value) {
    try {
      final long start = System.currentTimeMillis();
      arrayOutputStream.reset();
      this.serKryo.writeClassAndObject(this.serOutput, key);
      this.serOutput.flush();
      final int keyLength = arrayOutputStream.size();
      this.serKryo.writeClassAndObject(this.serOutput, value);
      this.serOutput.flush();
      serializeTime += System.currentTimeMillis() - start;
      byte[] serializedData = arrayOutputStream.getBuf();
      int serializedDataLength = arrayOutputStream.size();
      if (serializedDataLength == 0) {
        return null;
      }
      List<ShuffleBlockInfo> shuffleBlockInfos =
          addPartitionData(
              partitionId, serializedData, keyLength, serializedDataLength - keyLength, start);
      // records is a row based semantic, when in columnar shuffle records num should be taken from
      // ColumnarBatch
      // that is handled by rss shuffle writer implementation
      if (isRowBased) {
        shuffleWriteMetrics.incRecordsWritten(1L);
      }
      return shuffleBlockInfos;
    } catch (Exception e) {
      throw new RssException(e);
    }
  }

  private List<ShuffleBlockInfo> addPartitionData(
      int partitionId, byte[] serializedData, int keyLength, int valueLength, long start) {
    List<ShuffleBlockInfo> singleOrEmptySendingBlocks =
        insertIntoBuffer(partitionId, serializedData, keyLength, valueLength);

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

  private List<ShuffleBlockInfo> insertIntoBuffer(
      int partitionId, byte[] serializedData, int keyLength, int valueLength) {
    int recordLength = keyLength + valueLength;
    long required = Math.max(bufferSegmentSize, recordLength);
    // Asking memory from task memory manager for the existing writer buffer,
    // this may trigger current WriteBufferManager spill method, which will
    // make the current write buffer discard. So we have to recheck the buffer existence.
    boolean hasRequested = false;
    WriterBuffer wb = buffers.get(partitionId);
    if (wb != null) {
      if (wb.askForMemory(recordLength)) {
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
      wb.addRecord(serializedData, keyLength, valueLength);
    } else {
      // The true of hasRequested means the former partitioned buffer has been flushed, that is
      // triggered by the spill operation caused by asking for memory. So it needn't to re-request
      // the memory.
      if (!hasRequested) {
        requestMemory(required);
      }
      usedBytes.addAndGet(required);
      wb = new WriterBuffer(bufferSegmentSize);
      wb.addRecord(serializedData, keyLength, valueLength);
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

  // transform records to shuffleBlock
  @Override
  protected ShuffleBlockInfo createShuffleBlock(int partitionId, WriterBuffer wb) {
    byte[] data = wb.getData(instance, comparator);
    final int length = data.length;
    final long crc32 = ChecksumUtils.getCrc32(data);
    final long blockId =
        blockIdLayout.getBlockId(getNextSeqNo(partitionId), partitionId, taskAttemptId);
    blockCounter.incrementAndGet();
    uncompressedDataLen += data.length;
    shuffleWriteMetrics.incBytesWritten(data.length);
    // add memory to indicate bytes which will be sent to shuffle server
    inSendListBytes.addAndGet(wb.getMemoryUsed());
    return new ShuffleBlockInfo(
        shuffleId,
        partitionId,
        blockId,
        length,
        crc32,
        data,
        partitionAssignmentRetrieveFunc.apply(partitionId),
        length,
        wb.getMemoryUsed(),
        taskAttemptId);
  }

  @Override
  public void freeAllMemory() {
    super.freeAllMemory();
    if (this.instance != null && this.serKryo != null) {
      this.instance.releaseKryo(this.serKryo);
    }
    if (this.serOutput != null) {
      this.serOutput.close();
    }
  }
}
