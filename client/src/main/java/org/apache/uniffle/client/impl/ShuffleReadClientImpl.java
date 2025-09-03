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

package org.apache.uniffle.client.impl;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.response.CompressedShuffleBlock;
import org.apache.uniffle.client.util.DefaultIdHelper;
import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShuffleReadTimes;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssFetchFailedException;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.ChecksumUtils;
import org.apache.uniffle.common.util.IdHelper;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.storage.factory.ShuffleHandlerFactory;
import org.apache.uniffle.storage.handler.api.ClientReadHandler;
import org.apache.uniffle.storage.handler.impl.ShuffleServerReadCostTracker;
import org.apache.uniffle.storage.request.CreateShuffleReadHandlerRequest;

public class ShuffleReadClientImpl implements ShuffleReadClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleReadClientImpl.class);
  private List<ShuffleServerInfo> shuffleServerInfoList;
  private int shuffleId;
  private int partitionId;
  private ByteBuffer readBuffer;
  private ShuffleDataResult sdr;
  private Roaring64NavigableMap blockIdBitmap;
  private Roaring64NavigableMap taskIdBitmap;
  private Roaring64NavigableMap pendingBlockIds;
  private Set<Long> processedBlockIds = ConcurrentHashMap.newKeySet();
  private Queue<BufferSegment> bufferSegmentQueue = Queues.newLinkedBlockingQueue();
  private AtomicLong readDataTime = new AtomicLong(0);
  private AtomicLong copyTime = new AtomicLong(0);
  private AtomicLong crcCheckTime = new AtomicLong(0);
  private ClientReadHandler clientReadHandler;
  private IdHelper idHelper;
  private BlockIdLayout blockIdLayout;
  private ShuffleServerReadCostTracker readCostTracker;

  public ShuffleReadClientImpl(ShuffleClientFactory.ReadClientBuilder builder) {
    // add default value
    if (builder.getShuffleDataDistributionType() == null) {
      builder.shuffleDataDistributionType(ShuffleDataDistributionType.NORMAL);
    }
    if (builder.getHadoopConf() == null) {
      builder.hadoopConf(new Configuration());
    }
    if (builder.getRssConf() != null
        &&
        // if rssConf contains only block id config, consider this as test mode as well
        !builder
            .getRssConf()
            .getKeySet()
            .equals(
                Sets.newHashSet(
                    RssClientConf.BLOCKID_SEQUENCE_NO_BITS.key(),
                    RssClientConf.BLOCKID_PARTITION_ID_BITS.key(),
                    RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS.key()))) {
      final int indexReadLimit = builder.getRssConf().get(RssClientConf.RSS_INDEX_READ_LIMIT);
      final String storageType = builder.getRssConf().get(RssClientConf.RSS_STORAGE_TYPE);
      long readBufferSize =
          builder
              .getRssConf()
              .getSizeAsBytes(
                  RssClientConf.RSS_CLIENT_READ_BUFFER_SIZE.key(),
                  RssClientConf.RSS_CLIENT_READ_BUFFER_SIZE.defaultValue());
      if (readBufferSize > Integer.MAX_VALUE) {
        LOG.warn(RssClientConf.RSS_CLIENT_READ_BUFFER_SIZE.key() + " can support 2g as max");
        readBufferSize = Integer.MAX_VALUE;
      }
      boolean offHeapEnabled = builder.getRssConf().get(RssClientConf.OFF_HEAP_MEMORY_ENABLE);
      builder.indexReadLimit(indexReadLimit);
      builder.storageType(storageType);
      builder.readBufferSize(readBufferSize);
      builder.offHeapEnable(offHeapEnabled);
      if (builder.getClientType() == null) {
        builder.clientType(builder.getRssConf().get(RssClientConf.RSS_CLIENT_TYPE));
      }
    } else {
      // most for test
      RssConf rssConf = (builder.getRssConf() == null) ? new RssConf() : builder.getRssConf();
      rssConf.set(RssClientConf.RSS_STORAGE_TYPE, builder.getStorageType());
      rssConf.set(RssClientConf.RSS_INDEX_READ_LIMIT, builder.getIndexReadLimit());
      rssConf.set(
          RssClientConf.RSS_CLIENT_READ_BUFFER_SIZE, String.valueOf(builder.getReadBufferSize()));
      if (!rssConf.contains(RssClientConf.BLOCKID_SEQUENCE_NO_BITS)) {
        rssConf.setInteger(
            RssClientConf.BLOCKID_SEQUENCE_NO_BITS, BlockIdLayout.DEFAULT.sequenceNoBits);
      }
      if (!rssConf.contains(RssClientConf.BLOCKID_PARTITION_ID_BITS)) {
        rssConf.setInteger(
            RssClientConf.BLOCKID_PARTITION_ID_BITS, BlockIdLayout.DEFAULT.partitionIdBits);
      }
      if (!rssConf.contains(RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS)) {
        rssConf.setInteger(
            RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS, BlockIdLayout.DEFAULT.taskAttemptIdBits);
      }

      builder.rssConf(rssConf);
      builder.offHeapEnable(false);
      builder.expectedTaskIdsBitmapFilterEnable(false);
      if (builder.getClientType() == null) {
        builder.clientType(rssConf.get(RssClientConf.RSS_CLIENT_TYPE));
      }
    }
    if (builder.getIdHelper() == null) {
      builder.idHelper(new DefaultIdHelper(BlockIdLayout.from(builder.getRssConf())));
    }

    init(builder);
  }

  private void init(ShuffleClientFactory.ReadClientBuilder builder) {
    this.shuffleId = builder.getShuffleId();
    this.partitionId = builder.getPartitionId();
    this.blockIdBitmap = builder.getBlockIdBitmap();
    this.taskIdBitmap = builder.getTaskIdBitmap();
    this.idHelper = builder.getIdHelper();
    this.shuffleServerInfoList = builder.getShuffleServerInfoList();
    this.blockIdLayout = BlockIdLayout.from(builder.getRssConf());
    this.readCostTracker = builder.getReadCostTracker();

    CreateShuffleReadHandlerRequest request = new CreateShuffleReadHandlerRequest();
    request.setStorageType(builder.getStorageType());
    request.setAppId(builder.getAppId());
    request.setShuffleId(shuffleId);
    request.setPartitionId(partitionId);
    request.setIndexReadLimit(builder.getIndexReadLimit());
    request.setPartitionNumPerRange(builder.getPartitionNumPerRange());
    request.setPartitionNum(builder.getPartitionNum());
    request.setReadBufferSize((int) builder.getReadBufferSize());
    request.setStorageBasePath(builder.getBasePath());
    request.setShuffleServerInfoList(shuffleServerInfoList);
    request.setHadoopConf(builder.getHadoopConf());
    request.setExpectBlockIds(blockIdBitmap);
    request.setProcessBlockIds(processedBlockIds);
    request.setDistributionType(builder.getShuffleDataDistributionType());
    request.setIdHelper(idHelper);
    request.setExpectTaskIds(taskIdBitmap);
    request.setClientConf(builder.getRssConf());
    request.setClientType(builder.getClientType());
    request.setRetryMax(builder.getRetryMax());
    request.setRetryIntervalMax(builder.getRetryIntervalMax());
    request.setReadCostTracker(readCostTracker);
    if (builder.isExpectedTaskIdsBitmapFilterEnable()) {
      request.useExpectedTaskIdsBitmapFilter();
    }
    if (builder.isOffHeapEnable()) {
      request.enableOffHeap();
    }

    List<Long> removeBlockIds = Lists.newArrayList();
    blockIdBitmap.forEach(
        bid -> {
          if (!taskIdBitmap.contains(idHelper.getTaskAttemptId(bid))) {
            removeBlockIds.add(bid);
          }
        });

    for (long rid : removeBlockIds) {
      blockIdBitmap.removeLong(rid);
    }

    // copy blockIdBitmap to track all pending blocks
    pendingBlockIds = RssUtils.cloneBitMap(blockIdBitmap);

    clientReadHandler = ShuffleHandlerFactory.getInstance().createShuffleReadHandler(request);
  }

  @Override
  public CompressedShuffleBlock readShuffleBlockData() {
    while (true) {
      // empty data expected, just return null
      if (blockIdBitmap.isEmpty()) {
        return null;
      }

      // All blocks are processed, so just return
      if (pendingBlockIds.isEmpty()) {
        return null;
      }

      // if client need request new data from shuffle server
      if (bufferSegmentQueue.isEmpty()) {
        if (read() <= 0) {
          return null;
        }
      }

      // get next buffer segment
      BufferSegment bs = null;

      // blocks in bufferSegmentQueue may be from different partition in range partition mode,
      // or may be from speculation task, filter them and just read the necessary block
      while (true) {
        bs = bufferSegmentQueue.poll();
        if (bs == null) {
          break;
        }
        // check 1: if blockId is processed
        // check 2: if blockId is required for current partition
        // check 3: if blockId is generated by required task
        if (!processedBlockIds.contains(bs.getBlockId())
            && blockIdBitmap.contains(bs.getBlockId())
            && taskIdBitmap.contains(bs.getTaskAttemptId())) {
          long expectedCrc = -1;
          long actualCrc = -1;
          try {
            long start = System.currentTimeMillis();
            expectedCrc = bs.getCrc();
            actualCrc = ChecksumUtils.getCrc32(readBuffer, bs.getOffset(), bs.getLength());
            crcCheckTime.addAndGet(System.currentTimeMillis() - start);
          } catch (Exception e) {
            LOG.warn("Can't read data for " + blockIdLayout.asBlockId(bs.getBlockId()), e);
          }

          if (expectedCrc != actualCrc) {
            String errMsg =
                "Unexpected crc value for "
                    + blockIdLayout.asBlockId(bs.getBlockId())
                    + ", expected:"
                    + expectedCrc
                    + ", actual:"
                    + actualCrc;
            // If some blocks of one replica are corrupted,but maybe other replicas are not
            // corrupted,
            // so exception should not be thrown here if blocks have multiple replicas
            if (shuffleServerInfoList.size() > 1) {
              LOG.warn(errMsg);
              clientReadHandler.updateConsumedBlockInfo(bs, true);
              continue;
            } else {
              throw new RssFetchFailedException(errMsg);
            }
          }

          // mark block as processed
          processedBlockIds.add(bs.getBlockId());
          pendingBlockIds.removeLong(bs.getBlockId());
          // only update the statistics of necessary blocks
          clientReadHandler.updateConsumedBlockInfo(bs, false);
          break;
        }
        clientReadHandler.updateConsumedBlockInfo(bs, true);
        // mark block as processed
        processedBlockIds.add(bs.getBlockId());
        pendingBlockIds.removeLong(bs.getBlockId());
      }

      if (bs != null) {
        ByteBuffer compressedBuffer = readBuffer.duplicate();
        compressedBuffer.position(bs.getOffset());
        compressedBuffer.limit(bs.getOffset() + bs.getLength());
        return new CompressedShuffleBlock(compressedBuffer, bs.getUncompressLength());
      }
      // current segment hasn't data, try next segment
    }
  }

  @VisibleForTesting
  protected Set<Long> getProcessedBlockIds() {
    return processedBlockIds;
  }

  private int read() {
    long start = System.currentTimeMillis();
    // In order to avoid copying, we postpone the release here instead of in the Decoder.
    // RssUtils.releaseByteBuffer(readBuffer) cannot actually release the memory,
    // because PlatformDependent.freeDirectBuffer can only release the ByteBuffer with cleaner.
    if (sdr != null) {
      sdr.release();
      // We set sdr to null here to prevent IllegalReferenceCountException that could occur
      // if sdr.release() is called multiple times in the close() method,
      // when an exception is thrown by clientReadHandler.readShuffleData().
      sdr = null;
    }
    sdr = clientReadHandler.readShuffleData();
    readDataTime.addAndGet(System.currentTimeMillis() - start);
    if (sdr == null) {
      return 0;
    }
    readBuffer = sdr.getDataBuffer();
    if (readBuffer == null || readBuffer.capacity() == 0) {
      return 0;
    }
    bufferSegmentQueue.addAll(sdr.getBufferSegments());
    return sdr.getBufferSegments().size();
  }

  @Override
  public void checkProcessedBlockIds() {
    RssUtils.checkProcessedBlockIds(blockIdBitmap, processedBlockIds);
  }

  @Override
  public void close() {
    if (sdr != null) {
      sdr.release();
    }
    if (clientReadHandler != null) {
      clientReadHandler.close();
    }
  }

  @Override
  public void logStatics() {
    LOG.info(
        "Metrics for shuffleId["
            + shuffleId
            + "], partitionId["
            + partitionId
            + "]"
            + ", read data cost "
            + readDataTime
            + " ms, copy data cost "
            + copyTime
            + " ms, crc check cost "
            + crcCheckTime
            + " ms");
    clientReadHandler.logConsumedBlockInfo();
  }

  @Override
  public ShuffleReadTimes getShuffleReadTimes() {
    return new ShuffleReadTimes(readDataTime.get(), copyTime.get(), crcCheckTime.get());
  }
}
