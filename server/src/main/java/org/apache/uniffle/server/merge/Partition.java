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

package org.apache.uniffle.server.merge;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Range;
import io.netty.buffer.ByteBuf;
import io.netty.util.IllegalReferenceCountException;
import org.apache.hadoop.io.RawComparator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.FileNotFoundException;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.merger.MergeState;
import org.apache.uniffle.common.merger.Merger;
import org.apache.uniffle.common.merger.Segment;
import org.apache.uniffle.common.merger.StreamedSegment;
import org.apache.uniffle.common.netty.buffer.FileSegmentManagedBuffer;
import org.apache.uniffle.common.netty.buffer.ManagedBuffer;
import org.apache.uniffle.common.netty.buffer.NettyManagedBuffer;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.serializer.SerInputStream;
import org.apache.uniffle.common.serializer.SerOutputStream;
import org.apache.uniffle.server.ShuffleDataReadEvent;
import org.apache.uniffle.server.buffer.ShuffleBuffer;
import org.apache.uniffle.server.buffer.ShuffleBufferWithSkipList;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.handler.impl.LocalFileServerReadHandler;
import org.apache.uniffle.storage.request.CreateShuffleReadHandlerRequest;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.common.merger.MergeState.DONE;
import static org.apache.uniffle.common.merger.MergeState.INITED;
import static org.apache.uniffle.common.merger.MergeState.INTERNAL_ERROR;
import static org.apache.uniffle.common.merger.MergeState.MERGING;
import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MERGE_BLOCK_RING_BUFFER_SIZE;
import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MERGE_CACHE_MERGED_BLOCK_INIT_SLEEP_MS;
import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MERGE_CACHE_MERGED_BLOCK_MAX_SLEEP_MS;
import static org.apache.uniffle.server.merge.ShuffleMergeManager.MERGE_APP_SUFFIX;

public class Partition<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(Partition.class);

  private final Shuffle shuffle;
  private final int partitionId;

  private MergeState state = MergeState.INITED;
  private MergedResult result;
  private ShuffleMeta shuffleMeta = new ShuffleMeta();

  // These variable should be moved to ShuffleMergeManager, it is
  // not necessary to use partition granularity
  private final long initSleepTime;
  private final long maxSleepTime;
  private long sleepTime;
  private int ringBufferSize;
  private BlockFlushFileReader reader = null;

  public Partition(Shuffle shuffle, int partitionId) throws IOException {
    this.shuffle = shuffle;
    this.partitionId = partitionId;
    this.result =
        new MergedResult(
            shuffle.serverConf, this::cachedMergedBlock, shuffle.mergedBlockSize, this);
    this.initSleepTime = shuffle.serverConf.get(SERVER_MERGE_CACHE_MERGED_BLOCK_INIT_SLEEP_MS);
    this.maxSleepTime = shuffle.serverConf.get(SERVER_MERGE_CACHE_MERGED_BLOCK_MAX_SLEEP_MS);
    int tmpRingBufferSize = shuffle.serverConf.get(SERVER_MERGE_BLOCK_RING_BUFFER_SIZE);
    this.ringBufferSize =
        Integer.highestOneBit((Math.min(32, Math.max(2, tmpRingBufferSize)) - 1) << 1);
    if (tmpRingBufferSize != this.ringBufferSize) {
      LOG.info(
          "The ring buffer size will transient from {} to {}",
          tmpRingBufferSize,
          this.ringBufferSize);
    }
  }

  // startSortMerge is used to trigger to merger
  synchronized void startSortMerge(Roaring64NavigableMap expectedBlockIdMap) {
    if (getState() != INITED) {
      LOG.warn("Partition is already merging, so ignore duplicate reports, partition is {}", this);
    } else {
      if (!expectedBlockIdMap.isEmpty()) {
        setState(MERGING);
        MergeEvent event =
            new MergeEvent(
                shuffle.appId,
                shuffle.shuffleId,
                partitionId,
                shuffle.kClass,
                shuffle.vClass,
                expectedBlockIdMap);
        if (!shuffle.eventHandler.handle(event)) {
          setState(INTERNAL_ERROR);
        }
      } else {
        setState(DONE);
      }
    }
  }

  private ShufflePartitionedBlock getShufflePartitionedBlock(long blockId, boolean merged) {
    Map.Entry<Range<Integer>, ShuffleBuffer> entry =
        shuffle
            .shuffleServer
            .getShuffleBufferManager()
            .getShuffleBufferEntry(
                merged ? shuffle.appId + MERGE_APP_SUFFIX : shuffle.appId,
                shuffle.shuffleId,
                partitionId);
    if (entry != null) {
      ShuffleBuffer shuffleBuffer = entry.getValue();
      return ((ShuffleBufferWithSkipList) shuffleBuffer).getBlock(blockId);
    }
    return null;
  }

  public boolean collectBlocks(Iterator<Long> blockIds, Map<Long, ByteBuf> cachedBlocks) {
    boolean allCached = true;
    while (blockIds.hasNext()) {
      long blockId = blockIds.next();
      ShufflePartitionedBlock block = getShufflePartitionedBlock(blockId, false);
      if (block == null) {
        allCached = false;
        continue;
      }
      try {
        // If ByteBuf is released by flush cleanup will throw IllegalReferenceCountException.
        // Then we need get block buffer from file
        ByteBuf byteBuf = block.getData().retain().duplicate();
        cachedBlocks.put(blockId, byteBuf.slice(0, block.getDataLength()));
      } catch (IllegalReferenceCountException irce) {
        allCached = false;
        LOG.warn("Can't read bytes from block in memory, maybe already been flushed!");
      }
    }
    return allCached;
  }

  BlockFlushFileReader createReader(RssConf rssConf) {
    LocalFileServerReadHandler handler = getLocalFileServerReadHandler(rssConf, shuffle.appId);
    return new BlockFlushFileReader(
        handler.getDataFileName(), handler.getIndexFileName(), ringBufferSize, shuffle.direct);
  }

  public boolean collectSegments(
      RssConf rssConf,
      Iterator<Long> blockIds,
      Class keyClass,
      Class valueClass,
      Map<Long, ByteBuf> cachedBlock,
      List<Segment> segments,
      BlockFlushFileReader reader) {
    while (blockIds.hasNext()) {
      long blockId = blockIds.next();
      if (cachedBlock.containsKey(blockId)) {
        ByteBuf byteBuf = cachedBlock.get(blockId);
        SerInputStream serInputStream = SerInputStream.newInputStream(byteBuf);
        StreamedSegment segment =
            new StreamedSegment(
                rssConf,
                serInputStream,
                blockId,
                keyClass,
                valueClass,
                byteBuf.readableBytes(),
                (shuffle.comparator instanceof RawComparator));
        segments.add(segment);
      } else {
        BlockFlushFileReader.BlockInputStream inputStream =
            reader.registerBlockInputStream(blockId);
        if (inputStream == null) {
          LOG.warn("Can not find any buffer or file for block {}", blockId);
          return false;
        }
        segments.add(
            new StreamedSegment(
                rssConf,
                inputStream,
                blockId,
                keyClass,
                valueClass,
                inputStream.available(),
                (shuffle.comparator instanceof RawComparator)));
      }
    }
    return true;
  }

  SerOutputStream createSerOutputStream(long totalBytes) {
    return result.getOutputStream(shuffle.direct, totalBytes);
  }

  void merge(List<Segment> segments, SerOutputStream output, BlockFlushFileReader reader) {
    try {
      segments.forEach(segment -> segment.init());
      // start reader must happen after init segment to allocate ring buffer.
      if (reader != null) {
        reader.start();
      }
      Merger.merge(
          shuffle.serverConf,
          output,
          segments,
          shuffle.kClass,
          shuffle.vClass,
          shuffle.comparator,
          (shuffle.comparator instanceof RawComparator));
      setState(DONE);
    } catch (Exception e) {
      LOG.info("Found exception when merge for {}, caused by", this, e);
      setState(INTERNAL_ERROR);
    } finally {
      try {
        if (reader != null) {
          reader.close();
        }
      } catch (IOException ioe) {
        LOG.warn("Fail to close reader, caused by", this, ioe);
      }
      try {
        output.close();
      } catch (IOException ioe) {
        LOG.warn("Fail to close output, caused by ", ioe);
      }
      segments.forEach(
          segment -> {
            try {
              segment.close();
            } catch (IOException ioe) {
              LOG.warn("Fail to close segment, caused by ", ioe);
            }
          });
    }
  }

  public void setState(MergeState state) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Partition is {}, transient from {} to {}.", this, this.state.name(), state.name());
    }
    this.state = state;
  }

  public MergeState getState() {
    return state;
  }

  // Input: The first value is state, the second value is fetch block size
  // Output: left is the state, right is the blocks size that you can fetch
  public MergeStatus tryGetBlock(long blockId) {
    long size = -1L;
    MergeState currentState = state;
    if ((currentState == MERGING || currentState == DONE) && !result.isOutOfBound(blockId)) {
      size = result.getBlockSize(blockId);
    }
    return new MergeStatus(currentState, size);
  }

  public void requireMemory(int requireSize) throws IOException {
    while (!shuffle.shuffleServer.getShuffleTaskManager().requireMemory(requireSize, false)) {
      try {
        LOG.debug("Can not allocate enough memory for {}, then will sleep {}ms", this, sleepTime);
        Thread.sleep(sleepTime);
        sleepTime = Math.min(maxSleepTime, sleepTime * 2);
      } catch (InterruptedException ex) {
        LOG.warn("Found InterruptedException when sleep to wait require buffer {}", this);
        throw new IOException(ex);
      }
    }
  }

  public void releaseMemory(int requireSize) {
    shuffle.shuffleServer.getShuffleTaskManager().releaseMemory(requireSize, false, false);
  }

  // When we merge data, we will divide the merge results into blocks according to the specified
  // block size.
  // The merged block in a new appId field (${appd} + MERGE_APP_SUFFIX). We will process the merged
  // blocks in the
  // original way, cache them first, and flush them to disk when necessary.
  private boolean cachedMergedBlock(ByteBuf byteBuf, long blockId, int length) {
    String appId = shuffle.appId + MERGE_APP_SUFFIX;
    ShufflePartitionedBlock spb =
        new ShufflePartitionedBlock(length, length, -1, blockId, -1, byteBuf.retain());
    ShufflePartitionedData spd =
        new ShufflePartitionedData(partitionId, new ShufflePartitionedBlock[] {spb});
    StatusCode ret =
        shuffle
            .shuffleServer
            .getShuffleTaskManager()
            .cacheShuffleData(appId, shuffle.shuffleId, true, spd);
    if (ret == StatusCode.SUCCESS) {
      shuffle
          .shuffleServer
          .getShuffleTaskManager()
          .updateCachedBlockIds(appId, shuffle.shuffleId, spd.getPartitionId(), spd.getBlockList());
      sleepTime = initSleepTime;
      return true;
    } else {
      String shuffleDataInfo =
          "appId["
              + appId
              + "], shuffleId["
              + shuffle.shuffleId
              + "], partitionId["
              + spd.getPartitionId()
              + "]";
      LOG.warn(
          "Error happened when shuffleEngine.write for {}, statusCode={}", shuffleDataInfo, ret);
      byteBuf.release();
      return false;
    }
  }

  // get merged block
  public ShuffleDataResult getShuffleData(long blockId) throws IOException {
    // 1 Get result in memory
    // For merged block, we read and merge at the same time. Blocks may be added during the
    // traversal of blocks,
    // then may throw ConcurrentModificationException. So use cache block in Partition.
    ManagedBuffer managedBuffer = this.getMergedBlockBufferInMemory(blockId);
    if (managedBuffer != null) {
      return new ShuffleDataResult(managedBuffer);
    }

    // 2 Get result in flush file if we can't find block in memory.
    managedBuffer = this.getMergedBlockBufferInFile(shuffle.serverConf, blockId);
    return new ShuffleDataResult(managedBuffer);
  }

  private NettyManagedBuffer getMergedBlockBufferInMemory(long blockId) {
    try {
      ShufflePartitionedBlock block = this.getShufflePartitionedBlock(blockId, true);
      // We must make sure refCnt > 0, it means the ByteBuf is not released by flush cleanup
      if (block != null) {
        ByteBuf byteBuf = block.getData().retain();
        return new NettyManagedBuffer(byteBuf.duplicate());
      }
      return null;
    } catch (IllegalReferenceCountException e) {
      // If release that is triggered by flush cleanup before we retain, may throw
      // IllegalReferenceCountException.
      // It means ByteBuf is not available, we must get the block buffer from file.
      LOG.warn("Get ByteBuf from memory failed, cased by", e);
      return null;
    }
  }

  private synchronized ManagedBuffer getMergedBlockBufferInFile(RssConf rssConf, long blockId) {
    String appId = shuffle.appId + MERGE_APP_SUFFIX;
    if (!shuffleMeta.getSegments().containsKey(blockId)) {
      reloadShuffleMeta(rssConf, appId);
    }
    ShuffleMeta.Segment segment = shuffleMeta.getSegments().get(blockId);
    if (segment != null) {
      return new FileSegmentManagedBuffer(
          new File(shuffleMeta.getDataFileName()), segment.getOffset(), segment.getLength());
    }
    throw new RssException("Can not find block for blockId " + blockId);
  }

  // The index file is constantly growing and needs to be reloaded when necessary.
  private synchronized void reloadShuffleMeta(RssConf rssConf, String appId) {
    ShuffleIndexResult indexResult = loadShuffleIndexResult(rssConf, appId);
    shuffleMeta.setDataFileName(indexResult.getDataFileName());
    ByteBuffer indexData = indexResult.getIndexData();
    Map<Long, ShuffleMeta.Segment> segments = new HashMap<>();
    while (indexData.hasRemaining()) {
      long offset = indexData.getLong();
      int length = indexData.getInt();
      int uncompressLength = indexData.getInt();
      long crc = indexData.getLong();
      long blockId = indexData.getLong();
      long taskAttemptId = indexData.getLong();
      segments.put(blockId, new ShuffleMeta.Segment(offset, length));
    }
    shuffleMeta.getSegments().clear();
    shuffleMeta.getSegments().putAll(segments);
  }

  private ShuffleIndexResult loadShuffleIndexResult(RssConf rssConf, String appId) {
    CreateShuffleReadHandlerRequest request = new CreateShuffleReadHandlerRequest();
    request.setAppId(appId);
    request.setShuffleId(shuffle.shuffleId);
    request.setPartitionId(partitionId);
    request.setPartitionNumPerRange(1);
    request.setPartitionNum(Integer.MAX_VALUE); // ignore check partition number
    request.setStorageType(StorageType.LOCALFILE.name());
    request.setRssBaseConf((RssBaseConf) rssConf);
    Storage storage =
        shuffle
            .shuffleServer
            .getStorageManager()
            .selectStorage(
                new ShuffleDataReadEvent(appId, shuffle.shuffleId, partitionId, partitionId));
    if (storage == null) {
      throw new FileNotFoundException("No such data in current storage manager.");
    }
    ShuffleIndexResult index = storage.getOrCreateReadHandler(request).getShuffleIndex();
    return index;
  }

  private LocalFileServerReadHandler getLocalFileServerReadHandler(RssConf rssConf, String appId) {
    CreateShuffleReadHandlerRequest request = new CreateShuffleReadHandlerRequest();
    request.setAppId(appId);
    request.setShuffleId(shuffle.shuffleId);
    request.setPartitionId(partitionId);
    request.setPartitionNumPerRange(1);
    request.setPartitionNum(Integer.MAX_VALUE); // ignore check partition number
    request.setStorageType(StorageType.LOCALFILE.name());
    request.setRssBaseConf((RssBaseConf) rssConf);
    Storage storage =
        shuffle
            .shuffleServer
            .getStorageManager()
            .selectStorage(
                new ShuffleDataReadEvent(appId, shuffle.shuffleId, partitionId, partitionId));
    if (storage == null) {
      throw new FileNotFoundException("No such data in current storage manager.");
    }
    return (LocalFileServerReadHandler) storage.getOrCreateReadHandler(request);
  }

  void cleanup() {
    try {
      shuffleMeta.clear();
    } catch (Exception e) {
      LOG.warn("Partition {} clean up failed, caused by {}", this, e);
    }
  }

  @Override
  public String toString() {
    return "Partition{"
        + "appId="
        + shuffle.appId
        + ", shuffle="
        + shuffle.shuffleId
        + ", partitionId="
        + partitionId
        + ", state="
        + state
        + '}';
  }

  public static class ShuffleMeta {

    public static class Segment {
      private long offset;
      private int length;

      public Segment(long offset, int length) {
        this.offset = offset;
        this.length = length;
      }

      public long getOffset() {
        return offset;
      }

      public int getLength() {
        return length;
      }
    }

    private String dataFileName;
    private Map<Long, Segment> segments = new HashMap();

    public ShuffleMeta() {}

    public void setDataFileName(String dataFileName) {
      this.dataFileName = dataFileName;
    }

    public String getDataFileName() {
      return dataFileName;
    }

    public Map<Long, Segment> getSegments() {
      return segments;
    }

    public void clear() {
      this.segments.clear();
    }
  }
}
