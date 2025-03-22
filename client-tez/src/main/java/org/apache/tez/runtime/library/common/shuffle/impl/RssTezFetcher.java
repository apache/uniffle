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

package org.apache.tez.runtime.library.common.shuffle.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputAllocator;
import org.apache.tez.runtime.library.common.shuffle.FetcherCallback;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.RssTezBypassWriter;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.response.CompressedShuffleBlock;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.ByteBufferUtils;

public class RssTezFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(RssTezFetcher.class);
  private final FetcherCallback fetcherCallback;

  private final FetchedInputAllocator inputManager;

  private long copyBlockCount = 0;

  private volatile boolean stopped = false;

  private final ShuffleReadClient shuffleReadClient;
  Map<Integer, Roaring64NavigableMap> rssSuccessBlockIdBitmapMap;
  private int partitionId; // partition id to fetch for this task
  private long readTime = 0;
  private long decompressTime = 0;
  private long serializeTime = 0;
  private long waitTime = 0;
  private long copyTime = 0; // the sum of readTime + decompressTime + serializeTime + waitTime
  private long unCompressionLength = 0;
  private static int uniqueMapId = 0;

  private boolean hasPendingData = false;
  private long startWait;
  private int waitCount = 0;
  private byte[] uncompressedData = null;
  private Optional<Codec> codec;

  RssTezFetcher(
      FetcherCallback fetcherCallback,
      FetchedInputAllocator inputManager,
      ShuffleReadClient shuffleReadClient,
      Map<Integer, Roaring64NavigableMap> rssSuccessBlockIdBitmapMap,
      int partitionId,
      RssConf rssConf) {
    this.fetcherCallback = fetcherCallback;
    this.inputManager = inputManager;
    this.shuffleReadClient = shuffleReadClient;
    this.rssSuccessBlockIdBitmapMap = rssSuccessBlockIdBitmapMap;
    this.partitionId = partitionId;
    this.codec = Codec.newInstance(rssConf);
  }

  public void fetchAllRssBlocks() throws IOException {
    while (!stopped) {
      try {
        copyFromRssServer();
      } catch (Exception e) {
        LOG.error("Failed to fetchAllRssBlocks.", e);
        throw e;
      }
    }
  }

  @VisibleForTesting
  public void copyFromRssServer() throws IOException {
    CompressedShuffleBlock compressedBlock = null;
    ByteBuffer compressedData = null;
    long blockStartFetch = 0;
    // fetch a block
    if (!hasPendingData) {
      final long startFetch = System.currentTimeMillis();
      blockStartFetch = System.currentTimeMillis();
      compressedBlock = shuffleReadClient.readShuffleBlockData();
      if (compressedBlock != null) {
        compressedData = compressedBlock.getByteBuffer();
      }
      long fetchDuration = System.currentTimeMillis() - startFetch;
      readTime += fetchDuration;
    }

    // uncompress the block
    if (!hasPendingData && compressedData != null) {
      if (codec.isPresent()) {
        final long startDecompress = System.currentTimeMillis();
        int uncompressedLen = compressedBlock.getUncompressLength();
        ByteBuffer decompressedBuffer = ByteBuffer.allocate(uncompressedLen);
        codec.get().decompress(compressedData, uncompressedLen, decompressedBuffer, 0);
        uncompressedData = decompressedBuffer.array();
        unCompressionLength += compressedBlock.getUncompressLength();
        long decompressDuration = System.currentTimeMillis() - startDecompress;
        decompressTime += decompressDuration;
      } else {
        uncompressedData = ByteBufferUtils.bufferToArray(compressedData);
        unCompressionLength += uncompressedData.length;
      }
    }

    if (uncompressedData != null) {
      // start to merge
      final long startSerialization = System.currentTimeMillis();
      int compressedDataLength = compressedData != null ? compressedData.capacity() : 0;
      if (issueMapOutputMerge(compressedDataLength, blockStartFetch)) {
        long serializationDuration = System.currentTimeMillis() - startSerialization;
        serializeTime += serializationDuration;
        // if reserve successes, reset status for next fetch
        if (hasPendingData) {
          waitTime += System.currentTimeMillis() - startWait;
        }
        hasPendingData = false;
        uncompressedData = null;
      } else {
        LOG.info("UncompressedData is null");
        // if reserve fail, return and wait
        waitCount++;
        startWait = System.currentTimeMillis();
        return;
      }

      // update some status
      copyBlockCount++;
      copyTime = readTime + decompressTime + serializeTime + waitTime;
    } else {
      LOG.info("UncompressedData is null");
      // finish reading data, close related reader and check data consistent
      shuffleReadClient.close();
      shuffleReadClient.checkProcessedBlockIds();
      shuffleReadClient.logStatics();
      LOG.info(
          "Reduce task partition:"
              + partitionId
              + " read block cnt: "
              + copyBlockCount
              + " cost "
              + readTime
              + " ms to fetch and "
              + decompressTime
              + " ms to decompress with unCompressionLength["
              + unCompressionLength
              + "] and "
              + serializeTime
              + " ms to serialize and "
              + waitTime
              + " ms to wait resource, total copy time: "
              + copyTime);
      LOG.info("Stop fetcher");
      stopFetch();
    }
  }

  private boolean issueMapOutputMerge(int compressedLength, long blockStartFetch)
      throws IOException {
    // Allocate a MapOutput (either in-memory or on-disk) to put uncompressed block
    // In Rss, a MapOutput is sent as multiple blocks, so the reducer needs to
    // treat each "block" as a faked "mapout".
    // To avoid name conflicts, we use getNextUniqueTaskAttemptID instead.
    // It will generate a unique TaskAttemptID(increased_seq++, 0).
    InputAttemptIdentifier uniqueInputAttemptIdentifier = getNextUniqueInputAttemptIdentifier();
    FetchedInput fetchedInput = null;

    try {
      fetchedInput =
          inputManager.allocate(
              uncompressedData.length, compressedLength, uniqueInputAttemptIdentifier);
    } catch (IOException ioe) {
      // kill this reduce attempt
      throw ioe;
    }

    // Allocated space and then write data to mapOutput
    try {
      RssTezBypassWriter.write(fetchedInput, uncompressedData);
      // let the merger knows this block is ready for merging
      fetcherCallback.fetchSucceeded(
          null,
          uniqueInputAttemptIdentifier,
          fetchedInput,
          compressedLength,
          unCompressionLength,
          System.currentTimeMillis() - blockStartFetch);
    } catch (Throwable t) {
      LOG.error("Failed to write fetchedInput.", t);
      throw new RssException(
          "Partition: "
              + partitionId
              + " cannot write block to "
              + fetchedInput.getClass().getSimpleName()
              + " due to: "
              + t.getClass().getName());
    }
    return true;
  }

  private InputAttemptIdentifier getNextUniqueInputAttemptIdentifier() {
    return new InputAttemptIdentifier(uniqueMapId++, 0);
  }

  private void stopFetch() {
    stopped = true;
  }

  @VisibleForTesting
  public int getRetryCount() {
    return waitCount;
  }
}
