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

package org.apache.uniffle.storage.handler.impl;

import java.util.List;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.request.RssGetInMemoryShuffleDataRequest;
import org.apache.uniffle.client.response.RssGetInMemoryShuffleDataResponse;
import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.exception.RssFetchFailedException;
import org.apache.uniffle.common.util.Constants;

public class MemoryClientReadHandler extends PrefetchableClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryClientReadHandler.class);
  private long lastBlockId = Constants.INVALID_BLOCK_ID;
  private ShuffleServerClient shuffleServerClient;
  private Roaring64NavigableMap expectTaskIds;
  private int retryMax;
  private long retryIntervalMax;

  public MemoryClientReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int readBufferSize,
      ShuffleServerClient shuffleServerClient,
      Roaring64NavigableMap expectTaskIds,
      int retryMax,
      long retryIntervalMax,
      Optional<PrefetchableClientReadHandler.PrefetchOption> prefetchOption) {
    super(prefetchOption);
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.readBufferSize = readBufferSize;
    this.shuffleServerClient = shuffleServerClient;
    this.expectTaskIds = expectTaskIds;
    this.retryMax = retryMax;
    this.retryIntervalMax = retryIntervalMax;
  }

  @VisibleForTesting
  public MemoryClientReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int readBufferSize,
      ShuffleServerClient shuffleServerClient,
      Roaring64NavigableMap expectTaskIds) {
    this(
        appId,
        shuffleId,
        partitionId,
        readBufferSize,
        shuffleServerClient,
        expectTaskIds,
        1,
        0,
        Optional.empty());
  }

  @Override
  public ShuffleDataResult doReadShuffleData() {
    ShuffleDataResult result = null;

    RssGetInMemoryShuffleDataRequest request =
        new RssGetInMemoryShuffleDataRequest(
            appId,
            shuffleId,
            partitionId,
            lastBlockId,
            readBufferSize,
            expectTaskIds,
            retryMax,
            retryIntervalMax);

    try {
      RssGetInMemoryShuffleDataResponse response =
          shuffleServerClient.getInMemoryShuffleData(request);
      result = new ShuffleDataResult(response.getData(), response.getBufferSegments());
    } catch (RssFetchFailedException e) {
      throw e;
    } catch (Exception e) {
      // todo: fault tolerance solution should be added
      throw new RssFetchFailedException(
          "Failed to read in memory shuffle data with " + shuffleServerClient.getClientInfo(), e);
    }

    // update lastBlockId for next rpc call
    if (!result.isEmpty()) {
      List<BufferSegment> bufferSegments = result.getBufferSegments();
      lastBlockId = bufferSegments.get(bufferSegments.size() - 1).getBlockId();
    }

    return result;
  }
}
