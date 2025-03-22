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

package org.apache.uniffle.client.record.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.request.RssAppHeartBeatRequest;
import org.apache.uniffle.client.request.RssFinishShuffleRequest;
import org.apache.uniffle.client.request.RssGetInMemoryShuffleDataRequest;
import org.apache.uniffle.client.request.RssGetShuffleDataRequest;
import org.apache.uniffle.client.request.RssGetShuffleIndexRequest;
import org.apache.uniffle.client.request.RssGetShuffleResultForMultiPartRequest;
import org.apache.uniffle.client.request.RssGetShuffleResultRequest;
import org.apache.uniffle.client.request.RssGetSortedShuffleDataRequest;
import org.apache.uniffle.client.request.RssRegisterShuffleRequest;
import org.apache.uniffle.client.request.RssReportShuffleResultRequest;
import org.apache.uniffle.client.request.RssSendCommitRequest;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.client.request.RssStartSortMergeRequest;
import org.apache.uniffle.client.request.RssUnregisterShuffleByAppIdRequest;
import org.apache.uniffle.client.request.RssUnregisterShuffleRequest;
import org.apache.uniffle.client.response.RssAppHeartBeatResponse;
import org.apache.uniffle.client.response.RssFinishShuffleResponse;
import org.apache.uniffle.client.response.RssGetInMemoryShuffleDataResponse;
import org.apache.uniffle.client.response.RssGetShuffleDataResponse;
import org.apache.uniffle.client.response.RssGetShuffleIndexResponse;
import org.apache.uniffle.client.response.RssGetShuffleResultResponse;
import org.apache.uniffle.client.response.RssGetSortedShuffleDataResponse;
import org.apache.uniffle.client.response.RssRegisterShuffleResponse;
import org.apache.uniffle.client.response.RssReportShuffleResultResponse;
import org.apache.uniffle.client.response.RssSendCommitResponse;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.client.response.RssStartSortMergeResponse;
import org.apache.uniffle.client.response.RssUnregisterShuffleByAppIdResponse;
import org.apache.uniffle.client.response.RssUnregisterShuffleResponse;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.merger.MergeState;
import org.apache.uniffle.common.netty.buffer.NettyManagedBuffer;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.RssUtils;

public class MockedShuffleServerClient implements ShuffleServerClient {

  private Map<Integer, List<ByteBuf>> shuffleData;
  private Map<Integer, Integer> indexes;
  private long[] blockIds;

  public MockedShuffleServerClient(int[] partitionIds, ByteBuf[][] buffers, long[] blockIds) {
    if (partitionIds.length != buffers.length) {
      throw new RssException("partition id length is not matched");
    }
    this.shuffleData = new HashMap<>();
    for (int i = 0; i < partitionIds.length; i++) {
      int partition = partitionIds[i];
      shuffleData.put(partition, new ArrayList<>());
      for (ByteBuf byteBuffer : buffers[i]) {
        shuffleData.get(partition).add(byteBuffer);
      }
    }
    this.indexes = new HashMap<>();
    for (Integer pid : shuffleData.keySet()) {
      indexes.put(pid, 0);
    }
    this.blockIds = blockIds;
  }

  @Override
  public RssGetSortedShuffleDataResponse getSortedShuffleData(
      RssGetSortedShuffleDataRequest request) {
    int partitionId = request.getPartitionId();
    if (!shuffleData.containsKey(partitionId)) {
      throw new RssException("partitionid is not existed");
    }
    RssGetSortedShuffleDataResponse response;
    int index = indexes.get(partitionId);
    if (index < shuffleData.get(partitionId).size()) {
      // Offset is ignore in mock client, set unused value 10000;
      response =
          new RssGetSortedShuffleDataResponse(
              StatusCode.SUCCESS,
              "",
              new NettyManagedBuffer(shuffleData.get(partitionId).get(index).retain()),
              10000,
              MergeState.DONE.code());
    } else {
      response =
          new RssGetSortedShuffleDataResponse(
              StatusCode.SUCCESS,
              "",
              new NettyManagedBuffer(Unpooled.buffer(0)),
              -1,
              MergeState.DONE.code());
    }
    indexes.put(partitionId, index + 1);
    return response;
  }

  @Override
  public RssUnregisterShuffleResponse unregisterShuffle(RssUnregisterShuffleRequest request) {
    return null;
  }

  @Override
  public RssRegisterShuffleResponse registerShuffle(RssRegisterShuffleRequest request) {
    return null;
  }

  @Override
  public RssUnregisterShuffleByAppIdResponse unregisterShuffleByAppId(
      RssUnregisterShuffleByAppIdRequest request) {
    return null;
  }

  @Override
  public RssSendShuffleDataResponse sendShuffleData(RssSendShuffleDataRequest request) {
    return null;
  }

  @Override
  public RssSendCommitResponse sendCommit(RssSendCommitRequest request) {
    return null;
  }

  @Override
  public RssAppHeartBeatResponse sendHeartBeat(RssAppHeartBeatRequest request) {
    return null;
  }

  @Override
  public RssFinishShuffleResponse finishShuffle(RssFinishShuffleRequest request) {
    return null;
  }

  @Override
  public RssReportShuffleResultResponse reportShuffleResult(RssReportShuffleResultRequest request) {
    return null;
  }

  @Override
  public RssGetShuffleResultResponse getShuffleResult(RssGetShuffleResultRequest request) {

    try {
      Roaring64NavigableMap bitMap = Roaring64NavigableMap.bitmapOf();
      for (long blockId : blockIds) {
        bitMap.add(blockId);
      }
      return new RssGetShuffleResultResponse(StatusCode.SUCCESS, RssUtils.serializeBitMap(bitMap));
    } catch (IOException e) {
      throw new RssException(e);
    }
  }

  @Override
  public RssGetShuffleResultResponse getShuffleResultForMultiPart(
      RssGetShuffleResultForMultiPartRequest request) {
    return null;
  }

  @Override
  public RssGetShuffleIndexResponse getShuffleIndex(RssGetShuffleIndexRequest request) {
    return null;
  }

  @Override
  public RssGetShuffleDataResponse getShuffleData(RssGetShuffleDataRequest request) {
    return null;
  }

  @Override
  public RssStartSortMergeResponse startSortMerge(RssStartSortMergeRequest request) {
    return null;
  }

  @Override
  public RssGetInMemoryShuffleDataResponse getInMemoryShuffleData(
      RssGetInMemoryShuffleDataRequest request) {
    return null;
  }

  @Override
  public void close() {}

  @Override
  public String getClientInfo() {
    return null;
  }
}
