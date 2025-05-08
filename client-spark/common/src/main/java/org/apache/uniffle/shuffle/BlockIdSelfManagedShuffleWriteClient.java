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

package org.apache.uniffle.shuffle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;
import org.apache.uniffle.client.request.RssGetShuffleResultForMultiPartRequest;
import org.apache.uniffle.client.request.RssGetShuffleResultRequest;
import org.apache.uniffle.client.request.RssReportShuffleResultRequest;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.BlockIdLayout;

/**
 * This class delegates the blockIds reporting/getting operations from shuffleServer side to Spark
 * driver side.
 */
public class BlockIdSelfManagedShuffleWriteClient extends ShuffleWriteClientImpl {
  private Supplier<ShuffleManagerClient> managerClientSupplier;

  public BlockIdSelfManagedShuffleWriteClient(
      RssShuffleClientFactory.ExtendWriteClientBuilder builder) {
    super(builder);

    if (builder.getManagerClientSupplier() == null) {
      throw new RssException("Illegal empty shuffleManagerClient. This should not happen");
    }
    this.managerClientSupplier = builder.getManagerClientSupplier();
  }

  @Override
  public void reportShuffleResult(
      Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds,
      String appId,
      int shuffleId,
      long taskAttemptId,
      int bitmapNum) {
    Map<Integer, List<Long>> partitionToBlockIds = new HashMap<>();
    for (Map<Integer, Set<Long>> k : serverToPartitionToBlockIds.values()) {
      for (Map.Entry<Integer, Set<Long>> entry : k.entrySet()) {
        int partitionId = entry.getKey();
        partitionToBlockIds
            .computeIfAbsent(partitionId, x -> new ArrayList<>())
            .addAll(entry.getValue());
      }
    }

    RssReportShuffleResultRequest request =
        new RssReportShuffleResultRequest(
            appId, shuffleId, taskAttemptId, partitionToBlockIds, bitmapNum);
    managerClientSupplier.get().reportShuffleResult(request);
  }

  @Override
  public Roaring64NavigableMap getShuffleResult(
      String clientType,
      Set<ShuffleServerInfo> shuffleServerInfoSet,
      String appId,
      int shuffleId,
      int partitionId) {
    RssGetShuffleResultRequest request =
        new RssGetShuffleResultRequest(appId, shuffleId, partitionId, BlockIdLayout.DEFAULT);
    return managerClientSupplier.get().getShuffleResult(request).getBlockIdBitmap();
  }

  @Override
  public Roaring64NavigableMap getShuffleResultForMultiPart(
      String clientType,
      Map<ShuffleServerInfo, Set<Integer>> serverToPartitions,
      String appId,
      int shuffleId,
      Set<Integer> failedPartitions,
      PartitionDataReplicaRequirementTracking replicaRequirementTracking) {
    Set<Integer> partitionIds =
        serverToPartitions.values().stream().flatMap(x -> x.stream()).collect(Collectors.toSet());
    RssGetShuffleResultForMultiPartRequest request =
        new RssGetShuffleResultForMultiPartRequest(
            appId, shuffleId, partitionIds, BlockIdLayout.DEFAULT);
    return managerClientSupplier.get().getShuffleResultForMultiPart(request).getBlockIdBitmap();
  }
}
