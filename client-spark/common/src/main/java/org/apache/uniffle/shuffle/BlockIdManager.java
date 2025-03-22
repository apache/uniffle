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

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.RssUtils;

/** The class is to manage the shuffle data blockIds in spark driver side. */
public class BlockIdManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(BlockIdManager.class);

  // shuffleId -> partitionId -> blockIds
  private Map<Integer, Map<Integer, Roaring64NavigableMap>> blockIds;

  public BlockIdManager() {
    this.blockIds = JavaUtils.newConcurrentMap();
  }

  public void add(int shuffleId, int partitionId, List<Long> ids) {

    if (CollectionUtils.isEmpty(ids)) {
      return;
    }
    Map<Integer, Roaring64NavigableMap> partitionedBlockIds =
        blockIds.computeIfAbsent(shuffleId, k -> JavaUtils.newConcurrentMap());
    partitionedBlockIds.compute(
        partitionId,
        (id, bitmap) -> {
          Roaring64NavigableMap store = bitmap == null ? Roaring64NavigableMap.bitmapOf() : bitmap;
          ids.stream().forEach(x -> store.add(x));
          return store;
        });
  }

  public Roaring64NavigableMap get(int shuffleId, int partitionId) {
    Map<Integer, Roaring64NavigableMap> partitionedBlockIds = blockIds.get(shuffleId);
    if (partitionedBlockIds == null || partitionedBlockIds.isEmpty()) {
      return Roaring64NavigableMap.bitmapOf();
    }

    Roaring64NavigableMap idMap = partitionedBlockIds.get(partitionId);
    if (idMap == null || idMap.isEmpty()) {
      return Roaring64NavigableMap.bitmapOf();
    }

    return RssUtils.cloneBitMap(idMap);
  }

  public boolean remove(int shuffleId) {
    blockIds.remove(shuffleId);
    return true;
  }
}
