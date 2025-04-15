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

package org.apache.uniffle.storage.common;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.JavaUtils;

/**
 * Metadata has three dimensions from top to down including disk, shuffle, partition. And each
 * dimension contains two aspects, status data and indicator data. Disk status data contains
 * writable flag, Shuffle status data contains stable, uploading, deleting flag. Disk indicator data
 * contains size, fileNum, shuffleNum, Shuffle indicator contains size, partition list, uploaded
 * partition list and uploaded size.
 */
public class LocalStorageMeta {

  private static final Logger LOG = LoggerFactory.getLogger(LocalStorageMeta.class);
  private final AtomicLong size = new AtomicLong(0L);
  private final Map<String, ShuffleMeta> shuffleMetaMap = JavaUtils.newConcurrentMap();

  public void updateDiskSize(long delta) {
    size.addAndGet(delta);
  }

  public void updateShuffleSize(String shuffleId, long delta) {
    ShuffleMeta shuffleMeta = getShuffleMeta(shuffleId);
    if (shuffleMeta != null) {
      shuffleMeta.getSize().addAndGet(delta);
    }
  }

  public void addShufflePartitionList(String shuffleKey, List<Integer> partitions) {
    ShuffleMeta shuffleMeta = getShuffleMeta(shuffleKey);
    if (shuffleMeta != null) {
      RoaringBitmap bitmap = shuffleMeta.partitionBitmap;
      synchronized (bitmap) {
        partitions.forEach(bitmap::add);
      }
    }
  }

  public void prepareStartRead(String shuffleId) {
    ShuffleMeta shuffleMeta = getShuffleMeta(shuffleId);
    if (shuffleMeta != null) {
      shuffleMeta.markStartRead();
    }
  }

  public void removeShuffle(String shuffleKey) {
    shuffleMetaMap.remove(shuffleKey);
  }

  public AtomicLong getDiskSize() {
    return size;
  }

  public long getShuffleSize(String shuffleKey) {
    ShuffleMeta shuffleMeta = getShuffleMeta(shuffleKey);
    return shuffleMeta == null ? 0 : shuffleMeta.getSize().get();
  }

  public Set<String> getShuffleMetaSet() {
    return shuffleMetaMap.keySet();
  }

  @VisibleForTesting
  public void setSize(long diskSize) {
    this.size.set(diskSize);
  }

  public void createMetadataIfNotExist(String shuffleKey) {
    shuffleMetaMap.computeIfAbsent(
        shuffleKey,
        key -> {
          LOG.info("Create metadata of shuffle {}.", key);
          return new ShuffleMeta();
        });
  }

  private ShuffleMeta getShuffleMeta(String shuffleKey) {
    ShuffleMeta shuffleMeta = shuffleMetaMap.get(shuffleKey);
    if (shuffleMeta == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Shuffle {} metadata has been removed!", shuffleKey);
      }
    }
    return shuffleMeta;
  }

  public void updateShuffleLastReadTs(String shuffleKey) {
    ShuffleMeta shuffleMeta = getShuffleMeta(shuffleKey);
    if (shuffleMeta != null) {
      shuffleMeta.updateLastReadTs();
    }
  }

  // Consider that ShuffleMeta is a simple class, we keep the class ShuffleMeta as an inner class.
  private static class ShuffleMeta {
    private final AtomicLong size = new AtomicLong(0);
    private final RoaringBitmap partitionBitmap = RoaringBitmap.bitmapOf();
    private final AtomicBoolean isStartRead = new AtomicBoolean(false);
    private final AtomicLong lastReadTs = new AtomicLong(-1L);

    public AtomicLong getSize() {
      return size;
    }

    public void markStartRead() {
      isStartRead.set(true);
    }

    public void updateLastReadTs() {
      lastReadTs.set(System.currentTimeMillis());
    }
  }
}
