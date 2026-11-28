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

package org.apache.uniffle.server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.common.StorageType;
import org.apache.uniffle.common.exception.NoBufferException;
import org.apache.uniffle.common.netty.buffer.FileSegmentManagedBuffer;
import org.apache.uniffle.server.buffer.ShuffleBufferManager;
import org.apache.uniffle.server.storage.StorageManager;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.handler.api.ServerReadHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ShuffleIndexCacheTaskManagerTest {
  @TempDir Path directory;

  @Test
  void shuffleAndApplicationCleanupInvalidateCacheWithoutBreakingExistingReaders()
      throws Exception {
    ShuffleServerMetrics.clear();
    ShuffleServerMetrics.register();
    final Path file = Files.write(directory.resolve("index"), new byte[40]);
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.set(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.MEMORY_LOCALFILE);
    ShuffleBufferManager buffers = mock(ShuffleBufferManager.class);
    when(buffers.requireReadMemory(anyLong())).thenReturn(true);
    ShuffleTaskManager tasks = taskManager(conf, buffers, file);
    try {
      tasks.registerShuffle(
          "app",
          1,
          Collections.singletonList(new PartitionRange(0, 1)),
          new RemoteStorageInfo(""),
          "user");
      final ShuffleIndexCache.Lease first = tasks.getCachedShuffleIndex("app", 1, 0, 2, 2);
      final ShuffleIndexCache.Lease sameRange = tasks.getCachedShuffleIndex("app", 1, 1, 2, 2);
      ShuffleIndexCache cache = tasks.getShuffleIndexCache();
      assertEquals(1, cache.getEntryCount());
      long indexSize = cache.getReservedBytes();
      tasks.removeResourcesByShuffleIds("app", Collections.singletonList(1));
      assertEquals(0, cache.getEntryCount());
      assertEquals(indexSize, cache.getReservedBytes());
      assertEquals(40, first.getIndexData().remaining());
      first.close();
      sameRange.close();
      assertEquals(0, cache.getReservedBytes());
      assertEquals(0, ShuffleServerMetrics.gaugeReadLocalIndexFileBufferSize.get());

      tasks.registerShuffle(
          "app",
          2,
          Collections.singletonList(new PartitionRange(0, 0)),
          new RemoteStorageInfo(""),
          "user");
      final ShuffleIndexCache.Lease second = tasks.getCachedShuffleIndex("app", 2, 0, 1, 1);
      assertTrue(cache.getReservedBytes() > 0);
      tasks.removeResources("app", false);
      assertEquals(0, cache.getEntryCount());
      assertTrue(cache.getReservedBytes() > 0);
      second.close();
      assertEquals(0, cache.getReservedBytes());
      assertEquals(0, ShuffleServerMetrics.gaugeReadLocalIndexFileBufferSize.get());
    } finally {
      tasks.stop();
    }
  }

  @Test
  void cacheSharesReadCapacityWithOtherReadsAndEvictsIdleSnapshots() throws Exception {
    ShuffleServerMetrics.clear();
    ShuffleServerMetrics.register();
    Path file = Files.write(directory.resolve("index"), new byte[40]);
    long indexSize = 40;
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.set(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.MEMORY_LOCALFILE);
    conf.set(ShuffleServerConf.SERVER_READ_BUFFER_CAPACITY, 4 * indexSize);
    ShuffleBufferManager buffers =
        new ShuffleBufferManager(conf, mock(ShuffleFlushManager.class), false);
    ShuffleTaskManager tasks = taskManager(conf, buffers, file);
    try {
      tasks.registerShuffle(
          "app",
          1,
          Collections.singletonList(new PartitionRange(0, 1)),
          new RemoteStorageInfo(""),
          "user");
      assertTrue(buffers.requireReadMemory(2 * indexSize));
      try {
        ShuffleIndexCache cache = tasks.getShuffleIndexCache();
        try (ShuffleIndexCache.Lease first = tasks.getCachedShuffleIndex("app", 1, 0, 1, 2);
            ShuffleIndexCache.Lease hit = tasks.getCachedShuffleIndex("app", 1, 0, 1, 2)) {
          assertEquals(indexSize, cache.getReservedBytes());
          assertEquals(3 * indexSize, ShuffleServerMetrics.gaugeReadBufferUsedSize.get());
          assertEquals(indexSize, ShuffleServerMetrics.gaugeReadLocalIndexFileBufferSize.get());
          // Exactly reaching readCapacity is rejected by the existing read memory admission rule.
          assertThrows(
              NoBufferException.class, () -> tasks.getCachedShuffleIndex("app", 1, 1, 1, 2));
          assertEquals(1, cache.getEntryCount());
          assertEquals(3 * indexSize, ShuffleServerMetrics.gaugeReadBufferUsedSize.get());
        }
        // The first snapshot is now idle: reclaim it to admit the second partition's index.
        try (ShuffleIndexCache.Lease next = tasks.getCachedShuffleIndex("app", 1, 1, 1, 2)) {
          assertEquals(40, next.getIndexData().remaining());
          assertEquals(1, cache.getEntryCount());
          assertEquals(indexSize, cache.getReservedBytes());
          assertEquals(3 * indexSize, ShuffleServerMetrics.gaugeReadBufferUsedSize.get());
          assertThrows(
              NoBufferException.class, () -> tasks.getCachedShuffleIndex("app", 1, 0, 1, 2));
          cache.invalidate("app", null);
          assertEquals(3 * indexSize, ShuffleServerMetrics.gaugeReadBufferUsedSize.get());
        }
        assertEquals(2 * indexSize, ShuffleServerMetrics.gaugeReadBufferUsedSize.get());
        assertEquals(0, ShuffleServerMetrics.gaugeReadLocalIndexFileBufferSize.get());
      } finally {
        buffers.releaseReadMemory(2 * indexSize);
      }
      assertEquals(0, ShuffleServerMetrics.gaugeReadBufferUsedSize.get());
    } finally {
      tasks.stop();
    }
  }

  private ShuffleTaskManager taskManager(
      ShuffleServerConf conf, ShuffleBufferManager buffers, Path file) {
    StorageManager storageManager = mock(StorageManager.class);
    Storage storage = mock(Storage.class);
    ServerReadHandler handler = mock(ServerReadHandler.class);
    when(storageManager.selectStorageById(any())).thenReturn(storage);
    when(storage.getOrCreateReadHandler(any())).thenReturn(handler);
    when(handler.getShuffleIndex())
        .thenAnswer(
            invocation ->
                new ShuffleIndexResult(
                    new FileSegmentManagedBuffer(file.toFile(), 0, 40), 100, "data", 0));
    return new ShuffleTaskManager(conf, mock(ShuffleFlushManager.class), buffers, storageManager);
  }
}
