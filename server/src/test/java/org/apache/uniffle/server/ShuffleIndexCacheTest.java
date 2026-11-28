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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.common.exception.NoBufferException;
import org.apache.uniffle.common.netty.buffer.FileSegmentManagedBuffer;
import org.apache.uniffle.common.netty.buffer.MultiFileSegmentManagedBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(20)
class ShuffleIndexCacheTest {
  @TempDir Path directory;

  private Path file(String name, int length, byte value) throws IOException {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, value);
    return Files.write(directory.resolve(name), bytes);
  }

  private ShuffleIndexResult index(Path path, long dataLength) throws IOException {
    int length = (int) (Files.size(path) / 40 * 40);
    return new ShuffleIndexResult(
        new FileSegmentManagedBuffer(path.toFile(), 0, length), dataLength, "data", 7);
  }

  @Test
  void concurrentReadersShareOneLoadAndIndependentReadOnlyViews() throws Exception {
    Path path = file("index", 80, (byte) 12);
    AtomicInteger loads = new AtomicInteger();
    AtomicLong charged = new AtomicLong();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch proceed = new CountDownLatch(1);
    ShuffleIndexCache cache =
        new ShuffleIndexCache(
            1000,
            n -> {
              charged.addAndGet(n);
              return true;
            },
            n -> charged.addAndGet(-n)) {
          @Override
          byte[] load(Source source) throws IOException {
            loads.incrementAndGet();
            started.countDown();
            try {
              if (!proceed.await(10, TimeUnit.SECONDS)) {
                throw new IOException("Timed out waiting for readers");
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IOException(e);
            }
            return super.load(source);
          }
        };
    ExecutorService pool = Executors.newFixedThreadPool(24);
    List<Future<ShuffleIndexCache.Lease>> futures = new ArrayList<>();
    try {
      ShuffleIndexResult result = index(path, 100);
      for (int i = 0; i < 24; i++) {
        futures.add(pool.submit(() -> cache.acquire("app", 1, 0, result)));
      }
      assertTrue(started.await(5, TimeUnit.SECONDS));
      Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> cache.getReaderCount() == 24);
      proceed.countDown();
      List<ShuffleIndexCache.Lease> leases = new ArrayList<>();
      for (Future<ShuffleIndexCache.Lease> future : futures) {
        leases.add(future.get(5, TimeUnit.SECONDS));
      }
      assertEquals(1, loads.get());
      assertEquals(80L, charged.get());
      ByteBuffer first = leases.get(0).getIndexData();
      first.get();
      ByteBuffer second = leases.get(1).getIndexData();
      assertEquals(0, second.position());
      assertEquals(80, second.remaining());
      assertEquals(12, second.get());
      assertThrows(ReadOnlyBufferException.class, () -> second.put(0, (byte) 1));
      assertEquals(0, leases.get(0).getIndexData().position());
      cache.invalidate("app", null);
      assertEquals(0, cache.getEntryCount());
      for (int i = 0; i < leases.size() - 1; i++) {
        leases.get(i).close();
      }
      assertEquals(80L, charged.get());
      leases.get(23).close();
      leases.get(23).close();
      assertEquals(0, charged.get());
    } finally {
      proceed.countDown();
      pool.shutdownNow();
      cache.close();
    }
  }

  @Test
  void appendCreatesNewSnapshotButMetadataAloneDoesNotReload() throws Exception {
    Path path = file("index", 40, (byte) 1);
    AtomicInteger loads = new AtomicInteger();
    try (ShuffleIndexCache cache = countingCache(100_000, loads)) {
      final ShuffleIndexCache.Lease old = cache.acquire("app", 1, 0, index(path, 100));
      try (ShuffleIndexCache.Lease metadata = cache.acquire("app", 1, 0, index(path, 200))) {
        assertEquals(200, metadata.getDataFileLen());
        assertArrayEquals(new int[] {7}, metadata.getStorageIds());
        assertEquals(1, loads.get());
      }
      byte[] appended = new byte[40];
      Arrays.fill(appended, (byte) 2);
      Files.write(path, appended, StandardOpenOption.APPEND);
      ShuffleIndexCache.Lease next = cache.acquire("app", 1, 0, index(path, 300));
      assertEquals(2, loads.get());
      assertEquals(40, old.getIndexData().remaining());
      assertEquals(80, next.getIndexData().remaining());
      assertEquals(2, next.getIndexData().get(40));
      assertEquals(40L + 80L, cache.getReservedBytes());
      old.close();
      assertEquals(80L, cache.getReservedBytes());
      next.close();
    }
  }

  @Test
  void sameLengthReusesCacheWithoutCheckingFileIdentityOrTimestamp() throws Exception {
    Path path = file("index", 40, (byte) 1);
    FileTime modified = Files.getLastModifiedTime(path);
    AtomicInteger loads = new AtomicInteger();
    try (ShuffleIndexCache cache = countingCache(100_000, loads)) {
      cache.acquire("app", 1, 0, index(path, 100)).close();
      Files.move(path, directory.resolve("previous"));
      file("index", 40, (byte) 2);
      Files.setLastModifiedTime(path, FileTime.fromMillis(modified.toMillis() + 1000));
      try (ShuffleIndexCache.Lease hit = cache.acquire("app", 1, 0, index(path, 200))) {
        assertEquals(1, loads.get());
        assertEquals(1, hit.getIndexData().get(0));
        assertEquals(200, hit.getDataFileLen());
      }
      // Reusing the path for a same-length rewrite requires explicit lifecycle invalidation.
      cache.invalidate("app", null);
      try (ShuffleIndexCache.Lease next = cache.acquire("app", 1, 0, index(path, 200))) {
        assertEquals(2, loads.get());
        assertEquals(2, next.getIndexData().get(0));
      }
    }
  }

  @Test
  void incompleteAppendReusesCacheUntilACompleteRecordIsAvailable() throws Exception {
    Path path = file("index", 40, (byte) 1);
    AtomicInteger loads = new AtomicInteger();
    try (ShuffleIndexCache cache = countingCache(100_000, loads)) {
      cache.acquire("app", 1, 0, index(path, 100)).close();
      Files.write(path, new byte[3], StandardOpenOption.APPEND);
      try (ShuffleIndexCache.Lease hit = cache.acquire("app", 1, 0, index(path, 200))) {
        assertEquals(1, loads.get());
        assertEquals(40, hit.getIndexData().remaining());
      }
      Files.write(path, new byte[37], StandardOpenOption.APPEND);
      try (ShuffleIndexCache.Lease next = cache.acquire("app", 1, 0, index(path, 200))) {
        assertEquals(2, loads.get());
        assertEquals(80, next.getIndexData().remaining());
        assertEquals(1, cache.getEntryCount());
      }
    }
  }

  @Test
  void shorterFileReplacesCachedIndex() throws Exception {
    Path path = file("index", 80, (byte) 1);
    AtomicInteger loads = new AtomicInteger();
    try (ShuffleIndexCache cache = countingCache(100_000, loads)) {
      cache.acquire("app", 1, 0, index(path, 100)).close();
      file("index", 40, (byte) 2);
      try (ShuffleIndexCache.Lease next = cache.acquire("app", 1, 0, index(path, 50))) {
        assertEquals(2, loads.get());
        assertEquals(40, next.getIndexData().remaining());
        assertEquals(2, next.getIndexData().get(0));
        assertEquals(40L, cache.getReservedBytes());
        assertEquals(1, cache.getEntryCount());
      }
    }
  }

  @Test
  void compositeResultIsSharedAndFileOrderIsPreserved() throws Exception {
    Path a = file("a", 40, (byte) 1);
    Path b = file("b", 40, (byte) 2);
    AtomicInteger loads = new AtomicInteger();
    try (ShuffleIndexCache cache = countingCache(100_000, loads)) {
      ShuffleIndexResult forward = composite(a, b);
      try (ShuffleIndexCache.Lease first = cache.acquire("app", 1, 0, forward);
          ShuffleIndexCache.Lease second = cache.acquire("app", 1, 0, composite(a, b))) {
        assertEquals(1, loads.get());
        assertEquals(80, first.getIndexData().remaining());
        assertEquals(1, first.getIndexData().get(0));
        assertEquals(2, second.getIndexData().get(40));
        assertArrayEquals(new int[] {7, 8}, first.getStorageIds());
      }
      try (ShuffleIndexCache.Lease reversed = cache.acquire("app", 1, 0, composite(b, a))) {
        assertEquals(2, loads.get());
        assertEquals(2, reversed.getIndexData().get(0));
        assertEquals(1, reversed.getIndexData().get(40));
      }
    }
  }

  private ShuffleIndexResult composite(Path a, Path b) throws IOException {
    return new ShuffleIndexResult(
        new MultiFileSegmentManagedBuffer(
            Arrays.asList(index(a, 1).getManagedBuffer(), index(b, 1).getManagedBuffer())),
        2,
        "data",
        new int[] {7, 8});
  }

  @Test
  void pinnedEvictedSnapshotStaysChargedAndPreventsUnboundedReload() throws Exception {
    Path path = file("index", 40, (byte) 1);
    AtomicInteger loads = new AtomicInteger();
    try (ShuffleIndexCache cache = countingCache(40L + 1, loads)) {
      final ShuffleIndexCache.Lease lease = cache.acquire("app", 1, 0, index(path, 100));
      cache.invalidate("app", Collections.singletonList(1));
      assertEquals(40L, cache.getReservedBytes());
      assertThrows(NoBufferException.class, () -> cache.acquire("app", 1, 0, index(path, 100)));
      assertEquals(1, loads.get());
      assertEquals(1, lease.getIndexData().get(0));
      lease.close();
      try (ShuffleIndexCache.Lease next = cache.acquire("app", 1, 0, index(path, 100))) {
        assertEquals(2, loads.get());
      }
    }
  }

  @Test
  void idleExpiryAndCloseReleaseOnlyUnpinnedSnapshots() throws Exception {
    Path path = file("index", 40, (byte) 1);
    AtomicLong time = new AtomicLong();
    AtomicLong charged = new AtomicLong();
    ShuffleIndexCache cache =
        new ShuffleIndexCache(
            10,
            n -> {
              charged.addAndGet(n);
              return true;
            },
            n -> charged.addAndGet(-n),
            time::get);
    final ShuffleIndexCache.Lease lease = cache.acquire("app", 1, 0, index(path, 100));
    time.addAndGet(TimeUnit.SECONDS.toNanos(1));
    cache.cleanUp();
    assertEquals(1, cache.getEntryCount());
    lease.close();
    time.addAndGet(TimeUnit.MILLISECONDS.toNanos(11));
    cache.cleanUp();
    assertEquals(0, cache.getEntryCount());
    assertEquals(0, charged.get());
    final ShuffleIndexCache.Lease active = cache.acquire("app", 1, 0, index(path, 100));
    cache.close();
    assertEquals(40L, charged.get());
    assertThrows(IOException.class, () -> cache.acquire("app", 1, 0, index(path, 100)));
    active.close();
    assertEquals(0, charged.get());
  }

  @Test
  void readBudgetPressureEvictsLeastRecentlyUsedIdleEntry() throws Exception {
    Path path = file("index", 40, (byte) 1);
    AtomicInteger loads = new AtomicInteger();
    try (ShuffleIndexCache cache = countingCache(2 * 40L + 1, loads)) {
      cache.acquire("app", 1, 0, index(path, 100)).close();
      cache.acquire("app", 2, 0, index(path, 100)).close();
      cache.acquire("app", 1, 0, index(path, 100)).close();
      cache.acquire("app", 3, 0, index(path, 100)).close();
      assertEquals(3, loads.get());
      cache.acquire("app", 1, 0, index(path, 100)).close();
      assertEquals(3, loads.get());
      cache.acquire("app", 2, 0, index(path, 100)).close();
      assertEquals(4, loads.get());
      assertEquals(2 * 40L, cache.getReservedBytes());
    }
  }

  @Test
  void failedLoadReleasesBudgetAndAllowsRetry() throws Exception {
    Path path = file("index", 40, (byte) 1);
    AtomicInteger loads = new AtomicInteger();
    try (ShuffleIndexCache cache =
        new ShuffleIndexCache(1000, n -> true, n -> {}) {
          @Override
          byte[] load(Source source) throws IOException {
            if (loads.incrementAndGet() == 1) {
              throw new IOException("Injected read failure");
            }
            return super.load(source);
          }
        }) {
      assertThrows(IOException.class, () -> cache.acquire("app", 1, 0, index(path, 100)));
      assertEquals(0, cache.getReservedBytes());
      assertEquals(0, cache.getEntryCount());
      try (ShuffleIndexCache.Lease lease = cache.acquire("app", 1, 0, index(path, 100))) {
        assertEquals(1, lease.getIndexData().get(0));
        assertEquals(2, loads.get());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void invalidationDuringLoadReleasesReservationOnceAndNeverRepublishesEntry(boolean failLoad)
      throws Exception {
    Path path = file("index", 40, (byte) 1);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch proceed = new CountDownLatch(1);
    ShuffleIndexCache cache =
        new ShuffleIndexCache(1000, n -> true, n -> {}) {
          @Override
          byte[] load(Source source) throws IOException {
            entered.countDown();
            try {
              if (!proceed.await(5, TimeUnit.SECONDS)) {
                throw new IOException("timeout");
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IOException(e);
            }
            if (failLoad) {
              throw new IOException("Injected read failure after invalidation");
            }
            return super.load(source);
          }
        };
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<ShuffleIndexCache.Lease> future =
          pool.submit(() -> cache.acquire("app", 1, 0, index(path, 100)));
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      cache.invalidate("app", null);
      assertEquals(40L, cache.getReservedBytes());
      proceed.countDown();
      if (failLoad) {
        assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
      } else {
        try (ShuffleIndexCache.Lease lease = future.get(5, TimeUnit.SECONDS)) {
          assertEquals(40L, cache.getReservedBytes());
          assertEquals(40, lease.getIndexData().remaining());
        }
      }
      assertEquals(0, cache.getEntryCount());
      assertEquals(0, cache.getReservedBytes());
    } finally {
      proceed.countDown();
      pool.shutdownNow();
      cache.close();
    }
  }

  @Test
  void purgeOnlyRemovesSelectedApplicationAndShuffles() throws Exception {
    Path path = file("index", 40, (byte) 1);
    try (ShuffleIndexCache cache = countingCache(100_000, new AtomicInteger())) {
      cache.acquire("a", 1, 0, index(path, 100)).close();
      cache.acquire("a", 2, 0, index(path, 100)).close();
      cache.acquire("b", 1, 0, index(path, 100)).close();
      cache.invalidate("a", Collections.singletonList(1));
      assertEquals(2, cache.getEntryCount());
      cache.invalidate("a", null);
      assertEquals(1, cache.getEntryCount());
      assertEquals(40L, cache.getReservedBytes());
    }
  }

  @Test
  void insufficientReadCapacityRejectsBeforeLoading() throws Exception {
    Path path = file("index", 40, (byte) 1);
    AtomicInteger loads = new AtomicInteger();
    try (ShuffleIndexCache cache = countingCache(10, loads)) {
      assertThrows(NoBufferException.class, () -> cache.acquire("app", 1, 0, index(path, 100)));
      assertEquals(0, loads.get());
      assertEquals(0, cache.getReservedBytes());
    }
    try (ShuffleIndexCache cache = new ShuffleIndexCache(1000, n -> false, n -> {})) {
      assertThrows(NoBufferException.class, () -> cache.acquire("app", 1, 0, index(path, 100)));
      assertEquals(0, cache.getEntryCount());
      assertEquals(0, cache.getReservedBytes());
    }
  }

  @Test
  void appendDuringLoadKeepsCompletePrefixAndNextReadSeesNewVersion() throws Exception {
    Path path = file("index", 40, (byte) 1);
    AtomicInteger loads = new AtomicInteger();
    try (ShuffleIndexCache cache =
        new ShuffleIndexCache(60_000, n -> true, n -> {}) {
          @Override
          byte[] load(Source source) throws IOException {
            if (loads.incrementAndGet() == 1) {
              byte[] appended = new byte[43];
              Arrays.fill(appended, (byte) 2);
              Files.write(path, appended, StandardOpenOption.APPEND);
            }
            return super.load(source);
          }
        }) {
      try (ShuffleIndexCache.Lease first = cache.acquire("app", 1, 0, index(path, 100))) {
        assertEquals(40, first.getIndexData().remaining());
        assertEquals(1, first.getIndexData().get(39));
      }
      try (ShuffleIndexCache.Lease next = cache.acquire("app", 1, 0, index(path, 200))) {
        assertEquals(80, next.getIndexData().remaining());
        assertEquals(2, next.getIndexData().get(79));
        assertEquals(200, next.getDataFileLen());
      }
      assertEquals(2, loads.get());
    }
  }

  @Test
  void truncationDuringLoadIsNotPublishedAsAnEmptySuccessfulIndex() throws Exception {
    Path path = file("index", 80, (byte) 1);
    try (ShuffleIndexCache cache =
        new ShuffleIndexCache(60_000, n -> true, n -> {}) {
          @Override
          byte[] load(Source source) throws IOException {
            Files.write(path, new byte[40], StandardOpenOption.TRUNCATE_EXISTING);
            return super.load(source);
          }
        }) {
      assertThrows(IOException.class, () -> cache.acquire("app", 1, 0, index(path, 100)));
      assertEquals(0, cache.getEntryCount());
      assertEquals(0, cache.getReservedBytes());
    }
  }

  @Test
  void accessingExpiredEntryReloadsWithoutWaitingForPeriodicCleanup() throws Exception {
    Path path = file("index", 40, (byte) 1);
    AtomicLong time = new AtomicLong();
    AtomicInteger loads = new AtomicInteger();
    try (ShuffleIndexCache cache =
        new ShuffleIndexCache(10, n -> true, n -> {}, time::get) {
          @Override
          byte[] load(Source source) throws IOException {
            loads.incrementAndGet();
            return super.load(source);
          }
        }) {
      cache.acquire("app", 1, 0, index(path, 100)).close();
      time.addAndGet(TimeUnit.MILLISECONDS.toNanos(11));
      cache.acquire("app", 1, 0, index(path, 100)).close();
      assertEquals(2, loads.get());
      assertEquals(1, cache.getEntryCount());
      assertEquals(40L, cache.getReservedBytes());
    }
  }

  private ShuffleIndexCache countingCache(long readCapacity, AtomicInteger loads) {
    AtomicLong readMemory = new AtomicLong();
    return new ShuffleIndexCache(
        60_000,
        n -> {
          if (readMemory.get() + n >= readCapacity) {
            return false;
          }
          readMemory.addAndGet(n);
          return true;
        },
        n -> readMemory.addAndGet(-n)) {
      @Override
      byte[] load(Source source) throws IOException {
        loads.incrementAndGet();
        return super.load(source);
      }
    };
  }
}
