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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;
import java.util.function.LongSupplier;

import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.common.exception.NoBufferException;
import org.apache.uniffle.common.netty.buffer.FileSegmentManagedBuffer;
import org.apache.uniffle.common.netty.buffer.ManagedBuffer;
import org.apache.uniffle.common.netty.buffer.MultiFileSegmentManagedBuffer;

/**
 * Server-wide immutable index snapshots for gRPC responses, bounded by the shared read memory
 * budget. A reservation belongs to a snapshot, not a reader. Removing an entry does not release its
 * reservation while a load or an RPC still owns it. The map holds only the current entry for each
 * partition range. All state transitions are under this monitor; disk I/O and waits are not.
 */
public class ShuffleIndexCache implements AutoCloseable {
  private final long expireNanos;
  private final LongPredicate reserveMemory;
  private final LongConsumer releaseMemory;
  private final LongSupplier clock;
  private final Map<Key, Entry> entries = new LinkedHashMap<>(16, 0.75f, true);
  private long reservedBytes;
  private boolean closed;

  public ShuffleIndexCache(long expireMs, LongPredicate reserveMemory, LongConsumer releaseMemory) {
    this(expireMs, reserveMemory, releaseMemory, System::nanoTime);
  }

  ShuffleIndexCache(
      long expireMs, LongPredicate reserveMemory, LongConsumer releaseMemory, LongSupplier clock) {
    if (expireMs <= 0) {
      throw new IllegalArgumentException("Index cache expiry must be positive");
    }
    this.expireNanos = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(expireMs);
    this.reserveMemory = reserveMemory;
    this.releaseMemory = releaseMemory;
    this.clock = clock;
  }

  public Lease acquire(
      String appId, int shuffleId, int partitionRangeStart, ShuffleIndexResult result)
      throws IOException {
    Source source = Source.from(result);
    Key key = new Key(appId, shuffleId, partitionRangeStart);
    Entry entry;
    boolean load = false;
    synchronized (this) {
      if (closed) {
        throw new IOException("Index cache is closed");
      }
      entry = entries.get(key);
      // Keep cache hits O(1) in the number of cached partitions. The scheduled cleaner handles
      // other expired entries; capacity eviction can reclaim them when admitting a new load.
      if (entry != null && (isExpired(entry, clock.getAsLong()) || !entry.source.matches(source))) {
        entries.remove(key);
        release(entry);
        entry = null;
      }
      if (entry == null) {
        reserve(source.length);
        entry = new Entry(source, clock.getAsLong());
        entries.put(key, entry);
        load = true;
      }
      entry.references++;
      entry.lastAccessNanos = clock.getAsLong();
    }
    try {
      if (load) {
        try {
          entry.data.complete(load(source));
        } catch (Throwable failure) {
          synchronized (this) {
            if (entries.get(key) == entry) {
              entries.remove(key);
              release(entry);
            }
          }
          entry.data.completeExceptionally(failure);
        }
      }
      entry.data.get();
      return new Lease(this, entry, result.getDataFileLen(), result.getStorageIds());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      release(entry);
      throw new IOException("Interrupted waiting for a shared shuffle index", e);
    } catch (ExecutionException e) {
      release(entry);
      Throwable cause = e.getCause();
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException("Unable to load shared shuffle index", cause);
    }
  }

  // Kept separate to test concurrent misses, I/O failures and invalidation during a load.
  byte[] load(Source source) throws IOException {
    byte[] data = new byte[source.length];
    int offset = 0;
    for (FileSegmentManagedBuffer file : source.files) {
      // Read into the final array, including composite indexes. FileChannel.read(heapBuffer)
      // would additionally allocate/cache a large temporary direct buffer per loading thread.
      try (RandomAccessFile input = new RandomAccessFile(file.getFile(), "r")) {
        input.seek(file.getOffset());
        input.readFully(data, offset, file.size());
      }
      offset += file.size();
    }
    return data;
  }

  private void reserve(long size) {
    Iterator<Entry> iterator = entries.values().iterator();
    while (true) {
      if (reserveMemory.test(size)) {
        reservedBytes += size;
        return;
      }
      boolean evicted = false;
      while (iterator.hasNext()) {
        Entry candidate = iterator.next();
        if (candidate.references == 1) {
          iterator.remove();
          release(candidate);
          evicted = true;
          break;
        }
      }
      if (!evicted) {
        throw new NoBufferException("No memory available for a shared shuffle index snapshot");
      }
    }
  }

  public synchronized void cleanUp() {
    long now = clock.getAsLong();
    Iterator<Entry> iterator = entries.values().iterator();
    while (iterator.hasNext()) {
      Entry entry = iterator.next();
      // Active snapshots remain reusable; expiry starts after the last reader releases them.
      if (isExpired(entry, now)) {
        iterator.remove();
        release(entry);
      }
    }
  }

  private boolean isExpired(Entry entry, long now) {
    return entry.references == 1 && now - entry.lastAccessNanos >= expireNanos;
  }

  public synchronized void invalidate(String appId, List<Integer> shuffleIds) {
    Iterator<Map.Entry<Key, Entry>> iterator = entries.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Key, Entry> item = iterator.next();
      Key key = item.getKey();
      if (key.appId.equals(appId) && (shuffleIds == null || shuffleIds.contains(key.shuffleId))) {
        iterator.remove();
        release(item.getValue());
      }
    }
  }

  private synchronized void release(Entry entry) {
    entry.lastAccessNanos = clock.getAsLong();
    if (--entry.references == 0) {
      reservedBytes -= entry.source.length;
      releaseMemory.accept(entry.source.length);
    }
  }

  @Override
  public synchronized void close() {
    closed = true;
    entries.values().forEach(this::release);
    entries.clear();
  }

  synchronized long getReservedBytes() {
    return reservedBytes;
  }

  synchronized int getEntryCount() {
    return entries.size();
  }

  synchronized int getReaderCount() {
    return entries.values().stream().mapToInt(entry -> entry.references - 1).sum();
  }

  public static final class Lease implements AutoCloseable {
    private final ShuffleIndexCache owner;
    private final AtomicReference<Entry> entry;
    private final long dataFileLen;
    private final int[] storageIds;

    private Lease(ShuffleIndexCache owner, Entry entry, long dataFileLen, int[] storageIds) {
      this.owner = owner;
      this.entry = new AtomicReference<>(entry);
      this.dataFileLen = dataFileLen;
      this.storageIds = storageIds.clone();
    }

    public ByteBuffer getIndexData() {
      return ByteBuffer.wrap(getIndexBytes()).asReadOnlyBuffer();
    }

    // Only for immutable protobuf wrapping and shared transport views. Never modify this array.
    byte[] getIndexBytes() {
      Entry current = entry.get();
      if (current == null) {
        throw new IllegalStateException("Shuffle index lease is closed");
      }
      return current.data.join();
    }

    public long getDataFileLen() {
      return dataFileLen;
    }

    public int[] getStorageIds() {
      return storageIds.clone();
    }

    @Override
    public void close() {
      Entry current = entry.getAndSet(null);
      if (current != null) {
        owner.release(current);
      }
    }
  }

  private static final class Entry {
    private final Source source;
    private final CompletableFuture<byte[]> data = new CompletableFuture<>();
    private long lastAccessNanos;
    // One reference for the cache, plus one for each acquiring request (including the loader).
    private int references = 1;

    private Entry(Source source, long lastAccessNanos) {
      this.source = source;
      this.lastAccessNanos = lastAccessNanos;
    }
  }

  private static final class Key {
    private final String appId;
    private final int shuffleId;
    private final int partitionRangeStart;

    private Key(String appId, int shuffleId, int partitionRangeStart) {
      this.appId = appId;
      this.shuffleId = shuffleId;
      this.partitionRangeStart = partitionRangeStart;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Key)) {
        return false;
      }
      Key key = (Key) other;
      return shuffleId == key.shuffleId
          && partitionRangeStart == key.partitionRangeStart
          && appId.equals(key.appId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(appId, shuffleId, partitionRangeStart);
    }
  }

  static final class Source {
    private final List<FileSegmentManagedBuffer> files = new ArrayList<>();
    private int length;

    private static Source from(ShuffleIndexResult result) throws IOException {
      Source source = new Source();
      source.add(result.getManagedBuffer());
      return source;
    }

    private void add(ManagedBuffer buffer) throws IOException {
      if (buffer instanceof MultiFileSegmentManagedBuffer) {
        for (ManagedBuffer child : ((MultiFileSegmentManagedBuffer) buffer).getManagedBuffers()) {
          add(child);
        }
      } else if (buffer instanceof FileSegmentManagedBuffer) {
        FileSegmentManagedBuffer segment = (FileSegmentManagedBuffer) buffer;
        files.add(segment);
        length = Math.addExact(length, segment.size());
      } else {
        throw new IOException("Unsupported shuffle index buffer: " + buffer.getClass().getName());
      }
    }

    private boolean matches(Source other) {
      if (files.size() != other.files.size()) {
        return false;
      }
      // The same append-only file is reusable while its complete-record length is unchanged.
      // Preserve file order for composite indexes; no timestamp or file-identity checks are needed.
      for (int i = 0; i < files.size(); i++) {
        FileSegmentManagedBuffer file = files.get(i);
        FileSegmentManagedBuffer next = other.files.get(i);
        if (!file.getFile().equals(next.getFile())
            || file.getOffset() != next.getOffset()
            || file.size() != next.size()) {
          return false;
        }
      }
      return true;
    }
  }
}
