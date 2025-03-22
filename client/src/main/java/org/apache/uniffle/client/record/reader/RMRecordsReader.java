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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.factory.ShuffleServerClientFactory;
import org.apache.uniffle.client.record.Record;
import org.apache.uniffle.client.record.RecordBlob;
import org.apache.uniffle.client.record.RecordBuffer;
import org.apache.uniffle.client.record.metrics.MetricsReporter;
import org.apache.uniffle.client.record.writer.Combiner;
import org.apache.uniffle.client.request.RssGetSortedShuffleDataRequest;
import org.apache.uniffle.client.response.RssGetSortedShuffleDataResponse;
import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.merger.MergeState;
import org.apache.uniffle.common.merger.Merger;
import org.apache.uniffle.common.netty.buffer.ManagedBuffer;
import org.apache.uniffle.common.records.RecordsReader;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.serializer.SerInputStream;
import org.apache.uniffle.common.serializer.Serializer;
import org.apache.uniffle.common.serializer.SerializerFactory;
import org.apache.uniffle.common.serializer.SerializerInstance;
import org.apache.uniffle.common.serializer.writable.ComparativeOutputBuffer;
import org.apache.uniffle.common.util.JavaUtils;

import static org.apache.uniffle.common.config.RssClientConf.RSS_CLIENT_REMOTE_MERGE_FETCH_INIT_SLEEP_MS;
import static org.apache.uniffle.common.config.RssClientConf.RSS_CLIENT_REMOTE_MERGE_FETCH_MAX_SLEEP_MS;
import static org.apache.uniffle.common.config.RssClientConf.RSS_CLIENT_REMOTE_MERGE_READER_MAX_BUFFER;
import static org.apache.uniffle.common.config.RssClientConf.RSS_CLIENT_REMOTE_MERGE_READER_MAX_RECORDS_PER_BUFFER;

public class RMRecordsReader<K, V, C> {

  private static final Logger LOG = LoggerFactory.getLogger(RMRecordsReader.class);

  private String appId;
  private final int shuffleId;
  private final Set<Integer> partitionIds;
  private final RssConf rssConf;
  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final Comparator comparator;
  private boolean raw;
  private final Combiner combiner;
  private boolean isMapCombine;
  private final MetricsReporter metrics;
  private final String clientType;
  private SerializerInstance serializerInstance;

  private final int retryMax;
  private final long retryIntervalMax;
  private final long initFetchSleepTime;
  private final long maxFetchSleepTime;
  private final int maxBufferPerPartition;
  private final int maxRecordsNumPerBuffer;

  private Map<Integer, List<ShuffleServerInfo>> shuffleServerInfoMap;
  private volatile boolean stop = false;
  private volatile Throwable error = null;

  private Map<Integer, Queue<RecordBuffer>> combineBuffers = JavaUtils.newConcurrentMap();
  private Map<Integer, Queue<RecordBuffer>> mergeBuffers = JavaUtils.newConcurrentMap();
  private Queue<Record> results;

  public RMRecordsReader(
      String appId,
      int shuffleId,
      Set<Integer> partitionIds,
      Map<Integer, List<ShuffleServerInfo>> shuffleServerInfoMap,
      RssConf rssConf,
      Class<K> keyClass,
      Class<V> valueClass,
      Comparator<K> comparator,
      boolean raw,
      Combiner combiner,
      boolean isMapCombine,
      MetricsReporter metrics) {
    this(
        appId,
        shuffleId,
        partitionIds,
        shuffleServerInfoMap,
        rssConf,
        keyClass,
        valueClass,
        comparator,
        raw,
        combiner,
        isMapCombine,
        metrics,
        ClientType.GRPC.name());
  }

  public RMRecordsReader(
      String appId,
      int shuffleId,
      Set<Integer> partitionIds,
      Map<Integer, List<ShuffleServerInfo>> shuffleServerInfoMap,
      RssConf rssConf,
      Class<K> keyClass,
      Class<V> valueClass,
      Comparator<K> comparator,
      boolean raw,
      Combiner combiner,
      boolean isMapCombine,
      MetricsReporter metrics,
      String clientType) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionIds = partitionIds;
    this.shuffleServerInfoMap = shuffleServerInfoMap;
    this.rssConf = rssConf;
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.raw = raw;
    if (raw && comparator == null) {
      throw new RssException("RawComparator must be set!");
    }
    this.comparator =
        comparator != null
            ? comparator
            : new Comparator<K>() {
              @Override
              public int compare(K o1, K o2) {
                int h1 = (o1 == null) ? 0 : o1.hashCode();
                int h2 = (o2 == null) ? 0 : o2.hashCode();
                return h1 < h2 ? -1 : h1 == h2 ? 0 : 1;
              }
            };
    this.combiner = combiner;
    this.isMapCombine = isMapCombine;
    this.metrics = metrics;
    this.clientType = clientType;
    if (this.raw) {
      SerializerFactory factory = new SerializerFactory(rssConf);
      Serializer serializer = factory.getSerializer(keyClass);
      assert factory.getSerializer(valueClass).getClass().equals(serializer.getClass());
      this.serializerInstance = serializer.newInstance();
    }

    this.initFetchSleepTime = rssConf.get(RSS_CLIENT_REMOTE_MERGE_FETCH_INIT_SLEEP_MS);
    this.maxFetchSleepTime = rssConf.get(RSS_CLIENT_REMOTE_MERGE_FETCH_MAX_SLEEP_MS);
    int maxBuffer = rssConf.get(RSS_CLIENT_REMOTE_MERGE_READER_MAX_BUFFER);
    this.maxBufferPerPartition = Math.max(1, maxBuffer / partitionIds.size());
    this.maxRecordsNumPerBuffer =
        rssConf.get(RSS_CLIENT_REMOTE_MERGE_READER_MAX_RECORDS_PER_BUFFER);
    this.results =
        new Queue<>(maxBufferPerPartition * maxRecordsNumPerBuffer * partitionIds.size());
    this.retryMax =
        rssConf.getInteger(
            RssClientConfig.RSS_CLIENT_RETRY_MAX,
            RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE);
    this.retryIntervalMax =
        rssConf.getLong(
            RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
            RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE);
    LOG.info("RMRecordsReader constructed for partitions {}", partitionIds);
  }

  public void start() {
    for (int partitionId : partitionIds) {
      mergeBuffers.put(partitionId, new Queue<>(maxBufferPerPartition));
      if (this.combiner != null) {
        combineBuffers.put(partitionId, new Queue<>(maxBufferPerPartition));
      }
      RecordsFetcher fetcher = new RecordsFetcher(partitionId);
      fetcher.start();
      if (this.combiner != null) {
        RecordsCombiner combineThread = new RecordsCombiner(partitionId);
        combineThread.start();
      }
    }

    RecordsMerger recordMerger = new RecordsMerger();
    recordMerger.start();
  }

  public void close() {
    error = null;
    stop = true;
    for (Queue<RecordBuffer> buffer : mergeBuffers.values()) {
      buffer.clear();
    }
    mergeBuffers.clear();
    if (combiner != null) {
      for (Queue<RecordBuffer> buffer : combineBuffers.values()) {
        buffer.clear();
      }
      combineBuffers.clear();
    }
    if (results != null) {
      this.results.clear();
      this.results = null;
    }
  }

  private boolean isSameKey(Object k1, Object k2) {
    if (raw) {
      ComparativeOutputBuffer buffer1 = (ComparativeOutputBuffer) k1;
      ComparativeOutputBuffer buffer2 = (ComparativeOutputBuffer) k2;
      return ((RawComparator) this.comparator)
              .compare(
                  buffer1.getData(),
                  0,
                  buffer1.getLength(),
                  buffer2.getData(),
                  0,
                  buffer2.getLength())
          == 0;
    } else {
      return this.comparator.compare(k1, k2) == 0;
    }
  }

  public KeyValueReader<ComparativeOutputBuffer, ComparativeOutputBuffer> rawKeyValueReader() {
    if (!raw) {
      throw new RssException("rawKeyValueReader is not supported!");
    }
    return new KeyValueReader<ComparativeOutputBuffer, ComparativeOutputBuffer>() {

      private Record<ComparativeOutputBuffer, ComparativeOutputBuffer> curr = null;

      @Override
      public boolean hasNext() throws IOException {
        try {
          if (curr != null) {
            return true;
          }
          curr = results.take();
          return curr != null;
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }

      public Record<ComparativeOutputBuffer, ComparativeOutputBuffer> next() throws IOException {
        Record<ComparativeOutputBuffer, ComparativeOutputBuffer> next =
            Record.create(curr.getKey(), curr.getValue());
        curr = null;
        return next;
      }
    };
  }

  public KeyValueReader<K, C> keyValueReader() {
    return new KeyValueReader<K, C>() {

      private Record<K, C> curr = null;

      @Override
      public boolean hasNext() throws IOException {
        try {
          if (curr != null) {
            return true;
          }
          curr = results.take();
          return curr != null;
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }

      public Record<K, C> next() throws IOException {
        Record record = Record.create(getCurrentKey(), getCurrentValue());
        curr = null;
        return record;
      }

      public K getCurrentKey() throws IOException {
        if (raw) {
          ComparativeOutputBuffer keyBuffer = (ComparativeOutputBuffer) curr.getKey();
          DataInputBuffer keyInputBuffer = new DataInputBuffer();
          keyInputBuffer.reset(keyBuffer.getData(), 0, keyBuffer.getLength());
          return serializerInstance.deserialize(keyInputBuffer, keyClass);
        } else {
          return curr.getKey();
        }
      }

      public C getCurrentValue() throws IOException {
        if (raw) {
          ComparativeOutputBuffer valueBuffer = (ComparativeOutputBuffer) curr.getValue();
          DataInputBuffer valueInputBuffer = new DataInputBuffer();
          valueInputBuffer.reset(valueBuffer.getData(), 0, valueBuffer.getLength());
          return serializerInstance.deserialize(valueInputBuffer, valueClass);
        } else {
          return curr.getValue();
        }
      }
    };
  }

  public KeyValuesReader<K, C> keyValuesReader() {
    return new KeyValuesReader() {

      private Record<K, C> start = null;

      @Override
      public boolean next() throws IOException {
        try {
          if (start == null) {
            start = results.take();
            return start != null;
          } else {
            return true;
          }
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }

      @Override
      public K getCurrentKey() throws IOException {
        if (raw) {
          ComparativeOutputBuffer keyBuffer = (ComparativeOutputBuffer) start.getKey();
          DataInputBuffer keyInputBuffer = new DataInputBuffer();
          keyInputBuffer.reset(keyBuffer.getData(), 0, keyBuffer.getLength());
          return serializerInstance.deserialize(keyInputBuffer, keyClass);
        } else {
          return start.getKey();
        }
      }

      @Override
      public Iterable<C> getCurrentValues() throws IOException {
        return new Iterable<C>() {
          @Override
          public Iterator<C> iterator() {

            return new Iterator<C>() {

              Record<K, C> curr = start;

              @Override
              public boolean hasNext() {
                if (curr != null && isSameKey(curr.getKey(), start.getKey())) {
                  return true;
                } else {
                  start = curr;
                  return false;
                }
              }

              @Override
              public C next() {
                try {
                  C ret;
                  if (raw) {
                    ComparativeOutputBuffer valueBuffer = (ComparativeOutputBuffer) curr.getValue();
                    DataInputBuffer valueInputBuffer = new DataInputBuffer();
                    valueInputBuffer.reset(valueBuffer.getData(), 0, valueBuffer.getLength());
                    ret = serializerInstance.deserialize(valueInputBuffer, valueClass);
                  } else {
                    ret = curr.getValue();
                  }
                  curr = results.take();
                  return ret;
                } catch (InterruptedException | IOException e) {
                  throw new RssException(e);
                }
              }
            };
          }
        };
      }
    };
  }

  class Queue<E> {

    private LinkedBlockingQueue<E> queue;
    private volatile boolean producerDone = false;

    Queue(int maxBufferPerPartition) {
      this.queue = new LinkedBlockingQueue<>(maxBufferPerPartition);
    }

    public void setProducerDone(boolean producerDone) {
      this.producerDone = producerDone;
    }

    public void put(E recordBuffer) throws InterruptedException {
      this.queue.put(recordBuffer);
    }

    // Block until data arrives or the producer completes the work.
    // If null is returned, it means that all data has been processed
    public E take() throws InterruptedException {
      while (!producerDone && !stop) {
        E e = this.queue.poll(100, TimeUnit.MILLISECONDS);
        if (e != null) {
          return e;
        }
      }
      if (error != null) {
        throw new RssException("RMShuffleReader fetch record failed, caused by " + error);
      }
      return this.queue.poll(100, TimeUnit.MILLISECONDS);
    }

    public void clear() {
      this.queue.clear();
      this.producerDone = false;
    }
  }

  class RecordsFetcher extends Thread {

    private int partitionId;
    private long sleepTime;
    private long blockId = 1; // Merged blockId counting from 1
    private RecordBuffer recordBuffer;
    private Queue<RecordBuffer> nextQueue;
    private List<ShuffleServerInfo> serverInfos;
    private ShuffleServerClient client;
    private int choose;
    private String fetchError;

    RecordsFetcher(int partitionId) {
      this.partitionId = partitionId;
      this.sleepTime = initFetchSleepTime;
      this.recordBuffer = new RecordBuffer<>(partitionId);
      this.nextQueue =
          combiner == null ? mergeBuffers.get(partitionId) : combineBuffers.get(partitionId);
      this.serverInfos = shuffleServerInfoMap.get(partitionId);
      this.choose = serverInfos.size() - 1;
      this.client = createShuffleServerClient(serverInfos.get(choose));
      setName("RecordsFetcher-" + partitionId);
    }

    private void nextShuffleServerInfo() {
      if (this.choose <= 0) {
        throw new RssException("Fetch sorted record failed, last error message is " + fetchError);
      }
      choose--;
      this.client = createShuffleServerClient(serverInfos.get(choose));
    }

    @Override
    public void run() {
      while (!stop) {
        try {
          RssGetSortedShuffleDataRequest request =
              new RssGetSortedShuffleDataRequest(
                  appId, shuffleId, partitionId, blockId, retryMax, retryIntervalMax);
          RssGetSortedShuffleDataResponse response = client.getSortedShuffleData(request);
          if (response.getStatusCode() != StatusCode.SUCCESS
              || response.getMergeState() == MergeState.INTERNAL_ERROR.code()) {
            fetchError = response.getMessage();
            nextShuffleServerInfo();
            break;
          } else if (response.getMergeState() == MergeState.INITED.code()) {
            fetchError = "Remote merge should be started!";
            nextShuffleServerInfo();
            break;
          }
          if (response.getMergeState() == MergeState.MERGING.code()
              && response.getNextBlockId() == -1) {
            // All merged data has been read, but there may be data that has not yet been merged. So
            // wait done!
            LOG.info("RMRecordsFetcher will sleep {} ms", sleepTime);
            Thread.sleep(this.sleepTime);
            this.sleepTime = Math.min(this.sleepTime * 2, maxFetchSleepTime);
          } else if (response.getMergeState() == MergeState.DONE.code()
              && response.getNextBlockId() == -1) {
            // All data has been read. Send the last records.
            if (recordBuffer.size() > 0) {
              nextQueue.put(recordBuffer);
            }
            nextQueue.setProducerDone(true);
            break;
          } else if (response.getMergeState() == MergeState.DONE.code()
              || response.getMergeState() == MergeState.MERGING.code()) {
            this.sleepTime = initFetchSleepTime;
            blockId = response.getNextBlockId();
            ManagedBuffer managedBuffer = null;
            ByteBuf byteBuf = null;
            RecordsReader<K, V> reader = null;
            try {
              managedBuffer = response.getData();
              byteBuf = managedBuffer.byteBuf();
              // Fetch blocks and parsing blocks are a synchronous process. If the two processes are
              // split into two different threads, then will be asynchronous processes. Although it
              // seems to save time, it actually consumes more memory.
              reader =
                  new RecordsReader<>(
                      rssConf,
                      SerInputStream.newInputStream(byteBuf),
                      keyClass,
                      valueClass,
                      raw,
                      false);
              reader.init();
              while (reader.next()) {
                if (metrics != null) {
                  metrics.incRecordsRead(1);
                }
                if (recordBuffer.size() >= maxRecordsNumPerBuffer) {
                  nextQueue.put(recordBuffer);
                  recordBuffer = new RecordBuffer<>(partitionId);
                }
                recordBuffer.addRecord(reader.getCurrentKey(), reader.getCurrentValue());
              }
            } finally {
              if (reader != null) {
                reader.close();
              }
              if (byteBuf != null) {
                byteBuf.release();
              }
              if (managedBuffer != null) {
                managedBuffer.release();
              }
            }
          } else {
            fetchError = "Receive wrong offset from server, offset is " + response.getNextBlockId();
            nextShuffleServerInfo();
            break;
          }
        } catch (Throwable e) {
          error = e;
          stop = true;
          LOG.info("Found exception when fetch sorted record, caused by ", e);
        }
      }
    }
  }

  class RecordsCombiner extends Thread {

    private int partitionId;
    // The RecordBuffer has a capacity limit, records for the same key may be
    // distributed in different RecordBuffers. So we need a cachedBuffer used
    // to record the buffer of the last combine.
    private RecordBuffer<K, C> cached;
    private Queue<RecordBuffer> nextQueue;

    RecordsCombiner(int partitionId) {
      this.partitionId = partitionId;
      this.cached = new RecordBuffer<>(partitionId);
      this.nextQueue = mergeBuffers.get(partitionId);
      setName("RecordsCombiner-" + partitionId);
    }

    @Override
    public void run() {
      while (!stop) {
        try {
          // 1 try to get RecordBuffer from RecordFetcher
          RecordBuffer current = combineBuffers.get(partitionId).take();
          // current is null means that all upstream data has been read
          if (current == null) {
            if (cached.size() > 0) {
              sendCachedBuffer(cached);
            }
            nextQueue.setProducerDone(true);
            break;
          } else {
            // 2 If the last key of cached is not same with the first key of current,
            //   we can send the cached to downstream directly.
            if (cached.size() > 0 && !isSameKey(cached.getLastKey(), current.getFirstKey())) {
              sendCachedBuffer(cached);
              cached = new RecordBuffer<>(partitionId);
            }

            // 3 combine the current, then cache it. By this way, we can handle the specical case
            // that next record
            // buffer has same key in current.
            RecordBlob recordBlob = new RecordBlob<>(partitionId);
            recordBlob.addRecords(current);
            recordBlob.combine(combiner, isMapCombine);
            for (Object record : recordBlob.getResult()) {
              if (cached.size() >= maxRecordsNumPerBuffer
                  && !isSameKey(((Record) record).getKey(), cached.getLastKey())) {
                sendCachedBuffer(cached);
                cached = new RecordBuffer<>(partitionId);
              }
              cached.addRecord((Record) record);
            }
          }
        } catch (InterruptedException e) {
          throw new RssException(e);
        }
      }
    }

    private void sendCachedBuffer(RecordBuffer<K, C> cachedBuffer) throws InterruptedException {
      // Multiple records with the same key may span different recordbuffers. we were only combined
      // within the same recordbuffer. So before send to downstream, we should combine the cached.
      RecordBlob recordBlob = new RecordBlob<K, C, Object>(partitionId);
      recordBlob.addRecords(cachedBuffer);
      recordBlob.combine(combiner, true);
      RecordBuffer recordBuffer = new RecordBuffer<>(partitionId);
      recordBuffer.addRecords(recordBlob.getResult());
      nextQueue.put(recordBuffer);
    }
  }

  class RecordsMerger extends Thread {

    RecordsMerger() {
      setName("RecordsMerger");
    }

    @Override
    public void run() {
      try {
        List<BufferedSegment> segments = new ArrayList<>();
        for (int partitionId : partitionIds) {
          RecordBuffer recordBuffer = mergeBuffers.get(partitionId).take();
          if (recordBuffer != null) {
            BufferedSegment resolvedSegment = new BufferedSegment(recordBuffer);
            segments.add(resolvedSegment);
          }
        }
        Merger.MergeQueue mergeQueue =
            new Merger.MergeQueue(rssConf, segments, keyClass, valueClass, comparator, raw, false);
        try {
          // Here are BufferedSegment, no need to init
          mergeQueue.init();
          mergeQueue.setPopSegmentHook(
              pid -> {
                try {
                  RecordBuffer recordBuffer = mergeBuffers.get(pid).take();
                  if (recordBuffer == null) {
                    return null;
                  }
                  return new BufferedSegment(recordBuffer);
                } catch (InterruptedException ex) {
                  throw new RssException(ex);
                }
              });
          while (!stop && mergeQueue.next()) {
            results.put(Record.create(mergeQueue.getCurrentKey(), mergeQueue.getCurrentValue()));
          }
        } finally {
          mergeQueue.close();
        }
        if (!stop) {
          results.setProducerDone(true);
        }
      } catch (InterruptedException | IOException e) {
        error = e;
        stop = true;
      }
    }
  }

  @VisibleForTesting
  public ShuffleServerClient createShuffleServerClient(ShuffleServerInfo shuffleServerInfo) {
    return ShuffleServerClientFactory.getInstance()
        .getShuffleServerClient(this.clientType, shuffleServerInfo, rssConf);
  }
}
