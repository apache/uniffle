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

package org.apache.uniffle.common.merger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.util.PriorityQueue;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.records.RecordsWriter;
import org.apache.uniffle.common.serializer.SerOutputStream;

public class Merger {

  public static class MergeQueue<K, V> extends PriorityQueue<Segment> implements KeyValueIterator {

    private final RssConf rssConf;
    private final List<Segment> segments;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private Comparator comparator;
    private boolean raw;
    private boolean buffered;

    private Object currentKey;
    private Object currentValue;
    private Segment minSegment;
    private Function<Integer, Segment> popSegmentHook;

    public MergeQueue(
        RssConf rssConf,
        List<Segment> segments,
        Class<K> keyClass,
        Class<V> valueClass,
        Comparator<K> comparator,
        boolean raw,
        boolean buffered) {
      this.rssConf = rssConf;
      this.segments = segments;
      this.keyClass = keyClass;
      this.valueClass = valueClass;
      if (comparator == null) {
        throw new RssException("comparator is null!");
      }
      this.comparator = comparator;
      this.raw = raw;
      this.buffered = buffered;
    }

    public void setPopSegmentHook(Function<Integer, Segment> popSegmentHook) {
      this.popSegmentHook = popSegmentHook;
    }

    @Override
    protected boolean lessThan(Object o1, Object o2) {
      if (raw) {
        if (buffered) {
          Segment s1 = (Segment) o1;
          Segment s2 = (Segment) o2;
          ByteBuf key1 = (ByteBuf) s1.getCurrentKey();
          ByteBuf key2 = (ByteBuf) s2.getCurrentKey();
          // make sure key buffer is in heap, avoid byte array copy
          int c =
              ((RawComparator) comparator)
                  .compare(
                      key1.array(),
                      key1.arrayOffset() + key1.readerIndex(),
                      key1.readableBytes(),
                      key2.array(),
                      key2.arrayOffset() + key2.readerIndex(),
                      key2.readableBytes());
          return c < 0 || ((c == 0) && s1.getId() < s2.getId());
        } else {
          Segment s1 = (Segment) o1;
          Segment s2 = (Segment) o2;
          DataOutputBuffer key1 = (DataOutputBuffer) s1.getCurrentKey();
          DataOutputBuffer key2 = (DataOutputBuffer) s2.getCurrentKey();
          int c =
              ((RawComparator) comparator)
                  .compare(
                      key1.getData(), 0, key1.getLength(), key2.getData(), 0, key2.getLength());
          return c < 0 || ((c == 0) && s1.getId() < s2.getId());
        }
      } else {
        Segment s1 = (Segment) o1;
        Segment s2 = (Segment) o2;
        Object key1 = s1.getCurrentKey();
        Object key2 = s2.getCurrentKey();
        int c = comparator.compare(key1, key2);
        return c < 0 || ((c == 0) && s1.getId() < s2.getId());
      }
    }

    public void init() throws IOException {
      List<Segment> segmentsToMerge = new ArrayList();
      for (Segment segment : segments) {
        boolean hasNext = segment.next();
        if (hasNext) {
          segmentsToMerge.add(segment);
        } else {
          segment.close();
        }
      }
      initialize(segmentsToMerge.size());
      clear();
      for (Segment segment : segmentsToMerge) {
        put(segment);
      }
    }

    @Override
    public Object getCurrentKey() {
      return currentKey;
    }

    @Override
    public Object getCurrentValue() {
      return currentValue;
    }

    @Override
    public boolean next() throws IOException {
      if (size() == 0) {
        resetKeyValue();
        return false;
      }

      if (minSegment != null) {
        adjustPriorityQueue(minSegment);
        if (size() == 0) {
          minSegment = null;
          resetKeyValue();
          return false;
        }
      }
      minSegment = top();
      currentKey = minSegment.getCurrentKey();
      currentValue = minSegment.getCurrentValue();
      return true;
    }

    private void resetKeyValue() {
      currentKey = null;
      currentValue = null;
    }

    private void adjustPriorityQueue(Segment segment) throws IOException {
      if (segment.next()) {
        adjustTop();
      } else {
        pop();
        segment.close();
        if (popSegmentHook != null) {
          Segment newSegment = popSegmentHook.apply((int) segment.getId());
          if (newSegment != null) {
            newSegment.init();
            if (newSegment.next()) {
              put(newSegment);
            } else {
              newSegment.close();
            }
          }
        }
      }
    }

    public void merge(SerOutputStream output) throws IOException {
      RecordsWriter<K, V> writer =
          new RecordsWriter<K, V>(rssConf, output, keyClass, valueClass, raw, buffered);
      try {
        writer.init();
        while (this.next()) {
          writer.append(this.getCurrentKey(), this.getCurrentValue());
        }
        writer.flush();
      } finally {
        writer.close();
      }
    }

    @Override
    public void close() throws IOException {}
  }

  public static void merge(
      RssConf conf,
      SerOutputStream output,
      List<Segment> segments,
      Class keyClass,
      Class valueClass,
      Comparator comparator,
      boolean raw)
      throws IOException {
    MergeQueue mergeQueue =
        new MergeQueue(conf, segments, keyClass, valueClass, comparator, raw, true);
    try {
      mergeQueue.init();
      mergeQueue.merge(output);
    } finally {
      mergeQueue.close();
    }
  }
}
