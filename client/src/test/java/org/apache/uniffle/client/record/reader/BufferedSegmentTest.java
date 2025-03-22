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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.record.RecordBuffer;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.merger.Merger;
import org.apache.uniffle.common.merger.Segment;
import org.apache.uniffle.common.serializer.SerializerUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class BufferedSegmentTest {

  private static final int RECORDS = 1000;
  private static final int SEGMENTS = 4;

  @Test
  public void testMergeResolvedSegmentWithHook() throws Exception {
    RssConf rssConf = new RssConf();
    List<Segment> segments = new ArrayList<>();
    Comparator comparator = new Text.Comparator();
    for (int i = 0; i < SEGMENTS; i++) {
      if (i % 2 == 0) {
        segments.add(genResolvedSegment(Text.class, IntWritable.class, i, i, 4, RECORDS / 2));
      } else {
        segments.add(genResolvedSegment(Text.class, IntWritable.class, i, i, 4, RECORDS));
      }
    }
    Map<Integer, Segment> newSegments = new HashMap<>();
    for (int i = 0; i < SEGMENTS; i++) {
      if (i % 2 == 0) {
        newSegments.put(
            i,
            genResolvedSegment(Text.class, IntWritable.class, i, i + RECORDS * 2, 4, RECORDS / 2));
      } else {
        newSegments.put(
            i, genResolvedSegment(Text.class, IntWritable.class, i, i + RECORDS * 4, 4, RECORDS));
      }
    }
    Merger.MergeQueue mergeQueue =
        new Merger.MergeQueue(
            rssConf, segments, Text.class, IntWritable.class, comparator, false, false);
    mergeQueue.init();
    mergeQueue.setPopSegmentHook(
        id -> {
          Segment newSegment = newSegments.get(id);
          if (newSegment != null) {
            newSegments.remove(id);
          }
          return newSegment;
        });
    for (int i = 0; i < RECORDS * 8; i++) {
      if ((i >= 4 * RECORDS) && (i % 2 == 0)) {
        continue;
      }
      mergeQueue.next();
      assertEquals(SerializerUtils.genData(Text.class, i), mergeQueue.getCurrentKey());
      assertEquals(SerializerUtils.genData(IntWritable.class, i), mergeQueue.getCurrentValue());
    }
    assertFalse(mergeQueue.next());
  }

  private static BufferedSegment genResolvedSegment(
      Class keyClass, Class valueClass, int pid, int start, int interval, int length) {
    RecordBuffer buffer = new RecordBuffer(pid);
    for (int i = 0; i < length; i++) {
      buffer.addRecord(
          SerializerUtils.genData(keyClass, start + i * interval),
          SerializerUtils.genData(valueClass, start + i * interval));
    }
    return new BufferedSegment(buffer);
  }
}
