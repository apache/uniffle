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

package org.apache.uniffle.client.record.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;

import org.apache.uniffle.client.record.Record;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.serializer.SerializerInstance;
import org.apache.uniffle.common.serializer.writable.ComparativeOutputBuffer;

public class SumByKeyCombiner extends Combiner {

  private final boolean raw;
  private final SerializerInstance instance;
  private final Class keyClass;
  private final Class valueClass;

  public SumByKeyCombiner() {
    this(false, null, null, null);
  }

  public SumByKeyCombiner(
      boolean raw, SerializerInstance instance, Class keyClass, Class valueClass) {
    this.raw = raw;
    this.instance = instance;
    this.keyClass = keyClass;
    this.valueClass = valueClass;
  }

  @Override
  public List<Record> combineValues(Iterator recordIterator) {
    List<Record> collected = new ArrayList<>();
    while (recordIterator.hasNext()) {
      Map.Entry<Object, List<Record>> entry =
          (Map.Entry<Object, List<Record>>) recordIterator.next();
      Record current = null;
      List<Record> records = entry.getValue();
      for (Record record : records) {
        Record newRecord;
        if (raw) {
          ComparativeOutputBuffer keyBuffer = (ComparativeOutputBuffer) record.getKey();
          ComparativeOutputBuffer valueBuffer = (ComparativeOutputBuffer) record.getValue();
          DataInputBuffer keyInputBuffer = new DataInputBuffer();
          DataInputBuffer valueInputBuffer = new DataInputBuffer();
          keyInputBuffer.reset(keyBuffer.getData(), 0, keyBuffer.getLength());
          valueInputBuffer.reset(valueBuffer.getData(), 0, valueBuffer.getLength());
          try {
            Object key = instance.deserialize(keyInputBuffer, keyClass);
            Object value = instance.deserialize(valueInputBuffer, valueClass);
            newRecord = Record.create(key, value);
          } catch (IOException e) {
            throw new RssException(e);
          }
        } else {
          newRecord = record;
        }
        if (current == null) {
          current = newRecord;
          collected.add(current);
        } else {
          int v1 =
              newRecord.getValue() instanceof IntWritable
                  ? ((IntWritable) newRecord.getValue()).get()
                  : (int) newRecord.getValue();
          int v2 =
              current.getValue() instanceof IntWritable
                  ? ((IntWritable) current.getValue()).get()
                  : (int) current.getValue();
          if (current.getValue() instanceof IntWritable) {
            ((IntWritable) current.getValue()).set(v1 + v2);
          } else {
            current.setValue(v1 + v2);
          }
        }
      }
    }
    if (raw) {
      List<Record> ret = new ArrayList<>();
      for (Record record : collected) {
        ComparativeOutputBuffer keyBuffer = new ComparativeOutputBuffer();
        ComparativeOutputBuffer valueBuffer = new ComparativeOutputBuffer();
        try {
          instance.serialize(record.getKey(), keyBuffer);
          instance.serialize(record.getValue(), valueBuffer);
        } catch (IOException e) {
          throw new RssException(e);
        }
        ret.add(Record.create(keyBuffer, valueBuffer));
      }
      return ret;
    } else {
      return collected;
    }
  }

  @Override
  public List<Record> combineCombiners(Iterator recordIterator) {
    return combineValues(recordIterator);
  }
}
