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

package org.apache.spark.shuffle;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.Aggregator;

import org.apache.uniffle.client.record.Record;
import org.apache.uniffle.client.record.writer.Combiner;

public class SparkCombiner<K, V, C> extends Combiner<K, V, C> {

  private Aggregator<K, V, C> agg;

  public SparkCombiner(Aggregator<K, V, C> agg) {
    this.agg = agg;
  }

  @Override
  public List<Record> combineValues(Iterator<Map.Entry<K, List<Record>>> recordIterator) {
    List<Record> ret = new ArrayList<>();
    while (recordIterator.hasNext()) {
      Map.Entry<K, List<Record>> entry = recordIterator.next();
      List<Record> records = entry.getValue();
      Record<K, C> current = null;
      for (Record<K, V> record : records) {
        if (current == null) {
          // Handle new Key
          current = Record.create(record.getKey(), agg.createCombiner().apply(record.getValue()));
        } else {
          // Combine the values
          C newValue = agg.mergeValue().apply(current.getValue(), record.getValue());
          current.setValue(newValue);
        }
      }
      ret.add(current);
    }
    return ret;
  }

  @Override
  public List<Record> combineCombiners(Iterator<Map.Entry<K, List<Record>>> recordIterator) {
    List<Record> ret = new ArrayList<>();
    while (recordIterator.hasNext()) {
      Map.Entry<K, List<Record>> entry = recordIterator.next();
      List<Record> records = entry.getValue();
      Record<K, C> current = null;
      for (Record<K, C> record : records) {
        if (current == null) {
          // Handle new Key
          current = record;
        } else {
          // Combine the values
          C newValue = agg.mergeCombiners().apply(current.getValue(), record.getValue());
          current.setValue(newValue);
        }
      }
      ret.add(current);
    }
    return ret;
  }
}
