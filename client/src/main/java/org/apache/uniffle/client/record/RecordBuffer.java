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

package org.apache.uniffle.client.record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.records.RecordsWriter;

/*
 * RecordBuffer is used to store records. The records are stored in the form of List.
 * It can quickly index to record and supports sorting.
 * */
public class RecordBuffer<K, V> implements RecordCollection<K, V, V> {

  private final int partitionId;

  private int size = 0;
  private List<Record<K, V>> records = new ArrayList<>();

  public RecordBuffer(int partitionId) {
    this.partitionId = partitionId;
  }

  public void addRecord(K key, V value) {
    Record record;
    record = Record.create(key, value);
    this.records.add(record);
    this.size++;
  }

  public void addRecord(Record<K, V> record) {
    this.records.add(record);
    this.size++;
  }

  public void addRecords(List<Record<K, V>> records) {
    this.records.addAll(records);
    this.size += records.size();
  }

  public List<Record<K, V>> getRecords() {
    return records;
  }

  public void sort(Comparator comparator) {
    if (comparator == null) {
      throw new RssException("comparator is not set");
    }
    this.records.sort(
        new Comparator<Record>() {
          @Override
          public int compare(Record o1, Record o2) {
            return comparator.compare(o1.getKey(), o2.getKey());
          }
        });
  }

  public void serialize(RecordsWriter writer) throws IOException {
    for (Record record : records) {
      writer.append(record.getKey(), record.getValue());
    }
  }

  public void clear() {
    this.size = 0;
    this.records.clear();
  }

  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public int size() {
    return this.size;
  }

  public K getKey(int index) {
    return this.records.get(index).getKey();
  }

  public V getValue(int index) {
    return this.records.get(index).getValue();
  }

  public K getLastKey() {
    return this.records.get(this.records.size() - 1).getKey();
  }

  public K getFirstKey() {
    return this.records.get(0).getKey();
  }
}
