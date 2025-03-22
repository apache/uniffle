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

package org.apache.uniffle.client.shuffle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.record.Record;
import org.apache.uniffle.client.record.RecordBlob;
import org.apache.uniffle.client.record.RecordBuffer;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.SerializerFactory;
import org.apache.uniffle.common.serializer.SerializerInstance;
import org.apache.uniffle.common.serializer.writable.ComparativeOutputBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RecordCollectorTest {

  private void reduce(Text key, Iterable<IntWritable> values, RecordCollector collector)
      throws IOException, InterruptedException {
    Iterator<IntWritable> iterator = values.iterator();
    int sum = 0;
    while (iterator.hasNext()) {
      sum += iterator.next().get();
    }
    collector.write(key, new IntWritable(sum));
  }

  @Test
  public void testRecordCollector() throws IOException, InterruptedException {
    SerializerFactory factory = new SerializerFactory(new RssConf());
    SerializerInstance serializerInstance = factory.getSerializer(Text.class).newInstance();
    RecordBuffer<ComparativeOutputBuffer, ComparativeOutputBuffer> recordBuffer =
        new RecordBuffer(-1);
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j <= i; j++) {
        Text key = new Text("key" + i);
        IntWritable value = new IntWritable(j);
        ComparativeOutputBuffer keyBuffer = new ComparativeOutputBuffer();
        ComparativeOutputBuffer valueBuffer = new ComparativeOutputBuffer();
        serializerInstance.serialize(key, keyBuffer);
        serializerInstance.serialize(value, valueBuffer);
        recordBuffer.addRecord(keyBuffer, valueBuffer);
      }
    }
    RecordBlob recordBlob = new RecordBlob(-1);
    recordBlob.addRecords(recordBuffer);

    List<Record<DataOutputBuffer, DataOutputBuffer>> newRecords = new ArrayList<>();

    RecordCollector<Text, IntWritable> collector =
        new RecordCollector(
            recordBlob.getRecords().entrySet().iterator(),
            newRecords,
            serializerInstance,
            Text.class,
            IntWritable.class);
    while (collector.nextKey()) {
      reduce(collector.getCurrentKey(), collector.getValues(), collector);
    }
    assertEquals(5, newRecords.size());
    int[] sums = new int[] {0, 1, 3, 6, 10};
    for (int i = 0; i < sums.length; i++) {
      DataOutputBuffer keyBuffer = newRecords.get(i).getKey();
      DataOutputBuffer valueBuffer = newRecords.get(i).getValue();
      DataInputBuffer inputKeyBuffer = new DataInputBuffer();
      inputKeyBuffer.reset(keyBuffer.getData(), 0, keyBuffer.getLength());
      DataInputBuffer inputValueBuffer = new DataInputBuffer();
      inputValueBuffer.reset(valueBuffer.getData(), 0, valueBuffer.getLength());
      assertEquals(
          new Text(String.format("key%d", i)),
          serializerInstance.deserialize(inputKeyBuffer, Text.class));
      assertEquals(
          new IntWritable(sums[i]),
          serializerInstance.deserialize(inputValueBuffer, IntWritable.class));
    }
  }
}
