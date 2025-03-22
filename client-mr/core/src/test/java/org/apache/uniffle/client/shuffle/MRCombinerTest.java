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
import java.util.Iterator;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.apache.uniffle.client.record.Record;
import org.apache.uniffle.client.record.RecordBlob;
import org.apache.uniffle.client.record.RecordBuffer;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.serializer.SerializerFactory;
import org.apache.uniffle.common.serializer.SerializerInstance;
import org.apache.uniffle.common.serializer.writable.ComparativeOutputBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MRCombinerTest {

  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  @Test
  @Timeout(5)
  public void testMRCombiner() throws IOException {
    JobConf jobConf = new JobConf();
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

    MRCombiner<Text, IntWritable> combiner =
        new MRCombiner(
            jobConf, IntSumReducer.class, serializerInstance, Text.class, IntWritable.class);
    RecordBlob recordBlob = new RecordBlob(-1);
    recordBlob.addRecords(recordBuffer);
    recordBlob.combine(combiner, false);
    Iterator<Record<DataOutputBuffer, DataOutputBuffer>> newRecords =
        recordBlob.getResult().iterator();
    int index = 0;
    while (newRecords.hasNext()) {
      Record<DataOutputBuffer, DataOutputBuffer> record = newRecords.next();
      int expectedValue = 0;
      for (int i = 0; i <= index; i++) {
        expectedValue += i;
      }
      DataOutputBuffer keyBuffer = record.getKey();
      DataOutputBuffer valueBuffer = record.getValue();
      DataInputBuffer inputKeyBuffer = new DataInputBuffer();
      inputKeyBuffer.reset(keyBuffer.getData(), 0, keyBuffer.getLength());
      DataInputBuffer inputValueBuffer = new DataInputBuffer();
      inputValueBuffer.reset(valueBuffer.getData(), 0, valueBuffer.getLength());
      assertEquals(
          new Text(String.format("key%d", index)),
          serializerInstance.deserialize(inputKeyBuffer, Text.class));
      assertEquals(
          new IntWritable(expectedValue),
          serializerInstance.deserialize(inputValueBuffer, IntWritable.class));
      index++;
    }
    assertEquals(5, index);
  }
}
