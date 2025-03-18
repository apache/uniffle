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

import java.util.Iterator;

import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

import org.apache.spark.Aggregator;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.record.Record;
import org.apache.uniffle.client.record.RecordBlob;
import org.apache.uniffle.client.record.RecordBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SparkCombinerTest {

  @Test
  public void testSparkCombiner() {
    RecordBuffer<String, Integer> recordBuffer = new RecordBuffer(-1);
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j <= i; j++) {
        String key = "key" + i;
        recordBuffer.addRecord(key, j);
      }
    }
    SparkCombiner<String, Integer, String> combiner =
        new SparkCombiner<>(
            new Aggregator<>(
                new AbstractFunction1<Integer, String>() {
                  @Override
                  public String apply(Integer v1) {
                    return v1.toString();
                  }
                },
                new AbstractFunction2<String, Integer, String>() {
                  @Override
                  public String apply(String c, Integer v) {
                    return Integer.valueOf(Integer.parseInt(c) + v).toString();
                  }
                },
                new AbstractFunction2<String, String, String>() {
                  @Override
                  public String apply(String c1, String c2) {
                    return Integer.valueOf(Integer.parseInt(c1) + Integer.parseInt(c2)).toString();
                  }
                }));
    RecordBlob recordBlob = new RecordBlob(-1);
    recordBlob.addRecords(recordBuffer);
    recordBlob.combine(combiner, false);
    Iterator<Record<String, Integer>> newRecords = recordBlob.getResult().iterator();
    int index = 0;
    while (newRecords.hasNext()) {
      Record<String, Integer> record = newRecords.next();
      int expectedValue = 0;
      for (int i = 0; i <= index; i++) {
        expectedValue += i;
      }
      assertEquals("Record{key=key" + index + ", value=" + expectedValue + "}", record.toString());
      index++;
    }
    assertEquals(5, index);
  }
}
