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
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.uniffle.client.record.Record;
import org.apache.uniffle.client.record.writer.Combiner;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.serializer.SerializerInstance;

public class MRCombiner<K extends Writable, V extends Writable> extends Combiner<K, V, V> {

  private JobConf jobConf;
  private Class combineClass;
  private SerializerInstance instance;
  private Class<K> keyClass;
  private Class<V> valueClass;

  public MRCombiner(
      JobConf jobConf,
      Class combineClass,
      SerializerInstance instance,
      Class<K> keyClass,
      Class<V> valueClass) {
    this.jobConf = jobConf;
    this.combineClass = combineClass;
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.instance = instance;
  }

  @Override
  public List<Record> combineValues(Iterator<Map.Entry<K, List<Record>>> recordIterator) {
    try {
      // TODO: For now, only MapReduce new api is supported.
      List<Record> ret = new ArrayList<>();
      Reducer<K, V, K, V> reducer =
          (Reducer) ReflectionUtils.newInstance(this.combineClass, this.jobConf);
      Reducer.Context context =
          new WrappedReducer<K, V, K, V>()
              .getReducerContext(
                  new RecordCollector(recordIterator, ret, instance, keyClass, valueClass));
      reducer.run(context);
      return ret;
    } catch (IOException | InterruptedException e) {
      throw new RssException(e);
    }
  }

  @Override
  public List<Record> combineCombiners(Iterator<Map.Entry<K, List<Record>>> recordIterator) {
    return combineValues(recordIterator);
  }
}
