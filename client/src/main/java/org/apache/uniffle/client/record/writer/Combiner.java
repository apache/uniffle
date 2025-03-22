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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.uniffle.client.record.Record;

/*
 * If shuffle requires sorting, the input records are sorted by key, so they can be combined sequentially.
 * If shuffle does not require sorting. The input records are sorted according to the hashcode of the key.
 * Therefore, the same keys may not be organized together, so the data cannot be obtained sequentially.
 * So LinkedHashMap needs to be used.
 * */
public abstract class Combiner<K, V, C> {

  public abstract List<Record> combineValues(Iterator<Map.Entry<K, List<Record>>> recordIterator);

  public abstract List<Record> combineCombiners(
      Iterator<Map.Entry<K, List<Record>>> recordIterator);
}
