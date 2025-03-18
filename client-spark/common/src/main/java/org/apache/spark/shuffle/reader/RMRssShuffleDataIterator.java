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

package org.apache.spark.shuffle.reader;

import java.io.IOException;

import scala.collection.AbstractIterator;
import scala.runtime.BoxedUnit;

import org.apache.uniffle.client.record.Record;
import org.apache.uniffle.client.record.reader.KeyValueReader;
import org.apache.uniffle.client.record.reader.RMRecordsReader;
import org.apache.uniffle.common.exception.RssException;

public class RMRssShuffleDataIterator<K, C> extends AbstractIterator<Record<K, C>> {

  private RMRecordsReader<K, ?, C> reader;
  private KeyValueReader<K, C> keyValueReader;

  public RMRssShuffleDataIterator(RMRecordsReader<K, ?, C> reader) {
    this.reader = reader;
    this.keyValueReader = reader.keyValueReader();
  }

  @Override
  public boolean hasNext() {
    try {
      return this.keyValueReader.hasNext();
    } catch (IOException e) {
      throw new RssException(e);
    }
  }

  @Override
  public Record<K, C> next() {
    try {
      return this.keyValueReader.next();
    } catch (IOException e) {
      throw new RssException(e);
    }
  }

  public BoxedUnit cleanup() {
    reader.close();
    return BoxedUnit.UNIT;
  }
}
