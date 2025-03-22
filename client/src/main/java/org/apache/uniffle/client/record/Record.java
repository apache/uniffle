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

import com.google.common.base.Objects;

public class Record<K, V> {

  private K key;
  private V value;

  private Record(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public static <K, V> Record create(K key, V value) {
    return new Record<K, V>(key, value);
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  public void setValue(V value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "Record{" + "key=" + key + ", value=" + value + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Record<?, ?> record = (Record<?, ?>) o;
    return Objects.equal(key, record.key) && Objects.equal(value, record.value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(key, value);
  }
}
