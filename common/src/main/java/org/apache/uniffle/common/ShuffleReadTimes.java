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

package org.apache.uniffle.common;

import org.apache.uniffle.proto.RssProtos;

/** The unit is millis */
public class ShuffleReadTimes {
  private long fetch;
  private long crc;
  private long copy;
  private long deserialize;
  private long decompress;

  public ShuffleReadTimes() {}

  public ShuffleReadTimes(long fetch, long crc, long copy) {
    this.fetch = fetch;
    this.crc = crc;
    this.copy = copy;
  }

  public long getFetch() {
    return fetch;
  }

  public long getCrc() {
    return crc;
  }

  public long getCopy() {
    return copy;
  }

  public void withDeserialized(long deserialized) {
    this.deserialize = deserialized;
  }

  public void withDecompressed(long decompressed) {
    this.decompress = decompressed;
  }

  public long getDeserialize() {
    return deserialize;
  }

  public long getDecompress() {
    return decompress;
  }

  public void merge(ShuffleReadTimes other) {
    if (other == null) {
      return;
    }
    this.fetch += other.fetch;
    this.crc += other.crc;
    this.copy += other.copy;
    this.deserialize += other.deserialize;
    this.decompress += other.decompress;
  }

  public long getTotal() {
    return fetch + crc + copy + deserialize + decompress;
  }

  public RssProtos.ShuffleReadTimes toProto() {
    return RssProtos.ShuffleReadTimes.newBuilder()
        .setFetch(fetch)
        .setCrc(crc)
        .setCopy(copy)
        .setDecompress(decompress)
        .setDeserialize(deserialize)
        .build();
  }

  public static ShuffleReadTimes fromProto(RssProtos.ShuffleReadTimes proto) {
    ShuffleReadTimes time = new ShuffleReadTimes();
    time.fetch = proto.getFetch();
    time.crc = proto.getCrc();
    time.copy = proto.getCopy();
    time.decompress = proto.getDecompress();
    time.deserialize = proto.getDeserialize();
    return time;
  }
}
