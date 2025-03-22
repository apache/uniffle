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

package org.apache.uniffle.server.merge;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class MergeEvent {

  private final String appId;
  private final int shuffleId;
  private final int partitionId;
  private final Class kClass;
  private final Class vClass;
  private Roaring64NavigableMap expectedBlockIdMap;

  public MergeEvent(
      String appId,
      int shuffleId,
      int partitionId,
      Class kClass,
      Class vClass,
      Roaring64NavigableMap expectedBlockIdMap) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.kClass = kClass;
    this.vClass = vClass;
    this.expectedBlockIdMap = expectedBlockIdMap;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public Roaring64NavigableMap getExpectedBlockIdMap() {
    return expectedBlockIdMap;
  }

  public Class getKeyClass() {
    return kClass;
  }

  public Class getValueClass() {
    return vClass;
  }

  @Override
  public String toString() {
    return "MergeEvent{"
        + "appId='"
        + appId
        + '\''
        + ", shuffleId="
        + shuffleId
        + ", partitionId="
        + partitionId
        + ", kClass="
        + kClass
        + ", vClass="
        + vClass
        + ", expectedBlockIdMap="
        + expectedBlockIdMap
        + '}';
  }
}
