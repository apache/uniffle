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

import java.util.List;
import java.util.Map;

import org.apache.spark.ShuffleDependency;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.shuffle.handle.SimpleShuffleHandleInfo;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleServerInfo;

public class RssShuffleHandle<K, V, C> extends ShuffleHandle {
  private static final long serialVersionUID = 0L;

  private String appId;
  private int numMaps;
  private ShuffleDependency<K, V, C> dependency;
  private Broadcast<SimpleShuffleHandleInfo> handlerInfoBd;

  public RssShuffleHandle(
      int shuffleId,
      String appId,
      int numMaps,
      ShuffleDependency<K, V, C> dependency,
      Broadcast<SimpleShuffleHandleInfo> handlerInfoBd) {
    super(shuffleId);
    this.appId = appId;
    this.numMaps = numMaps;
    this.dependency = dependency;
    this.handlerInfoBd = handlerInfoBd;
  }

  public String getAppId() {
    return appId;
  }

  public int getNumMaps() {
    return numMaps;
  }

  public ShuffleDependency<K, V, C> getDependency() {
    return dependency;
  }

  public int getShuffleId() {
    return shuffleId();
  }

  public RemoteStorageInfo getRemoteStorage() {
    return handlerInfoBd.value().getRemoteStorage();
  }

  public Map<Integer, List<ShuffleServerInfo>> getPartitionToServers() {
    return handlerInfoBd.value().getAvailablePartitionServersForWriter();
  }
}
