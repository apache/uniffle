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
import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.uniffle.common.ShuffleServerInfo;

/**
 * Class for holding partition ID -> shuffle servers mapping.
 * It's to be broadcast to executors and referenced by shuffle tasks.
 */
public class PartitionShuffleServerMap {

  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  // shuffle servers which is for store shuffle data
  private Set<ShuffleServerInfo> shuffleServersForData;

  public PartitionShuffleServerMap(Map<Integer, List<ShuffleServerInfo>> partitionToServers) {
    this.partitionToServers = partitionToServers;
    this.shuffleServersForData = Sets.newHashSet();
    for (List<ShuffleServerInfo> ssis : partitionToServers.values()) {
      this.shuffleServersForData.addAll(ssis);
    }
  }

  public Map<Integer, List<ShuffleServerInfo>> getPartitionToServers() {
    return partitionToServers;
  }

  public Set<ShuffleServerInfo> getShuffleServersForData() {
    return shuffleServersForData;
  }

}
