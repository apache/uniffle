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

package org.apache.uniffle.coordinator;

import java.io.Closeable;
import java.util.List;
import java.util.Set;

public interface ClusterManager extends Closeable {

  /**
   * Add or update a server to the cluster.
   *
   * @param shuffleServerInfo server info
   */
  void add(ServerNode shuffleServerInfo);

  /**
   * Get available nodes from the cluster
   *
   * @param requiredTags tags for filter
   * @return list of available server nodes
   */
  List<ServerNode> getServerList(Set<String> requiredTags);

  /**
   * Get available nodes from the cluster
   *
   * @param requiredTags tags for filter
   * @param faultyServerIds servernode information that is excluded because of an exception
   * @return list of available server nodes
   */
  List<ServerNode> getServerList(Set<String> requiredTags, Set<String> faultyServerIds);

  /**
   * Get lost nodes from the cluster
   *
   * @return list of lost nodes
   */
  List<ServerNode> getLostServerList();

  /**
   * Get unhealthy nodes from the cluster
   *
   * @return list of unhealthy nodes
   */
  List<ServerNode> getUnhealthyServerList();

  Set<String> getExcludedNodes();

  /** @return number of server nodes in the cluster */
  int getNodesNum();

  /** @return list all server nodes in the cluster */
  List<ServerNode> list();

  boolean deleteLostServerById(String serverId);

  int getShuffleNodesMax();

  /** @return whether to be ready for serving */
  boolean isReadyForServe();

  ServerNode getServerNodeById(String serverId);

  void decommission(String serverId);

  void cancelDecommission(String serverId);

  /** Add blacklist. */
  boolean addExcludedNodes(List<String> excludedNodeIds);

  boolean removeExcludedNodesFromFile(List<String> excludedNodeIds);
}
