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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.impl.grpc.ShuffleServerInternalGrpcClient;
import org.apache.uniffle.client.request.RssCancelDecommissionRequest;
import org.apache.uniffle.client.request.RssDecommissionRequest;
import org.apache.uniffle.common.ReconfigurableConfManager;
import org.apache.uniffle.common.ServerStatus;
import org.apache.uniffle.common.exception.InvalidRequestException;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.filesystem.HadoopFilesystemProvider;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;

public class SimpleClusterManager implements ClusterManager {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleClusterManager.class);
  private final Map<String, ServerNode> servers = JavaUtils.newConcurrentMap();
  private final Cache<ServerNode, ShuffleServerInternalGrpcClient> clientCache;
  private Set<String> excludedNodes = Sets.newConcurrentHashSet();
  /** ServerNode whose heartbeat is lost */
  Set<ServerNode> lostNodes = Sets.newHashSet();
  /** Unhealthy ServerNode */
  Set<ServerNode> unhealthyNodes = Sets.newHashSet();
  // tag -> nodes
  private Map<String, Set<ServerNode>> tagToNodes = JavaUtils.newConcurrentMap();
  private AtomicLong excludeLastModify = new AtomicLong(0L);
  private long heartbeatTimeout;
  private ReconfigurableConfManager.Reconfigurable<Integer> shuffleNodesMax;
  private ScheduledExecutorService scheduledExecutorService;
  private ScheduledExecutorService checkNodesExecutorService;
  private FileSystem hadoopFileSystem;

  private long outputAliveServerCount = 0;
  private final long periodicOutputIntervalTimes;

  private long startTime;
  private boolean startupSilentPeriodEnabled;
  private long startupSilentPeriodDurationMs;
  private boolean readyForServe = false;
  private String excludedNodesPath;

  public SimpleClusterManager(CoordinatorConf conf, Configuration hadoopConf) throws Exception {
    this.shuffleNodesMax =
        conf.getReconfigurableConf(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX);
    this.heartbeatTimeout = conf.getLong(CoordinatorConf.COORDINATOR_HEARTBEAT_TIMEOUT);
    // the thread for checking if shuffle server report heartbeat in time
    scheduledExecutorService =
        ThreadUtils.getDaemonSingleThreadScheduledExecutor("SimpleClusterManager");

    this.startupSilentPeriodEnabled =
        conf.get(CoordinatorConf.COORDINATOR_START_SILENT_PERIOD_ENABLED);
    this.startupSilentPeriodDurationMs =
        conf.get(CoordinatorConf.COORDINATOR_START_SILENT_PERIOD_DURATION);

    periodicOutputIntervalTimes =
        conf.get(CoordinatorConf.COORDINATOR_NODES_PERIODIC_OUTPUT_INTERVAL_TIMES);

    scheduledExecutorService.scheduleAtFixedRate(
        this::nodesCheck, heartbeatTimeout / 3, heartbeatTimeout / 3, TimeUnit.MILLISECONDS);

    this.excludedNodesPath =
        conf.getString(CoordinatorConf.COORDINATOR_EXCLUDE_NODES_FILE_PATH, "");
    if (!StringUtils.isEmpty(excludedNodesPath)) {
      this.hadoopFileSystem =
          HadoopFilesystemProvider.getFilesystem(new Path(excludedNodesPath), hadoopConf);
      long updateNodesInterval =
          conf.getLong(CoordinatorConf.COORDINATOR_EXCLUDE_NODES_CHECK_INTERVAL);
      checkNodesExecutorService =
          ThreadUtils.getDaemonSingleThreadScheduledExecutor("UpdateExcludedNodes");
      checkNodesExecutorService.scheduleAtFixedRate(
          () -> updateExcludedNodes(excludedNodesPath),
          0,
          updateNodesInterval,
          TimeUnit.MILLISECONDS);
    }

    long clientExpiredTime = conf.get(CoordinatorConf.COORDINATOR_NODES_CLIENT_CACHE_EXPIRED);
    int maxClient = conf.get(CoordinatorConf.COORDINATOR_NODES_CLIENT_CACHE_MAX);
    clientCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(clientExpiredTime, TimeUnit.MILLISECONDS)
            .maximumSize(maxClient)
            .removalListener(
                notify -> ((ShuffleServerInternalGrpcClient) notify.getValue()).close())
            .build();
    this.startTime = System.currentTimeMillis();
  }

  void nodesCheck() {
    try {
      long timestamp = System.currentTimeMillis();
      for (ServerNode sn : servers.values()) {
        if (timestamp - sn.getTimestamp() > heartbeatTimeout) {
          LOG.warn("Heartbeat timeout detect, {} will be removed from node list.", sn);
          sn.setStatus(ServerStatus.LOST);
          lostNodes.add(sn);
        } else if (ServerStatus.UNHEALTHY.equals(sn.getStatus())) {
          LOG.warn("Found server {} was unhealthy, will not assign it.", sn);
          unhealthyNodes.add(sn);
          lostNodes.remove(sn);
        } else {
          lostNodes.remove(sn);
          unhealthyNodes.remove(sn);
        }
      }
      for (ServerNode server : lostNodes) {
        ServerNode sn = servers.remove(server.getId());
        unhealthyNodes.remove(sn);
        if (sn != null) {
          clientCache.invalidate(sn);
          for (Set<ServerNode> nodesWithTag : tagToNodes.values()) {
            nodesWithTag.remove(sn);
          }
        }
      }
      if (!lostNodes.isEmpty() || outputAliveServerCount % periodicOutputIntervalTimes == 0) {
        LOG.info(
            "Alive servers number: {}, ids: {}",
            servers.size(),
            servers.keySet().stream().collect(Collectors.toList()));
      }
      outputAliveServerCount++;

      CoordinatorMetrics.gaugeUnhealthyServerNum.set(unhealthyNodes.size());
      CoordinatorMetrics.gaugeTotalServerNum.set(servers.size());
      CoordinatorMetrics.gaugeLostServerNum.set(lostNodes.size());

      // get the active server num.
      Set<String> allServers = new HashSet<>(servers.keySet());
      allServers.removeAll(excludedNodes);
      for (ServerNode unhealthyNode : unhealthyNodes) {
        allServers.remove(unhealthyNode.getId());
      }
      CoordinatorMetrics.gaugeActiveServerNum.set(allServers.size());

    } catch (Exception e) {
      LOG.warn("Error happened in nodesCheck", e);
    }
  }

  @VisibleForTesting
  public void nodesCheckTest() {
    nodesCheck();
  }

  private synchronized void updateExcludedNodes(String path) {
    int originalExcludedNodesNumber = excludedNodes.size();
    try {
      Path hadoopPath = new Path(path);
      FileStatus fileStatus = hadoopFileSystem.getFileStatus(hadoopPath);
      if (fileStatus != null && fileStatus.isFile()) {
        long latestModificationTime = fileStatus.getModificationTime();
        if (excludeLastModify.get() != latestModificationTime) {
          excludedNodes = parseExcludedNodesFile(hadoopFileSystem.open(hadoopPath));
          LOG.info(
              "Updated exclude nodes and {} nodes were marked as excluded nodes",
              excludedNodes.size());
          // update exclude nodes and last modify time
          excludeLastModify.set(latestModificationTime);
        }
      } else {
        excludedNodes = Sets.newConcurrentHashSet();
      }
    } catch (FileNotFoundException fileNotFoundException) {
      excludedNodes = Sets.newConcurrentHashSet();
    } catch (Exception e) {
      LOG.warn("Error when updating exclude nodes, the exclude nodes file path: {}.", path, e);
    }
    int newlyExcludedNodesNumber = excludedNodes.size();
    if (newlyExcludedNodesNumber != originalExcludedNodesNumber) {
      LOG.info("Exclude nodes number: {}, nodes list: {}", newlyExcludedNodesNumber, excludedNodes);
    }
    CoordinatorMetrics.gaugeExcludeServerNum.set(excludedNodes.size());
  }

  private Set<String> parseExcludedNodesFile(DataInputStream fsDataInputStream) throws IOException {
    Set<String> nodes = Sets.newConcurrentHashSet();
    try (BufferedReader br =
        new BufferedReader(new InputStreamReader(fsDataInputStream, StandardCharsets.UTF_8))) {
      String line;
      while ((line = br.readLine()) != null) {
        if (!StringUtils.isEmpty(line) && !line.trim().startsWith("#")) {
          nodes.add(line.trim());
        }
      }
    }
    return nodes;
  }

  private void writeExcludedNodes2File(List<String> excludedNodes) throws IOException {
    if (hadoopFileSystem == null) {
      return;
    }
    Path hadoopPath = new Path(excludedNodesPath);
    FileStatus fileStatus = hadoopFileSystem.getFileStatus(hadoopPath);
    if (fileStatus != null && fileStatus.isFile()) {
      String tempExcludedNodesPath = excludedNodesPath.concat("_tmp");
      Path tmpHadoopPath = new Path(tempExcludedNodesPath);
      if (hadoopFileSystem.exists(tmpHadoopPath)) {
        hadoopFileSystem.delete(tmpHadoopPath, true);
      }
      try (BufferedWriter bufferedWriter =
          new BufferedWriter(
              new OutputStreamWriter(
                  hadoopFileSystem.create(tmpHadoopPath, true), StandardCharsets.UTF_8))) {
        for (String excludedNode : excludedNodes) {
          bufferedWriter.write(excludedNode);
          bufferedWriter.newLine();
        }
      }
      hadoopFileSystem.delete(hadoopPath, true);
      hadoopFileSystem.rename(tmpHadoopPath, hadoopPath);
    }
  }

  private synchronized boolean putInExcludedNodesFile(List<String> excludedNodes)
      throws IOException {
    if (hadoopFileSystem == null) {
      return false;
    }
    Path hadoopPath = new Path(excludedNodesPath);
    // Obtains the existing excluded node.
    Set<String> alreadyExistExcludedNodes =
        parseExcludedNodesFile(hadoopFileSystem.open(hadoopPath));
    List<String> newAddExcludedNodes =
        excludedNodes.stream()
            .filter(node -> !alreadyExistExcludedNodes.contains(node))
            .collect(Collectors.toList());
    newAddExcludedNodes.addAll(alreadyExistExcludedNodes);
    // Writes to the new excluded node.
    writeExcludedNodes2File(newAddExcludedNodes);
    return true;
  }

  private synchronized boolean removeExcludedNodesFile(List<String> excludedNodes)
      throws IOException {
    if (hadoopFileSystem == null) {
      return false;
    }
    Path hadoopPath = new Path(excludedNodesPath);
    // Obtains the existing excluded node.
    Set<String> alreadyExistExcludedNodes =
        parseExcludedNodesFile(hadoopFileSystem.open(hadoopPath));
    // Writes to the new excluded node.
    alreadyExistExcludedNodes.removeAll(excludedNodes);
    writeExcludedNodes2File(Lists.newArrayList(alreadyExistExcludedNodes));
    return true;
  }

  @Override
  public void add(ServerNode node) {
    ServerNode pre = servers.get(node.getId());
    if (pre == null) {
      LOG.info("Newly registering node: {}", node.getId());
    } else {
      long regTime = pre.getRegistrationTime();
      // inherit registration time
      node.setRegistrationTime(regTime);
    }
    servers.put(node.getId(), node);

    Set<String> tags = node.getTags();
    // remove node with all tags to deal with the situation of tag change
    for (Set<ServerNode> nodes : tagToNodes.values()) {
      nodes.remove(node);
    }
    // add node to related tags
    for (String tag : tags) {
      tagToNodes.computeIfAbsent(tag, key -> Sets.newConcurrentHashSet());
      tagToNodes.get(tag).add(node);
    }
  }

  @Override
  public List<ServerNode> getServerList(Set<String> requiredTags) {
    List<ServerNode> availableNodes = Lists.newArrayList();
    for (ServerNode node : servers.values()) {
      if (!ServerStatus.ACTIVE.equals(node.getStatus())) {
        continue;
      }
      if (!excludedNodes.contains(node.getId()) && node.getTags().containsAll(requiredTags)) {
        availableNodes.add(node);
      }
    }
    return availableNodes;
  }

  @Override
  public List<ServerNode> getServerList(Set<String> requiredTags, Set<String> faultyServerIds) {
    List<ServerNode> availableNodes = Lists.newArrayList();
    for (ServerNode node : servers.values()) {
      if (!ServerStatus.ACTIVE.equals(node.getStatus())) {
        continue;
      }
      if (isNodeAvailable(requiredTags, faultyServerIds, node)) {
        availableNodes.add(node);
      }
    }
    return availableNodes;
  }

  private boolean isNodeAvailable(
      Set<String> requiredTags, Set<String> faultyServerIds, ServerNode node) {
    if (faultyServerIds != null && faultyServerIds.contains(node.getId())) {
      return false;
    }
    return !excludedNodes.contains(node.getId()) && node.getTags().containsAll(requiredTags);
  }

  @Override
  public List<ServerNode> getLostServerList() {
    return Lists.newArrayList(lostNodes);
  }

  @Override
  public List<ServerNode> getUnhealthyServerList() {
    return Lists.newArrayList(unhealthyNodes);
  }

  @Override
  public Set<String> getExcludedNodes() {
    return excludedNodes;
  }

  public Map<String, Set<ServerNode>> getTagToNodes() {
    return tagToNodes;
  }

  @Override
  public int getNodesNum() {
    return servers.size();
  }

  @Override
  public List<ServerNode> list() {
    for (Map.Entry<String, ServerNode> entry : servers.entrySet()) {
      ServerNode server = entry.getValue();
    }
    return Lists.newArrayList(servers.values());
  }

  @Override
  public boolean deleteLostServerById(String serverId) {
    if (StringUtils.isNotBlank(serverId)) {
      return lostNodes.remove(new ServerNode(serverId));
    }
    return false;
  }

  /** Add blacklist. */
  @Override
  public boolean addExcludedNodes(List<String> excludedNodeIds) {
    try {
      boolean successFlag = putInExcludedNodesFile(excludedNodeIds);
      excludedNodes.addAll(excludedNodeIds);
      return successFlag;
    } catch (IOException e) {
      LOG.warn("Because {}, failed to add blacklist.", e.getMessage());
      return false;
    }
  }

  @Override
  public boolean removeExcludedNodesFromFile(List<String> excludedNodeIds) {
    try {
      if (removeExcludedNodesFile(excludedNodeIds)) {
        excludedNodes.removeAll(excludedNodeIds);
        return true;
      }
    } catch (IOException e) {
      LOG.warn("Because {}, failed to add blacklist.", e.getMessage());
    }
    return false;
  }

  @VisibleForTesting
  public void clear() {
    servers.clear();
  }

  @Override
  public int getShuffleNodesMax() {
    return shuffleNodesMax.get();
  }

  @Override
  public boolean isReadyForServe() {
    if (!startupSilentPeriodEnabled) {
      return true;
    }

    if (!readyForServe && System.currentTimeMillis() - startTime > startupSilentPeriodDurationMs) {
      readyForServe = true;
    }

    return readyForServe;
  }

  @Override
  public void decommission(String serverId) {
    ServerNode serverNode = getServerNodeById(serverId);
    getShuffleServerClient(serverNode).decommission(new RssDecommissionRequest());
  }

  @Override
  public void cancelDecommission(String serverId) {
    ServerNode serverNode = getServerNodeById(serverId);
    getShuffleServerClient(serverNode).cancelDecommission(new RssCancelDecommissionRequest());
  }

  private ShuffleServerInternalGrpcClient getShuffleServerClient(ServerNode serverNode) {
    try {
      return clientCache.get(
          serverNode,
          () -> new ShuffleServerInternalGrpcClient(serverNode.getIp(), serverNode.getGrpcPort()));
    } catch (ExecutionException e) {
      throw new RssException(e);
    }
  }

  @Override
  public ServerNode getServerNodeById(String serverId) {
    ServerNode serverNode = servers.get(serverId);
    if (serverNode == null) {
      throw new InvalidRequestException("Server Id [" + serverId + "] not found!");
    }
    return serverNode;
  }

  @Override
  public void close() throws IOException {
    if (hadoopFileSystem != null) {
      hadoopFileSystem.close();
    }

    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdown();
    }

    if (checkNodesExecutorService != null) {
      checkNodesExecutorService.shutdown();
    }
  }

  @VisibleForTesting
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  @VisibleForTesting
  public void setReadyForServe(boolean readyForServe) {
    this.readyForServe = readyForServe;
  }

  @VisibleForTesting
  public void setStartupSilentPeriodEnabled(boolean startupSilentPeriodEnabled) {
    this.startupSilentPeriodEnabled = startupSilentPeriodEnabled;
  }

  @VisibleForTesting
  public Map<String, ServerNode> getServers() {
    return servers;
  }
}
