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

package org.apache.tez.dag.app;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.token.Token;
import org.apache.tez.common.GetShuffleServerRequest;
import org.apache.tez.common.GetShuffleServerResponse;
import org.apache.tez.common.RssTezConfig;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.common.ServicePluginLifecycle;
import org.apache.tez.common.ShuffleAssignmentsInfoWritable;
import org.apache.tez.common.TezRemoteShuffleUmbilicalProtocol;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.security.authorize.RssTezAMPolicyProvider;
import org.apache.tez.dag.records.TezDAGID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.RetryUtils;
import org.apache.uniffle.proto.RssProtos;

import static org.apache.uniffle.common.config.RssClientConf.MAX_CONCURRENCY_PER_PARTITION_TO_WRITE;

public class TezRemoteShuffleManager implements ServicePluginLifecycle {
  private static final Logger LOG = LoggerFactory.getLogger(TezRemoteShuffleManager.class);

  private InetSocketAddress address;

  protected volatile Server server;
  private String tokenIdentifier;
  private Token<JobTokenIdentifier> sessionToken;
  private Configuration conf;
  private TezRemoteShuffleUmbilicalProtocolImpl tezRemoteShuffleUmbilical;
  private ShuffleWriteClient rssClient;
  private String appId;
  private UserGroupInformation requestUgi;
  private RemoteStorageInfo remoteStorage;

  public TezRemoteShuffleManager(
      String tokenIdentifier,
      Token<JobTokenIdentifier> sessionToken,
      Configuration conf,
      String appId,
      ShuffleWriteClient rssClient,
      RemoteStorageInfo remoteStorage)
      throws IOException {
    this.tokenIdentifier = tokenIdentifier;
    this.sessionToken = sessionToken;
    this.conf = conf;
    this.appId = appId;
    this.rssClient = rssClient;
    this.tezRemoteShuffleUmbilical = new TezRemoteShuffleUmbilicalProtocolImpl();
    this.requestUgi = UserGroupInformation.getCurrentUser();
    this.remoteStorage = remoteStorage;
  }

  @Override
  public void initialize() throws Exception {}

  @Override
  public void start() throws Exception {
    startRpcServer();
  }

  @Override
  public void shutdown() throws Exception {
    unregisterShuffle();
    server.stop();
  }

  public void unregisterShuffle() {
    if (rssClient != null) {
      LOG.info("unregister all shuffle for appid {}", appId);
      rssClient.unregisterShuffle(appId);
    }
  }

  public boolean unregisterShuffleByDagId(TezDAGID dagId) {
    try {
      Set<Integer> shuffleIds =
          tezRemoteShuffleUmbilical.getShuffleIdToShuffleAssignsInfo().keySet().stream()
              .filter(shuffleId -> dagId.getId() == RssTezUtils.parseDagId(shuffleId))
              .collect(Collectors.toSet());

      shuffleIds.forEach(
          shuffleId -> {
            long startTime = System.currentTimeMillis();
            rssClient.unregisterShuffle(appId, shuffleId);
            tezRemoteShuffleUmbilical.removeShuffleInfo(shuffleId);
            LOG.info(
                "Unregister shuffle successfully, appId={}, dagId={}, shuffleId={}, cost={}ms",
                appId,
                dagId,
                shuffleId,
                System.currentTimeMillis() - startTime);
          });
    } catch (Exception e) {
      LOG.info("Failed to unregister shuffle by dagId: {}", dagId, e);
      return false;
    }
    return true;
  }

  public InetSocketAddress getAddress() {
    return address;
  }

  private class TezRemoteShuffleUmbilicalProtocolImpl implements TezRemoteShuffleUmbilicalProtocol {
    private Map<Integer, ShuffleAssignmentsInfo> shuffleIdToShuffleAssignsInfo =
        JavaUtils.newConcurrentMap();

    @Override
    public long getProtocolVersion(String s, long l) throws IOException {
      return versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(
        String protocol, long clientVersion, int clientMethodsHash) throws IOException {
      return ProtocolSignature.getProtocolSignature(
          this, protocol, clientVersion, clientMethodsHash);
    }

    @Override
    public GetShuffleServerResponse getShuffleAssignments(GetShuffleServerRequest request)
        throws IOException, TezException {

      GetShuffleServerResponse response = new GetShuffleServerResponse();
      if (request != null) {
        LOG.info("getShuffleAssignments with request = " + request);
      } else {
        LOG.error("getShuffleAssignments with request is null");
        response.setStatus(-1);
        response.setRetMsg("GetShuffleServerRequest is null");
        return response;
      }

      int shuffleId = request.getShuffleId();
      ShuffleAssignmentsInfo shuffleAssignmentsInfo;
      try {
        synchronized (TezRemoteShuffleUmbilicalProtocolImpl.class) {
          if (shuffleIdToShuffleAssignsInfo.containsKey(shuffleId)) {
            shuffleAssignmentsInfo = shuffleIdToShuffleAssignsInfo.get(shuffleId);
          } else {
            shuffleAssignmentsInfo =
                getShuffleWorks(
                    request.getPartitionNum(),
                    shuffleId,
                    request.getKeyClassName(),
                    request.getValueClassName(),
                    request.getComparatorClassName());
          }

          if (shuffleAssignmentsInfo == null) {
            response.setStatus(-1);
            response.setRetMsg("shuffleAssignmentsInfo is null");
          } else {
            response.setStatus(0);
            response.setRetMsg("");
            response.setShuffleAssignmentsInfoWritable(
                new ShuffleAssignmentsInfoWritable(shuffleAssignmentsInfo));
            shuffleIdToShuffleAssignsInfo.put(shuffleId, shuffleAssignmentsInfo);
          }
        }
      } catch (Exception rssException) {
        response.setStatus(-2);
        response.setRetMsg(rssException.getMessage());
      }

      return response;
    }

    Map<Integer, ShuffleAssignmentsInfo> getShuffleIdToShuffleAssignsInfo() {
      return shuffleIdToShuffleAssignsInfo;
    }

    void removeShuffleInfo(int shuffleId) {
      shuffleIdToShuffleAssignsInfo.remove(shuffleId);
    }
  }

  private ShuffleAssignmentsInfo getShuffleWorks(
      int partitionNum,
      int shuffleId,
      String keyClassName,
      String valueClassName,
      String comparatorClassName) {
    ShuffleAssignmentsInfo shuffleAssignmentsInfo;
    int requiredAssignmentShuffleServersNum =
        RssTezUtils.getRequiredShuffleServerNumber(conf, 200, partitionNum);
    // retryInterval must bigger than `rss.server.heartbeat.timeout`, or maybe it will return the
    // same result
    long retryInterval =
        conf.getLong(
            RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL,
            RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL_DEFAULT_VALUE);
    int retryTimes =
        conf.getInt(
            RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES,
            RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES_DEFAULT_VALUE);

    // Get the configured server assignment tags and it will also add default shuffle version tag.
    Set<String> assignmentTags = new HashSet<>();
    String rawTags = conf.get(RssTezConfig.RSS_CLIENT_ASSIGNMENT_TAGS, "");
    if (StringUtils.isNotEmpty(rawTags)) {
      rawTags = rawTags.trim();
      assignmentTags.addAll(Arrays.asList(rawTags.split(",")));
    }
    assignmentTags.add(Constants.SHUFFLE_SERVER_VERSION);
    String clientType =
        conf.get(RssTezConfig.RSS_CLIENT_TYPE, RssTezConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE);
    ClientUtils.validateClientType(clientType);
    assignmentTags.add(clientType);

    try {
      shuffleAssignmentsInfo =
          RetryUtils.retry(
              // When communicate with TezRemoteShuffleUmbilicalProtocol, tez use applicationId
              // as ugi name. In security hdfs cluster, if we communicate with shuffle server with
              // applicationId ugi, the user of remote storage will be application_xxx_xx
              // As we knonw, the max id of hadoop user is 16777215. So we should use execute ugi.
              () ->
                  requestUgi.doAs(
                      new PrivilegedExceptionAction<ShuffleAssignmentsInfo>() {
                        @Override
                        public ShuffleAssignmentsInfo run() throws Exception {
                          ShuffleAssignmentsInfo shuffleAssignments =
                              rssClient.getShuffleAssignments(
                                  appId,
                                  shuffleId,
                                  partitionNum,
                                  1,
                                  Sets.newHashSet(assignmentTags),
                                  requiredAssignmentShuffleServersNum,
                                  -1);

                          Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges =
                              shuffleAssignments.getServerToPartitionRanges();

                          if (serverToPartitionRanges == null
                              || serverToPartitionRanges.isEmpty()) {
                            return null;
                          }
                          LOG.info("Start to register shuffle");
                          long start = System.currentTimeMillis();
                          serverToPartitionRanges
                              .entrySet()
                              .forEach(
                                  entry ->
                                      rssClient.registerShuffle(
                                          entry.getKey(),
                                          appId,
                                          shuffleId,
                                          entry.getValue(),
                                          remoteStorage,
                                          ShuffleDataDistributionType.NORMAL,
                                          RssTezConfig.toRssConf(conf)
                                              .get(MAX_CONCURRENCY_PER_PARTITION_TO_WRITE),
                                          0,
                                          StringUtils.isBlank(keyClassName)
                                              ? null
                                              : RssProtos.MergeContext.newBuilder()
                                                  .setKeyClass(keyClassName)
                                                  .setValueClass(valueClassName)
                                                  .setComparatorClass(comparatorClassName)
                                                  .setMergedBlockSize(
                                                      conf.getInt(
                                                          RssTezConfig.RSS_MERGED_BLOCK_SZIE,
                                                          RssTezConfig
                                                              .RSS_MERGED_BLOCK_SZIE_DEFAULT))
                                                  .setMergeClassLoader(
                                                      conf.get(
                                                          RssTezConfig
                                                              .RSS_REMOTE_MERGE_CLASS_LOADER,
                                                          ""))
                                                  .build()));
                          LOG.info(
                              "Finish register shuffle with "
                                  + (System.currentTimeMillis() - start)
                                  + " ms");
                          return shuffleAssignments;
                        }
                      }),
              retryInterval,
              retryTimes);
    } catch (Throwable throwable) {
      LOG.error("registerShuffle failed!", throwable);
      throw new RssException("registerShuffle failed!", throwable);
    }

    return shuffleAssignmentsInfo;
  }

  protected void startRpcServer() {
    try {
      String rssAmRpcBindAddress;
      Integer rssAmRpcBindPort;
      if (conf.getBoolean(RssTezConfig.RSS_AM_SHUFFLE_MANAGER_DEBUG, false)) {
        rssAmRpcBindAddress = conf.get(RssTezConfig.RSS_AM_SHUFFLE_MANAGER_ADDRESS, "0.0.0.0");
        rssAmRpcBindPort = conf.getInt(RssTezConfig.RSS_AM_SHUFFLE_MANAGER_PORT, 0);
      } else {
        rssAmRpcBindAddress = "0.0.0.0";
        rssAmRpcBindPort = 0;
      }

      JobTokenSecretManager jobTokenSecretManager = new JobTokenSecretManager();
      jobTokenSecretManager.addTokenForJob(tokenIdentifier, sessionToken);
      server =
          new RPC.Builder(conf)
              .setProtocol(TezRemoteShuffleUmbilicalProtocol.class)
              .setBindAddress(rssAmRpcBindAddress)
              .setPort(rssAmRpcBindPort)
              .setInstance(tezRemoteShuffleUmbilical)
              .setNumHandlers(
                  conf.getInt(
                      TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT,
                      TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT_DEFAULT))
              .setPortRangeConfig(TezConfiguration.TEZ_AM_TASK_AM_PORT_RANGE)
              .setSecretManager(jobTokenSecretManager)
              .build();

      // Enable service authorization?
      if (conf.getBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
        refreshServiceAcls(conf, new RssTezAMPolicyProvider());
      }

      server.start();
      InetSocketAddress serverBindAddress = NetUtils.getConnectAddress(server);
      this.address =
          NetUtils.createSocketAddrForHost(
              serverBindAddress.getAddress().getCanonicalHostName(), serverBindAddress.getPort());
      LOG.info("Instantiated TezRemoteShuffleManager RPC at " + this.address);
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
  }

  private void refreshServiceAcls(Configuration configuration, PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }
}
