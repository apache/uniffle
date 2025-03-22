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

package org.apache.uniffle.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.util.internal.PlatformDependent;
import io.prometheus.client.CollectorRegistry;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import org.apache.uniffle.client.request.RssSendHeartBeatRequest;
import org.apache.uniffle.common.Arguments;
import org.apache.uniffle.common.ReconfigurableConfManager;
import org.apache.uniffle.common.ServerStatus;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.exception.InvalidRequestException;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.metrics.GRPCMetrics;
import org.apache.uniffle.common.metrics.JvmMetrics;
import org.apache.uniffle.common.metrics.MetricReporter;
import org.apache.uniffle.common.metrics.MetricReporterFactory;
import org.apache.uniffle.common.metrics.NettyMetrics;
import org.apache.uniffle.common.rpc.ServerInterface;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.security.SecurityConfig;
import org.apache.uniffle.common.security.SecurityContextFactory;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.ExitUtils;
import org.apache.uniffle.common.util.JvmPauseMonitor;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.common.web.CoalescedCollectorRegistry;
import org.apache.uniffle.common.web.JettyServer;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.server.buffer.ShuffleBufferManager;
import org.apache.uniffle.server.buffer.ShuffleBufferType;
import org.apache.uniffle.server.merge.ShuffleMergeManager;
import org.apache.uniffle.server.netty.StreamServer;
import org.apache.uniffle.server.storage.StorageManager;
import org.apache.uniffle.server.storage.StorageManagerFactory;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.common.config.RssBaseConf.RSS_SECURITY_HADOOP_KERBEROS_ENABLE;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_SECURITY_HADOOP_KERBEROS_KEYTAB_FILE;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_SECURITY_HADOOP_KERBEROS_PRINCIPAL;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_SECURITY_HADOOP_KERBEROS_PROXY_USER_ENABLE;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_SECURITY_HADOOP_KERBEROS_RELOGIN_INTERVAL_SEC;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_SECURITY_HADOOP_KRB5_CONF_FILE;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_STORAGE_TYPE;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_TEST_MODE_ENABLE;
import static org.apache.uniffle.common.metrics.CommonMetrics.JVM_PAUSE_INFO_TIME_EXCEEDED;
import static org.apache.uniffle.common.metrics.CommonMetrics.JVM_PAUSE_TOTAL_EXTRA_TIME;
import static org.apache.uniffle.common.metrics.CommonMetrics.JVM_PAUSE_WARN_TIME_EXCEEDED;
import static org.apache.uniffle.server.ShuffleServerConf.SERVER_DECOMMISSION_CHECK_INTERVAL;
import static org.apache.uniffle.server.ShuffleServerConf.SERVER_DECOMMISSION_SHUTDOWN;
import static org.apache.uniffle.server.ShuffleServerMetrics.USED_DIRECT_MEMORY_SIZE;
import static org.apache.uniffle.server.ShuffleServerMetrics.USED_DIRECT_MEMORY_SIZE_BY_GRPC_NETTY;
import static org.apache.uniffle.server.ShuffleServerMetrics.USED_DIRECT_MEMORY_SIZE_BY_NETTY;

/** Server that manages startup/shutdown of a {@code Greeter} server. */
public class ShuffleServer {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServer.class);
  private RegisterHeartBeat registerHeartBeat;
  private String id;
  private String ip;
  private int grpcPort;
  private int nettyPort;
  private ShuffleServerConf shuffleServerConf;
  private JettyServer jettyServer;
  private int jettyPort;
  private ShuffleTaskManager shuffleTaskManager;
  private ServerInterface server;
  private ShuffleFlushManager shuffleFlushManager;
  private ShuffleBufferManager shuffleBufferManager;
  private StorageManager storageManager;
  private boolean remoteMergeEnable;
  private ShuffleMergeManager shuffleMergeManager;
  private HealthCheck healthCheck;
  private Set<String> tags = Sets.newHashSet();
  private GRPCMetrics grpcMetrics;
  private NettyMetrics nettyMetrics;
  private MetricReporter metricReporter;

  private AtomicReference<ServerStatus> serverStatus = new AtomicReference(ServerStatus.ACTIVE);
  private volatile boolean running;
  private ExecutorService executorService;
  private Future<?> decommissionFuture;
  private boolean nettyServerEnabled;
  private StreamServer streamServer;
  private JvmPauseMonitor jvmPauseMonitor;

  private final long startTimeMs;

  public ShuffleServer(ShuffleServerConf shuffleServerConf) throws Exception {
    this.shuffleServerConf = shuffleServerConf;
    this.startTimeMs = System.currentTimeMillis();
    try {
      initialization();
    } catch (Exception e) {
      LOG.error("Errors on initializing shuffle server.", e);
      throw e;
    }
  }

  /** Main launches the server from the command line. */
  public static void main(String[] args) throws Exception {
    Arguments arguments = new Arguments();
    CommandLine commandLine = new CommandLine(arguments);
    commandLine.parseArgs(args);
    String configFile = arguments.getConfigFile();
    LOG.info("Start to init shuffle server using config {}", configFile);

    ShuffleServerConf shuffleServerConf = new ShuffleServerConf(configFile);
    ReconfigurableConfManager.init(shuffleServerConf, configFile);

    final ShuffleServer shuffleServer = new ShuffleServer(shuffleServerConf);
    shuffleServer.start();

    shuffleServer.blockUntilShutdown();
  }

  public void start() throws Exception {
    LOG.info(
        "{} version: {}", this.getClass().getSimpleName(), Constants.VERSION_AND_REVISION_SHORT);
    jettyPort = jettyServer.start();
    grpcPort = server.start();
    if (nettyServerEnabled) {
      nettyPort = streamServer.start();
    }

    if (nettyServerEnabled) {
      id = ip + "-" + grpcPort + "-" + nettyPort;
    } else {
      id = ip + "-" + grpcPort;
    }
    shuffleServerConf.setString(ShuffleServerConf.SHUFFLE_SERVER_ID, id);
    LOG.info("Start to shuffle server with id {}", id);
    initMetricsReporter();

    registerHeartBeat.startHeartBeat();
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread() {
              @Override
              public void run() {
                LOG.info("*** shutting down gRPC server since JVM is shutting down");
                try {
                  stopServer();
                } catch (Exception e) {
                  LOG.error(e.getMessage());
                }
                LOG.info("*** server shut down");
              }
            });
    running = true;
    LOG.info("Shuffle server start successfully!");
  }

  public void stopServer() throws Exception {
    if (jettyServer != null) {
      jettyServer.stop();
      LOG.info("Jetty Server Stopped!");
    }
    if (registerHeartBeat != null) {
      registerHeartBeat.shutdown();
      LOG.info("HeartBeat Stopped!");
    }
    if (storageManager != null) {
      storageManager.stop();
      LOG.info("MultiStorage Stopped!");
    }
    if (healthCheck != null) {
      healthCheck.stop();
      LOG.info("HealthCheck stopped!");
    }
    if (metricReporter != null) {
      metricReporter.stop();
      LOG.info("Metric Reporter Stopped!");
    }
    SecurityContextFactory.get().getSecurityContext().close();
    server.stop();
    if (nettyServerEnabled && streamServer != null) {
      streamServer.stop();
    }
    if (executorService != null) {
      executorService.shutdownNow();
    }
    if (shuffleTaskManager != null) {
      shuffleTaskManager.stop();
    }
    if (jvmPauseMonitor != null) {
      jvmPauseMonitor.close();
    }
    running = false;
    LOG.info("RPC Server Stopped!");
  }

  private void initialization() throws Exception {
    // setup jvm pause monitor
    final JvmPauseMonitor monitor = new JvmPauseMonitor(shuffleServerConf);
    monitor.start();
    this.jvmPauseMonitor = monitor;

    boolean testMode = shuffleServerConf.getBoolean(RSS_TEST_MODE_ENABLE);
    String storageType = shuffleServerConf.get(RSS_STORAGE_TYPE).name();
    if (!testMode
        && (StorageType.LOCALFILE.name().equals(storageType)
            || (StorageType.HDFS.name()).equals(storageType))) {
      throw new IllegalArgumentException(
          "RSS storage type about LOCALFILE and HADOOP should be used in test mode, "
              + "because of the poor performance of these two types.");
    }
    ip = RssUtils.getHostIp();
    if (ip == null) {
      throw new RssException("Couldn't acquire host Ip");
    }
    grpcPort = shuffleServerConf.getInteger(ShuffleServerConf.RPC_SERVER_PORT);
    nettyPort = shuffleServerConf.getInteger(ShuffleServerConf.NETTY_SERVER_PORT);

    initServerTags();

    jettyServer = new JettyServer(shuffleServerConf);
    registerMetrics();
    // register packages and instances for jersey
    jettyServer.addResourcePackages(
        "org.apache.uniffle.server.web.resource", "org.apache.uniffle.common.web.resource");
    jettyServer.registerInstance(ShuffleServer.class, this);
    jettyServer.registerInstance(
        CollectorRegistry.class.getCanonicalName() + "#server",
        ShuffleServerMetrics.getCollectorRegistry());
    jettyServer.registerInstance(
        CollectorRegistry.class.getCanonicalName() + "#grpc", grpcMetrics.getCollectorRegistry());
    jettyServer.registerInstance(
        CollectorRegistry.class.getCanonicalName() + "#netty", nettyMetrics.getCollectorRegistry());
    jettyServer.registerInstance(
        CollectorRegistry.class.getCanonicalName() + "#jvm", JvmMetrics.getCollectorRegistry());
    jettyServer.registerInstance(
        CollectorRegistry.class.getCanonicalName() + "#all",
        new CoalescedCollectorRegistry(
            ShuffleServerMetrics.getCollectorRegistry(),
            grpcMetrics.getCollectorRegistry(),
            nettyMetrics.getCollectorRegistry(),
            JvmMetrics.getCollectorRegistry()));

    SecurityConfig securityConfig = null;
    if (shuffleServerConf.getBoolean(RSS_SECURITY_HADOOP_KERBEROS_ENABLE)) {
      securityConfig =
          SecurityConfig.newBuilder()
              .krb5ConfPath(shuffleServerConf.getString(RSS_SECURITY_HADOOP_KRB5_CONF_FILE))
              .keytabFilePath(shuffleServerConf.getString(RSS_SECURITY_HADOOP_KERBEROS_KEYTAB_FILE))
              .principal(shuffleServerConf.getString(RSS_SECURITY_HADOOP_KERBEROS_PRINCIPAL))
              .reloginIntervalSec(
                  shuffleServerConf.getLong(RSS_SECURITY_HADOOP_KERBEROS_RELOGIN_INTERVAL_SEC))
              .enableProxyUser(
                  shuffleServerConf.getBoolean(RSS_SECURITY_HADOOP_KERBEROS_PROXY_USER_ENABLE))
              .build();
    }
    SecurityContextFactory.get().init(securityConfig);

    storageManager = StorageManagerFactory.getInstance().createStorageManager(shuffleServerConf);
    storageManager.start();

    boolean healthCheckEnable = shuffleServerConf.getBoolean(ShuffleServerConf.HEALTH_CHECK_ENABLE);
    if (healthCheckEnable) {
      List<Checker> builtInCheckers = Lists.newArrayList();
      builtInCheckers.add(storageManager.getStorageChecker());
      healthCheck = new HealthCheck(serverStatus, shuffleServerConf, builtInCheckers);
      healthCheck.start();
    }

    nettyServerEnabled =
        shuffleServerConf.get(ShuffleServerConf.RPC_SERVER_TYPE) == ServerType.GRPC_NETTY;
    if (nettyServerEnabled) {
      if (nettyPort < 0) {
        throw new RssException(
            String.format(
                "%s must be set during startup when using GRPC_NETTY",
                ShuffleServerConf.NETTY_SERVER_PORT.key()));
      }
      streamServer = new StreamServer(this);
    }

    registerHeartBeat = new RegisterHeartBeat(this);
    shuffleFlushManager = new ShuffleFlushManager(shuffleServerConf, this, storageManager);
    shuffleBufferManager =
        new ShuffleBufferManager(shuffleServerConf, shuffleFlushManager, nettyServerEnabled);
    remoteMergeEnable = shuffleServerConf.get(ShuffleServerConf.SERVER_MERGE_ENABLE);
    if (remoteMergeEnable) {
      if (shuffleBufferManager.getShuffleBufferType() != ShuffleBufferType.SKIP_LIST) {
        throw new RssException(
            "Shuffle buffer type must be SKIP_LIST when remote merge is enable!");
      }
      shuffleMergeManager = new ShuffleMergeManager(shuffleServerConf, this);
    }
    shuffleTaskManager =
        new ShuffleTaskManager(
            shuffleServerConf,
            shuffleFlushManager,
            shuffleBufferManager,
            storageManager,
            shuffleMergeManager);
    shuffleTaskManager.start();
    ShuffleServerMetrics.addLabeledGauge(
        USED_DIRECT_MEMORY_SIZE_BY_NETTY, PlatformDependent::usedDirectMemory);
    ShuffleServerMetrics.addLabeledGauge(
        USED_DIRECT_MEMORY_SIZE_BY_GRPC_NETTY,
        io.grpc.netty.shaded.io.netty.util.internal.PlatformDependent::usedDirectMemory);
    ShuffleServerMetrics.addLabeledGauge(
        USED_DIRECT_MEMORY_SIZE,
        () ->
            (PlatformDependent.usedDirectMemory()
                + io.grpc.netty.shaded.io.netty.util.internal.PlatformDependent
                    .usedDirectMemory()));
    ShuffleServerMetrics.addLabeledGauge(
        JVM_PAUSE_TOTAL_EXTRA_TIME, jvmPauseMonitor::getTotalGcExtraSleepTime);
    ShuffleServerMetrics.addLabeledGauge(
        JVM_PAUSE_INFO_TIME_EXCEEDED, jvmPauseMonitor::getNumGcInfoThresholdExceeded);
    ShuffleServerMetrics.addLabeledGauge(
        JVM_PAUSE_WARN_TIME_EXCEEDED, jvmPauseMonitor::getNumGcWarnThresholdExceeded);

    setServer();
  }

  private void initServerTags() {
    // it's the system tag for server's version
    tags.add(Constants.SHUFFLE_SERVER_VERSION);
    // the rpc service type bound into tags
    tags.add(shuffleServerConf.get(RssBaseConf.RPC_SERVER_TYPE).name());

    List<String> configuredTags = shuffleServerConf.get(ShuffleServerConf.TAGS);
    if (CollectionUtils.isNotEmpty(configuredTags)) {
      tags.addAll(configuredTags);
    }

    LOG.info("Server tags: {}", tags);
  }

  private void registerMetrics() {
    LOG.info("Register metrics");
    CollectorRegistry shuffleServerCollectorRegistry = new CollectorRegistry(true);
    String rawTags = getEncodedTags();
    ShuffleServerMetrics.register(shuffleServerCollectorRegistry, rawTags, shuffleServerConf);
    grpcMetrics = new ShuffleServerGrpcMetrics(this.shuffleServerConf, rawTags);
    grpcMetrics.register(new CollectorRegistry(true));
    nettyMetrics = new ShuffleServerNettyMetrics(shuffleServerConf, rawTags);
    nettyMetrics.register(new CollectorRegistry(true));
    CollectorRegistry jvmCollectorRegistry = new CollectorRegistry(true);
    boolean verbose =
        shuffleServerConf.getBoolean(ShuffleServerConf.RSS_JVM_METRICS_VERBOSE_ENABLE);
    JvmMetrics.register(jvmCollectorRegistry, verbose);
  }

  private void initMetricsReporter() throws Exception {
    metricReporter = MetricReporterFactory.getMetricReporter(shuffleServerConf, id);
    if (metricReporter != null) {
      metricReporter.addCollectorRegistry(ShuffleServerMetrics.getCollectorRegistry());
      metricReporter.addCollectorRegistry(grpcMetrics.getCollectorRegistry());
      metricReporter.addCollectorRegistry(nettyMetrics.getCollectorRegistry());
      metricReporter.addCollectorRegistry(JvmMetrics.getCollectorRegistry());
      metricReporter.start();
    }
  }

  /** Await termination on the main thread since the grpc library uses daemon threads. */
  private void blockUntilShutdown() throws InterruptedException {
    server.blockUntilShutdown();
  }

  public ServerStatus getServerStatus() {
    return serverStatus.get();
  }

  public synchronized void decommission() {
    if (isDecommissioning() || isDecommissioned()) {
      LOG.info("Shuffle Server is decommissioning. Nothing needs to be done.");
      return;
    }
    boolean wasActive =
        serverStatus.compareAndSet(ServerStatus.ACTIVE, ServerStatus.DECOMMISSIONING);
    if (!wasActive) {
      throw new InvalidRequestException(
          "Shuffle Server is processing other procedures, current status:" + serverStatus);
    }

    LOG.info("Shuffle Server is decommissioning.");
    if (executorService == null) {
      executorService = ThreadUtils.getDaemonSingleThreadExecutor("shuffle-server-decommission");
    }
    decommissionFuture = executorService.submit(this::waitDecommissionFinish);
  }

  private void waitDecommissionFinish() {
    long checkInterval = shuffleServerConf.get(SERVER_DECOMMISSION_CHECK_INTERVAL);
    boolean shutdownAfterDecommission = shuffleServerConf.get(SERVER_DECOMMISSION_SHUTDOWN);
    int remainApplicationNum;
    while (isDecommissioning()) {
      remainApplicationNum = shuffleTaskManager.getAppIds().size();
      if (remainApplicationNum == 0) {
        boolean wasDecommissioning =
            serverStatus.compareAndSet(ServerStatus.DECOMMISSIONING, ServerStatus.DECOMMISSIONED);
        LOG.info("All applications finished. Current status is " + serverStatus);
        if (!wasDecommissioning) {
          LOG.info("Ready to decommission but decommissioning state left unexpectedly.");
          break;
        }

        if (shutdownAfterDecommission) {
          LOG.info("Exiting...");
          try {
            stopServer();
          } catch (Exception e) {
            ExitUtils.terminate(1, "Stop server failed!", e, LOG);
          }
        }
        break;
      }
      LOG.info(
          "Shuffle server is decommissioning, remaining {} applications not finished.",
          remainApplicationNum);
      try {
        Thread.sleep(checkInterval);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for decommission to finish");
        break;
      }
    }
    remainApplicationNum = shuffleTaskManager.getAppIds().size();
    if (remainApplicationNum > 0) {
      LOG.info(
          "Decommission exiting, remaining {} applications not finished.", remainApplicationNum);
    }
  }

  public synchronized void cancelDecommission() {
    boolean wasDecommissioning =
        serverStatus.compareAndSet(ServerStatus.DECOMMISSIONING, ServerStatus.ACTIVE);
    boolean wasDecommissioned =
        serverStatus.compareAndSet(ServerStatus.DECOMMISSIONED, ServerStatus.ACTIVE);
    if (!wasDecommissioning && !wasDecommissioned) {
      LOG.info("Shuffle server is not decommissioning. Nothing needs to be done.");
      return;
    }

    if (wasDecommissioning) {
      if (decommissionFuture.cancel(true)) {
        LOG.info("Decommission canceled.");
      } else {
        LOG.warn("Failed to cancel decommission.");
      }
      decommissionFuture = null;
    }
  }

  public String getIp() {
    return this.ip;
  }

  public String getId() {
    return this.id;
  }

  public int getGrpcPort() {
    return this.grpcPort;
  }

  public ShuffleServerConf getShuffleServerConf() {
    return this.shuffleServerConf;
  }

  public ServerInterface getServer() {
    return server;
  }

  @VisibleForTesting
  public void setServer() {
    ShuffleServerFactory shuffleServerFactory = new ShuffleServerFactory(this);
    server = shuffleServerFactory.getServer();
  }

  public void setServer(ServerInterface server) {
    this.server = server;
  }

  public ShuffleTaskManager getShuffleTaskManager() {
    return shuffleTaskManager;
  }

  public ShuffleFlushManager getShuffleFlushManager() {
    return shuffleFlushManager;
  }

  public long getUsedMemory() {
    return shuffleBufferManager.getUsedMemory();
  }

  public long getPreAllocatedMemory() {
    return shuffleBufferManager.getPreAllocatedSize();
  }

  public long getAvailableMemory() {
    return shuffleBufferManager.getCapacity() - shuffleBufferManager.getUsedMemory();
  }

  public int getEventNumInFlush() {
    return shuffleFlushManager.getEventNumInFlush();
  }

  public ShuffleBufferManager getShuffleBufferManager() {
    return shuffleBufferManager;
  }

  public StorageManager getStorageManager() {
    return storageManager;
  }

  public Set<String> getTags() {
    return Collections.unmodifiableSet(tags);
  }

  @VisibleForTesting
  public void markUnhealthy() {
    serverStatus.set(ServerStatus.UNHEALTHY);
  }

  public GRPCMetrics getGrpcMetrics() {
    return grpcMetrics;
  }

  public NettyMetrics getNettyMetrics() {
    return nettyMetrics;
  }

  public boolean isDecommissioning() {
    return ServerStatus.DECOMMISSIONING.equals(serverStatus.get());
  }

  public boolean isDecommissioned() {
    return ServerStatus.DECOMMISSIONED.equals(serverStatus.get());
  }

  @VisibleForTesting
  public boolean isRunning() {
    return running;
  }

  public int getNettyPort() {
    return nettyPort;
  }

  public int getJettyPort() {
    return jettyPort;
  }

  public String getEncodedTags() {
    return StringUtils.join(tags, ",");
  }

  public long getStartTimeMs() {
    return startTimeMs;
  }

  public List<RssProtos.ApplicationInfo> getAppInfos() {
    List<RssProtos.ApplicationInfo> appInfos = new ArrayList<>();
    Map<String, ShuffleTaskInfo> taskInfos = getShuffleTaskManager().getShuffleTaskInfos();
    taskInfos.forEach(
        (appId, taskInfo) -> {
          RssProtos.ApplicationInfo applicationInfo =
              RssProtos.ApplicationInfo.newBuilder()
                  .setAppId(appId)
                  .setPartitionNum(taskInfo.getPartitionNum())
                  .setMemorySize(taskInfo.getInMemoryDataSize())
                  .setLocalTotalSize(taskInfo.getOnLocalFileDataSize())
                  .setHadoopTotalSize(taskInfo.getOnHadoopDataSize())
                  .setTotalSize(taskInfo.getTotalDataSize())
                  .build();

          appInfos.add(applicationInfo);
        });
    return appInfos;
  }

  @VisibleForTesting
  public void sendHeartbeat() {
    ShuffleServer shuffleServer = this;
    RssSendHeartBeatRequest request =
        new RssSendHeartBeatRequest(
            shuffleServer.getId(),
            shuffleServer.getIp(),
            shuffleServer.getGrpcPort(),
            shuffleServer.getUsedMemory(),
            shuffleServer.getPreAllocatedMemory(),
            shuffleServer.getAvailableMemory(),
            shuffleServer.getEventNumInFlush(),
            registerHeartBeat.getHeartBeatInterval(),
            shuffleServer.getTags(),
            shuffleServer.getServerStatus(),
            shuffleServer.getStorageManager().getStorageInfo(),
            shuffleServer.getNettyPort(),
            shuffleServer.getJettyPort(),
            shuffleServer.getStartTimeMs(),
            shuffleServer.getAppInfos(),
            shuffleServer.getDisplayMetrics());
    registerHeartBeat.sendHeartBeat(request);
  }

  public ShuffleMergeManager getShuffleMergeManager() {
    return shuffleMergeManager;
  }

  public boolean isRemoteMergeEnable() {
    return remoteMergeEnable;
  }

  public Map<String, String> getDisplayMetrics() {
    List<String> list = shuffleServerConf.get(ShuffleServerConf.SERVER_DISPLAY_METRICS_LIST);
    return list.stream()
        .map(metric -> metric.split(":", 2))
        .filter(parts -> parts.length == 2)
        .collect(
            Collectors.toMap(
                parts -> parts[0], // 使用第一部分作为键
                parts -> String.valueOf(ShuffleServerMetrics.getMetricsValue(parts[1]))));
  }
}
