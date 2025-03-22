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

import java.util.function.Consumer;

import io.prometheus.client.CollectorRegistry;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import org.apache.uniffle.common.Arguments;
import org.apache.uniffle.common.ReconfigurableConfManager;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.metrics.GRPCMetrics;
import org.apache.uniffle.common.metrics.JvmMetrics;
import org.apache.uniffle.common.metrics.MetricReporter;
import org.apache.uniffle.common.metrics.MetricReporterFactory;
import org.apache.uniffle.common.rpc.ServerInterface;
import org.apache.uniffle.common.security.SecurityConfig;
import org.apache.uniffle.common.security.SecurityContextFactory;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.common.web.CoalescedCollectorRegistry;
import org.apache.uniffle.common.web.JettyServer;
import org.apache.uniffle.coordinator.conf.ClientConf;
import org.apache.uniffle.coordinator.conf.DynamicClientConfService;
import org.apache.uniffle.coordinator.conf.RssClientConfApplyManager;
import org.apache.uniffle.coordinator.metric.CoordinatorGrpcMetrics;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;
import org.apache.uniffle.coordinator.strategy.assignment.AssignmentStrategy;
import org.apache.uniffle.coordinator.strategy.assignment.AssignmentStrategyFactory;
import org.apache.uniffle.coordinator.util.CoordinatorUtils;

import static org.apache.uniffle.common.config.RssBaseConf.RSS_SECURITY_HADOOP_KERBEROS_ENABLE;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_SECURITY_HADOOP_KERBEROS_KEYTAB_FILE;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_SECURITY_HADOOP_KERBEROS_PRINCIPAL;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_SECURITY_HADOOP_KERBEROS_RELOGIN_INTERVAL_SEC;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_SECURITY_HADOOP_KRB5_CONF_FILE;

/** The main entrance of coordinator service */
public class CoordinatorServer {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorServer.class);

  private final CoordinatorConf coordinatorConf;
  private final long startTimeMs;
  private JettyServer jettyServer;
  private int jettyPort;
  private ServerInterface server;
  private ClusterManager clusterManager;
  private AssignmentStrategy assignmentStrategy;
  private RssClientConfApplyManager clientConfApplyManager;
  private AccessManager accessManager;
  private ApplicationManager applicationManager;
  private GRPCMetrics grpcMetrics;
  private MetricReporter metricReporter;
  private String id;
  private int rpcListenPort;

  public CoordinatorServer(CoordinatorConf coordinatorConf) throws Exception {
    this.startTimeMs = System.currentTimeMillis();
    this.coordinatorConf = coordinatorConf;
    try {
      initialization();
    } catch (Exception e) {
      LOG.error("Errors on initializing coordinator server.", e);
      throw e;
    }
  }

  public static void main(String[] args) throws Exception {
    Arguments arguments = new Arguments();
    CommandLine commandLine = new CommandLine(arguments);
    commandLine.parseArgs(args);
    String configFile = arguments.getConfigFile();
    LOG.info("Start to init coordinator server using config {}", configFile);

    // Load configuration from config files
    final CoordinatorConf coordinatorConf = new CoordinatorConf(configFile);
    ReconfigurableConfManager.init(coordinatorConf, configFile);

    // Start the coordinator service
    final CoordinatorServer coordinatorServer = new CoordinatorServer(coordinatorConf);

    coordinatorServer.start();
    coordinatorServer.blockUntilShutdown();
  }

  public void start() throws Exception {
    LOG.info(
        "{} version: {}", this.getClass().getSimpleName(), Constants.VERSION_AND_REVISION_SHORT);
    jettyPort = jettyServer.start();
    rpcListenPort = server.start();
    if (metricReporter != null) {
      metricReporter.start();
    }

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
  }

  public void stopServer() throws Exception {
    if (jettyServer != null) {
      jettyServer.stop();
    }
    if (applicationManager != null) {
      applicationManager.close();
    }
    if (clusterManager != null) {
      clusterManager.close();
    }
    if (accessManager != null) {
      accessManager.close();
    }
    if (clientConfApplyManager != null) {
      clientConfApplyManager.close();
    }
    if (metricReporter != null) {
      metricReporter.stop();
      LOG.info("Metric Reporter Stopped!");
    }
    SecurityContextFactory.get().getSecurityContext().close();
    server.stop();
  }

  private void initialization() throws Exception {
    String ip = RssUtils.getHostIp();
    if (ip == null) {
      throw new RssException("Couldn't acquire host Ip");
    }
    int port = coordinatorConf.getInteger(CoordinatorConf.RPC_SERVER_PORT);
    id = ip + "-" + port;
    LOG.info("Start to initialize coordinator {}", id);
    // register metrics first to avoid NPE problem when add dynamic metrics
    registerMetrics();
    coordinatorConf.setString(CoordinatorUtils.COORDINATOR_ID, id);
    this.applicationManager = new ApplicationManager(coordinatorConf);

    SecurityConfig securityConfig = null;
    if (coordinatorConf.getBoolean(RSS_SECURITY_HADOOP_KERBEROS_ENABLE)) {
      securityConfig =
          SecurityConfig.newBuilder()
              .krb5ConfPath(coordinatorConf.getString(RSS_SECURITY_HADOOP_KRB5_CONF_FILE))
              .keytabFilePath(coordinatorConf.getString(RSS_SECURITY_HADOOP_KERBEROS_KEYTAB_FILE))
              .principal(coordinatorConf.getString(RSS_SECURITY_HADOOP_KERBEROS_PRINCIPAL))
              .reloginIntervalSec(
                  coordinatorConf.getLong(RSS_SECURITY_HADOOP_KERBEROS_RELOGIN_INTERVAL_SEC))
              .build();
    }
    SecurityContextFactory.get().init(securityConfig);

    // load default hadoop configuration
    Configuration hadoopConf = new Configuration();
    ClusterManagerFactory clusterManagerFactory =
        new ClusterManagerFactory(coordinatorConf, hadoopConf);

    this.clusterManager = clusterManagerFactory.getClusterManager();

    DynamicClientConfService dynamicClientConfService =
        new DynamicClientConfService(
            coordinatorConf,
            hadoopConf,
            new Consumer[] {(Consumer<ClientConf>) applicationManager::refreshRemoteStorages});
    this.clientConfApplyManager =
        new RssClientConfApplyManager(coordinatorConf, dynamicClientConfService);

    AssignmentStrategyFactory assignmentStrategyFactory =
        new AssignmentStrategyFactory(coordinatorConf, clusterManager);
    this.assignmentStrategy = assignmentStrategyFactory.getAssignmentStrategy();
    this.accessManager =
        new AccessManager(
            coordinatorConf, clusterManager, applicationManager.getQuotaManager(), hadoopConf);
    CoordinatorFactory coordinatorFactory = new CoordinatorFactory(this);
    server = coordinatorFactory.getServer();
    jettyServer = new JettyServer(coordinatorConf);
    jettyServer.registerInstance(
        RssBaseConf.REST_AUTHORIZATION_CREDENTIALS.key(),
        coordinatorConf.getString(RssBaseConf.REST_AUTHORIZATION_CREDENTIALS));
    // register packages and instances for jersey
    jettyServer.addResourcePackages(
        "org.apache.uniffle.coordinator.web.resource", "org.apache.uniffle.common.web.resource");
    jettyServer.registerInstance(CoordinatorServer.class, this);
    jettyServer.registerInstance(ClusterManager.class, clusterManager);
    jettyServer.registerInstance(AccessManager.class, accessManager);
    jettyServer.registerInstance(ApplicationManager.class, applicationManager);
    jettyServer.registerInstance(
        CollectorRegistry.class.getCanonicalName() + "#server",
        CoordinatorMetrics.getCollectorRegistry());
    jettyServer.registerInstance(
        CollectorRegistry.class.getCanonicalName() + "#grpc", grpcMetrics.getCollectorRegistry());
    jettyServer.registerInstance(
        CollectorRegistry.class.getCanonicalName() + "#jvm", JvmMetrics.getCollectorRegistry());
    jettyServer.registerInstance(
        CollectorRegistry.class.getCanonicalName() + "#all",
        new CoalescedCollectorRegistry(
            CoordinatorMetrics.getCollectorRegistry(),
            grpcMetrics.getCollectorRegistry(),
            JvmMetrics.getCollectorRegistry()));
  }

  private void registerMetrics() throws Exception {
    LOG.info("Register metrics");
    CollectorRegistry coordinatorCollectorRegistry = new CollectorRegistry(true);
    CoordinatorMetrics.register(coordinatorCollectorRegistry);
    grpcMetrics = new CoordinatorGrpcMetrics(coordinatorConf);
    grpcMetrics.register(new CollectorRegistry(true));
    boolean verbose = coordinatorConf.getBoolean(CoordinatorConf.RSS_JVM_METRICS_VERBOSE_ENABLE);
    CollectorRegistry jvmCollectorRegistry = new CollectorRegistry(true);
    JvmMetrics.register(jvmCollectorRegistry, verbose);

    metricReporter = MetricReporterFactory.getMetricReporter(coordinatorConf, id);
    if (metricReporter != null) {
      metricReporter.addCollectorRegistry(CoordinatorMetrics.getCollectorRegistry());
      metricReporter.addCollectorRegistry(grpcMetrics.getCollectorRegistry());
      metricReporter.addCollectorRegistry(JvmMetrics.getCollectorRegistry());
    }
  }

  public ClusterManager getClusterManager() {
    return clusterManager;
  }

  public AssignmentStrategy getAssignmentStrategy() {
    return assignmentStrategy;
  }

  public CoordinatorConf getCoordinatorConf() {
    return coordinatorConf;
  }

  public ApplicationManager getApplicationManager() {
    return applicationManager;
  }

  public AccessManager getAccessManager() {
    return accessManager;
  }

  public RssClientConfApplyManager getClientConfApplyManager() {
    return clientConfApplyManager;
  }

  public GRPCMetrics getGrpcMetrics() {
    return grpcMetrics;
  }

  /** Await termination on the main thread since the grpc library uses daemon threads. */
  protected void blockUntilShutdown() throws InterruptedException {
    server.blockUntilShutdown();
  }

  public long getStartTimeMs() {
    return startTimeMs;
  }

  public int getRpcListenPort() {
    return rpcListenPort;
  }

  public int getJettyPort() {
    return jettyPort;
  }
}
