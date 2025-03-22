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

package org.apache.uniffle.dashboard.web;

import java.net.BindException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.org.glassfish.jersey.server.ServerProperties;
import org.apache.hbase.thirdparty.org.glassfish.jersey.servlet.ServletContainer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import org.apache.uniffle.common.Arguments;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.ExitUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.dashboard.web.config.DashboardConf;
import org.apache.uniffle.dashboard.web.proxy.WebProxyServlet;
import org.apache.uniffle.dashboard.web.utils.DashboardUtils;

public class Dashboard {

  private static final Logger LOG = LoggerFactory.getLogger(Dashboard.class);
  private final long startTimeMs;

  private DashboardConf conf;
  // Jetty Server
  private Server server;
  // FrontEnd Port
  private int httpPort;

  public Dashboard(DashboardConf coordinatorConf) {
    this.conf = coordinatorConf;
    this.startTimeMs = System.currentTimeMillis();
    initialization();
  }

  public static void main(String[] args) {
    Arguments arguments = new Arguments();
    CommandLine commandLine = new CommandLine(arguments);
    commandLine.parseArgs(args);
    String configFile = arguments.getConfigFile();
    LOG.info("Start to init dashboard http server using config {}", configFile);

    // Load configuration from config files
    final DashboardConf coodConf = new DashboardConf(configFile);
    Dashboard dashboard = new Dashboard(coodConf);
    dashboard.start();
  }

  private void initialization() {
    httpPort = conf.getInteger(DashboardConf.DASHBOARD_HTTP_PORT);
    ExecutorThreadPool threadPool = createThreadPool(conf);
    server = new Server(threadPool);
    server.setStopAtShutdown(true);
    server.setStopTimeout(conf.getLong(DashboardConf.DASHBOARD_STOP_TIMEOUT));
    server.addBean(new ScheduledExecutorScheduler("jetty-thread-pool", true));
    setRootServletHandler();
    HttpConfiguration httpConfig = new HttpConfiguration();
    addHttpConnector(httpPort, httpConfig, conf.getLong(DashboardConf.DASHBOARD_IDLE_TIMEOUT));
  }

  private void setRootServletHandler() {
    HandlerList handlers = new HandlerList();
    ResourceHandler resourceHandler = addResourceHandler();
    String coordinatorWebAddress = conf.getString(DashboardConf.COORDINATOR_WEB_ADDRESS);
    Preconditions.checkNotNull(coordinatorWebAddress, "Coordinator web address is null");
    Map<String, String> stringStringMap =
        DashboardUtils.convertAddressesStrToMap(coordinatorWebAddress);

    ServletContextHandler servletContextHandler = addProxyHandler(stringStringMap);
    handlers.setHandlers(new Handler[] {resourceHandler, servletContextHandler});
    server.setHandler(handlers);
  }

  private static ResourceHandler addResourceHandler() {
    ResourceHandler resourceHandler = new ResourceHandler();
    resourceHandler.setDirectoriesListed(true);
    resourceHandler.setBaseResource(
        Resource.newResource(Dashboard.class.getClassLoader().getResource("static")));
    resourceHandler.setWelcomeFiles(new String[] {"index.html"});
    return resourceHandler;
  }

  private ServletContextHandler addProxyHandler(Map<String, String> coordinatorServerAddresses) {
    ServletContextHandler contextHandler = new ServletContextHandler();
    ServletHolder holder = new ServletHolder(new WebProxyServlet(coordinatorServerAddresses));
    contextHandler.addServlet(holder, "/api/*");
    ServletHolder servletHolder = contextHandler.addServlet(ServletContainer.class, "/*");
    servletHolder.setInitParameter(
        ServerProperties.PROVIDER_PACKAGES, "org.apache.uniffle.dashboard.web.resource");
    contextHandler.setAttribute("coordinatorServerAddresses", coordinatorServerAddresses);
    contextHandler.setAttribute(Dashboard.class.getCanonicalName(), this);
    return contextHandler;
  }

  private void addHttpConnector(int port, HttpConfiguration httpConfig, long idleTimeout) {
    ServerConnector httpConnector =
        new ServerConnector(server, new HttpConnectionFactory(httpConfig));
    httpConnector.setPort(port);
    httpConnector.setIdleTimeout(idleTimeout);
    server.addConnector(httpConnector);
  }

  private ExecutorThreadPool createThreadPool(DashboardConf conf) {
    int corePoolSize = conf.getInteger(DashboardConf.DASHBOARD_CORE_POOL_SIZE);
    int maxPoolSize = conf.getInteger(DashboardConf.DASHBOARD_MAX_POOL_SIZE);
    ExecutorThreadPool pool =
        new ExecutorThreadPool(
            new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                ThreadUtils.getThreadFactory("DashboardServer")));
    return pool;
  }

  public void start() {
    LOG.info(
        "{} version: {}", this.getClass().getSimpleName(), Constants.VERSION_AND_REVISION_SHORT);
    try {
      server.start();
      server.join();
    } catch (BindException e) {
      ExitUtils.terminate(1, "Fail to dashboard http server", e, LOG);
    } catch (Exception e) {
      ExitUtils.terminate(1, "Fail to start dashboard http server", e, LOG);
    }
    LOG.info("Dashboard http server started, listening on port {}", httpPort);
  }

  public long getStartTimeMs() {
    return startTimeMs;
  }

  public DashboardConf getConf() {
    return conf;
  }
}
