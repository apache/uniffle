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

package org.apache.uniffle.common.web;

import java.io.FileNotFoundException;
import java.net.BindException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hbase.thirdparty.org.glassfish.jersey.server.ServerProperties;
import org.apache.hbase.thirdparty.org.glassfish.jersey.servlet.ServletContainer;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.util.ExitUtils;
import org.apache.uniffle.common.util.ThreadUtils;

import static org.apache.uniffle.common.web.resource.ConfOpsResource.SERVLET_CONTEXT_ATTR_CONF;

public class JettyServer {

  private static final Logger LOG = LoggerFactory.getLogger(JettyServer.class);

  private Server server;
  private ServletContextHandler servletContextHandler;
  private int httpPort;
  private ServletHolder servletHolder;
  private Set<String> resourcePackages = new HashSet<>();

  public JettyServer(RssBaseConf conf) throws FileNotFoundException {
    createServer(conf);
  }

  public void createServer(RssBaseConf conf) throws FileNotFoundException {
    httpPort = conf.getInteger(RssBaseConf.JETTY_HTTP_PORT);
    ExecutorThreadPool threadPool = createThreadPool(conf);
    server = new Server(threadPool);
    server.setStopAtShutdown(true);
    server.setStopTimeout(conf.getLong(RssBaseConf.JETTY_STOP_TIMEOUT));
    server.addBean(new ScheduledExecutorScheduler("jetty-thread-pool", true));

    HttpConfiguration httpConfig = new HttpConfiguration();
    addHttpConnector(
        conf.getInteger(RssBaseConf.JETTY_HTTP_PORT),
        httpConfig,
        conf.getLong(RssBaseConf.JETTY_HTTP_IDLE_TIMEOUT));

    setRootServletHandler();

    if (conf.getBoolean(RssBaseConf.JETTY_SSL_ENABLE)) {
      addHttpsConnector(httpConfig, conf);
    }

    if (servletContextHandler != null) {
      servletContextHandler.setAttribute(SERVLET_CONTEXT_ATTR_CONF, conf);
    }
  }

  private ExecutorThreadPool createThreadPool(RssBaseConf conf) {
    int corePoolSize = conf.getInteger(RssBaseConf.JETTY_CORE_POOL_SIZE);
    int maxPoolSize = conf.getInteger(RssBaseConf.JETTY_MAX_POOL_SIZE);
    ExecutorThreadPool pool =
        new ExecutorThreadPool(
            new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                ThreadUtils.getThreadFactory("Jetty")));
    return pool;
  }

  private void addHttpConnector(int port, HttpConfiguration httpConfig, long idleTimeout) {
    ServerConnector httpConnector =
        new ServerConnector(server, new HttpConnectionFactory(httpConfig));
    httpConnector.setPort(port);
    httpConnector.setIdleTimeout(idleTimeout);
    server.addConnector(httpConnector);
  }

  private void addHttpsConnector(HttpConfiguration httpConfig, RssBaseConf conf)
      throws FileNotFoundException {
    LOG.info("Create https connector");
    Path keystorePath = Paths.get(conf.get(RssBaseConf.JETTY_SSL_KEYSTORE_PATH)).toAbsolutePath();
    if (!Files.exists(keystorePath)) {
      throw new FileNotFoundException(keystorePath.toString());
    }

    SslContextFactory sslContextFactory = new SslContextFactory();
    sslContextFactory.setKeyStorePath(keystorePath.toString());
    sslContextFactory.setKeyStorePassword(conf.get(RssBaseConf.JETTY_SSL_KEYSTORE_PASSWORD));
    sslContextFactory.setKeyManagerPassword(conf.get(RssBaseConf.JETTY_SSL_KEYMANAGER_PASSWORD));
    sslContextFactory.setTrustStorePath(keystorePath.toString());
    sslContextFactory.setTrustStorePassword(conf.get(RssBaseConf.JETTY_SSL_TRUSTSTORE_PASSWORD));

    int securePort = conf.getInteger(RssBaseConf.JETTY_HTTPS_PORT);
    httpConfig.setSecurePort(securePort);
    HttpConfiguration httpsConfig = new HttpConfiguration(httpConfig);
    httpsConfig.addCustomizer(new SecureRequestCustomizer());

    ServerConnector sslConnector =
        new ServerConnector(
            server,
            new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
            new HttpConnectionFactory(httpsConfig));
    sslConnector.setPort(securePort);

    server.addConnector(sslConnector);
  }

  private void setRootServletHandler() {
    servletContextHandler = new ServletContextHandler();
    servletContextHandler.setContextPath("/");
    server.setHandler(servletContextHandler);
    servletHolder = servletContextHandler.addServlet(ServletContainer.class, "/*");
  }

  public void addResourcePackages(String... packages) {
    resourcePackages.addAll(Arrays.asList(packages));
    servletHolder.setInitParameter(
        ServerProperties.PROVIDER_PACKAGES, String.join(",", resourcePackages));
  }

  public void registerInstance(Class<?> clazz, Object instance) {
    registerInstance(clazz.getCanonicalName(), instance);
  }

  public void registerInstance(String name, Object instance) {
    servletContextHandler.setAttribute(name, instance);
  }

  public Server getServer() {
    return this.server;
  }

  public int start() throws Exception {
    try {
      server.start();
      httpPort = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
    } catch (BindException e) {
      ExitUtils.terminate(1, "Fail to start jetty http server, port is " + httpPort, e, LOG);
    }
    LOG.info("Jetty http server started, listening on port {}", httpPort);
    return httpPort;
  }

  public void stop() throws Exception {
    server.stop();
  }

  public int getHttpPort() {
    return httpPort;
  }
}
