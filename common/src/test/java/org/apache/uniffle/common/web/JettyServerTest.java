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

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.util.ExitUtils;
import org.apache.uniffle.common.util.ExitUtils.ExitException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JettyServerTest {
  @Test
  public void jettyServerTest() throws Exception {
    RssBaseConf conf = new RssBaseConf();
    conf.setInteger("rss.jetty.http.port", 0);
    JettyServer jettyServer = new JettyServer(conf);
    Server server = jettyServer.getServer();

    assertEquals(4, server.getBeans().size());
    assertEquals(30000, server.getStopTimeout());
    assertTrue(server.getThreadPool() instanceof ExecutorThreadPool);

    assertEquals(1, server.getConnectors().length);
    assertEquals(server, server.getHandler().getServer());
    assertTrue(server.getConnectors()[0] instanceof ServerConnector);
    ServerConnector connector = (ServerConnector) server.getConnectors()[0];
    assertEquals(0, connector.getPort());
    jettyServer.start();
    assertEquals(jettyServer.getHttpPort(), connector.getLocalPort());
    assertNotEquals(jettyServer.getHttpPort(), 0);

    assertEquals(1, server.getHandlers().length);
    Handler handler = server.getHandler();
    assertTrue(handler instanceof ServletContextHandler);
    jettyServer.stop();
  }

  @Test
  public void jettyServerStartTest() throws Exception {
    RssBaseConf conf = new RssBaseConf();
    conf.setInteger("rss.jetty.http.port", 0);
    JettyServer jettyServer1 = new JettyServer(conf);

    int portExist = jettyServer1.start();
    conf.setInteger("rss.jetty.http.port", portExist);
    JettyServer jettyServer2 = new JettyServer(conf);
    ExitUtils.disableSystemExit();
    final String expectMessage = "Fail to start jetty http server";
    final int expectStatus = 1;
    try {
      jettyServer2.start();
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith(expectMessage));
      assertEquals(expectStatus, ((ExitException) e).getStatus());
    }

    final Thread t =
        new Thread(
            null,
            () -> {
              throw new AssertionError("TestUncaughtException");
            },
            "testThread");
    t.start();
    t.join();
  }
}
