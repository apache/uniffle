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

package org.apache.uniffle.test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import org.apache.uniffle.client.factory.ShuffleManagerClientFactory;
import org.apache.uniffle.client.impl.grpc.ShuffleManagerGrpcClient;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.rpc.GrpcServer;
import org.apache.uniffle.shuffle.manager.DummyRssShuffleManager;
import org.apache.uniffle.shuffle.manager.RssShuffleManagerInterface;
import org.apache.uniffle.shuffle.manager.ShuffleManagerServerFactory;

import static org.apache.uniffle.common.config.RssBaseConf.RPC_SERVER_PORT;

public class ShuffleServerManagerTestBase {
  protected ShuffleManagerClientFactory factory = ShuffleManagerClientFactory.getInstance();
  protected ShuffleManagerGrpcClient client;
  protected static final String LOCALHOST = "localhost";
  protected GrpcServer shuffleManagerServer;
  protected RssConf rssConf;

  protected RssShuffleManagerInterface getShuffleManager() {
    return new DummyRssShuffleManager();
  }

  protected ShuffleServerManagerTestBase() {
    this.rssConf = getRssConf();
  }

  private RssConf getRssConf() {
    RssConf conf = new RssConf();
    // use a random port
    conf.set(RPC_SERVER_PORT, 0);
    return conf;
  }

  protected GrpcServer createShuffleManagerServer() {
    return new ShuffleManagerServerFactory(getShuffleManager(), rssConf).getServer();
  }

  @BeforeEach
  public void createServerAndClient() throws Exception {
    shuffleManagerServer = createShuffleManagerServer();
    shuffleManagerServer.start();
    int port = shuffleManagerServer.getPort();
    long rpcTimeout = rssConf.getLong(RssClientConf.RPC_TIMEOUT_MS);
    client = factory.createShuffleManagerClient(ClientType.GRPC, LOCALHOST, port, rpcTimeout);
  }

  @AfterEach
  public void close() throws Exception {
    if (client != null) {
      client.close();
    }
    if (shuffleManagerServer != null) {
      shuffleManagerServer.stop();
    }
  }
}
