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

package org.apache.uniffle.client.factory;

import java.util.HashSet;
import java.util.Map;

import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.common.ShuffleServerInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShuffleServerClientTest {

  @Test
  public void testCleanUselessClient() {
    ShuffleServerInfo shuffleServerInfo1 = new ShuffleServerInfo("127.0.0.1", 19999);
    ShuffleServerInfo shuffleServerInfo2 = new ShuffleServerInfo("127.0.0.1", 29999);
    ShuffleServerInfo shuffleServerInfo3 = new ShuffleServerInfo("127.0.0.1", 39999);
    String clientType = "grpc_netty";
    ShuffleServerClientFactory shuffleServerClientFactory =
        ShuffleServerClientFactory.getInstance();
    shuffleServerClientFactory.getShuffleServerClient(clientType, shuffleServerInfo1);
    shuffleServerClientFactory.getShuffleServerClient(clientType, shuffleServerInfo2);
    shuffleServerClientFactory.getShuffleServerClient(clientType, shuffleServerInfo3);
    Map<ShuffleServerInfo, ShuffleServerClient> clientMap =
        shuffleServerClientFactory.getClients().get(clientType);
    assertEquals(3, clientMap.size());
    HashSet<ShuffleServerInfo> remainShuffleServers =
        Sets.newHashSet(shuffleServerInfo1, shuffleServerInfo2);
    shuffleServerClientFactory.cleanUselessShuffleServerClients(remainShuffleServers);
    assertEquals(2, clientMap.size());
    assertTrue(CollectionUtils.isEqualCollection(clientMap.keySet(), remainShuffleServers));
  }
}
