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

package org.apache.uniffle.common;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.config.RssConf;

import static org.apache.uniffle.common.config.RssBaseConf.JETTY_HTTP_PORT;
import static org.apache.uniffle.common.config.RssBaseConf.RPC_SERVER_PORT;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_RECONFIGURE_INTERVAL_SEC;
import static org.apache.uniffle.common.config.RssBaseConf.RSS_STORAGE_BASE_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ReconfigurableConfManagerTest {

  @Test
  public void test() throws InterruptedException {
    AtomicInteger i = new AtomicInteger(0);
    Supplier<RssConf> supplier =
        () -> {
          if (i.getAndIncrement() <= 1) {
            return new RssConf();
          }
          RssConf conf = new RssConf();
          conf.set(JETTY_HTTP_PORT, 100);
          conf.set(RPC_SERVER_PORT, 200);
          conf.set(RSS_STORAGE_BASE_PATH, Arrays.asList("/d1"));
          return conf;
        };

    RssConf base = new RssConf();
    base.set(RSS_RECONFIGURE_INTERVAL_SEC, 1L);
    ReconfigurableConfManager.initForTest(base, supplier);

    ReconfigurableConfManager.Reconfigurable<Integer> portReconfigurable =
        base.getReconfigurableConf(JETTY_HTTP_PORT);
    ReconfigurableConfManager.Reconfigurable<Integer> rpcReconfigurable =
        base.getReconfigurableConf(RPC_SERVER_PORT);
    ReconfigurableConfManager.Reconfigurable<List<String>> typeReconfigurable =
        base.getReconfigurableConf(RSS_STORAGE_BASE_PATH);
    assertEquals(19998, portReconfigurable.get());
    assertEquals(19999, rpcReconfigurable.get());
    assertNull(typeReconfigurable.get());

    Awaitility.await()
        .timeout(5, TimeUnit.SECONDS)
        .until(() -> portReconfigurable.get().equals(100));
    assertEquals(200, rpcReconfigurable.get());
    assertEquals(Arrays.asList("/d1"), typeReconfigurable.get());
    // The base config should be updated too
    assertEquals(200, base.getInteger(RPC_SERVER_PORT));
  }

  @Test
  public void testWithoutInitialization() {
    RssConf base = new RssConf();
    base.set(JETTY_HTTP_PORT, 100);
    ReconfigurableConfManager.Reconfigurable<Integer> portReconfigurable =
        base.getReconfigurableConf(JETTY_HTTP_PORT);
    assertEquals(100, portReconfigurable.get());
  }
}
