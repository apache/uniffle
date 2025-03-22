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

import java.util.HashMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.api.AdminRestApi;
import org.apache.uniffle.client.UniffleRestClient;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.web.resource.Response;
import org.apache.uniffle.coordinator.CoordinatorConf;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CoordinatorAdminServiceTest extends IntegrationTestBase {
  private static final String accessChecker =
      "org.apache.uniffle.test.AccessClusterTest$MockedAccessChecker";

  private ObjectMapper objectMapper = new ObjectMapper();

  protected AdminRestApi adminRestApi;

  @BeforeAll
  public static void setUp() throws Exception {
    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    coordinatorConf.set(RssBaseConf.JETTY_CORE_POOL_SIZE, 128);
    coordinatorConf.setString(CoordinatorConf.COORDINATOR_ACCESS_CHECKERS.key(), accessChecker);
    storeCoordinatorConf(coordinatorConf);
    startServersWithRandomPorts();
  }

  @BeforeEach
  public void createClient() {
    String hostUrl = String.format("http://%s:%d", LOCALHOST, coordinators.get(0).getJettyPort());
    adminRestApi = new AdminRestApi(UniffleRestClient.builder(hostUrl).build());
  }

  @Test
  public void test() throws Exception {
    String content = adminRestApi.refreshAccessChecker();
    Response<HashMap> response =
        objectMapper.readValue(content, new TypeReference<Response<HashMap>>() {});
    assertEquals(0, response.getCode());
  }
}
