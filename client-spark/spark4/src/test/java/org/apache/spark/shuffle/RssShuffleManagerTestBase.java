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

package org.apache.spark.shuffle;

import java.util.List;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import org.apache.uniffle.client.api.CoordinatorClient;
import org.apache.uniffle.client.impl.grpc.CoordinatorGrpcRetryableClient;
import org.apache.uniffle.client.response.RssAccessClusterResponse;
import org.apache.uniffle.common.rpc.StatusCode;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class RssShuffleManagerTestBase {
  protected static MockedStatic<RssSparkShuffleUtils> mockedStaticRssShuffleUtils;

  @BeforeAll
  public static void setUp() {
    mockedStaticRssShuffleUtils =
        mockStatic(RssSparkShuffleUtils.class, Mockito.CALLS_REAL_METHODS);
  }

  @AfterAll
  public static void tearDown() {
    mockedStaticRssShuffleUtils.close();
  }

  protected CoordinatorClient createCoordinatorClient(StatusCode status) {
    CoordinatorClient mockedCoordinatorClient = mock(CoordinatorClient.class);
    when(mockedCoordinatorClient.accessCluster(any()))
        .thenReturn(new RssAccessClusterResponse(status, ""));
    return mockedCoordinatorClient;
  }

  CoordinatorClient setupMockedRssShuffleUtils(StatusCode status) {
    CoordinatorClient mockCoordinatorClient = createCoordinatorClient(status);
    List<CoordinatorClient> coordinatorClients = Lists.newArrayList();
    coordinatorClients.add(mockCoordinatorClient);
    CoordinatorGrpcRetryableClient client =
        new CoordinatorGrpcRetryableClient(coordinatorClients, 0, 1, 1);
    mockedStaticRssShuffleUtils
        .when(() -> RssSparkShuffleUtils.createCoordinatorClientsForAccessCluster(any()))
        .thenReturn(client);
    return mockCoordinatorClient;
  }
}
