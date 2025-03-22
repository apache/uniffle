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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.CoordinatorClient;
import org.apache.uniffle.client.impl.grpc.CoordinatorGrpcClient;
import org.apache.uniffle.client.impl.grpc.CoordinatorGrpcRetryableClient;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.exception.RssException;

public class CoordinatorClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorClientFactory.class);

  private CoordinatorClientFactory() {}

  private static class LazyHolder {
    static final CoordinatorClientFactory INSTANCE = new CoordinatorClientFactory();
  }

  public static CoordinatorClientFactory getInstance() {
    return LazyHolder.INSTANCE;
  }

  @VisibleForTesting
  public synchronized CoordinatorClient createCoordinatorClient(
      ClientType clientType, String host, int port) {
    if (clientType.equals(ClientType.GRPC) || clientType.equals(ClientType.GRPC_NETTY)) {
      return new CoordinatorGrpcClient(host, port);
    } else {
      throw new UnsupportedOperationException("Unsupported client type " + clientType);
    }
  }

  @VisibleForTesting
  public synchronized List<CoordinatorClient> createCoordinatorClient(
      ClientType clientType, String coordinators) {
    LOG.info("Start to create coordinator clients from {}", coordinators);

    String[] coordinatorList = coordinators.trim().split(",");
    if (coordinatorList.length == 0) {
      String msg = "Invalid " + coordinators;
      LOG.error(msg);
      throw new RssException(msg);
    }

    List<CoordinatorClient> coordinatorClients = Lists.newLinkedList();
    for (String coordinator : coordinatorList) {
      String[] ipPort = coordinator.trim().split(":");
      if (ipPort.length != 2) {
        String msg = "Invalid coordinator format " + Arrays.toString(ipPort);
        LOG.error(msg);
        throw new RssException(msg);
      }

      String host = ipPort[0];
      int port = Integer.parseInt(ipPort[1]);
      CoordinatorClient coordinatorClient = createCoordinatorClient(clientType, host, port);
      coordinatorClients.add(coordinatorClient);
      LOG.info("Add coordinator client {}", coordinatorClient.getDesc());
    }
    LOG.info(
        "Finish create coordinator clients {}",
        coordinatorClients.stream()
            .map(CoordinatorClient::getDesc)
            .collect(Collectors.joining(", ")));
    return coordinatorClients;
  }

  public synchronized CoordinatorGrpcRetryableClient createCoordinatorClient(
      ClientType clientType,
      String coordinators,
      long retryIntervalMs,
      int retryTimes,
      int heartBeatThreadNum) {
    List<CoordinatorClient> coordinatorClients = createCoordinatorClient(clientType, coordinators);
    return new CoordinatorGrpcRetryableClient(
        coordinatorClients, retryIntervalMs, retryTimes, heartBeatThreadNum);
  }

  public synchronized CoordinatorGrpcRetryableClient createCoordinatorClientWithoutHeartbeat(
      ClientType clientType, String coordinators, long retryIntervalMs, int retryTimes) {
    List<CoordinatorClient> coordinatorClients = createCoordinatorClient(clientType, coordinators);
    return new CoordinatorGrpcRetryableClient(coordinatorClients, retryIntervalMs, retryTimes, 0);
  }
}
