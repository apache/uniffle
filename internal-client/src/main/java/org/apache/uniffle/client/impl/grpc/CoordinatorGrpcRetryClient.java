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

package org.apache.uniffle.client.impl.grpc;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.CoordinatorClient;
import org.apache.uniffle.client.request.RssAccessClusterRequest;
import org.apache.uniffle.client.request.RssAppHeartBeatRequest;
import org.apache.uniffle.client.request.RssApplicationInfoRequest;
import org.apache.uniffle.client.request.RssFetchClientConfRequest;
import org.apache.uniffle.client.request.RssFetchRemoteStorageRequest;
import org.apache.uniffle.client.request.RssGetShuffleAssignmentsRequest;
import org.apache.uniffle.client.request.RssSendHeartBeatRequest;
import org.apache.uniffle.client.response.RssAccessClusterResponse;
import org.apache.uniffle.client.response.RssAppHeartBeatResponse;
import org.apache.uniffle.client.response.RssApplicationInfoResponse;
import org.apache.uniffle.client.response.RssFetchClientConfResponse;
import org.apache.uniffle.client.response.RssFetchRemoteStorageResponse;
import org.apache.uniffle.client.response.RssGetShuffleAssignmentsResponse;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.RetryUtils;
import org.apache.uniffle.common.util.ThreadUtils;

public class CoordinatorGrpcRetryClient {
  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorGrpcRetryClient.class);
  private List<CoordinatorClient> coordinatorClients;
  private long retryIntervalMs;
  private int retryTimes;
  private ExecutorService heartBeatExecutorService;

  public CoordinatorGrpcRetryClient(
      List<CoordinatorClient> coordinatorClients,
      long retryIntervalMs,
      int retryTimes,
      int heartBeatThreadNum) {
    this.coordinatorClients = coordinatorClients;
    this.retryIntervalMs = retryIntervalMs;
    this.retryTimes = retryTimes;
    this.heartBeatExecutorService =
        ThreadUtils.getDaemonFixedThreadPool(heartBeatThreadNum, "client-heartbeat");
  }

  public void sendAppHeartBeat(RssAppHeartBeatRequest request, long timeoutMs) {
    ThreadUtils.executeTasks(
        heartBeatExecutorService,
        coordinatorClients,
        coordinatorClient -> {
          try {
            RssAppHeartBeatResponse response = coordinatorClient.sendAppHeartBeat(request);
            if (response.getStatusCode() != StatusCode.SUCCESS) {
              LOG.warn("Failed to send heartbeat to " + coordinatorClient.getDesc());
            } else {
              LOG.info("Successfully send heartbeat to " + coordinatorClient.getDesc());
            }
          } catch (Exception e) {
            LOG.warn("Error happened when send heartbeat to " + coordinatorClient.getDesc(), e);
          }
          return null;
        },
        timeoutMs,
        "send heartbeat to coordinator");
  }

  public void registerApplicationInfo(RssApplicationInfoRequest request, long timeoutMs) {
    ThreadUtils.executeTasks(
        heartBeatExecutorService,
        coordinatorClients,
        coordinatorClient -> {
          try {
            RssApplicationInfoResponse response =
                coordinatorClient.registerApplicationInfo(request);
            if (response.getStatusCode() != StatusCode.SUCCESS) {
              LOG.error("Failed to send applicationInfo to " + coordinatorClient.getDesc());
            } else {
              LOG.info("Successfully send applicationInfo to " + coordinatorClient.getDesc());
            }
          } catch (Exception e) {
            LOG.warn(
                "Error happened when send applicationInfo to " + coordinatorClient.getDesc(), e);
          }
          return null;
        },
        timeoutMs,
        "register application");
  }

  public boolean sendHeartBeat(RssSendHeartBeatRequest request) {
    AtomicBoolean sendSuccessfully = new AtomicBoolean(false);
    ThreadUtils.executeTasks(
        heartBeatExecutorService,
        coordinatorClients,
        client -> client.sendHeartBeat(request),
        request.getTimeout() * 2,
        "send heartbeat",
        future -> {
          try {
            if (future.get(request.getTimeout() * 2, TimeUnit.MILLISECONDS).getStatusCode()
                == StatusCode.SUCCESS) {
              sendSuccessfully.set(true);
            }
          } catch (Exception e) {
            LOG.error(e.getMessage());
            return null;
          }
          return null;
        });

    return sendSuccessfully.get();
  }

  public RssGetShuffleAssignmentsResponse getShuffleAssignments(
      RssGetShuffleAssignmentsRequest request, long retryIntervalMs, int retryTimes) {
    try {
      return RetryUtils.retry(
          () -> {
            RssGetShuffleAssignmentsResponse response = null;
            for (CoordinatorClient coordinatorClient : this.coordinatorClients) {
              try {
                response = coordinatorClient.getShuffleAssignments(request);
              } catch (Exception e) {
                LOG.error(e.getMessage());
              }

              if (response.getStatusCode() == StatusCode.SUCCESS) {
                LOG.info(
                    "Success to get shuffle server assignment from {}",
                    coordinatorClient.getDesc());
                break;
              }
            }
            if (response.getStatusCode() != StatusCode.SUCCESS) {
              LOG.info("Failed to get shuffle server assignment");
            }
            return response;
          },
          retryIntervalMs,
          retryTimes);
    } catch (Throwable throwable) {
      throw new RssException("getShuffleAssignments failed!", throwable);
    }
  }

  public RssAccessClusterResponse accessCluster(
      RssAccessClusterRequest request, long retryIntervalMs, int retryTimes) {
    try {
      return RetryUtils.retry(
          () -> {
            RssAccessClusterResponse response = null;
            for (CoordinatorClient coordinatorClient : this.coordinatorClients) {
              response = coordinatorClient.accessCluster(request);
              if (response.getStatusCode() == StatusCode.SUCCESS) {
                LOG.info("Success to access cluster from {}", coordinatorClient.getDesc());
                break;
              }
            }
            if (response.getStatusCode() != StatusCode.SUCCESS) {
              LOG.info("Failed to access cluster");
            }
            return response;
          },
          retryIntervalMs,
          retryTimes);
    } catch (Throwable throwable) {
      throw new RssException("getShuffleAssignments failed!", throwable);
    }
  }

  public RssFetchClientConfResponse fetchClientConf(RssFetchClientConfRequest request) {
    try {
      return RetryUtils.retry(
          () -> {
            RssFetchClientConfResponse response = null;
            for (CoordinatorClient coordinatorClient : this.coordinatorClients) {
              response = coordinatorClient.fetchClientConf(request);
              if (response.getStatusCode() == StatusCode.SUCCESS) {
                LOG.info("Success to fetch client conf from {}", coordinatorClient.getDesc());
                break;
              }
            }
            if (response.getStatusCode() != StatusCode.SUCCESS) {
              LOG.info("Failed to fetch client conf");
            }
            return response;
          },
          this.retryIntervalMs,
          this.retryTimes);
    } catch (Throwable throwable) {
      throw new RssException("fetchClientConf failed!", throwable);
    }
  }

  public RemoteStorageInfo fetchRemoteStorage(RssFetchRemoteStorageRequest request) {
    try {
      return RetryUtils.retry(
          () -> {
            RssFetchRemoteStorageResponse response = null;
            for (CoordinatorClient coordinatorClient : this.coordinatorClients) {
              response = coordinatorClient.fetchRemoteStorage(request);
              if (response.getStatusCode() == StatusCode.SUCCESS) {
                LOG.info("Success to fetch remote storage from {}", coordinatorClient.getDesc());
                break;
              }
            }
            if (response.getStatusCode() != StatusCode.SUCCESS) {
              LOG.info("Failed to fetch remote storage");
            }
            return response.getRemoteStorageInfo();
          },
          this.retryIntervalMs,
          this.retryTimes);
    } catch (Throwable throwable) {
      throw new RssException("fetchRemoteStorage failed!", throwable);
    }
  }

  public void close() {
    coordinatorClients.forEach(CoordinatorClient::close);
  }
}
