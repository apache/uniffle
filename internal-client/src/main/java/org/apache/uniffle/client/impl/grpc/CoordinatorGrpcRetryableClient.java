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
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
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
import org.apache.uniffle.client.response.RssSendHeartBeatResponse;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.RetryUtils;
import org.apache.uniffle.common.util.ThreadUtils;

public class CoordinatorGrpcRetryableClient implements CoordinatorClient {
  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorGrpcRetryableClient.class);
  private List<CoordinatorClient> coordinatorClients;
  private long retryIntervalMs;
  private int retryTimes;
  private ExecutorService heartBeatExecutorService;

  public CoordinatorGrpcRetryableClient(
      List<CoordinatorClient> coordinatorClients,
      long retryIntervalMs,
      int retryTimes,
      int heartBeatThreadNum) {
    this.coordinatorClients = coordinatorClients;
    this.retryIntervalMs = retryIntervalMs;
    this.retryTimes = retryTimes;
    if (heartBeatThreadNum > 0) {
      this.heartBeatExecutorService =
          ThreadUtils.getDaemonFixedThreadPool(heartBeatThreadNum, "client-heartbeat");
    }
  }

  @Override
  public RssAppHeartBeatResponse scheduleAtFixedRateToSendAppHeartBeat(
      RssAppHeartBeatRequest request) {
    AtomicReference<RssAppHeartBeatResponse> rssResponse = new AtomicReference<>();
    rssResponse.set(new RssAppHeartBeatResponse(StatusCode.INTERNAL_ERROR));
    Preconditions.checkNotNull(heartBeatExecutorService);
    ThreadUtils.executeTasks(
        heartBeatExecutorService,
        coordinatorClients,
        coordinatorClient -> {
          try {
            RssAppHeartBeatResponse response =
                coordinatorClient.scheduleAtFixedRateToSendAppHeartBeat(request);
            if (response.getStatusCode() != StatusCode.SUCCESS) {
              LOG.warn("Failed to send heartbeat to " + coordinatorClient.getDesc());
            } else {
              rssResponse.set(response);
              LOG.info("Successfully send heartbeat to " + coordinatorClient.getDesc());
            }
          } catch (Exception e) {
            LOG.warn("Error happened when send heartbeat to " + coordinatorClient.getDesc(), e);
          }
          return null;
        },
        request.getTimeoutMs(),
        "send heartbeat to coordinator");
    return rssResponse.get();
  }

  @Override
  public RssApplicationInfoResponse registerApplicationInfo(RssApplicationInfoRequest request) {
    AtomicReference<RssApplicationInfoResponse> rssResponse = new AtomicReference<>();
    rssResponse.set(new RssApplicationInfoResponse(StatusCode.INTERNAL_ERROR));
    Preconditions.checkNotNull(heartBeatExecutorService);
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
              rssResponse.set(response);
              LOG.info("Successfully send applicationInfo to " + coordinatorClient.getDesc());
            }
          } catch (Exception e) {
            LOG.warn(
                "Error happened when send applicationInfo to " + coordinatorClient.getDesc(), e);
          }
          return null;
        },
        request.getTimeoutMs(),
        "register application");
    return rssResponse.get();
  }

  @Override
  public RssSendHeartBeatResponse sendHeartBeat(RssSendHeartBeatRequest request) {
    AtomicBoolean sendSuccessfully = new AtomicBoolean(false);
    Preconditions.checkNotNull(heartBeatExecutorService);
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
          }
          return null;
        });

    if (sendSuccessfully.get()) {
      return new RssSendHeartBeatResponse(StatusCode.SUCCESS);
    } else {
      return new RssSendHeartBeatResponse(StatusCode.INTERNAL_ERROR);
    }
  }

  @Override
  public RssGetShuffleAssignmentsResponse getShuffleAssignments(
      RssGetShuffleAssignmentsRequest request) {
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
                return response;
              }
            }
            if (response.getStatusCode() != StatusCode.SUCCESS) {
              throw new RssException(response.getMessage());
            }
            return response;
          },
          retryIntervalMs,
          retryTimes);
    } catch (Throwable throwable) {
      throw new RssException("getShuffleAssignments failed!", throwable);
    }
  }

  @Override
  public RssAccessClusterResponse accessCluster(RssAccessClusterRequest request) {
    try {
      return RetryUtils.retry(
          () -> {
            RssAccessClusterResponse response = null;
            for (CoordinatorClient coordinatorClient : this.coordinatorClients) {
              response = coordinatorClient.accessCluster(request);
              if (response.getStatusCode() == StatusCode.SUCCESS) {
                LOG.warn(
                    "Success to access cluster {} using {}",
                    coordinatorClient.getDesc(),
                    request.getAccessId());
                return response;
              }
            }
            if (response.getStatusCode() == StatusCode.ACCESS_DENIED) {
              throw new RssException(
                  "Request to access cluster is denied using "
                      + request.getAccessId()
                      + " for "
                      + response.getMessage());
            } else {
              throw new RssException("Fail to reach cluster for " + response.getMessage());
            }
          },
          request.getRetryIntervalMs(),
          request.getRetryTimes());
    } catch (Throwable throwable) {
      throw new RssException("accessCluster failed!", throwable);
    }
  }

  @Override
  public RssFetchClientConfResponse fetchClientConf(RssFetchClientConfRequest request) {
    try {
      return RetryUtils.retry(
          () -> {
            RssFetchClientConfResponse response = null;
            for (CoordinatorClient coordinatorClient : this.coordinatorClients) {
              response = coordinatorClient.fetchClientConf(request);
              if (response.getStatusCode() == StatusCode.SUCCESS) {
                LOG.info("Success to get conf from {}", coordinatorClient.getDesc());
                break;
              }
            }
            if (response.getStatusCode() != StatusCode.SUCCESS) {
              throw new RssException(response.getMessage());
            }
            return response;
          },
          this.retryIntervalMs,
          this.retryTimes);
    } catch (Throwable throwable) {
      throw new RssException("Fail to get conf", throwable);
    }
  }

  @Override
  public RssFetchRemoteStorageResponse fetchRemoteStorage(RssFetchRemoteStorageRequest request) {
    try {
      return RetryUtils.retry(
          () -> {
            RssFetchRemoteStorageResponse response = null;
            for (CoordinatorClient coordinatorClient : this.coordinatorClients) {
              response = coordinatorClient.fetchRemoteStorage(request);
              if (response.getStatusCode() == StatusCode.SUCCESS) {
                LOG.info(
                    "Success to get storage {} from {}",
                    response.getRemoteStorageInfo(),
                    coordinatorClient.getDesc());
                break;
              }
            }
            if (response.getStatusCode() != StatusCode.SUCCESS) {
              throw new RssException(response.getMessage());
            }
            return response;
          },
          this.retryIntervalMs,
          this.retryTimes);
    } catch (Throwable throwable) {
      throw new RssException("Fail to get conf", throwable);
    }
  }

  @Override
  public String getDesc() {
    StringBuilder result = new StringBuilder("CoordinatorGrpcRetryableClient:");
    for (CoordinatorClient coordinatorClient : coordinatorClients) {
      result.append("\n");
      result.append(coordinatorClient.getDesc());
    }
    return result.toString();
  }

  @Override
  public void close() {
    if (heartBeatExecutorService != null) {
      heartBeatExecutorService.shutdownNow();
    }
    coordinatorClients.forEach(CoordinatorClient::close);
  }
}
