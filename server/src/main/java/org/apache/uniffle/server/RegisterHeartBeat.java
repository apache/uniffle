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

package org.apache.uniffle.server;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.factory.CoordinatorClientFactory;
import org.apache.uniffle.client.impl.grpc.CoordinatorGrpcRetryableClient;
import org.apache.uniffle.client.request.RssSendHeartBeatRequest;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.ThreadUtils;

public class RegisterHeartBeat {

  private static final Logger LOG = LoggerFactory.getLogger(RegisterHeartBeat.class);

  private final long heartBeatInitialDelay;
  private final long heartBeatInterval;
  private final ShuffleServer shuffleServer;
  private final String coordinatorQuorum;
  private final CoordinatorGrpcRetryableClient coordinatorClient;
  private final ScheduledExecutorService service =
      ThreadUtils.getDaemonSingleThreadScheduledExecutor("startHeartBeat");

  public RegisterHeartBeat(ShuffleServer shuffleServer) {
    ShuffleServerConf conf = shuffleServer.getShuffleServerConf();
    this.heartBeatInitialDelay = conf.getLong(ShuffleServerConf.SERVER_HEARTBEAT_DELAY);
    this.heartBeatInterval = conf.getLong(ShuffleServerConf.SERVER_HEARTBEAT_INTERVAL);
    this.coordinatorQuorum = conf.getString(ShuffleServerConf.RSS_COORDINATOR_QUORUM);
    CoordinatorClientFactory factory = CoordinatorClientFactory.getInstance();
    this.coordinatorClient =
        factory.createCoordinatorClient(
            conf.get(ShuffleServerConf.RSS_COORDINATOR_CLIENT_TYPE),
            this.coordinatorQuorum,
            0,
            0,
            conf.getInteger(ShuffleServerConf.SERVER_HEARTBEAT_THREAD_NUM));
    this.shuffleServer = shuffleServer;
  }

  public void startHeartBeat() {
    LOG.info(
        "Start heartbeat to coordinator {} after {}ms and interval is {}ms",
        coordinatorQuorum,
        heartBeatInitialDelay,
        heartBeatInterval);
    Runnable runnable =
        () -> {
          try {
            // use `rss.server.heartbeat.interval` as the timeout option
            RssSendHeartBeatRequest request =
                new RssSendHeartBeatRequest(
                    shuffleServer.getId(),
                    shuffleServer.getIp(),
                    shuffleServer.getGrpcPort(),
                    shuffleServer.getUsedMemory(),
                    shuffleServer.getPreAllocatedMemory(),
                    shuffleServer.getAvailableMemory(),
                    shuffleServer.getEventNumInFlush(),
                    heartBeatInterval,
                    shuffleServer.getTags(),
                    shuffleServer.getServerStatus(),
                    shuffleServer.getStorageManager().getStorageInfo(),
                    shuffleServer.getNettyPort(),
                    shuffleServer.getJettyPort(),
                    shuffleServer.getStartTimeMs(),
                    shuffleServer.getAppInfos(),
                    shuffleServer.getDisplayMetrics());
            sendHeartBeat(request);
          } catch (Exception e) {
            LOG.warn("Error happened when send heart beat to coordinator");
          }
        };
    service.scheduleAtFixedRate(
        runnable, heartBeatInitialDelay, heartBeatInterval, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  public boolean sendHeartBeat(RssSendHeartBeatRequest request) {
    return coordinatorClient.sendHeartBeat(request).getStatusCode() == StatusCode.SUCCESS;
  }

  public long getHeartBeatInterval() {
    return heartBeatInterval;
  }

  public void shutdown() {
    coordinatorClient.close();
    service.shutdownNow();
  }
}
