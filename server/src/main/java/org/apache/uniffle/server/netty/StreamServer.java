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

package org.apache.uniffle.server.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.netty.util.internal.SystemPropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.netty.TransportFrameDecoder;
import org.apache.uniffle.common.netty.client.TransportConf;
import org.apache.uniffle.common.netty.client.TransportContext;
import org.apache.uniffle.common.rpc.ServerInterface;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.ExitUtils;
import org.apache.uniffle.common.util.NettyUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;

public class StreamServer implements ServerInterface {

  private static final Logger LOG = LoggerFactory.getLogger(StreamServer.class);

  /** Use this value to ensure the minimum throughput. */
  private static final int MIN_NETTY_SERVER_WORKER_THREAD_COUNT_DEFAULT = 100;

  private ShuffleServer shuffleServer;
  private EventLoopGroup shuffleBossGroup;
  private EventLoopGroup shuffleWorkerGroup;
  private ShuffleServerConf shuffleServerConf;
  private ChannelFuture channelFuture;

  private final ScheduledExecutorService nettyPendingTasksNumTracker =
      Executors.newSingleThreadScheduledExecutor(
          ThreadUtils.getThreadFactory("NettyPendingTasksNumTracker"));
  /** Interval to poll for Netty pending tasks number for Netty metrics, in milliseconds */
  private final long pendingTasksNumMetricsPollingInterval;

  public StreamServer(ShuffleServer shuffleServer) {
    this.shuffleServer = shuffleServer;
    this.shuffleServerConf = shuffleServer.getShuffleServerConf();
    boolean isEpollEnable =
        shuffleServerConf.getBoolean(ShuffleServerConf.NETTY_SERVER_EPOLL_ENABLE);
    int acceptThreads = shuffleServerConf.getInteger(ShuffleServerConf.NETTY_SERVER_ACCEPT_THREAD);
    int workerThreads = shuffleServerConf.getInteger(ShuffleServerConf.NETTY_SERVER_WORKER_THREAD);
    if (workerThreads == 0) {
      workerThreads =
          Math.max(
              MIN_NETTY_SERVER_WORKER_THREAD_COUNT_DEFAULT,
              SystemPropertyUtil.getInt(
                  "io.netty.eventLoopThreads", NettyRuntime.availableProcessors() * 2));
    }
    if (isEpollEnable) {
      shuffleBossGroup = new EpollEventLoopGroup(acceptThreads);
      shuffleWorkerGroup = new EpollEventLoopGroup(workerThreads);
    } else {
      shuffleBossGroup = new NioEventLoopGroup(acceptThreads);
      shuffleWorkerGroup = new NioEventLoopGroup(workerThreads);
    }
    this.pendingTasksNumMetricsPollingInterval =
        shuffleServerConf.getLong(
            ShuffleServerConf.SERVER_NETTY_PENDING_TASKS_NUM_TRACKER_INTERVAL);
    startMonitoringPendingTasks();
  }

  private void startMonitoringPendingTasks() {
    nettyPendingTasksNumTracker.scheduleAtFixedRate(
        () -> {
          int pendingTasksNumForBossGroup = getPendingTasksForEventLoopGroup(shuffleBossGroup);
          shuffleServer
              .getNettyMetrics()
              .getGaugeNettyPendingTasksNumForBossGroup()
              .set(pendingTasksNumForBossGroup);

          int pendingTasksNumForWorkerGroup = getPendingTasksForEventLoopGroup(shuffleWorkerGroup);
          shuffleServer
              .getNettyMetrics()
              .getGaugeNettyPendingTasksNumForWorkerGroup()
              .set(pendingTasksNumForWorkerGroup);
        },
        0L,
        pendingTasksNumMetricsPollingInterval,
        TimeUnit.MILLISECONDS);
  }

  private int getPendingTasksForEventLoopGroup(EventLoopGroup eventLoopGroup) {
    return StreamSupport.stream(eventLoopGroup.spliterator(), false)
        .filter(eventExecutor -> eventExecutor instanceof SingleThreadEventExecutor)
        .mapToInt(eventExecutor -> ((SingleThreadEventExecutor) eventExecutor).pendingTasks())
        .sum();
  }

  private ServerBootstrap bootstrapChannel(
      EventLoopGroup bossGroup,
      EventLoopGroup workerGroup,
      int backlogSize,
      int timeoutMillis,
      int sendBuf,
      int receiveBuf) {
    ServerBootstrap serverBootstrap = new ServerBootstrap().group(bossGroup, workerGroup);
    if (bossGroup instanceof EpollEventLoopGroup) {
      serverBootstrap.channel(EpollServerSocketChannel.class);
    } else {
      serverBootstrap.channel(NioServerSocketChannel.class);
    }

    ShuffleServerNettyHandler serverNettyHandler = new ShuffleServerNettyHandler(shuffleServer);
    TransportContext transportContext =
        new TransportContext(new TransportConf(shuffleServerConf), serverNettyHandler, true);
    serverBootstrap
        .childHandler(
            new ChannelInitializer<SocketChannel>() {
              @Override
              public void initChannel(final SocketChannel ch) {
                transportContext.initializePipeline(ch, new TransportFrameDecoder());
                ch.pipeline()
                    .addLast(
                        "metricHandler",
                        new StreamServerMetricHandler(shuffleServer.getNettyMetrics()));
              }
            })
        .option(ChannelOption.SO_BACKLOG, backlogSize)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMillis)
        .option(ChannelOption.ALLOCATOR, NettyUtils.getSharedUnpooledByteBufAllocator(true))
        .childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMillis)
        .childOption(ChannelOption.ALLOCATOR, NettyUtils.getSharedUnpooledByteBufAllocator(true))
        .childOption(ChannelOption.TCP_NODELAY, true)
        .childOption(ChannelOption.SO_KEEPALIVE, true);

    if (sendBuf > 0) {
      serverBootstrap.childOption(ChannelOption.SO_SNDBUF, sendBuf);
    }
    if (receiveBuf > 0) {
      serverBootstrap.childOption(ChannelOption.SO_RCVBUF, receiveBuf);
    }
    return serverBootstrap;
  }

  @Override
  public int start() throws Exception {
    int port = shuffleServerConf.getInteger(ShuffleServerConf.NETTY_SERVER_PORT);
    try {
      port =
          RssUtils.startServiceOnPortWithFallback(
              this::startOnPort, Constants.NETTY_STREAM_SERVICE_NAME, port);
    } catch (Exception e) {
      ExitUtils.terminate(1, "Fail to start stream server", e, LOG);
    }
    return port;
  }

  private int startOnPort(int port) throws Exception {

    ServerBootstrap serverBootstrap =
        bootstrapChannel(
            shuffleBossGroup,
            shuffleWorkerGroup,
            shuffleServerConf.getInteger(ShuffleServerConf.NETTY_SERVER_CONNECT_BACKLOG),
            shuffleServerConf.getInteger(ShuffleServerConf.NETTY_SERVER_CONNECT_TIMEOUT),
            shuffleServerConf.getInteger(ShuffleServerConf.NETTY_SERVER_SEND_BUF),
            shuffleServerConf.getInteger(ShuffleServerConf.NETTY_SERVER_RECEIVE_BUF));

    // Bind the ports and save the results so that the channels can be closed later.
    // If the second bind fails, the first one gets cleaned up in the shutdown.
    try {
      channelFuture = serverBootstrap.bind(port);
      channelFuture.syncUninterruptibly();
      LOG.info("bind localAddress is " + channelFuture.channel().localAddress());
      int actualPort = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
      LOG.info("Start stream server successfully with port " + actualPort);
      return actualPort;
    } catch (Exception e) {
      throw e;
    }
  }

  @Override
  public void stop() {
    if (channelFuture != null) {
      channelFuture.channel().close().awaitUninterruptibly(10L, TimeUnit.SECONDS);
      channelFuture = null;
    }
    if (shuffleBossGroup != null) {
      shuffleBossGroup.shutdownGracefully();
      shuffleWorkerGroup.shutdownGracefully();
      shuffleBossGroup = null;
      shuffleWorkerGroup = null;
    }
    if (!nettyPendingTasksNumTracker.isShutdown()) {
      nettyPendingTasksNumTracker.shutdown();
    }
  }

  @Override
  public void blockUntilShutdown() throws InterruptedException {}
}
