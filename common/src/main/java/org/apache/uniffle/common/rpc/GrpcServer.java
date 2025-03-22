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

package org.apache.uniffle.common.rpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.executor.ThreadPoolManager;
import org.apache.uniffle.common.metrics.GRPCMetrics;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.ExitUtils;
import org.apache.uniffle.common.util.GrpcNettyUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.common.util.ThreadUtils;

public class GrpcServer implements ServerInterface {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class);

  private static volatile boolean poolExecutorHasExecuted;
  private Server server;
  private final int port;
  private int listenPort;
  private final GrpcThreadPoolExecutor pool;
  private List<Pair<BindableService, List<ServerInterceptor>>> servicesWithInterceptors;
  private GRPCMetrics grpcMetrics;
  private RssBaseConf rssConf;

  protected GrpcServer(
      RssBaseConf conf,
      List<Pair<BindableService, List<ServerInterceptor>>> servicesWithInterceptors,
      GRPCMetrics grpcMetrics) {
    this.rssConf = conf;
    this.port = rssConf.getInteger(RssBaseConf.RPC_SERVER_PORT);
    this.servicesWithInterceptors = servicesWithInterceptors;
    this.grpcMetrics = grpcMetrics;

    int rpcExecutorSize = conf.getInteger(RssBaseConf.RPC_EXECUTOR_SIZE);
    int queueSize = conf.getInteger(RssBaseConf.RPC_EXECUTOR_QUEUE_SIZE);
    pool =
        new GrpcThreadPoolExecutor(
            rpcExecutorSize,
            rpcExecutorSize * 2,
            10,
            TimeUnit.MINUTES,
            Queues.newLinkedBlockingQueue(queueSize),
            ThreadUtils.getThreadFactory("Grpc"),
            grpcMetrics);
    ThreadPoolManager.registerThreadPool(
        "Grpc",
        () -> conf.getInteger(RssBaseConf.RPC_EXECUTOR_SIZE),
        () -> conf.getInteger(RssBaseConf.RPC_EXECUTOR_SIZE) * 2,
        () -> TimeUnit.MINUTES.toMillis(10),
        pool);
  }

  // This method is only used for the sake of synchronizing one test
  static boolean isPoolExecutorHasExecuted() {
    return poolExecutorHasExecuted;
  }

  // This method is only used for the sake of synchronizing one test
  static void reset() {
    poolExecutorHasExecuted = false;
  }

  private Server buildGrpcServer(int serverPort) {
    boolean isMetricsEnabled = rssConf.getBoolean(RssBaseConf.RPC_METRICS_ENABLED);
    long maxInboundMessageSize = rssConf.getLong(RssBaseConf.RPC_MESSAGE_MAX_SIZE);
    ServerType serverType = rssConf.get(RssBaseConf.RPC_SERVER_TYPE);
    int pageSize = rssConf.getInteger(RssBaseConf.RPC_NETTY_PAGE_SIZE);
    int maxOrder = rssConf.getInteger(RssBaseConf.RPC_NETTY_MAX_ORDER);
    int smallCacheSize = rssConf.getInteger(RssBaseConf.RPC_NETTY_SMALL_CACHE_SIZE);
    PooledByteBufAllocator pooledByteBufAllocator =
        serverType == ServerType.GRPC
            ? GrpcNettyUtils.createPooledByteBufAllocator(true, 0, 0, 0, 0)
            : GrpcNettyUtils.createPooledByteBufAllocatorWithSmallCacheOnly(
                true, 0, pageSize, maxOrder, smallCacheSize);
    ServerBuilder<?> builder =
        NettyServerBuilder.forPort(serverPort)
            .executor(pool)
            .maxInboundMessageSize((int) maxInboundMessageSize)
            .withOption(ChannelOption.ALLOCATOR, pooledByteBufAllocator)
            .withChildOption(ChannelOption.ALLOCATOR, pooledByteBufAllocator);
    if (isMetricsEnabled) {
      builder.addTransportFilter(new MonitoringServerTransportFilter(grpcMetrics));
    }
    servicesWithInterceptors.forEach(
        (serviceWithInterceptors) -> {
          List<ServerInterceptor> interceptors = serviceWithInterceptors.getRight();
          interceptors.add(new ClientContextServerInterceptor());
          if (isMetricsEnabled) {
            MonitoringServerInterceptor monitoringInterceptor =
                new MonitoringServerInterceptor(grpcMetrics);
            List<ServerInterceptor> newInterceptors = Lists.newArrayList(interceptors);
            newInterceptors.add(monitoringInterceptor);
            interceptors = newInterceptors;
          }
          builder.addService(
              ServerInterceptors.intercept(serviceWithInterceptors.getLeft(), interceptors));
        });
    return builder.build();
  }

  public static class Builder {

    private RssBaseConf rssBaseConf;
    private GRPCMetrics grpcMetrics;

    private List<Pair<BindableService, List<ServerInterceptor>>> servicesWithInterceptors =
        new ArrayList<>();

    public static Builder newBuilder() {
      return new Builder();
    }

    public Builder conf(RssBaseConf rssBaseConf) {
      this.rssBaseConf = rssBaseConf;
      return this;
    }

    public Builder addService(BindableService bindableService, ServerInterceptor... interceptors) {
      this.servicesWithInterceptors.add(Pair.of(bindableService, Lists.newArrayList(interceptors)));
      return this;
    }

    public Builder grpcMetrics(GRPCMetrics metrics) {
      this.grpcMetrics = metrics;
      return this;
    }

    public GrpcServer build() {
      return new GrpcServer(rssBaseConf, servicesWithInterceptors, grpcMetrics);
    }
  }

  public static class GrpcThreadPoolExecutor extends ThreadPoolExecutor {
    private final GRPCMetrics grpcMetrics;
    private final AtomicLong activeThreadSize = new AtomicLong(0L);

    public GrpcThreadPoolExecutor(
        int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue,
        ThreadFactory threadFactory,
        GRPCMetrics grpcMetrics) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
      this.grpcMetrics = grpcMetrics;
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
      grpcMetrics.incGauge(GRPCMetrics.GRPC_SERVER_EXECUTOR_ACTIVE_THREADS_KEY);
      grpcMetrics.setGauge(
          GRPCMetrics.GRPC_SERVER_EXECUTOR_BLOCKING_QUEUE_SIZE_KEY, getQueue().size());
      poolExecutorHasExecuted = true;
      super.beforeExecute(t, r);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
      grpcMetrics.decGauge(GRPCMetrics.GRPC_SERVER_EXECUTOR_ACTIVE_THREADS_KEY);
      grpcMetrics.setGauge(
          GRPCMetrics.GRPC_SERVER_EXECUTOR_BLOCKING_QUEUE_SIZE_KEY, getQueue().size());
      super.afterExecute(r, t);
    }

    @VisibleForTesting
    void correctMetrics() {
      grpcMetrics.setGauge(
          GRPCMetrics.GRPC_SERVER_EXECUTOR_BLOCKING_QUEUE_SIZE_KEY, getQueue().size());
    }
  }

  @Override
  public int start() throws IOException {
    try {
      this.listenPort =
          RssUtils.startServiceOnPort(this, Constants.GRPC_SERVICE_NAME, port, rssConf);
    } catch (Exception e) {
      ExitUtils.terminate(1, "Fail to start grpc server on conf port:" + port, e, LOG);
    }
    return listenPort;
  }

  @Override
  public void startOnPort(int startPort) throws Exception {
    this.server = buildGrpcServer(startPort);
    try {
      server.start();
      listenPort = server.getPort();
    } catch (Exception e) {
      throw e;
    }
    LOG.info("Grpc server started, configured port: {}, listening on {}.", port, listenPort);
  }

  @Override
  public void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(10, TimeUnit.SECONDS);
      LOG.info("GRPC server stopped!");
    }
    if (pool != null) {
      ThreadPoolManager.unregister(pool);
      pool.shutdown();
    }
  }

  @Override
  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  public int getPort() {
    return listenPort;
  }
}
