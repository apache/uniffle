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

package org.apache.uniffle.common.netty.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.netty.IOMode;
import org.apache.uniffle.common.netty.TransportFrameDecoder;
import org.apache.uniffle.common.netty.handle.TransportChannelHandler;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.NettyUtils;

public class TransportClientFactory implements Closeable {

  /** A simple data structure to track the pool of clients between two peer nodes. */
  private static class ClientPool {
    TransportClient[] clients;
    Object[] locks;

    ClientPool(int size) {
      clients = new TransportClient[size];
      locks = new Object[size];
      for (int i = 0; i < size; i++) {
        locks[i] = new Object();
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(TransportClientFactory.class);

  private final TransportContext context;
  private final TransportConf conf;
  private final ConcurrentHashMap<SocketAddress, ClientPool> connectionPool;

  /** Random number generator for picking connections between peers. */
  private final Random rand;

  private final int numConnectionsPerPeer;

  private final Class<? extends Channel> socketChannelClass;
  private EventLoopGroup workerGroup;
  private ByteBufAllocator byteBufAllocator;

  public TransportClientFactory(TransportContext context) {
    this.context = Objects.requireNonNull(context);
    this.conf = context.getConf();
    this.connectionPool = JavaUtils.newConcurrentMap();
    this.numConnectionsPerPeer = conf.numConnectionsPerPeer();
    this.rand = new Random();

    IOMode ioMode = conf.ioMode();
    this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
    int clientThreads =
        conf.clientThreads() > 0
            ? conf.clientThreads()
            : (int) (numConnectionsPerPeer * conf.clientThreadsRatio());
    this.workerGroup = NettyUtils.createEventLoop(ioMode, clientThreads, "netty-rpc-client");
    if (conf.isSharedAllocatorEnabled()) {
      this.byteBufAllocator =
          conf.isPooledAllocatorEnabled()
              ? NettyUtils.getSharedPooledByteBufAllocator(
                  conf.preferDirectBufs(), false, clientThreads)
              : NettyUtils.getSharedUnpooledByteBufAllocator(conf.preferDirectBufs());
    } else {
      this.byteBufAllocator =
          conf.isPooledAllocatorEnabled()
              ? NettyUtils.createPooledByteBufAllocator(
                  conf.preferDirectBufs(), false, clientThreads)
              : NettyUtils.createUnpooledByteBufAllocator(conf.preferDirectBufs());
    }
    if (logger.isDebugEnabled()) {
      logger.debug(
          "isPooledAllocatorEnabled={}, isSharedAllocatorEnabled={}, preferDirectBufs={}, byteBufAllocator={}",
          conf.isPooledAllocatorEnabled(),
          conf.isSharedAllocatorEnabled(),
          conf.preferDirectBufs(),
          byteBufAllocator);
    }
  }

  public TransportClient createClient(String remoteHost, int remotePort, int partitionId)
      throws IOException, InterruptedException {
    return createClient(remoteHost, remotePort, partitionId, new TransportFrameDecoder());
  }

  public TransportClient createClient(
      String remoteHost, int remotePort, int partitionId, ChannelInboundHandlerAdapter decoder)
      throws IOException, InterruptedException {
    // Get connection from the connection pool first.
    // If it is not found or not active, create a new one.
    // Use unresolved address here to avoid DNS resolution each time we create a client.
    final InetSocketAddress unresolvedAddress =
        InetSocketAddress.createUnresolved(remoteHost, remotePort);

    // Create the ClientPool if we don't have it yet.
    ClientPool clientPool =
        connectionPool.computeIfAbsent(
            unresolvedAddress, x -> new ClientPool(numConnectionsPerPeer));

    int clientIndex =
        partitionId < 0 ? rand.nextInt(numConnectionsPerPeer) : partitionId % numConnectionsPerPeer;
    TransportClient cachedClient = clientPool.clients[clientIndex];

    if (cachedClient != null && cachedClient.isActive()) {
      // Make sure that the channel will not timeout by updating the last use time of the
      // handler. Then check that the client is still alive, in case it timed out before
      // this code was able to update things.
      TransportChannelHandler handler =
          cachedClient.getChannel().pipeline().get(TransportChannelHandler.class);
      if (handler != null) {
        synchronized (handler) {
          handler.getResponseHandler().updateTimeOfLastRequest();
        }
      }

      if (cachedClient.isActive()) {
        logger.trace(
            "Returning cached connection to {}: {}", cachedClient.getSocketAddress(), cachedClient);
        return cachedClient;
      }
    }

    // If we reach here, we don't have an existing connection open. Let's create a new one.
    // Multiple threads might race here to create new connections. Keep only one of them active.
    final long preResolveHost = System.nanoTime();
    final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost, remotePort);
    final long hostResolveTimeMs = (System.nanoTime() - preResolveHost) / 1000000;
    if (hostResolveTimeMs > 2000) {
      logger.warn("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    } else {
      logger.trace("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    }

    synchronized (clientPool.locks[clientIndex]) {
      cachedClient = clientPool.clients[clientIndex];

      if (cachedClient != null) {
        if (cachedClient.isActive()) {
          logger.trace("Returning cached connection to {}: {}", resolvedAddress, cachedClient);
          return cachedClient;
        } else {
          logger.info("Found inactive connection to {}, creating a new one.", resolvedAddress);
        }
      }
      clientPool.clients[clientIndex] = internalCreateClient(resolvedAddress, decoder);
      return clientPool.clients[clientIndex];
    }
  }

  public TransportClient createClient(String remoteHost, int remotePort)
      throws IOException, InterruptedException {
    return createClient(remoteHost, remotePort, -1);
  }

  /**
   * Create a completely new {@link TransportClient} to the given remote host / port. This
   * connection is not pooled.
   *
   * <p>As with {@link #createClient(String, int)}, this method is blocking.
   */
  private TransportClient internalCreateClient(
      InetSocketAddress address, ChannelInboundHandlerAdapter decoder)
      throws IOException, InterruptedException {
    Bootstrap bootstrap = new Bootstrap();
    bootstrap
        .group(workerGroup)
        .channel(socketChannelClass)
        // Disable Nagle's Algorithm since we don't want packets to wait
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectTimeoutMs())
        .option(ChannelOption.ALLOCATOR, byteBufAllocator);

    if (conf.receiveBuf() > 0) {
      bootstrap.option(ChannelOption.SO_RCVBUF, conf.receiveBuf());
    }

    if (conf.sendBuf() > 0) {
      bootstrap.option(ChannelOption.SO_SNDBUF, conf.sendBuf());
    }

    final AtomicReference<TransportClient> clientRef = new AtomicReference<>();
    final AtomicReference<Channel> channelRef = new AtomicReference<>();

    bootstrap.handler(
        new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) {
            TransportChannelHandler transportResponseHandler =
                context.initializePipeline(ch, decoder);
            clientRef.set(transportResponseHandler.getClient());
            channelRef.set(ch);
          }
        });

    // Connect to the remote server
    ChannelFuture cf = bootstrap.connect(address);
    if (!cf.await(conf.connectTimeoutMs())) {
      throw new IOException(
          String.format("Connecting to %s timed out (%s ms)", address, conf.connectTimeoutMs()));
    } else if (cf.cause() != null) {
      throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
    }

    TransportClient client = clientRef.get();
    if (client == null) {
      throw new RssException("Channel future completed unsuccessfully with null client");
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Connection to {} successful", address);
    }

    return client;
  }

  /** Close all connections in the connection pool, and shutdown the worker thread pool. */
  @Override
  public void close() {
    // Go through all clients and close them if they are active.
    for (ClientPool clientPool : connectionPool.values()) {
      for (int i = 0; i < clientPool.clients.length; i++) {
        TransportClient client = clientPool.clients[i];
        if (client != null) {
          clientPool.clients[i] = null;
          JavaUtils.closeQuietly(client);
        }
      }
    }
    connectionPool.clear();

    if (workerGroup != null && !workerGroup.isShuttingDown()) {
      workerGroup.shutdownGracefully();
    }
  }

  public TransportContext getContext() {
    return context;
  }
}
