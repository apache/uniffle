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

package org.apache.uniffle.common.util;

import java.util.Optional;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReferenceArray;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.netty.IOMode;
import org.apache.uniffle.common.netty.protocol.Message;

public class NettyUtils {
  private static final Logger logger = LoggerFactory.getLogger(NettyUtils.class);

  private static final long MAX_DIRECT_MEMORY_IN_BYTES = PlatformDependent.maxDirectMemory();

  /** Specifies an upper bound on the number of Netty threads that Uniffle requires by default. */
  private static int MAX_DEFAULT_NETTY_THREADS = 8;

  private static final AtomicReferenceArray<PooledByteBufAllocator>
      SHARED_POOLED_BYTE_BUF_ALLOCATOR = new AtomicReferenceArray<>(2);
  private static volatile UnpooledByteBufAllocator sharedUnpooledByteBufAllocator;

  /** Creates a Netty EventLoopGroup based on the IOMode. */
  public static EventLoopGroup createEventLoop(IOMode mode, int numThreads, String threadPrefix) {
    ThreadFactory threadFactory = ThreadUtils.getNettyThreadFactory(threadPrefix);

    switch (mode) {
      case NIO:
        return new NioEventLoopGroup(numThreads, threadFactory);
      case EPOLL:
        return new EpollEventLoopGroup(numThreads, threadFactory);
      default:
        throw new IllegalArgumentException("Unknown io mode: " + mode);
    }
  }

  /** Returns the correct (client) SocketChannel class based on IOMode. */
  public static Class<? extends Channel> getClientChannelClass(IOMode mode) {
    switch (mode) {
      case NIO:
        return NioSocketChannel.class;
      case EPOLL:
        return EpollSocketChannel.class;
      default:
        throw new IllegalArgumentException("Unknown io mode: " + mode);
    }
  }

  /**
   * Returns the default number of threads for both the Netty client and server thread pools. If
   * numUsableCores is 0, we will use Runtime get an approximate number of available cores.
   */
  public static int defaultNumThreads(int numUsableCores) {
    final int availableCores;
    if (numUsableCores > 0) {
      availableCores = numUsableCores;
    } else {
      availableCores = Runtime.getRuntime().availableProcessors();
    }
    return Math.min(availableCores, MAX_DEFAULT_NETTY_THREADS);
  }

  /**
   * Returns the lazily created shared pooled ByteBuf allocator for the specified allowCache
   * parameter value.
   */
  public static PooledByteBufAllocator getSharedPooledByteBufAllocator(
      boolean allowDirectBufs, boolean allowCache, int numCores) {
    final int index = allowCache ? 0 : 1;
    PooledByteBufAllocator allocator = SHARED_POOLED_BYTE_BUF_ALLOCATOR.get(index);
    if (allocator == null) {
      synchronized (NettyUtils.class) {
        allocator = SHARED_POOLED_BYTE_BUF_ALLOCATOR.get(index);
        if (allocator == null) {
          allocator = createPooledByteBufAllocator(allowDirectBufs, allowCache, numCores);
          SHARED_POOLED_BYTE_BUF_ALLOCATOR.set(index, allocator);
        }
      }
    }
    return allocator;
  }

  /**
   * Returns the lazily created shared un-pooled ByteBuf allocator for the specified allowCache
   * parameter value.
   */
  public static synchronized UnpooledByteBufAllocator getSharedUnpooledByteBufAllocator(
      boolean allowDirectBufs) {
    if (sharedUnpooledByteBufAllocator == null) {
      synchronized (NettyUtils.class) {
        if (sharedUnpooledByteBufAllocator == null) {
          sharedUnpooledByteBufAllocator = createUnpooledByteBufAllocator(allowDirectBufs);
        }
      }
    }
    return sharedUnpooledByteBufAllocator;
  }

  public static PooledByteBufAllocator createPooledByteBufAllocator(
      boolean allowDirectBufs, boolean allowCache, int numCores) {
    numCores = defaultNumThreads(numCores);
    return new PooledByteBufAllocator(
        allowDirectBufs && PlatformDependent.directBufferPreferred(),
        Math.min(PooledByteBufAllocator.defaultNumHeapArena(), numCores),
        Math.min(PooledByteBufAllocator.defaultNumDirectArena(), allowDirectBufs ? numCores : 0),
        PooledByteBufAllocator.defaultPageSize(),
        PooledByteBufAllocator.defaultMaxOrder(),
        allowCache ? PooledByteBufAllocator.defaultSmallCacheSize() : 0,
        allowCache ? PooledByteBufAllocator.defaultNormalCacheSize() : 0,
        allowCache && PooledByteBufAllocator.defaultUseCacheForAllThreads());
  }

  /** Returns the remote address on the channel or "&lt;unknown remote&gt;" if none exists. */
  public static String getRemoteAddress(Channel channel) {
    if (channel != null && channel.remoteAddress() != null) {
      return channel.remoteAddress().toString();
    }
    return "<unknown remote>";
  }

  public static ChannelFuture writeResponseMsg(
      ChannelHandlerContext ctx, Message msg, boolean doWriteType) {
    ByteBuf responseMsgBuf = ctx.alloc().buffer(msg.encodedLength());
    try {
      if (doWriteType) {
        responseMsgBuf.writeByte(msg.type().id());
      }
      msg.encode(responseMsgBuf);
      return ctx.writeAndFlush(responseMsgBuf);
    } catch (Throwable ex) {
      logger.warn("Caught exception, releasing ByteBuf", ex);
      responseMsgBuf.release();
      throw ex;
    }
  }

  public static String getServerConnectionInfo(ChannelHandlerContext ctx) {
    return getServerConnectionInfo(ctx.channel());
  }

  public static String getServerConnectionInfo(Channel channel) {
    return String.format("[%s -> %s]", channel.localAddress(), channel.remoteAddress());
  }

  public static UnpooledByteBufAllocator createUnpooledByteBufAllocator(boolean preferDirect) {
    return new UnpooledByteBufAllocator(preferDirect);
  }

  public static long getMaxDirectMemory() {
    return MAX_DIRECT_MEMORY_IN_BYTES;
  }

  private static final String PREFER_DIRECT_FOR_COMPOSITE_BUFFER_PROPERTY_KEY =
      "rss.netty.preferDirectForCompositeBuffer";
  private static final String PREFER_DIRECT_FOR_COMPOSITE_BUFFER_ENV_KEY =
      "RSS_NETTY_PREFER_DIRECT_FOR_COMPOSITE_BUFFER";
  private static final boolean PREFER_DIRECT_FOR_COMPOSITE_BUFFER_DEFAULT = false;
  private static final boolean _preferDirectForCompositeBuffer;

  static {
    _preferDirectForCompositeBuffer =
        Optional.ofNullable(System.getProperty(PREFER_DIRECT_FOR_COMPOSITE_BUFFER_PROPERTY_KEY))
            .map(Boolean::new)
            .orElse(
                Optional.ofNullable(System.getenv(PREFER_DIRECT_FOR_COMPOSITE_BUFFER_ENV_KEY))
                    .map(Boolean::new)
                    .orElse(PREFER_DIRECT_FOR_COMPOSITE_BUFFER_DEFAULT));
  }

  public static boolean preferDirectForCompositeBuffer() {
    return _preferDirectForCompositeBuffer;
  }
}
