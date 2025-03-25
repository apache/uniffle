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
import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.netty.handle.TransportResponseHandler;
import org.apache.uniffle.common.netty.protocol.Message;
import org.apache.uniffle.common.netty.protocol.RpcResponse;
import org.apache.uniffle.common.util.NettyUtils;

public class TransportClient implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(TransportClient.class);

  private Channel channel;
  private TransportResponseHandler handler;
  private volatile boolean timedOut;

  private static final AtomicLong counter = new AtomicLong();

  public TransportClient(Channel channel, TransportResponseHandler handler) {
    this.channel = Objects.requireNonNull(channel);
    this.handler = Objects.requireNonNull(handler);
    this.timedOut = false;
  }

  public Channel getChannel() {
    return channel;
  }

  public boolean isActive() {
    return !timedOut && (channel.isOpen() || channel.isActive());
  }

  public SocketAddress getSocketAddress() {
    return channel.remoteAddress();
  }

  public ChannelFuture sendRpc(Message message, RpcResponseCallback callback) {
    if (logger.isTraceEnabled()) {
      logger.trace("Pushing data to {}", NettyUtils.getRemoteAddress(channel));
    }
    long requestId = message.getRequestId();
    handler.addResponseCallback(requestId, callback);
    RpcChannelListener listener = new RpcChannelListener(requestId, callback);
    return channel.writeAndFlush(message).addListener(listener);
  }

  public RpcResponse sendRpcSync(Message message, long timeoutMs) {
    SettableFuture<RpcResponse> result = SettableFuture.create();
    RpcResponseCallback callback =
        new RpcResponseCallback() {
          @Override
          public void onSuccess(RpcResponse response) {
            result.set(response);
          }

          @Override
          public void onFailure(Throwable e) {
            result.setException(e);
          }
        };
    sendRpc(message, callback);
    try {
      return result.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RssException(e);
    } catch (Exception e) {
      throw new RssException(e);
    }
  }

  public static long requestId() {
    return counter.getAndIncrement();
  }

  public class StdChannelListener implements GenericFutureListener<Future<? super Void>> {
    final long startTime;
    final Object requestId;

    public StdChannelListener(Object requestId) {
      this.startTime = System.currentTimeMillis();
      this.requestId = requestId;
    }

    @Override
    public void operationComplete(Future<? super Void> future) throws Exception {
      if (future.isSuccess()) {
        if (logger.isTraceEnabled()) {
          long timeTaken = System.currentTimeMillis() - startTime;
          logger.trace(
              "Sending request {} to {} took {} ms",
              requestId,
              NettyUtils.getRemoteAddress(channel),
              timeTaken);
        }
      } else {
        String errorMsg =
            String.format(
                "Failed to send request %s to %s: %s, channel will be closed",
                requestId, NettyUtils.getRemoteAddress(channel), future.cause());
        logger.warn(errorMsg);
        channel.close();
        try {
          handleFailure(errorMsg, future.cause());
        } catch (Exception e) {
          logger.error("Uncaught exception in RPC response callback handler!", e);
        }
      }
    }

    protected void handleFailure(String errorMsg, Throwable cause) {
      logger.error("Error encountered " + errorMsg, cause);
    }
  }

  private class RpcChannelListener extends StdChannelListener {
    final long rpcRequestId;
    final RpcResponseCallback callback;

    RpcChannelListener(long rpcRequestId, RpcResponseCallback callback) {
      super("RPC " + rpcRequestId);
      this.rpcRequestId = rpcRequestId;
      this.callback = callback;
    }

    @Override
    protected void handleFailure(String errorMsg, Throwable cause) {
      handler.removeRpcRequest(rpcRequestId);
      callback.onFailure(new IOException(errorMsg, cause));
    }
  }

  @Override
  public void close() throws IOException {
    // close is a local operation and should finish with milliseconds; timeout just to be safe
    channel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
  }

  public void timeOut() {
    this.timedOut = true;
  }
}
