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

import javax.annotation.Nullable;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/**
 * Server side interceptor that is used to put remote client's IP Address to thread local storage.
 */
public class ClientContextServerInterceptor implements ServerInterceptor {

  /**
   * A {@link ThreadLocal} variable to maintain the client's IP address along with a specific
   * thread.
   */
  private static final ThreadLocal<String> IP_ADDRESS_THREAD_LOCAL = new ThreadLocal<>();

  /** @return IP address of the gRPC client that is making the call */
  @Nullable public static String getIpAddress() {
    return IP_ADDRESS_THREAD_LOCAL.get();
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    /**
     * For streaming calls, below will make sure remote IP address and client version are injected
     * prior to creating the stream.
     */
    setRemoteIpAddress(call);

    /**
     * For non-streaming calls to server, below listener will be invoked in the same thread that is
     * serving the call.
     */
    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
        next.startCall(call, headers)) {
      @Override
      public void onHalfClose() {
        setRemoteIpAddress(call);
        super.onHalfClose();
      }
    };
  }

  private <ReqT, RespT> void setRemoteIpAddress(ServerCall<ReqT, RespT> call) {
    String remoteIpAddress = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR).toString();
    IP_ADDRESS_THREAD_LOCAL.set(remoteIpAddress);
  }
}
