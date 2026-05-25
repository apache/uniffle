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

package org.apache.uniffle.common.netty.protocol;

import io.netty.buffer.ByteBuf;

import org.apache.uniffle.common.netty.buffer.ManagedBuffer;
import org.apache.uniffle.common.netty.buffer.NettyManagedBuffer;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.ByteBufUtils;

public class GetLocalShuffleDataResponse extends RpcResponse {

  public GetLocalShuffleDataResponse(
      long requestId, StatusCode statusCode, String retMessage, ManagedBuffer data) {
    super(requestId, statusCode, retMessage, data);
  }

  public static GetLocalShuffleDataResponse decode(ByteBuf byteBuf, boolean decodeBody) {
    long requestId = byteBuf.readLong();
    StatusCode statusCode = StatusCode.fromCode(byteBuf.readInt());
    String retMessage = ByteBufUtils.readLengthAndString(byteBuf);
    if (decodeBody) {
      NettyManagedBuffer nettyManagedBuffer = new NettyManagedBuffer(byteBuf.retain());
      return new GetLocalShuffleDataResponse(requestId, statusCode, retMessage, nettyManagedBuffer);
    } else {
      return new GetLocalShuffleDataResponse(
          requestId, statusCode, retMessage, NettyManagedBuffer.EMPTY_BUFFER);
    }
  }

  @Override
  public Type type() {
    return Type.GET_LOCAL_SHUFFLE_DATA_RESPONSE;
  }
}
