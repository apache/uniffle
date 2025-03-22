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

public class GetSortedShuffleDataResponse extends RpcResponse {
  private long nextBlockId;
  private int mergeState;

  public GetSortedShuffleDataResponse(
      long requestId,
      StatusCode statusCode,
      String retMessage,
      long nextBlockId,
      int mergeState,
      ByteBuf data) {
    this(requestId, statusCode, retMessage, nextBlockId, mergeState, new NettyManagedBuffer(data));
  }

  public GetSortedShuffleDataResponse(
      long requestId,
      StatusCode statusCode,
      String retMessage,
      long nextBlockId,
      int mergeState,
      ManagedBuffer managedBuffer) {
    super(requestId, statusCode, retMessage, managedBuffer);
    this.nextBlockId = nextBlockId;
    this.mergeState = mergeState;
  }

  public long getNextBlockId() {
    return nextBlockId;
  }

  public int getMergeState() {
    return mergeState;
  }

  @Override
  public int encodedLength() {
    return super.encodedLength() + 8 + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    super.encode(buf);
    buf.writeLong(nextBlockId);
    buf.writeInt(mergeState);
  }

  public static GetSortedShuffleDataResponse decode(ByteBuf buf, boolean decodeBody) {
    long requestId = buf.readLong();
    StatusCode statusCode = StatusCode.fromCode(buf.readInt());
    String retMessage = ByteBufUtils.readLengthAndString(buf);
    NettyManagedBuffer nettyManagedBuffer;
    if (decodeBody) {
      nettyManagedBuffer = new NettyManagedBuffer(buf);
    } else {
      nettyManagedBuffer = NettyManagedBuffer.EMPTY_BUFFER;
    }
    long nextBlockId = buf.readLong();
    int mergeState = buf.readInt();
    return new GetSortedShuffleDataResponse(
        requestId, statusCode, retMessage, nextBlockId, mergeState, nettyManagedBuffer);
  }

  @Override
  public Type type() {
    return Type.GET_SORTED_SHUFFLE_DATA_RESPONSE;
  }
}
