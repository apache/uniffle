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

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.netty.buffer.ManagedBuffer;
import org.apache.uniffle.common.netty.buffer.NettyManagedBuffer;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.ByteBufUtils;

public class GetMemoryShuffleDataResponse extends RpcResponse {
  private List<BufferSegment> bufferSegments;

  public GetMemoryShuffleDataResponse(
      long requestId, StatusCode statusCode, List<BufferSegment> bufferSegments, byte[] data) {
    this(requestId, statusCode, null, bufferSegments, data);
  }

  public GetMemoryShuffleDataResponse(
      long requestId,
      StatusCode statusCode,
      String retMessage,
      List<BufferSegment> bufferSegments,
      byte[] data) {
    this(requestId, statusCode, retMessage, bufferSegments, Unpooled.wrappedBuffer(data));
  }

  public GetMemoryShuffleDataResponse(
      long requestId,
      StatusCode statusCode,
      String retMessage,
      List<BufferSegment> bufferSegments,
      ByteBuf data) {
    this(requestId, statusCode, retMessage, bufferSegments, new NettyManagedBuffer(data));
  }

  public GetMemoryShuffleDataResponse(
      long requestId,
      StatusCode statusCode,
      String retMessage,
      List<BufferSegment> bufferSegments,
      ManagedBuffer managedBuffer) {
    super(requestId, statusCode, retMessage, managedBuffer);
    this.bufferSegments = bufferSegments;
  }

  @Override
  public int encodedLength() {
    return super.encodedLength() + Encoders.encodeLengthOfBufferSegments(bufferSegments);
  }

  @Override
  public void encode(ByteBuf buf) {
    super.encode(buf);
    Encoders.encodeBufferSegments(bufferSegments, buf);
  }

  public static GetMemoryShuffleDataResponse decode(ByteBuf byteBuf, boolean decodeBody) {
    long requestId = byteBuf.readLong();
    StatusCode statusCode = StatusCode.fromCode(byteBuf.readInt());
    String retMessage = ByteBufUtils.readLengthAndString(byteBuf);
    List<BufferSegment> bufferSegments = Decoders.decodeBufferSegments(byteBuf);
    if (decodeBody) {
      NettyManagedBuffer nettyManagedBuffer = new NettyManagedBuffer(byteBuf.retain());
      return new GetMemoryShuffleDataResponse(
          requestId, statusCode, retMessage, bufferSegments, nettyManagedBuffer);
    } else {
      return new GetMemoryShuffleDataResponse(
          requestId, statusCode, retMessage, bufferSegments, NettyManagedBuffer.EMPTY_BUFFER);
    }
  }

  @Override
  public Type type() {
    return Type.GET_MEMORY_SHUFFLE_DATA_RESPONSE;
  }

  public List<BufferSegment> getBufferSegments() {
    return bufferSegments;
  }
}
