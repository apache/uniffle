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

public class GetMemoryShuffleDataV2Response extends GetMemoryShuffleDataResponse {
  private boolean isEnd = false;

  public GetMemoryShuffleDataV2Response(
      long requestId, StatusCode statusCode, List<BufferSegment> bufferSegments, byte[] data) {
    super(requestId, statusCode, null, bufferSegments, data);
  }

  public GetMemoryShuffleDataV2Response(
      long requestId,
      StatusCode statusCode,
      String retMessage,
      List<BufferSegment> bufferSegments,
      byte[] data,
      boolean isEnd) {
    super(requestId, statusCode, retMessage, bufferSegments, Unpooled.wrappedBuffer(data));
    this.isEnd = isEnd;
  }

  public GetMemoryShuffleDataV2Response(
      long requestId,
      StatusCode statusCode,
      String retMessage,
      List<BufferSegment> bufferSegments,
      ByteBuf data,
      boolean isEnd) {
    super(requestId, statusCode, retMessage, bufferSegments, new NettyManagedBuffer(data));
    this.isEnd = isEnd;
  }

  public GetMemoryShuffleDataV2Response(
      long requestId,
      StatusCode statusCode,
      String retMessage,
      List<BufferSegment> bufferSegments,
      ManagedBuffer managedBuffer,
      boolean isEnd) {
    super(requestId, statusCode, retMessage, bufferSegments, managedBuffer);
    this.isEnd = isEnd;
  }

  @Override
  public int encodedLength() {
    return super.encodedLength() + 1;
  }

  @Override
  public void encode(ByteBuf buf) {
    super.encode(buf);
    buf.writeByte(isEnd ? 1 : 0);
  }

  public static GetMemoryShuffleDataResponse decode(ByteBuf byteBuf, boolean decodeBody) {
    long requestId = byteBuf.readLong();
    StatusCode statusCode = StatusCode.fromCode(byteBuf.readInt());
    String retMessage = ByteBufUtils.readLengthAndString(byteBuf);
    List<BufferSegment> bufferSegments = Decoders.decodeBufferSegments(byteBuf);
    boolean isEnd = byteBuf.readByte() == 1;
    if (decodeBody) {
      NettyManagedBuffer nettyManagedBuffer = new NettyManagedBuffer(byteBuf);
      return new GetMemoryShuffleDataV2Response(
          requestId, statusCode, retMessage, bufferSegments, nettyManagedBuffer, isEnd);
    } else {
      return new GetMemoryShuffleDataV2Response(
          requestId,
          statusCode,
          retMessage,
          bufferSegments,
          NettyManagedBuffer.EMPTY_BUFFER,
          isEnd);
    }
  }

  @Override
  public Type type() {
    return Type.GET_MEMORY_SHUFFLE_DATA_V2_RESPONSE;
  }

  public boolean isEnd() {
    return isEnd;
  }
}
