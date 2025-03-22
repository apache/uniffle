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
import io.netty.buffer.Unpooled;

import org.apache.uniffle.common.netty.buffer.ManagedBuffer;
import org.apache.uniffle.common.netty.buffer.NettyManagedBuffer;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.ByteBufUtils;
import org.apache.uniffle.common.util.Constants;

public class GetLocalShuffleIndexV2Response extends GetLocalShuffleIndexResponse {

  private final int[] storageIds;

  public GetLocalShuffleIndexV2Response(
      long requestId, StatusCode statusCode, String retMessage, byte[] indexData, long fileLength) {
    this(
        requestId,
        statusCode,
        retMessage,
        indexData == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(indexData),
        fileLength);
  }

  public GetLocalShuffleIndexV2Response(
      long requestId,
      StatusCode statusCode,
      String retMessage,
      ByteBuf indexData,
      long fileLength) {
    this(
        requestId,
        statusCode,
        retMessage,
        new NettyManagedBuffer(indexData),
        fileLength,
        Constants.EMPTY_INT_ARRAY);
  }

  public GetLocalShuffleIndexV2Response(
      long requestId,
      StatusCode statusCode,
      String retMessage,
      ManagedBuffer managedBuffer,
      long fileLength,
      int[] storageIds) {
    super(requestId, statusCode, retMessage, managedBuffer, fileLength);
    this.storageIds = storageIds;
  }

  @Override
  public int encodedLength() {
    // super encodedLength + 4(storageIds.length) + 4 * storageIds.length
    return super.encodedLength() + Integer.BYTES + Integer.BYTES * storageIds.length;
  }

  @Override
  public void encode(ByteBuf buf) {
    super.encode(buf);
    buf.writeInt(storageIds.length);
    for (int storageId : storageIds) {
      buf.writeInt(storageId);
    }
  }

  public static GetLocalShuffleIndexV2Response decode(ByteBuf byteBuf, boolean decodeBody) {
    long requestId = byteBuf.readLong();
    StatusCode statusCode = StatusCode.fromCode(byteBuf.readInt());
    String retMessage = ByteBufUtils.readLengthAndString(byteBuf);
    long fileLength = byteBuf.readLong();
    int[] storageIds = new int[byteBuf.readInt()];
    for (int i = 0; i < storageIds.length; i++) {
      storageIds[i] = byteBuf.readInt();
    }
    if (decodeBody) {
      NettyManagedBuffer nettyManagedBuffer = new NettyManagedBuffer(byteBuf);
      return new GetLocalShuffleIndexV2Response(
          requestId, statusCode, retMessage, nettyManagedBuffer, fileLength, storageIds);
    } else {
      return new GetLocalShuffleIndexV2Response(
          requestId,
          statusCode,
          retMessage,
          NettyManagedBuffer.EMPTY_BUFFER,
          fileLength,
          storageIds);
    }
  }

  @Override
  public Type type() {
    return Type.GET_LOCAL_SHUFFLE_INDEX_V2_RESPONSE;
  }

  @Override
  public int[] getStorageIds() {
    return storageIds;
  }
}
