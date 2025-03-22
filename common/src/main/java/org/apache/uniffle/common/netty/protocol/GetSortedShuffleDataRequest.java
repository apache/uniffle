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

import org.apache.uniffle.common.netty.DecodeException;
import org.apache.uniffle.common.netty.EncodeException;
import org.apache.uniffle.common.util.ByteBufUtils;

public class GetSortedShuffleDataRequest extends RequestMessage {
  private final String appId;
  private final int shuffleId;
  private final int partitionId;
  private final long blockId;
  private final int length;
  private final long timestamp;

  public GetSortedShuffleDataRequest(
      long requestId,
      String appId,
      int shuffleId,
      int partitionId,
      long blockId,
      int length,
      long timestamp) {
    super(requestId);
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.blockId = blockId;
    this.length = length;
    this.timestamp = timestamp;
  }

  public String getOperationType() {
    return "getSortedShuffleData";
  }

  public Type type() {
    return Type.GET_SORTED_SHUFFLE_DATA_REQUEST;
  }

  public int encodedLength() {
    return REQUEST_ID_ENCODE_LENGTH
        + ByteBufUtils.encodedLength(appId)
        + 3 * Integer.BYTES
        + 2 * Long.BYTES;
  }

  public void encode(ByteBuf buf) throws EncodeException {
    buf.writeLong(getRequestId());
    ByteBufUtils.writeLengthAndString(buf, appId);
    buf.writeInt(shuffleId);
    buf.writeInt(partitionId);
    buf.writeLong(blockId);
    buf.writeInt(length);
    buf.writeLong(timestamp);
  }

  public static GetSortedShuffleDataRequest decode(ByteBuf buf) throws DecodeException {
    long requestId = buf.readLong();
    String appId = ByteBufUtils.readLengthAndString(buf);
    int shuffleId = buf.readInt();
    int partitionId = buf.readInt();
    long blockId = buf.readLong();
    int length = buf.readInt();
    long timestamp = buf.readLong();
    return new GetSortedShuffleDataRequest(
        requestId, appId, shuffleId, partitionId, blockId, length, timestamp);
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public long getBlockId() {
    return blockId;
  }

  public int getLength() {
    return length;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
