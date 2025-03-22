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
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;

import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.util.ByteBufUtils;

public class SendShuffleDataRequest extends RequestMessage {
  private String appId;
  private int shuffleId;

  private int stageAttemptNumber;
  private long requireId;
  private Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks;
  private long timestamp;

  public SendShuffleDataRequest(
      long requestId,
      String appId,
      int shuffleId,
      long requireId,
      Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks,
      long timestamp) {
    this(requestId, appId, shuffleId, 0, requireId, partitionToBlocks, timestamp);
  }

  public SendShuffleDataRequest(
      long requestId,
      String appId,
      int shuffleId,
      int stageAttemptNumber,
      long requireId,
      Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks,
      long timestamp) {
    super(requestId);
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.requireId = requireId;
    this.partitionToBlocks = partitionToBlocks;
    this.timestamp = timestamp;
    this.stageAttemptNumber = stageAttemptNumber;
  }

  @Override
  public Type type() {
    return Type.SEND_SHUFFLE_DATA_REQUEST;
  }

  @Override
  public int encodedLength() {
    int encodeLength =
        REQUEST_ID_ENCODE_LENGTH
            + ByteBufUtils.encodedLength(appId)
            + Integer.BYTES
            + Long.BYTES
            + Integer.BYTES;
    for (Map.Entry<Integer, List<ShuffleBlockInfo>> entry : partitionToBlocks.entrySet()) {
      encodeLength += 2 * Integer.BYTES;
      for (ShuffleBlockInfo sbi : entry.getValue()) {
        encodeLength += Encoders.encodeLengthOfShuffleBlockInfo(sbi);
      }
    }
    return encodeLength + Long.BYTES;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(getRequestId());
    ByteBufUtils.writeLengthAndString(buf, appId);
    buf.writeInt(shuffleId);
    buf.writeLong(requireId);
    encodePartitionData(buf);
    buf.writeLong(timestamp);
  }

  private static Map<Integer, List<ShuffleBlockInfo>> decodePartitionData(ByteBuf byteBuf) {
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    int lengthOfPartitionData = byteBuf.readInt();
    for (int i = 0; i < lengthOfPartitionData; i++) {
      int partitionId = byteBuf.readInt();
      int lengthOfShuffleBlocks = byteBuf.readInt();
      List<ShuffleBlockInfo> shuffleBlockInfoList = Lists.newArrayList();
      for (int j = 0; j < lengthOfShuffleBlocks; j++) {
        try {
          shuffleBlockInfoList.add(Decoders.decodeShuffleBlockInfo(byteBuf));
        } catch (Throwable t) {
          // An OutOfDirectMemoryError will be thrown, when the direct memory reaches the limit.
          // OutOfDirectMemoryError will not cause the JVM to exit, but may lead to direct memory
          // leaks.
          // Note: You can refer to docs/server_guide.md to set MAX_DIRECT_MEMORY_SIZE to a
          // reasonable value.
          shuffleBlockInfoList.forEach(sbi -> sbi.getData().release());
          partitionToBlocks.forEach(
              (integer, shuffleBlockInfos) -> {
                shuffleBlockInfos.forEach(sbi -> sbi.getData().release());
              });
          throw t;
        }
      }
      partitionToBlocks.put(partitionId, shuffleBlockInfoList);
    }
    return partitionToBlocks;
  }

  public static SendShuffleDataRequest decode(ByteBuf byteBuf) {
    long requestId = byteBuf.readLong();
    String appId = ByteBufUtils.readLengthAndString(byteBuf);
    int shuffleId = byteBuf.readInt();
    long requireId = byteBuf.readLong();
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = decodePartitionData(byteBuf);
    long timestamp = byteBuf.readLong();
    return new SendShuffleDataRequest(
        requestId, appId, shuffleId, requireId, partitionToBlocks, timestamp);
  }

  private void encodePartitionData(ByteBuf buf) {
    buf.writeInt(partitionToBlocks.size());
    for (Map.Entry<Integer, List<ShuffleBlockInfo>> entry : partitionToBlocks.entrySet()) {
      buf.writeInt(entry.getKey());
      buf.writeInt(entry.getValue().size());
      for (ShuffleBlockInfo sbi : entry.getValue()) {
        Encoders.encodeShuffleBlockInfo(sbi, buf);
      }
    }
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public long getRequireId() {
    return requireId;
  }

  public void setRequireId(long requireId) {
    this.requireId = requireId;
  }

  public Map<Integer, List<ShuffleBlockInfo>> getPartitionToBlocks() {
    return partitionToBlocks;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public int getStageAttemptNumber() {
    return stageAttemptNumber;
  }

  @Override
  public String getOperationType() {
    return "sendShuffleData";
  }
}
