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

import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.util.ByteBufUtils;

public class SendShuffleDataRequestV1 extends RequestMessage {
  private String appId;
  private int shuffleId;

  private int stageAttemptNumber;
  private long requireId;
  private Map<Integer, List<ShufflePartitionedBlock>> partitionToBlocks;
  private long timestamp;
  private int decodedLength;

  public SendShuffleDataRequestV1(long requestId) {
    super(requestId);
  }

  @Override
  public Type type() {
    return Type.SEND_SHUFFLE_DATA_REQUEST;
  }

  @Override
  public int encodedLength() {
    return -1;
  }

  @Override
  public void encode(ByteBuf buf) {}

  public void decodeShuffleData(ByteBuf byteBuf) {
    final int startIndex = byteBuf.readerIndex();
    this.appId = ByteBufUtils.readLengthAndString(byteBuf);
    this.shuffleId = byteBuf.readInt();
    this.requireId = byteBuf.readLong();
    this.partitionToBlocks = decodePartitionData(byteBuf);
    this.timestamp = byteBuf.readLong();
    int endIndex = byteBuf.readerIndex();
    decodedLength += endIndex - startIndex;
  }

  public int getDecodedLength() {
    return decodedLength;
  }

  private Map<Integer, List<ShufflePartitionedBlock>> decodePartitionData(ByteBuf byteBuf) {
    Map<Integer, List<ShufflePartitionedBlock>> partitionToBlocks = Maps.newHashMap();
    int lengthOfPartitionData = byteBuf.readInt();
    for (int i = 0; i < lengthOfPartitionData; i++) {
      int partitionId = byteBuf.readInt();
      int lengthOfShuffleBlocks = byteBuf.readInt();
      List<ShufflePartitionedBlock> shufflePartitionedBlocks = Lists.newArrayList();
      for (int j = 0; j < lengthOfShuffleBlocks; j++) {
        try {
          shufflePartitionedBlocks.add(Decoders.decodeShufflePartitionedBlockV1(byteBuf));
        } catch (Throwable t) {
          shufflePartitionedBlocks.forEach(sbi -> sbi.getData().release());
          if (!partitionToBlocks.isEmpty()) {
            partitionToBlocks.forEach(
                (integer, shuffleBlockInfos) -> {
                  shuffleBlockInfos.forEach(sbi -> sbi.getData().release());
                });
          }
          throw t;
        }
      }
      partitionToBlocks.put(partitionId, shufflePartitionedBlocks);
    }
    return partitionToBlocks;
  }

  public static SendShuffleDataRequestV1 decode(ByteBuf byteBuf) {
    int startIndex = byteBuf.readerIndex();
    long requestId = byteBuf.readLong();
    SendShuffleDataRequestV1 req = new SendShuffleDataRequestV1(requestId);
    req.decodeShuffleData(byteBuf);
    int endIndex = byteBuf.readerIndex();
    req.setDecodedLength(endIndex - startIndex);

    return req;
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

  public Map<Integer, List<ShufflePartitionedBlock>> getPartitionToBlocks() {
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
    return "sendShuffleDataV1";
  }

  public void setDecodedLength(int decodedLength) {
    this.decodedLength = decodedLength;
  }
}
