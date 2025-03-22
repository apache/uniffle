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

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.util.ByteBufUtils;
import org.apache.uniffle.common.util.NettyUtils;

public class Decoders {
  public static ShuffleServerInfo decodeShuffleServerInfo(ByteBuf byteBuf) {
    String id = ByteBufUtils.readLengthAndString(byteBuf);
    String host = ByteBufUtils.readLengthAndString(byteBuf);
    int grpcPort = byteBuf.readInt();
    int nettyPort = byteBuf.readInt();
    return new ShuffleServerInfo(id, host, grpcPort, nettyPort);
  }

  public static ShuffleBlockInfo decodeShuffleBlockInfo(ByteBuf byteBuf) {
    int partId = byteBuf.readInt();
    long blockId = byteBuf.readLong();
    int length = byteBuf.readInt();
    int shuffleId = byteBuf.readInt();
    long crc = byteBuf.readLong();
    long taskAttemptId = byteBuf.readLong();
    int dataLength = byteBuf.readInt();
    ByteBuf data = NettyUtils.getSharedUnpooledByteBufAllocator(true).directBuffer(dataLength);
    data.writeBytes(byteBuf, dataLength);
    int lengthOfShuffleServers = byteBuf.readInt();
    List<ShuffleServerInfo> serverInfos = Lists.newArrayList();
    for (int k = 0; k < lengthOfShuffleServers; k++) {
      serverInfos.add(decodeShuffleServerInfo(byteBuf));
    }
    int uncompressLength = byteBuf.readInt();
    long freeMemory = byteBuf.readLong();
    return new ShuffleBlockInfo(
        shuffleId,
        partId,
        blockId,
        length,
        crc,
        data,
        serverInfos,
        uncompressLength,
        freeMemory,
        taskAttemptId);
  }

  public static ShufflePartitionedBlock decodeShufflePartitionedBlockV1(ByteBuf byteBuf) {
    // To ensure upgrade compatibility, only the server-side code was modified.
    // Some unused information will be skipped during decoding.
    // TODO: Remove the ShuffleBlockInfo used in the communication between client and server via
    // handleSendShuffleDataRequest.
    //    https://github.com/apache/incubator-uniffle/issues/2201
    byteBuf.skipBytes(4); // partId Int
    final long blockId = byteBuf.readLong();
    final int length = byteBuf.readInt();
    byteBuf.skipBytes(4); // shuffleId Int
    final long crc = byteBuf.readLong();
    final long taskAttemptId = byteBuf.readLong();
    int dataLength = byteBuf.readInt();
    ByteBuf data = NettyUtils.getSharedUnpooledByteBufAllocator(true).directBuffer(dataLength);
    data.writeBytes(byteBuf, dataLength);
    int lengthOfShuffleServers = byteBuf.readInt();
    List<ShuffleServerInfo> serverInfos = Lists.newArrayList();
    for (int k = 0; k < lengthOfShuffleServers; k++) {
      serverInfos.add(decodeShuffleServerInfo(byteBuf));
    }
    final int uncompressLength = byteBuf.readInt();
    byteBuf.skipBytes(8); // freeMemory Long

    return new ShufflePartitionedBlock(length, uncompressLength, crc, blockId, taskAttemptId, data);
  }

  public static Map<Integer, List<Long>> decodePartitionToBlockIds(ByteBuf byteBuf) {
    Map<Integer, List<Long>> partitionToBlockIds = Maps.newHashMap();
    int mapSize = byteBuf.readInt();
    for (int i = 0; i < mapSize; i++) {
      int partitionId = byteBuf.readInt();
      int blockListSize = byteBuf.readInt();
      List<Long> blocks = Lists.newArrayList();
      for (int j = 0; j < blockListSize; j++) {
        blocks.add(byteBuf.readLong());
      }
      partitionToBlockIds.put(partitionId, blocks);
    }
    return partitionToBlockIds;
  }

  public static List<BufferSegment> decodeBufferSegments(ByteBuf byteBuf) {
    List<BufferSegment> bufferSegments = Lists.newArrayList();
    int size = byteBuf.readInt();
    for (int i = 0; i < size; i++) {
      long blockId = byteBuf.readLong();
      int offset = byteBuf.readInt();
      int length = byteBuf.readInt();
      int uncompressLength = byteBuf.readInt();
      long crc = byteBuf.readLong();
      long taskAttemptId = byteBuf.readLong();
      BufferSegment bufferSegment =
          new BufferSegment(blockId, offset, length, uncompressLength, crc, taskAttemptId);
      bufferSegments.add(bufferSegment);
    }
    return bufferSegments;
  }
}
