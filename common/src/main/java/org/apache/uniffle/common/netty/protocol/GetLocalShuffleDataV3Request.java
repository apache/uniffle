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

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;

import org.apache.uniffle.common.ReadSegment;
import org.apache.uniffle.common.util.ByteBufUtils;

public class GetLocalShuffleDataV3Request extends GetLocalShuffleDataV2Request {
  private final List<ReadSegment> nextReadSegments;
  private final long taskAttemptId;

  public GetLocalShuffleDataV3Request(
      long requestId,
      String appId,
      int shuffleId,
      int partitionId,
      int partitionNumPerRange,
      int partitionNum,
      long offset,
      int length,
      int storageId,
      List<ReadSegment> nextReadSegments,
      long timestamp,
      long taskAttemptId) {
    super(
        requestId,
        appId,
        shuffleId,
        partitionId,
        partitionNumPerRange,
        partitionNum,
        offset,
        length,
        storageId,
        timestamp);
    this.nextReadSegments = nextReadSegments;
    this.taskAttemptId = taskAttemptId;
  }

  @Override
  public Type type() {
    return Type.GET_LOCAL_SHUFFLE_DATA_V3_REQUEST;
  }

  @Override
  public int encodedLength() {
    return super.encodedLength() + Long.BYTES * 2 * nextReadSegments.size();
  }

  @Override
  public void encode(ByteBuf buf) {
    super.encode(buf);
    buf.writeInt(nextReadSegments.size());
    for (ReadSegment segment : nextReadSegments) {
      buf.writeLong(segment.getOffset());
      buf.writeLong(segment.getLength());
    }
    buf.writeLong(taskAttemptId);
  }

  public static GetLocalShuffleDataV3Request decode(ByteBuf byteBuf) {
    long requestId = byteBuf.readLong();
    String appId = ByteBufUtils.readLengthAndString(byteBuf);
    int shuffleId = byteBuf.readInt();
    int partitionId = byteBuf.readInt();
    int partitionNumPerRange = byteBuf.readInt();
    int partitionNum = byteBuf.readInt();
    long offset = byteBuf.readLong();
    int length = byteBuf.readInt();
    long timestamp = byteBuf.readLong();
    int storageId = byteBuf.readInt();

    int readSegmentCount = byteBuf.readInt();
    List<ReadSegment> readSegments = new ArrayList<>(readSegmentCount);
    for (int i = 0; i < readSegmentCount; i++) {
      readSegments.add(new ReadSegment(byteBuf.readLong(), byteBuf.readLong()));
    }
    long taskAttemptId = byteBuf.readLong();
    return new GetLocalShuffleDataV3Request(
        requestId,
        appId,
        shuffleId,
        partitionId,
        partitionNumPerRange,
        partitionNum,
        offset,
        length,
        storageId,
        readSegments,
        timestamp,
        taskAttemptId);
  }

  @Override
  public String getOperationType() {
    return "getLocalShuffleDataV3";
  }
}
