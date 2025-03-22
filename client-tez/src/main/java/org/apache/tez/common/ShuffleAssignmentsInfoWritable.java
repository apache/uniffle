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

package org.apache.tez.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleServerInfo;

public class ShuffleAssignmentsInfoWritable implements Writable {
  private ShuffleAssignmentsInfo shuffleAssignmentsInfo;
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleAssignmentsInfoWritable.class);

  public ShuffleAssignmentsInfoWritable() {}

  public ShuffleAssignmentsInfoWritable(ShuffleAssignmentsInfo shuffleAssignmentsInfo) {
    this.shuffleAssignmentsInfo = shuffleAssignmentsInfo;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    if (shuffleAssignmentsInfo == null) {
      dataOutput.writeInt(-1);
      LOG.warn("shuffleAssignmentsInfo is null, no need write");
      return;
    } else {
      dataOutput.writeInt(1);
    }

    Map<Integer, List<ShuffleServerInfo>> partitionToServers =
        shuffleAssignmentsInfo.getPartitionToServers();
    if (MapUtils.isEmpty(partitionToServers)) {
      dataOutput.writeInt(-1);
    } else {
      dataOutput.writeInt(partitionToServers.size());
      for (Map.Entry<Integer, List<ShuffleServerInfo>> entry : partitionToServers.entrySet()) {
        dataOutput.writeInt(entry.getKey());
        if (CollectionUtils.isEmpty(entry.getValue())) {
          dataOutput.writeInt(-1);
        } else {
          dataOutput.writeInt(entry.getValue().size());
          for (ShuffleServerInfo serverInfo : entry.getValue()) {
            writeShuffleServerInfo(dataOutput, serverInfo);
          }
        }
      }
    }

    Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges =
        shuffleAssignmentsInfo.getServerToPartitionRanges();
    if (MapUtils.isEmpty(serverToPartitionRanges)) {
      dataOutput.writeInt(-1);
    } else {
      dataOutput.writeInt(serverToPartitionRanges.size());
      for (Map.Entry<ShuffleServerInfo, List<PartitionRange>> entry :
          serverToPartitionRanges.entrySet()) {
        writeShuffleServerInfo(dataOutput, entry.getKey());
        if (CollectionUtils.isEmpty(entry.getValue())) {
          dataOutput.writeInt(-1);
        } else {
          dataOutput.writeInt(entry.getValue().size());
          for (PartitionRange range : entry.getValue()) {
            dataOutput.writeInt(range.getStart());
            dataOutput.writeInt(range.getEnd());
          }
        }
      }
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    if (dataInput.readInt() == -1) {
      LOG.warn("shuffleAssignmentsInfo is null, no need read");
      return;
    }

    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    int partitionToServersSize = dataInput.readInt();
    if (partitionToServersSize != -1) {
      for (int i = 0; i < partitionToServersSize; i++) {
        int partitionId = dataInput.readInt();
        List<ShuffleServerInfo> shuffleServerInfoList = new ArrayList<>();
        int shuffleServerInfoListSize = dataInput.readInt();
        if (shuffleServerInfoListSize != -1) {
          for (int i1 = 0; i1 < shuffleServerInfoListSize; i1++) {
            shuffleServerInfoList.add(getShuffleServerInfo(dataInput));
          }
        }

        partitionToServers.put(partitionId, shuffleServerInfoList);
      }
    }

    Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges = new HashMap<>();
    int serverToPartitionRangesSize = dataInput.readInt();
    if (serverToPartitionRangesSize != -1) {
      for (int i = 0; i < serverToPartitionRangesSize; i++) {
        List<PartitionRange> partitionRangeList = new ArrayList<>();
        ShuffleServerInfo shuffleServerInfo = getShuffleServerInfo(dataInput);
        int partitionRangeListSize = dataInput.readInt();
        if (partitionRangeListSize != -1) {
          for (int i1 = 0; i1 < partitionRangeListSize; i1++) {
            int start = dataInput.readInt();
            int end = dataInput.readInt();
            PartitionRange partitionRange = new PartitionRange(start, end);
            partitionRangeList.add(partitionRange);
          }
        }
        serverToPartitionRanges.put(shuffleServerInfo, partitionRangeList);
      }
    }

    shuffleAssignmentsInfo =
        new ShuffleAssignmentsInfo(partitionToServers, serverToPartitionRanges);
  }

  private ShuffleServerInfo getShuffleServerInfo(DataInput dataInput) throws IOException {
    ShuffleServerInfo shuffleServerInfo;
    String id = dataInput.readUTF();
    String host = dataInput.readUTF();
    int grpcPort = dataInput.readInt();
    int nettyPort = dataInput.readInt();
    shuffleServerInfo = new ShuffleServerInfo(id, host, grpcPort, nettyPort);
    return shuffleServerInfo;
  }

  private void writeShuffleServerInfo(DataOutput dataOutput, ShuffleServerInfo shuffleServerInfo)
      throws IOException {
    dataOutput.writeUTF(shuffleServerInfo.getId());
    dataOutput.writeUTF(shuffleServerInfo.getHost());
    dataOutput.writeInt(shuffleServerInfo.getGrpcPort());
    dataOutput.writeInt(shuffleServerInfo.getNettyPort());
  }

  public ShuffleAssignmentsInfo getShuffleAssignmentsInfo() {
    return shuffleAssignmentsInfo;
  }
}
