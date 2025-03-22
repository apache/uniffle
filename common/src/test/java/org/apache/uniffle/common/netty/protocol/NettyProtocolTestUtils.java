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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShufflePartitionedBlock;

public class NettyProtocolTestUtils {

  private static boolean compareShuffleBlockInfo(
      ShuffleBlockInfo blockInfo1, ShuffleBlockInfo blockInfo2) {
    return blockInfo1.getBlockId() == blockInfo2.getBlockId()
        && blockInfo1.getLength() == blockInfo2.getLength()
        && blockInfo1.getCrc() == blockInfo2.getCrc()
        && blockInfo1.getTaskAttemptId() == blockInfo2.getTaskAttemptId()
        && blockInfo1.getUncompressLength() == blockInfo2.getUncompressLength()
        && blockInfo1.getData().equals(blockInfo2.getData());
  }

  private static boolean compareShuffleBlockInfoV1(
      ShuffleBlockInfo blockInfo1, ShufflePartitionedBlock blockInfo2) {
    return blockInfo1.getBlockId() == blockInfo2.getBlockId()
        && blockInfo1.getLength() == blockInfo2.getDataLength()
        && blockInfo1.getCrc() == blockInfo2.getCrc()
        && blockInfo1.getTaskAttemptId() == blockInfo2.getTaskAttemptId()
        && blockInfo1.getUncompressLength() == blockInfo2.getUncompressLength()
        && blockInfo1.getData().equals(blockInfo2.getData());
  }

  private static boolean compareBlockList(
      List<ShuffleBlockInfo> list1, List<ShuffleBlockInfo> list2) {
    if (list1 == null || list2 == null || list1.size() != list2.size()) {
      return false;
    }
    for (int i = 0; i < list1.size(); i++) {
      if (!compareShuffleBlockInfo(list1.get(i), list2.get(i))) {
        return false;
      }
    }
    return true;
  }

  private static boolean compareBlockListV1(
      List<ShuffleBlockInfo> list1, List<ShufflePartitionedBlock> list2) {
    if (list1 == null || list2 == null || list1.size() != list2.size()) {
      return false;
    }
    for (int i = 0; i < list1.size(); i++) {
      if (!compareShuffleBlockInfoV1(list1.get(i), list2.get(i))) {
        return false;
      }
    }
    return true;
  }

  private static boolean comparePartitionToBlockList(
      Map<Integer, List<ShuffleBlockInfo>> m1, Map<Integer, List<ShuffleBlockInfo>> m2) {
    if (m1 == null || m2 == null || m1.size() != m2.size()) {
      return false;
    }
    Iterator<Map.Entry<Integer, List<ShuffleBlockInfo>>> iter1 = m1.entrySet().iterator();
    while (iter1.hasNext()) {
      Map.Entry<Integer, List<ShuffleBlockInfo>> entry1 = iter1.next();
      if (!compareBlockList(entry1.getValue(), m2.get(entry1.getKey()))) {
        return false;
      }
    }
    return true;
  }

  private static boolean comparePartitionToBlockListV1(
      Map<Integer, List<ShuffleBlockInfo>> m1, Map<Integer, List<ShufflePartitionedBlock>> m2) {
    if (m1 == null || m2 == null || m1.size() != m2.size()) {
      return false;
    }
    Iterator<Map.Entry<Integer, List<ShuffleBlockInfo>>> iter1 = m1.entrySet().iterator();
    while (iter1.hasNext()) {
      Map.Entry<Integer, List<ShuffleBlockInfo>> entry1 = iter1.next();
      if (!compareBlockListV1(entry1.getValue(), m2.get(entry1.getKey()))) {
        return false;
      }
    }
    return true;
  }

  public static boolean compareSendShuffleDataRequest(
      SendShuffleDataRequest req1, SendShuffleDataRequest req2) {
    if (req1 == req2) {
      return true;
    }
    if (req1 == null || req2 == null) {
      return false;
    }
    boolean isEqual =
        req1.getRequestId() == req2.getRequestId()
            && req1.getShuffleId() == req2.getShuffleId()
            && req1.getRequireId() == req2.getRequireId()
            && req1.getTimestamp() == req2.getTimestamp()
            && req1.getAppId().equals(req2.getAppId());
    if (!isEqual) {
      return false;
    }
    return comparePartitionToBlockList(req1.getPartitionToBlocks(), req2.getPartitionToBlocks());
  }

  public static boolean compareSendShuffleDataRequestV1(
      SendShuffleDataRequest req1, SendShuffleDataRequestV1 req2) {
    if (req1 == null || req2 == null) {
      return false;
    }
    boolean isEqual =
        req1.getRequestId() == req2.getRequestId()
            && req1.getShuffleId() == req2.getShuffleId()
            && req1.getRequireId() == req2.getRequireId()
            && req1.getTimestamp() == req2.getTimestamp()
            && req1.getAppId().equals(req2.getAppId());
    if (!isEqual) {
      return false;
    }
    return comparePartitionToBlockListV1(req1.getPartitionToBlocks(), req2.getPartitionToBlocks());
  }
}
