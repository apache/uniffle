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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.tez.dag.records.TezTaskAttemptID;

public class GetShuffleServerRequest implements Writable {
  private TezTaskAttemptID currentTaskAttemptID;
  private int startIndex;
  private int partitionNum;
  private int shuffleId;
  private String keyClassName;
  private String valueClassName;
  private String comparatorClassName;

  public GetShuffleServerRequest() {}

  public GetShuffleServerRequest(
      TezTaskAttemptID currentTaskAttemptID, int startIndex, int partitionNum, int shuffleId) {
    this(currentTaskAttemptID, startIndex, partitionNum, shuffleId, "", "", "");
  }

  public GetShuffleServerRequest(
      TezTaskAttemptID currentTaskAttemptID,
      int startIndex,
      int partitionNum,
      int shuffleId,
      String keyClassName,
      String valueClassName,
      String comparatorClassName) {
    this.currentTaskAttemptID = currentTaskAttemptID;
    this.startIndex = startIndex;
    this.partitionNum = partitionNum;
    this.shuffleId = shuffleId;
    this.keyClassName = keyClassName;
    this.valueClassName = valueClassName;
    this.comparatorClassName = comparatorClassName;
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(startIndex);
    output.writeInt(partitionNum);
    output.writeInt(shuffleId);
    if (currentTaskAttemptID != null) {
      output.writeBoolean(true);
      currentTaskAttemptID.write(output);
    } else {
      output.writeBoolean(false);
    }
    WritableUtils.writeString(output, keyClassName);
    WritableUtils.writeString(output, valueClassName);
    WritableUtils.writeString(output, comparatorClassName);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    startIndex = dataInput.readInt();
    partitionNum = dataInput.readInt();
    shuffleId = dataInput.readInt();
    boolean hasTaskTaskAttemptID = dataInput.readBoolean();
    if (hasTaskTaskAttemptID) {
      currentTaskAttemptID = new TezTaskAttemptID();
      currentTaskAttemptID.readFields(dataInput);
    }
    keyClassName = WritableUtils.readString(dataInput);
    valueClassName = WritableUtils.readString(dataInput);
    comparatorClassName = WritableUtils.readString(dataInput);
  }

  @Override
  public String toString() {
    return "GetShuffleServerRequest{"
        + "currentTaskAttemptID="
        + currentTaskAttemptID
        + ", startIndex="
        + startIndex
        + ", partitionNum="
        + partitionNum
        + ", shuffleId="
        + shuffleId
        + '}';
  }

  public TezTaskAttemptID getCurrentTaskAttemptID() {
    return currentTaskAttemptID;
  }

  public int getStartIndex() {
    return startIndex;
  }

  public int getPartitionNum() {
    return partitionNum;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public String getKeyClassName() {
    return keyClassName;
  }

  public String getValueClassName() {
    return valueClassName;
  }

  public String getComparatorClassName() {
    return comparatorClassName;
  }
}
