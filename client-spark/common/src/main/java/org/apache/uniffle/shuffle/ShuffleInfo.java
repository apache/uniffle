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

package org.apache.uniffle.shuffle;

import java.nio.ByteBuffer;
import java.util.Optional;

public class ShuffleInfo {
  private Optional<ShuffleValidationInfo> validationInfo;
  private long taskAttemptId;

  public ShuffleInfo(Optional<ShuffleValidationInfo> validationInfo, long taskAttemptId) {
    this.validationInfo = validationInfo;
    this.taskAttemptId = taskAttemptId;
  }

  public byte[] encode() {
    byte[] validationBytes =
        validationInfo.isPresent() ? validationInfo.get().encode().array() : new byte[0];
    int totalSize = Long.BYTES + Integer.BYTES + validationBytes.length;
    ByteBuffer buffer = ByteBuffer.allocate(totalSize);
    buffer.putLong(taskAttemptId);
    buffer.putInt(validationBytes.length);
    buffer.put(validationBytes);
    return buffer.array();
  }

  public static ShuffleInfo decode(byte[] data) {
    if (data == null || data.length < Long.BYTES + Integer.BYTES) {
      return null;
    }
    ByteBuffer buffer = ByteBuffer.wrap(data);
    long taskAttemptId = buffer.getLong();
    int validationLength = buffer.getInt();

    Optional<ShuffleValidationInfo> validationInfo = Optional.empty();
    if (validationLength > 0) {
      byte[] validationBytes = new byte[validationLength];
      buffer.get(validationBytes);
      ShuffleValidationInfo info = ShuffleValidationInfo.decode(ByteBuffer.wrap(validationBytes));
      validationInfo = Optional.of(info);
    }
    return new ShuffleInfo(validationInfo, taskAttemptId);
  }

  public Optional<ShuffleValidationInfo> getValidationInfo() {
    return validationInfo;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }
}
