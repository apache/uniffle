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

package org.apache.uniffle.common;

import java.nio.ByteBuffer;

import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.netty.buffer.ManagedBuffer;
import org.apache.uniffle.common.netty.buffer.NettyManagedBuffer;
import org.apache.uniffle.common.util.ByteBufUtils;

public class ShuffleIndexResult {
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleIndexResult.class);
  private static final int[] DEFAULT_STORAGE_IDS = new int[] {0};

  private final ManagedBuffer buffer;
  private final int[] storageIds;
  private long dataFileLen;
  private String dataFileName;

  public ShuffleIndexResult() {
    this(ByteBuffer.wrap(new byte[0]), -1);
  }

  public ShuffleIndexResult(byte[] data, long dataFileLen) {
    this(data != null ? ByteBuffer.wrap(data) : null, dataFileLen);
  }

  public ShuffleIndexResult(ByteBuffer data, long dataFileLen) {
    this(
        new NettyManagedBuffer(data != null ? Unpooled.wrappedBuffer(data) : Unpooled.EMPTY_BUFFER),
        dataFileLen,
        null,
        DEFAULT_STORAGE_IDS);
  }

  public ShuffleIndexResult(ManagedBuffer buffer, long dataFileLen, String dataFileName) {
    this(buffer, dataFileLen, dataFileName, DEFAULT_STORAGE_IDS);
  }

  public ShuffleIndexResult(
      ManagedBuffer buffer, long dataFileLen, String dataFileName, int storageId) {
    this(buffer, dataFileLen, dataFileName, new int[] {storageId});
  }

  public ShuffleIndexResult(
      ManagedBuffer buffer, long dataFileLen, String dataFileName, int[] storageIds) {
    this.buffer = buffer;
    this.dataFileLen = dataFileLen;
    this.dataFileName = dataFileName;
    this.storageIds = storageIds;
  }

  public byte[] getData() {
    if (buffer == null) {
      return null;
    }
    if (buffer.nioByteBuffer().hasArray()) {
      return buffer.nioByteBuffer().array();
    }
    return ByteBufUtils.readBytes(buffer.byteBuf());
  }

  public ByteBuffer getIndexData() {
    return buffer.nioByteBuffer();
  }

  public long getDataFileLen() {
    return dataFileLen;
  }

  public boolean isEmpty() {
    return buffer == null || buffer.size() == 0;
  }

  public void release() {
    if (this.buffer != null) {
      try {
        this.buffer.release();
      } catch (IllegalReferenceCountException e) {
        LOG.warn(
            "Failed to release shuffle index result with length {} of {}. "
                + "Maybe it has been released by others.",
            dataFileLen,
            dataFileName,
            e);
      }
    }
  }

  public ManagedBuffer getManagedBuffer() {
    return buffer;
  }

  public String getDataFileName() {
    return dataFileName;
  }

  public int[] getStorageIds() {
    return storageIds;
  }
}
