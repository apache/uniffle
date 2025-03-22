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

package org.apache.uniffle.common.segment;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataSegment;
import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.common.exception.RssException;

public abstract class AbstractSegmentSplitter implements SegmentSplitter {
  protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractSegmentSplitter.class);

  protected int readBufferSize;

  public AbstractSegmentSplitter(int readBufferSize) {
    this.readBufferSize = readBufferSize;
  }

  protected List<ShuffleDataSegment> splitCommon(
      ShuffleIndexResult shuffleIndexResult, Predicate<Long> taskFilter) {
    if (shuffleIndexResult == null || shuffleIndexResult.isEmpty()) {
      return Lists.newArrayList();
    }

    ByteBuffer indexData = shuffleIndexResult.getIndexData();
    long dataFileLen = shuffleIndexResult.getDataFileLen();
    int[] storageIds = shuffleIndexResult.getStorageIds();

    List<BufferSegment> bufferSegments = Lists.newArrayList();
    List<ShuffleDataSegment> dataFileSegments = Lists.newArrayList();
    int bufferOffset = 0;
    long fileOffset = -1;
    long totalLength = 0;

    int storageIndex = 0;
    long preOffset = -1;
    int preStorageId = -1;
    int currentStorageId = 0;

    while (indexData.hasRemaining()) {
      try {
        final long offset = indexData.getLong();
        final int length = indexData.getInt();
        final int uncompressLength = indexData.getInt();
        final long crc = indexData.getLong();
        final long blockId = indexData.getLong();
        final long taskAttemptId = indexData.getLong();

        if (storageIds.length == 0) {
          currentStorageId = -1;
        } else if (preOffset > offset) {
          storageIndex++;
          if (storageIndex >= storageIds.length) {
            LOGGER.warn("storageIds length {} is not enough.", storageIds.length);
          }
          currentStorageId = storageIds[storageIndex];
        } else {
          currentStorageId = storageIds[storageIndex];
        }
        preOffset = offset;

        totalLength += length;

        if (dataFileLen != -1 && totalLength > dataFileLen) {
          LOGGER.info(
              "Abort inconsistent data, the data length: {}(bytes) recorded in index file is greater than "
                  + "the real data file length: {}(bytes). Block id: {}"
                  + "This may happen when the data is flushing, please ignore.",
              totalLength,
              dataFileLen,
              blockId);
          break;
        }

        boolean storageChanged = preStorageId != -1 && currentStorageId != preStorageId;

        if (bufferOffset >= readBufferSize
            || storageChanged
            || (taskFilter != null && !taskFilter.test(taskAttemptId))) {
          if (bufferOffset > 0) {
            ShuffleDataSegment sds =
                new ShuffleDataSegment(fileOffset, bufferOffset, preStorageId, bufferSegments);
            dataFileSegments.add(sds);
            bufferSegments = Lists.newArrayList();
            bufferOffset = 0;
            fileOffset = -1;
          }
        }

        if (taskFilter == null || taskFilter.test(taskAttemptId)) {
          if (fileOffset == -1) {
            fileOffset = offset;
          }
          bufferSegments.add(
              new BufferSegment(
                  blockId, bufferOffset, length, uncompressLength, crc, taskAttemptId));
          preStorageId = currentStorageId;
          bufferOffset += length;
        }
      } catch (BufferUnderflowException ue) {
        throw new RssException("Read index data under flow", ue);
      }
    }

    if (bufferOffset > 0) {
      ShuffleDataSegment sds =
          new ShuffleDataSegment(fileOffset, bufferOffset, currentStorageId, bufferSegments);
      dataFileSegments.add(sds);
    }

    return dataFileSegments;
  }
}
