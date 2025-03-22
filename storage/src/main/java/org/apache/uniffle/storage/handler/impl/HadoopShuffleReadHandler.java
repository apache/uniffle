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

package org.apache.uniffle.storage.handler.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShuffleDataSegment;
import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.storage.common.FileBasedShuffleSegment;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;

/**
 * HadoopShuffleFileReadHandler is a shuffle-specific file read handler, it contains two
 * HadoopFileReader instances created by using the index file and its indexed data file.
 */
public class HadoopShuffleReadHandler extends DataSkippableReadHandler {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopShuffleReadHandler.class);

  protected final String filePrefix;
  protected final HadoopFileReader indexReader;
  protected final HadoopFileReader dataReader;
  protected final boolean offHeapEnabled;

  public HadoopShuffleReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      String filePrefix,
      int readBufferSize,
      Roaring64NavigableMap expectBlockIds,
      Roaring64NavigableMap processBlockIds,
      Configuration conf,
      ShuffleDataDistributionType distributionType,
      Roaring64NavigableMap expectTaskIds,
      boolean offHeapEnabled,
      Optional<PrefetchOption> prefetchOption)
      throws Exception {
    super(
        appId,
        shuffleId,
        partitionId,
        readBufferSize,
        expectBlockIds,
        processBlockIds,
        distributionType,
        expectTaskIds,
        prefetchOption);
    this.filePrefix = filePrefix;
    this.indexReader =
        createHadoopReader(ShuffleStorageUtils.generateIndexFileName(filePrefix), conf);
    this.dataReader =
        createHadoopReader(ShuffleStorageUtils.generateDataFileName(filePrefix), conf);
    this.offHeapEnabled = offHeapEnabled;
  }

  // Only for test
  public HadoopShuffleReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      String filePrefix,
      int readBufferSize,
      Roaring64NavigableMap expectBlockIds,
      Roaring64NavigableMap processBlockIds,
      Configuration conf)
      throws Exception {
    this(
        appId,
        shuffleId,
        partitionId,
        filePrefix,
        readBufferSize,
        expectBlockIds,
        processBlockIds,
        conf,
        ShuffleDataDistributionType.NORMAL,
        Roaring64NavigableMap.bitmapOf(),
        false,
        Optional.empty());
  }

  @Override
  protected ShuffleIndexResult readShuffleIndex() {
    long start = System.currentTimeMillis();
    try {
      ByteBuffer indexData = null;
      if (offHeapEnabled) {
        indexData = indexReader.readAsByteBuffer();
      } else {
        indexData = ByteBuffer.wrap(indexReader.read());
      }
      int indexDataLength = indexData.limit() - indexData.position();
      int segmentNumber = indexDataLength / FileBasedShuffleSegment.SEGMENT_SIZE;
      int expectedLen = segmentNumber * FileBasedShuffleSegment.SEGMENT_SIZE;
      if (indexDataLength != expectedLen) {
        LOG.warn(
            "Maybe the index file: {} is being written due to the shuffle-buffer flushing.",
            filePrefix);
        indexData.limit(expectedLen);
      }
      long dateFileLen = getDataFileLen();
      LOG.info(
          "Read index files {}.index for {} ms", filePrefix, System.currentTimeMillis() - start);
      return new ShuffleIndexResult(indexData, dateFileLen);
    } catch (Exception e) {
      LOG.info("Fail to read index files {}.index", filePrefix, e);
    }
    return new ShuffleIndexResult();
  }

  protected ShuffleDataResult readShuffleData(ShuffleDataSegment shuffleDataSegment) {
    // Here we make an assumption that the rest of the file is corrupted, if an unexpected data is
    // read.
    int expectedLength = shuffleDataSegment.getLength();
    if (expectedLength <= 0) {
      LOG.warn("Invalid data segment is {} from file {}.data", shuffleDataSegment, filePrefix);
      return null;
    }

    ByteBuffer data;
    if (offHeapEnabled) {
      data = readShuffleDataByteBuffer(shuffleDataSegment.getOffset(), expectedLength);
    } else {
      data = ByteBuffer.wrap(readShuffleData(shuffleDataSegment.getOffset(), expectedLength));
    }
    int length = data.limit() - data.position();
    if (length == 0) {
      LOG.warn(
          "Fail to read expected[{}] data, actual[{}] and segment is {} from file {}.data",
          expectedLength,
          length,
          shuffleDataSegment,
          filePrefix);
      return null;
    }

    ShuffleDataResult shuffleDataResult =
        new ShuffleDataResult(data, shuffleDataSegment.getBufferSegments());
    if (shuffleDataResult.isEmpty()) {
      LOG.warn(
          "Shuffle data is empty, expected length {}, data length {}, segment {} in file {}.data",
          expectedLength,
          length,
          shuffleDataSegment,
          filePrefix);
      return null;
    }

    return shuffleDataResult;
  }

  protected byte[] readShuffleData(long offset, int expectedLength) {
    byte[] data = dataReader.read(offset, expectedLength);
    if (data.length != expectedLength) {
      LOG.warn(
          "Fail to read expected[{}] data, actual[{}] from file {}.data",
          expectedLength,
          data.length,
          filePrefix);
      return new byte[0];
    }
    return data;
  }

  private ByteBuffer readShuffleDataByteBuffer(long offset, int expectedLength) {
    ByteBuffer data = dataReader.readAsByteBuffer(offset, expectedLength);
    int length = data.limit() - data.position();
    if (length != expectedLength) {
      LOG.warn(
          "Fail to read byte buffer expected[{}] data, actual[{}] from file {}.data",
          expectedLength,
          length,
          filePrefix);
      return ByteBuffer.allocateDirect(0);
    }
    return data;
  }

  private long getDataFileLen() {
    try {
      return dataReader.getFileLen();
    } catch (IOException ioException) {
      LOG.error(
          "getDataFileLen failed for " + ShuffleStorageUtils.generateDataFileName(filePrefix),
          ioException);
      return -1;
    }
  }

  public synchronized void close() {
    try {
      dataReader.close();
    } catch (IOException ioe) {
      String message = "Error happened when close index filer reader for " + filePrefix + ".data";
      LOG.warn(message, ioe);
    }

    try {
      indexReader.close();
    } catch (IOException ioe) {
      String message = "Error happened when close data file reader for " + filePrefix + ".index";
      LOG.warn(message, ioe);
    }
  }

  protected HadoopFileReader createHadoopReader(String fileName, Configuration hadoopConf)
      throws Exception {
    Path path = new Path(fileName);
    return new HadoopFileReader(path, hadoopConf);
  }

  public List<ShuffleDataSegment> getShuffleDataSegments() {
    return shuffleDataSegments;
  }
}
