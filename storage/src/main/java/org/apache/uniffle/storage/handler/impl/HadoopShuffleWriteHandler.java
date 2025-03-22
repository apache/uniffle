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
import java.util.Collection;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.filesystem.HadoopFilesystemProvider;
import org.apache.uniffle.storage.common.FileBasedShuffleSegment;
import org.apache.uniffle.storage.handler.api.ShuffleWriteHandler;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;

public class HadoopShuffleWriteHandler implements ShuffleWriteHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopShuffleWriteHandler.class);

  private RssBaseConf rssBaseConf;
  private Configuration hadoopConf;
  private String basePath;
  private String fileNamePrefix;
  private Lock writeLock = new ReentrantLock();
  private int failTimes = 0;
  private String user;
  private FileSystem fileSystem;
  private final int dataBufferSize;
  private final int indexBufferSize;

  // Only for test cases when using non-kerberized dfs cluster.
  @VisibleForTesting
  public HadoopShuffleWriteHandler(
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      String storageBasePath,
      String fileNamePrefix,
      Configuration hadoopConf)
      throws Exception {
    this(
        new RssBaseConf(),
        appId,
        shuffleId,
        startPartition,
        endPartition,
        storageBasePath,
        fileNamePrefix,
        hadoopConf,
        "");
  }

  @VisibleForTesting
  public HadoopShuffleWriteHandler(
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      String storageBasePath,
      String fileNamePrefix,
      Configuration hadoopConf,
      String user)
      throws Exception {
    this(
        new RssBaseConf(),
        appId,
        shuffleId,
        startPartition,
        endPartition,
        storageBasePath,
        fileNamePrefix,
        hadoopConf,
        user);
  }

  public HadoopShuffleWriteHandler(
      RssBaseConf rssBaseConf,
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      String storageBasePath,
      String fileNamePrefix,
      Configuration hadoopConf,
      String user)
      throws Exception {
    this.rssBaseConf = rssBaseConf;
    this.hadoopConf = hadoopConf;
    this.fileNamePrefix = fileNamePrefix;
    this.basePath =
        ShuffleStorageUtils.getFullShuffleDataFolder(
            storageBasePath,
            ShuffleStorageUtils.getShuffleDataPath(appId, shuffleId, startPartition, endPartition));
    this.user = user;
    this.dataBufferSize =
        (int)
            this.rssBaseConf.getSizeAsBytes(
                RssBaseConf.RSS_STORAGE_HDFS_WRITE_DATA_BUFFER_SIZE.key(),
                RssBaseConf.RSS_STORAGE_HDFS_WRITE_DATA_BUFFER_SIZE.defaultValue());
    this.indexBufferSize =
        (int)
            this.rssBaseConf.getSizeAsBytes(
                RssBaseConf.RSS_STORAGE_HDFS_WRITE_INDEX_BUFFER_SIZE.key(),
                RssBaseConf.RSS_STORAGE_HDFS_WRITE_INDEX_BUFFER_SIZE.defaultValue());
    initialize();
  }

  private void initialize() throws Exception {
    Path path = new Path(basePath);
    LOG.info("User: {}, Path: {}", user, path);
    this.fileSystem = HadoopFilesystemProvider.getFilesystem(user, path, hadoopConf);
    // check if shuffle folder exist
    if (!fileSystem.exists(path)) {
      try {
        // try to create folder, it may be created by other Shuffle Server
        fileSystem.mkdirs(path);
      } catch (IOException ioe) {
        // if folder exist, ignore the exception
        if (!fileSystem.exists(path)) {
          LOG.error("Can't create shuffle folder:" + basePath, ioe);
          throw ioe;
        }
      }
    }
  }

  @Override
  public void write(Collection<ShufflePartitionedBlock> shuffleBlocks) throws Exception {
    final long start = System.currentTimeMillis();
    writeLock.lock();
    try {
      final long ss = System.currentTimeMillis();
      // Write to HDFS will be failed with lease problem, and can't write the same file again
      // change the prefix of file name if write failed before
      String dataFileName =
          ShuffleStorageUtils.generateDataFileName(fileNamePrefix + "_" + failTimes);
      String indexFileName =
          ShuffleStorageUtils.generateIndexFileName(fileNamePrefix + "_" + failTimes);
      try (HadoopFileWriter dataWriter = createWriter(dataFileName, dataBufferSize);
          HadoopFileWriter indexWriter = createWriter(indexFileName, indexBufferSize)) {
        for (ShufflePartitionedBlock block : shuffleBlocks) {
          long blockId = block.getBlockId();
          long crc = block.getCrc();
          long startOffset = dataWriter.nextOffset();
          dataWriter.writeData(block.getData());

          FileBasedShuffleSegment segment =
              new FileBasedShuffleSegment(
                  blockId,
                  startOffset,
                  block.getDataLength(),
                  block.getUncompressLength(),
                  crc,
                  block.getTaskAttemptId());
          indexWriter.writeIndex(segment);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Write handler inside cost {} ms for {}",
              (System.currentTimeMillis() - ss),
              fileNamePrefix);
        }
      } catch (IOException e) {
        LOG.warn(
            "Write failed with "
                + shuffleBlocks.size()
                + " blocks for "
                + fileNamePrefix
                + "_"
                + failTimes,
            e);
        failTimes++;
        throw new RssException(e);
      }
    } finally {
      writeLock.unlock();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Write handler outside write {} blocks cost {} ms for {}",
          shuffleBlocks.size(),
          (System.currentTimeMillis() - start),
          fileNamePrefix);
    }
  }

  @VisibleForTesting
  public HadoopFileWriter createWriter(String fileName) throws IOException, IllegalStateException {
    Path path = new Path(basePath, fileName);
    HadoopFileWriter writer = new HadoopFileWriter(fileSystem, path, hadoopConf);
    return writer;
  }

  public HadoopFileWriter createWriter(String fileName, int bufferSize)
      throws IOException, IllegalStateException {
    Path path = new Path(basePath, fileName);
    HadoopFileWriter writer = new HadoopFileWriter(fileSystem, path, hadoopConf, bufferSize);
    return writer;
  }

  @VisibleForTesting
  public void setFailTimes(int failTimes) {
    this.failTimes = failTimes;
  }
}
