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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.storage.api.FileWriter;
import org.apache.uniffle.storage.common.FileBasedShuffleSegment;
import org.apache.uniffle.storage.handler.api.ShuffleWriteHandler;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;

public class LocalFileWriteHandler implements ShuffleWriteHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFileWriteHandler.class);

  private final RssBaseConf rssBaseConf;
  private String fileNamePrefix;
  private String basePath;
  private final int dataBufferSize;
  private final int indexBufferSize;

  public LocalFileWriteHandler(
      RssBaseConf rssBaseConf,
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      String storageBasePath,
      String fileNamePrefix) {
    this.rssBaseConf = rssBaseConf;
    this.fileNamePrefix = fileNamePrefix;
    this.basePath =
        ShuffleStorageUtils.getFullShuffleDataFolder(
            storageBasePath,
            ShuffleStorageUtils.getShuffleDataPath(appId, shuffleId, startPartition, endPartition));
    this.dataBufferSize =
        (int)
            this.rssBaseConf.getSizeAsBytes(
                RssBaseConf.RSS_STORAGE_LOCALFILE_WRITE_DATA_BUFFER_SIZE.key(),
                RssBaseConf.RSS_STORAGE_LOCALFILE_WRITE_DATA_BUFFER_SIZE.defaultValue());
    this.indexBufferSize =
        (int)
            this.rssBaseConf.getSizeAsBytes(
                RssBaseConf.RSS_STORAGE_LOCALFILE_WRITE_INDEX_BUFFER_SIZE.key(),
                RssBaseConf.RSS_STORAGE_LOCALFILE_WRITE_INDEX_BUFFER_SIZE.defaultValue());
    createBasePath();
  }

  @VisibleForTesting
  public LocalFileWriteHandler(
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      String storageBasePath,
      String fileNamePrefix) {
    this(
        new RssBaseConf(),
        appId,
        shuffleId,
        startPartition,
        endPartition,
        storageBasePath,
        fileNamePrefix);
  }

  private void createBasePath() {
    File baseFolder = new File(basePath);
    if (baseFolder.isDirectory()) {
      return;
    }
    try {
      Files.createDirectories(baseFolder.toPath());
    } catch (IOException e) {
      throw new RssException("Failed to create shuffle folder: " + basePath, e);
    }
  }

  // pick base path by hashcode
  private String pickBasePath(
      String[] storageBasePaths, String appId, int shuffleId, int startPartition) {
    if (storageBasePaths == null || storageBasePaths.length == 0) {
      throw new RssException(
          "Base path can't be empty, please check rss.storage.localFile.basePaths");
    }
    int index =
        ShuffleStorageUtils.getStorageIndex(
            storageBasePaths.length, appId, shuffleId, startPartition);
    return storageBasePaths[index];
  }

  @Override
  public synchronized void write(Collection<ShufflePartitionedBlock> shuffleBlocks)
      throws Exception {

    // Ignore this write, if the shuffle directory is deleted after being uploaded in multi mode
    // or after its app heartbeat times out.
    File baseFolder = new File(basePath);
    if (!baseFolder.exists()) {
      LOG.warn("{} don't exist, the app or shuffle may be deleted", baseFolder.getAbsolutePath());
      return;
    }

    long accessTime = System.currentTimeMillis();
    String dataFileName = ShuffleStorageUtils.generateDataFileName(fileNamePrefix);
    String indexFileName = ShuffleStorageUtils.generateIndexFileName(fileNamePrefix);

    try (FileWriter dataWriter =
            LocalFileWriterFactory.getLocalFileWriter(
                rssBaseConf, new File(basePath, dataFileName), dataBufferSize);
        FileWriter indexWriter = createWriter(indexFileName, indexBufferSize)) {

      long startTime = System.currentTimeMillis();
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
            "Write handler write {} blocks cost {} ms without file open close",
            shuffleBlocks.size(),
            (System.currentTimeMillis() - startTime));
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Write handler write {} blocks cost {} ms with file open close",
          shuffleBlocks.size(),
          (System.currentTimeMillis() - accessTime));
    }
  }

  private FileWriter createWriter(String fileName, int bufferSize)
      throws IOException, IllegalStateException {
    File file = new File(basePath, fileName);
    return new LocalFileWriter(file, bufferSize);
  }

  @VisibleForTesting
  protected String getBasePath() {
    return basePath;
  }
}
