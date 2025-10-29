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

package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FileChunk;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.combine.Combiner;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.filesystem.HadoopFilesystemProvider;

import static org.apache.tez.common.RssTezConfig.RSS_REMOTE_SPILL_STORAGE_PATH;

public class RssMergeManager extends MergeManager {

  private static final Logger LOG = LoggerFactory.getLogger(RssMergeManager.class);

  private Configuration conf;
  private InputContext inputContext;
  private ExceptionReporter exceptionReporter;
  private RssInMemoryMerger inMemoryMerger;
  private Combiner combiner;
  private CompressionCodec codec;
  private boolean ifileReadAhead;
  private int ifileReadAheadLength;
  private int ifileBufferSize;
  private String appAttemptId;

  private final long initialMemoryAvailable;
  private final long memoryLimit;
  private final long maxSingleShuffleLimit;
  private long mergeThreshold;
  private long commitMemory;
  private long usedMemory;

  private String spillBasePath;
  private FileSystem remoteFS;

  // Variables for stats and logging
  private long lastInMemSegmentLogTime = -1L;
  private final SegmentStatsTracker statsInMemTotal = new SegmentStatsTracker();
  private final SegmentStatsTracker statsInMemLastLog = new SegmentStatsTracker();

  private final TezCounter spilledRecordsCounter;
  private final TezCounter mergedMapOutputsCounter;
  private final TezCounter additionalBytesRead;

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);
  private final boolean cleanup;

  private final Progressable progressable =
      new Progressable() {
        @Override
        public void progress() {
          inputContext.notifyProgress();
        }
      };

  public RssMergeManager(
      Configuration conf,
      FileSystem localFS,
      InputContext inputContext,
      Combiner combiner,
      TezCounter spilledRecordsCounter,
      TezCounter reduceCombineInputCounter,
      TezCounter mergedMapOutputsCounter,
      ExceptionReporter exceptionReporter,
      long initialMemoryAvailable,
      CompressionCodec codec,
      boolean ifileReadAheadEnabled,
      int ifileReadAheadLength,
      Configuration remoteConf,
      int replication,
      int retries,
      String appAttemptId) {
    super(
        conf,
        localFS,
        null,
        inputContext,
        combiner,
        spilledRecordsCounter,
        reduceCombineInputCounter,
        mergedMapOutputsCounter,
        exceptionReporter,
        initialMemoryAvailable,
        codec,
        ifileReadAheadEnabled,
        ifileReadAheadLength);
    this.conf = conf;
    this.inputContext = inputContext;
    this.exceptionReporter = exceptionReporter;
    this.codec = codec;
    this.combiner = combiner;
    this.initialMemoryAvailable = initialMemoryAvailable;
    this.ifileReadAhead = ifileReadAheadEnabled;
    if (this.ifileReadAhead) {
      this.ifileReadAheadLength = ifileReadAheadLength;
    } else {
      this.ifileReadAheadLength = 0;
    }
    this.ifileBufferSize =
        conf.getInt(
            "io.file.buffer.size", TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_BUFFER_SIZE_DEFAULT);
    this.appAttemptId = appAttemptId;
    this.cleanup =
        conf.getBoolean(
            TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT,
            TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT_DEFAULT);

    // Set memory, here ignore some check which have done in MergeManager
    final float maxInMemCopyUse =
        conf.getFloat(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT_DEFAULT);
    long memLimit =
        conf.getLong(
            Constants.TEZ_RUNTIME_TASK_MEMORY,
            (long) (inputContext.getTotalMemoryAvailableToTask() * maxInMemCopyUse));
    if (this.initialMemoryAvailable < memLimit) {
      this.memoryLimit = this.initialMemoryAvailable;
    } else {
      this.memoryLimit = memLimit;
    }
    this.mergeThreshold =
        (long)
            (this.memoryLimit
                * conf.getFloat(
                    TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT,
                    TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT_DEFAULT));
    final float singleShuffleMemoryLimitPercent =
        conf.getFloat(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT_DEFAULT);
    this.maxSingleShuffleLimit =
        (long) Math.min((memoryLimit * singleShuffleMemoryLimitPercent), Integer.MAX_VALUE);

    // counter
    this.spilledRecordsCounter = spilledRecordsCounter;
    this.mergedMapOutputsCounter = mergedMapOutputsCounter;
    this.additionalBytesRead =
        inputContext.getCounters().findCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_READ);

    // remote fs
    Configuration remoteConfCopied = new Configuration(remoteConf);
    this.spillBasePath = conf.get(RSS_REMOTE_SPILL_STORAGE_PATH);
    try {
      remoteConfCopied.setInt("dfs.replication", replication);
      remoteConfCopied.setInt("dfs.client.block.write.retries", retries);
      this.remoteFS =
          HadoopFilesystemProvider.getFilesystem(new Path(spillBasePath), remoteConfCopied);
    } catch (Exception e) {
      throw new RssException("Cannot init remoteFS on path:" + spillBasePath);
    }
    if (StringUtils.isBlank(this.spillBasePath)) {
      throw new RssException("You must set remote spill path!");
    }
    this.inMemoryMerger = createRssInMemoryMerger();
  }

  private RssInMemoryMerger createRssInMemoryMerger() {
    return new RssInMemoryMerger(
        this,
        this.conf,
        inputContext,
        combiner,
        exceptionReporter,
        codec,
        remoteFS,
        spillBasePath,
        appAttemptId);
  }

  @Override
  void configureAndStart() {
    this.inMemoryMerger.start();
  }

  @Override
  public void waitForInMemoryMerge() throws InterruptedException {
    inMemoryMerger.waitForMerge();

    /**
     * Memory released during merge process could have been used by active fetchers and if they
     * merge was already in progress, this would not have kicked off another merge and fetchers
     * could get into indefinite wait state later. To address this, trigger another merge process if
     * needed and wait for it to complete (to release committedMemory & usedMemory).
     */
    boolean triggerAdditionalMerge = false;
    synchronized (this) {
      if (commitMemory >= mergeThreshold) {
        startMemToRemoteMerge();
        triggerAdditionalMerge = true;
      }
    }
    if (triggerAdditionalMerge) {
      inMemoryMerger.waitForMerge();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Additional in-memory merge triggered");
      }
    }
  }

  private boolean canShuffleToMemory(long requestedSize) {
    // TODO: large shuffle data should be store in remote fs directly
    return true;
  }

  @Override
  public synchronized void waitForShuffleToMergeMemory() throws InterruptedException {
    long startTime = System.currentTimeMillis();
    while (usedMemory > memoryLimit) {
      wait();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Waited for "
              + (System.currentTimeMillis() - startTime)
              + " for memory to become"
              + " available");
    }
  }

  private final MapOutput stallShuffle = MapOutput.createWaitMapOutput(null);

  @Override
  public synchronized MapOutput reserve(
      InputAttemptIdentifier srcAttemptIdentifier,
      long requestedSize,
      long compressedLength,
      int fetcher)
      throws IOException {
    if (!canShuffleToMemory(requestedSize)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            srcAttemptIdentifier
                + ": Shuffling to remote fs since "
                + requestedSize
                + " is greater than maxSingleShuffleLimit ("
                + maxSingleShuffleLimit
                + ")");
      }
      throw new RssException("Shuffle large date is not implemented!");
    }

    // Stall shuffle if we are above the memory limit

    // It is possible that all threads could just be stalling and not make
    // progress at all. This could happen when:
    //
    // requested size is causing the used memory to go above limit &&
    // requested size < singleShuffleLimit &&
    // current used size < mergeThreshold (merge will not get triggered)
    //
    // To avoid this from happening, we allow exactly one thread to go past
    // the memory limit. We check (usedMemory > memoryLimit) and not
    // (usedMemory + requestedSize > memoryLimit). When this thread is done
    // fetching, this will automatically trigger a merge thereby unlocking
    // all the stalled threads

    if (usedMemory > memoryLimit) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            srcAttemptIdentifier
                + ": Stalling shuffle since usedMemory ("
                + usedMemory
                + ") is greater than memoryLimit ("
                + memoryLimit
                + ")."
                + " CommitMemory is ("
                + commitMemory
                + ")");
      }
      return stallShuffle;
    }

    // Allow the in-memory shuffle to progress
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          srcAttemptIdentifier
              + ": Proceeding with shuffle since usedMemory ("
              + usedMemory
              + ") is lesser than memoryLimit ("
              + memoryLimit
              + ")."
              + "CommitMemory is ("
              + commitMemory
              + ")");
    }
    return unconditionalReserve(srcAttemptIdentifier, requestedSize, true);
  }

  public synchronized MapOutput reserve(
      InputAttemptIdentifier srcAttemptIdentifier, byte[] data, boolean primaryMapOutput)
      throws IOException {
    if (usedMemory > memoryLimit) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            srcAttemptIdentifier
                + ": Stalling shuffle since usedMemory ("
                + usedMemory
                + ") is greater than memoryLimit ("
                + memoryLimit
                + ")."
                + " CommitMemory is ("
                + commitMemory
                + ")");
      }
      return stallShuffle;
    }
    return unconditionalReserve(srcAttemptIdentifier, data, primaryMapOutput);
  }

  private synchronized MapOutput unconditionalReserve(
      InputAttemptIdentifier srcAttemptIdentifier, long requestedSize, boolean primaryMapOutput)
      throws IOException {
    usedMemory += requestedSize;
    return MapOutput.createMemoryMapOutput(
        srcAttemptIdentifier, this, (int) requestedSize, primaryMapOutput);
  }

  private synchronized MapOutput unconditionalReserve(
      InputAttemptIdentifier srcAttemptIdentifier, byte[] data, boolean primaryMapOutput)
      throws IOException {
    usedMemory += data.length;
    return MapOutput.createMemoryMapOutput(srcAttemptIdentifier, this, data, primaryMapOutput);
  }

  @Override
  public synchronized void unreserve(long size) {
    usedMemory -= size;
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Notifying unreserve : size="
              + size
              + ", commitMemory="
              + commitMemory
              + ", usedMemory="
              + usedMemory
              + ", mergeThreshold="
              + mergeThreshold);
    }
    notifyAll();
  }

  @Override
  public synchronized void releaseCommittedMemory(long size) {
    commitMemory -= size;
    unreserve(size);
  }

  @Override
  public synchronized void closeInMemoryFile(MapOutput mapOutput) {
    inMemoryMapOutputs.add(mapOutput);
    trackAndLogCloseInMemoryFile(mapOutput);
    commitMemory += mapOutput.getSize();
    if (commitMemory >= mergeThreshold) {
      startMemToRemoteMerge();
    }
  }

  private void trackAndLogCloseInMemoryFile(MapOutput mapOutput) {
    statsInMemTotal.updateStats(mapOutput.getSize());

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "closeInMemoryFile -> map-output of size: "
              + mapOutput.getSize()
              + ", inMemoryMapOutputs.size() -> "
              + inMemoryMapOutputs.size()
              + ", commitMemory -> "
              + commitMemory
              + ", usedMemory ->"
              + usedMemory
              + ", mapOutput="
              + mapOutput);
    } else {
      statsInMemLastLog.updateStats(mapOutput.getSize());
      long now = Time.monotonicNow();
      if (now > lastInMemSegmentLogTime + 30 * 1000L) {
        LOG.info(
            "CloseInMemoryFile. Current state: inMemoryMapOutputs.size={},"
                + " commitMemory={},"
                + " usedMemory={}. Since last log:"
                + " count={},"
                + " min={},"
                + " max={},"
                + " total={},"
                + " avg={}",
            inMemoryMapOutputs.size(),
            commitMemory,
            usedMemory,
            statsInMemLastLog.count,
            statsInMemLastLog.minSize,
            statsInMemLastLog.maxSize,
            statsInMemLastLog.size,
            (statsInMemLastLog.count == 0
                ? "nan"
                : (statsInMemLastLog.size / (double) statsInMemLastLog.count)));
        statsInMemLastLog.reset();
        lastInMemSegmentLogTime = now;
      }
    }
  }

  private void startMemToRemoteMerge() {
    synchronized (inMemoryMerger) {
      if (!inMemoryMerger.isInProgress()) {
        LOG.info(
            inputContext.getSourceVertexName()
                + ": "
                + "Starting inMemoryMerger's merge since commitMemory="
                + commitMemory
                + " > mergeThreshold="
                + mergeThreshold
                + ". Current usedMemory="
                + usedMemory);
        inMemoryMerger.startMerge(inMemoryMapOutputs);
      }
    }
  }

  @Override
  public synchronized void closeOnDiskFile(FileChunk file) {
    onDiskMapOutputs.add(file);
    logCloseOnDiskFile(file);
  }

  @Override
  public TezRawKeyValueIterator close(boolean tryFinalMerge) throws Throwable {
    if (!isShutdown.getAndSet(true)) {
      // Wait for on-going merges to complete
      inMemoryMerger.startMerge(inMemoryMapOutputs);
      inMemoryMerger.close();

      // Wait for on-going merges to complete
      if (!inMemoryMapOutputs.isEmpty()) {
        throw new RssException("InMemoryMapOutputs should be empty");
      }

      if (statsInMemTotal.count > 0) {
        LOG.info(
            "TotalInMemFetchStats: count={}, totalSize={}, min={}, max={}, avg={}",
            statsInMemTotal.count,
            statsInMemTotal.size,
            statsInMemTotal.minSize,
            statsInMemTotal.maxSize,
            (statsInMemTotal.size / (float) statsInMemTotal.size));
      }

      // Don't attempt a final merge if close is invoked as a result of a previous
      // shuffle exception / error.
      if (tryFinalMerge) {
        try {
          TezRawKeyValueIterator kvIter = finalMerge();
          return kvIter;
        } catch (InterruptedException e) {
          // Cleanup the disk segments
          if (cleanup) {
            cleanup(remoteFS, onDiskMapOutputs);
          }
          Thread.currentThread().interrupt(); // reset interrupt status
          throw e;
        }
      }
    }
    return null;
  }

  long createInMemorySegments(
      List<MapOutput> inMemoryMapOutputs, List<TezMerger.Segment> inMemorySegments, long leaveBytes)
      throws IOException {
    long totalSize = 0L;
    // We could use fullSize could come from the RamManager, but files can be
    // closed but not yet present in inMemoryMapOutputs
    long fullSize = 0L;
    for (MapOutput mo : inMemoryMapOutputs) {
      fullSize += mo.getSize();
    }
    int inMemoryMapOutputsOffset = 0;
    while ((fullSize > leaveBytes) && !Thread.currentThread().isInterrupted()) {
      MapOutput mo = inMemoryMapOutputs.get(inMemoryMapOutputsOffset++);
      byte[] data = mo.getMemory();
      long size = data.length;
      totalSize += size;
      fullSize -= size;
      IFile.Reader reader =
          new InMemoryReader(RssMergeManager.this, mo.getAttemptIdentifier(), data, 0, (int) size);
      inMemorySegments.add(
          new TezMerger.Segment(
              reader, (mo.isPrimaryMapOutput() ? mergedMapOutputsCounter : null)));
    }
    // Bulk remove removed in-memory map outputs efficiently
    inMemoryMapOutputs.subList(0, inMemoryMapOutputsOffset).clear();
    return totalSize;
  }

  private long lastOnDiskSegmentLogTime = -1L;

  private void logCloseOnDiskFile(FileChunk file) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "close onDiskFile="
              + file.getPath()
              + ", len="
              + file.getLength()
              + ", onDisMapOutputs="
              + onDiskMapOutputs.size());
    } else {
      long now = Time.monotonicNow();
      if (now > lastOnDiskSegmentLogTime + 30 * 1000L) {
        LOG.info(
            "close onDiskFile. State: NumOnDiskFiles={}. Current: path={}, len={}",
            onDiskMapOutputs.size(),
            file.getPath(),
            file.getLength());
        lastOnDiskSegmentLogTime = now;
      }
    }
  }

  /*
   * Since merge remote files with memory files or other remote files is a
   * time-consuming operation, the implementation of FinalMerge is simplified,
   * sort io factor will be ignored. We do not merge any files that have been
   * archived to the remote file system, but give all remote files to the iterator
   * for direct reading.
   * */
  private TezRawKeyValueIterator finalMerge() throws IOException, InterruptedException {
    Class keyClass = ConfigUtils.getIntermediateInputKeyClass(conf);
    Class valueClass = ConfigUtils.getIntermediateInputValueClass(conf);
    Path[] remotePaths =
        onDiskMapOutputs.stream()
            .map(output -> output.getPath())
            .toArray(num -> new Path[onDiskMapOutputs.size()]);
    final RawComparator comparator = ConfigUtils.getIntermediateInputKeyComparator(conf);
    return TezMerger.merge(
        conf,
        remoteFS,
        keyClass,
        valueClass,
        codec,
        ifileReadAhead,
        ifileReadAheadLength,
        ifileBufferSize,
        remotePaths,
        true,
        Integer.MAX_VALUE,
        null,
        comparator,
        progressable,
        spilledRecordsCounter,
        null,
        additionalBytesRead,
        null);
  }

  public boolean isCleanup() {
    return cleanup;
  }

  public Progressable getProgressable() {
    return progressable;
  }

  private static class SegmentStatsTracker {
    private long size;
    private int count;
    private long minSize;
    private long maxSize;

    SegmentStatsTracker() {
      reset();
    }

    void updateStats(long segSize) {
      size += segSize;
      count++;
      minSize = (segSize < minSize ? segSize : minSize);
      maxSize = (segSize > maxSize ? segSize : maxSize);
    }

    void reset() {
      size = 0L;
      count = 0;
      minSize = Long.MAX_VALUE;
      maxSize = Long.MIN_VALUE;
    }
  }

  @VisibleForTesting
  public RssInMemoryMerger getInMemoryMerger() {
    return inMemoryMerger;
  }
}
