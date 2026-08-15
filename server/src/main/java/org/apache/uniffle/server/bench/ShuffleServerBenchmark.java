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

package org.apache.uniffle.server.bench;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import picocli.CommandLine;

import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.ChecksumUtils;
import org.apache.uniffle.common.util.NettyUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.common.util.UnitConverter;
import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleFlushManager;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.server.buffer.ShuffleBuffer;
import org.apache.uniffle.server.buffer.ShuffleBufferType;
import org.apache.uniffle.server.buffer.ShuffleBufferWithLinkedList;
import org.apache.uniffle.server.buffer.ShuffleBufferWithSkipList;
import org.apache.uniffle.server.web.vo.BenchArgumentVO;

public class ShuffleServerBenchmark implements AutoCloseable {
  private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(ShuffleServerBenchmark.class);
  private static final AtomicLong JOB_ID_INDEX = new AtomicLong(0);

  // partitionId -> ShuffleBuffer
  private Map<Integer, ShuffleBuffer> shuffleBuffers = new ConcurrentHashMap<>();
  //  private static final Map<Integer, Pair<byte[], Long>> datas = new HashMap<>();
  private ByteBuf blockData = null;
  private long blockDataCrc;

  private static Map<String, BenchHandle> benchHandles = new TreeMap<>();

  public ShuffleServerBenchmark() {}

  public static Map<String, BenchHandle> getBenchHandle(Set<String> ids) {
    Objects.requireNonNull(ids);
    return benchHandles.entrySet().stream()
        .filter(e -> ids.isEmpty() || ids.contains(e.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue()));
  }

  public static boolean removeBench(String id) throws Exception {
    BenchHandle benchHandle = benchHandles.get(id);
    if (benchHandle != null) {
      benchHandle.close();
      benchHandles.remove(id);
      return true;
    } else {
      return false;
    }
  }

  public static boolean stopBench(String id) throws Exception {
    BenchHandle benchHandle = benchHandles.get(id);
    if (benchHandle != null) {
      benchHandle.close();
      return true;
    } else {
      return false;
    }
  }

  /**
   * start shuffle server benchmark
   *
   * @param arguments the benchmark arguments
   * @param shuffleServerConf the configuration of shuffle server
   * @param shuffleServer the shuffle server
   * @param async if async, the thread will return immediately, otherwise block util finish or
   *     timeout
   * @param timeoutMs if async, this is timeout in milliseconds, otherwise not used
   * @return
   * @throws Exception
   */
  public static String runBench(
      BenchArgumentVO arguments,
      ShuffleServerConf shuffleServerConf,
      ShuffleServer shuffleServer,
      boolean async,
      long timeoutMs)
      throws Exception {
    final String appId = "ShuffleServerBenchmarkAppId" + JOB_ID_INDEX.getAndIncrement();
    BenchHandle benchHandle = new BenchHandle(appId, arguments);
    benchHandles.put(appId, benchHandle);
    startAppHeartbeat(shuffleServer, appId, benchHandle);

    FutureTask<Void> futureTask =
        new FutureTask<>(
            () -> {
              runBenchInternal(shuffleServerConf, shuffleServer, benchHandle);
              return null;
            });
    benchHandle.registerClosable("runBenchTask", () -> futureTask.cancel(true));

    Thread thread = new Thread(futureTask);
    thread.setDaemon(true);
    thread.setName("ShuffleServerBenchmarkThread-" + appId);
    thread.start();
    if (async) {
      return appId;
    }
    try {
      futureTask.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      LOG.warn("run bench timeout {} over {}s", benchHandle, timeoutMs / 1000);
      removeBench(appId);
    } catch (Exception e) {
      LOG.error("Failed to run server bench {} in futureTask, will cancel it.", e);
      removeBench(appId);
    }
    return appId;
  }

  private static void startAppHeartbeat(
      ShuffleServer shuffleServer, String appId, BenchHandle benchHandle) {
    String factoryName = "rss-heartbeat-" + appId;
    ScheduledExecutorService heartbeatScheduledExecutorService =
        ThreadUtils.getDaemonSingleThreadScheduledExecutor(factoryName);
    heartbeatScheduledExecutorService.scheduleAtFixedRate(
        () -> {
          try {
            shuffleServer.getShuffleTaskManager().refreshAppId(appId);
            LOG.info("Finish send heartbeat to coordinator and servers");
          } catch (Exception e) {
            LOG.warn("Fail to send heartbeat to coordinator and servers", e);
          }
        },
        5000 / 2,
        5000,
        TimeUnit.MILLISECONDS);
    benchHandle.registerClosable(
        "heartbeat threadPool: " + factoryName, heartbeatScheduledExecutorService::shutdown);
  }

  private static void runBenchInternal(
      ShuffleServerConf shuffleServerConf, ShuffleServer shuffleServer, BenchHandle benchHandle)
      throws Exception {
    String appId = benchHandle.getId();
    BenchArgumentVO arguments = benchHandle.getArgument();
    final int threadNumGenerateEvent = arguments.getThreadNumGenerateEvent();
    int blockNumThresholdInMemory = arguments.getBlockNumThresholdInMemory();

    // Start a thread pool for producer to generate flush event as fast as possible
    ExecutorService executorService =
        ThreadUtils.getDaemonFixedThreadPool(threadNumGenerateEvent, "Bench-ThreadPool-" + appId);
    benchHandle.registerClosable(
        "Bench-ThreadPool-" + appId,
        () -> {
          executorService.shutdown();
          Thread.sleep(5000);
          executorService.shutdownNow();
          while (!executorService.isShutdown()) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              LOG.warn("Shutting Bench-ThreadPool-" + appId + " was interrupted.");
            }
          }
        });

    final AtomicLong generatedEventNums = new AtomicLong(0);
    final AtomicLong completedEventNums = new AtomicLong(0);
    benchHandle.setCompletedEventNumsCounter(completedEventNums);
    // Create a future as a barrier to wait for the bench completed
    CompletableFuture<Void> future = new CompletableFuture<>();
    // register callback to get the exact test running duration
    ShuffleServerMetrics.callbackBenchmark =
        () -> {
          if (completedEventNums.incrementAndGet() == benchHandle.getTotalEventNum()) {
            future.complete(null);
          }
          return null;
        };
    if (arguments.getDumpfile() == null) {
      int blockSize = (int) UnitConverter.byteStringAsBytes(arguments.getBlockSize());
      int blockNumPerEvent = arguments.getBlockNumPerEvent();
      int eventNum = arguments.getEventNum();
      int partitionNum = arguments.getPartitionNum();
      final int inQueueEventLimit =
          arguments.getInQueueEventLimit() > 0
              ? arguments.getInQueueEventLimit()
              : blockNumThresholdInMemory / blockNumPerEvent;

      System.out.printf(
          "%s Starting write, blockSize: %s, block num: %d, event num: %d, partition num: %d, config file: %s%n",
          LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
          UnitConverter.formatSize(blockSize),
          blockNumPerEvent,
          eventNum,
          partitionNum,
          arguments.getConfigFile());

      ShuffleServerBenchmark shuffleServerBenchmark =
          benchHandle.registerClosable("shuffleServerBenchmark", new ShuffleServerBenchmark());

      final int shuffleId = 0;
      final int taskAttemptId = 0;
      List<PartitionRange> partitionRanges =
          IntStream.range(0, partitionNum)
              .mapToObj(i -> new PartitionRange(i, i))
              .collect(Collectors.toList());

      // init
      for (int i = 0; i < partitionNum; i++) {
        shuffleServerBenchmark.shuffleBuffers.put(
            i,
            shuffleServerConf.get(ShuffleServerConf.SERVER_SHUFFLE_BUFFER_TYPE)
                    == ShuffleBufferType.SKIP_LIST
                ? new ShuffleBufferWithSkipList()
                : new ShuffleBufferWithLinkedList());
      }

      // Register shuffle to avoid encounter exception
      shuffleServer
          .getShuffleTaskManager()
          .registerShuffle(
              appId,
              shuffleId,
              0,
              partitionRanges,
              new RemoteStorageInfo(""),
              "ShuffleServerBenchmarkUser",
              ShuffleDataDistributionType.NORMAL,
              0,
              Collections.emptyMap());
      benchHandle.registerClosable(
          "registerShuffle",
          () -> shuffleServer.getShuffleTaskManager().removeShuffleDataAsync(appId));
      shuffleServerBenchmark.mockFixedBlockData(blockSize);
      benchHandle.setTotalEventNum(partitionNum * eventNum);
      String msg =
          String.format(
              "%s Start to generate %d(from args) events, inQueueEventLimit is %d%n",
              LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
              benchHandle.getTotalEventNum(),
              inQueueEventLimit);
      System.out.printf(msg);
      benchHandle.addLog(msg);
      // generate common event
      final ShuffleDataFlushEvent shuffleDataFlushEvent =
          shuffleServerBenchmark.generateEvent(appId, shuffleId, 0, 0, blockSize, blockNumPerEvent);
      // Start trace the bench metrics
      benchHandle.startTrace();
      benchHandle.registerClosable("trace", () -> benchHandle.endTrace());
      IntStream.range(0, eventNum)
          .forEach(
              eventId -> {
                IntStream.range(0, partitionNum)
                    .forEach(
                        partitionId -> {
                          executorService.submit(
                              () -> {
                                try {
                                  ShuffleDataFlushEvent event =
                                      shuffleServerBenchmark.dumpShuffleDataFlushEvent(
                                          shuffleDataFlushEvent,
                                          shuffleId,
                                          partitionId,
                                          taskAttemptId,
                                          blockNumPerEvent,
                                          blockSize);
                                  shuffleServer.getShuffleFlushManager().addToFlushQueue(event);
                                  progressControl(
                                      benchHandle, generatedEventNums, inQueueEventLimit);
                                } catch (Throwable e) {
                                  e.printStackTrace();
                                }
                              });
                        });
              });
    } else {
      // TODO(baoloongmao): Add support for dump file
    }
    benchHandle.addLog(
        "generate event done, waiting flush closed." + " total:" + benchHandle.getTotalEventNum());
    // Blocking until finish bench
    future.get();
    // Close bench handle, release resource, end trace
    benchHandle.close();
    generateBenchResult(shuffleServerConf, benchHandle);
  }

  private void mockFixedBlockData(int blockSize) {
    byte[] data = new byte[blockSize];
    ThreadLocalRandom.current().nextBytes(data);
    blockDataCrc = ChecksumUtils.getCrc32(data);
    blockData = NettyUtils.getSharedUnpooledByteBufAllocator(true).directBuffer(blockSize);
    blockData.writeBytes(data);
  }

  private static void progressControl(
      BenchHandle benchHandle, AtomicLong generatedEventNums, long inQueueEventLimit)
      throws InterruptedException {
    long progress = generatedEventNums.incrementAndGet();
    benchHandle.setGenerateEventNums(progress);
    long totalEventNums = benchHandle.getTotalEventNum();
    if (progress % (totalEventNums / 5) == 0) {
      int progressNum = (int) (progress * 50 / totalEventNums);
      StringBuilder sb = new StringBuilder(50);
      sb.append("\rGenerate event [");
      String equals = StringUtils.repeat("=", progressNum);
      String spaces = StringUtils.repeat(" ", 50 - progressNum);
      sb.append(equals)
          .append(spaces)
          .append("] ")
          .append(progress)
          .append("/")
          .append(totalEventNums);
      System.out.print(sb);
    }
    int sleepMsExceedBlockNumThreshold =
        benchHandle.getArgument().getSleepMsExceedBlockNumThreshold();
    while (generatedEventNums.get() - benchHandle.getCompletedEventNum() > inQueueEventLimit
        && sleepMsExceedBlockNumThreshold > 0) {
      Thread.sleep(sleepMsExceedBlockNumThreshold);
    }
  }

  private static void generateBenchResult(
      ShuffleServerConf shuffleServerConf, BenchHandle benchHandle) {
    BenchArgumentVO arguments = benchHandle.getArgument();
    int blockSize = (int) UnitConverter.byteStringAsBytes(arguments.getBlockSize());
    int blockNumPerEvent = arguments.getBlockNumPerEvent();
    int eventNum = arguments.getEventNum();
    int partitionNum = arguments.getPartitionNum();
    String msg;
    double flushWriteTotal = benchHandle.getBenchTrace().getTotalWriteTime();
    double flushWriteMsPerEvent = flushWriteTotal / benchHandle.getTotalEventNum();
    int flushThreadPoolSize =
        shuffleServerConf.getInteger(ShuffleServerConf.SERVER_FLUSH_LOCALFILE_THREAD_POOL_SIZE);

    System.out.println();
    String argStr =
        String.format(
            "%s ShuffleServerBenchmark arguments blockSize: %s, block num: %d, event num: %d, partition num: %d, config file: %s, flushThreadPoolSize=%d%n",
            LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
            UnitConverter.formatSize(blockSize),
            blockNumPerEvent,
            eventNum,
            partitionNum,
            arguments.getConfigFile(),
            flushThreadPoolSize);

    long expectedBlockNum = (long) eventNum * partitionNum * blockNumPerEvent;
    double expectedDataSize = (double) eventNum * partitionNum * blockSize * blockNumPerEvent;

    double actualTotalWriteBlockCount = benchHandle.getBenchTrace().getTotalWriteBlockSize();
    double actualTotalWriteBlockDataSize = benchHandle.getBenchTrace().getTotalWriteDataSize();

    if (actualTotalWriteBlockCount != expectedBlockNum) {
      msg =
          String.format(
              "%s\tShuffleServerBenchmark run Fail! Block nums expect: %d actual: %f%n",
              argStr, expectedBlockNum, actualTotalWriteBlockCount);
      benchHandle.addLog(msg);
      System.out.printf(msg);
    } else if (actualTotalWriteBlockDataSize != expectedDataSize) {
      msg =
          String.format(
              "%s\tShuffleServerBenchmark run Fail! Total size expect: %.2f actual: %.2f%n",
              argStr, expectedDataSize, actualTotalWriteBlockDataSize);
      benchHandle.addLog(msg);
      System.out.printf(msg);
    } else {
      msg =
          String.format(
              "%s\tShuffleServerBenchmark run Success. Cost: total=%d ms, flushWriteTotal=%s ms, flushWritePerEvent=%.2f ms%n",
              argStr, benchHandle.getDuration(), flushWriteTotal, flushWriteMsPerEvent);
      benchHandle.addLog(msg);
      System.out.printf(msg);
    }
  }

  private ShuffleDataFlushEvent dumpShuffleDataFlushEvent(
      ShuffleDataFlushEvent shuffleDataFlushEvent,
      int shuffleId,
      int partitionId,
      int taskAttemptId,
      int blockNum,
      int blockSize) {
    long eventId = ShuffleFlushManager.ATOMIC_EVENT_ID.getAndIncrement();
    ShufflePartitionedBlock[] blocks =
        generateBlocks(partitionId, taskAttemptId, blockNum, blockSize);
    return new ShuffleDataFlushEvent(
        eventId,
        shuffleDataFlushEvent.getAppId(),
        shuffleId,
        partitionId,
        partitionId,
        shuffleDataFlushEvent.getEncodedLength(),
        shuffleDataFlushEvent.getDataLength(),
        Arrays.asList(blocks),
        () -> shuffleDataFlushEvent.isValid(),
        shuffleDataFlushEvent.getShuffleBuffer());
  }

  private ShufflePartitionedBlock[] generateBlocks(
      int partition, int taskAttemptId, int blockNum, int blockSize) {
    ShufflePartitionedBlock[] blocks = new ShufflePartitionedBlock[blockNum];
    for (int i = 0; i < blockNum; i++) {
      long blockId = BlockIdLayout.DEFAULT.getBlockId(i + 1, partition, taskAttemptId);
      ByteBuf buf = Unpooled.wrappedBuffer(blockData);
      blocks[i] =
          new ShufflePartitionedBlock(
              blockSize, blockSize, blockDataCrc, blockId, taskAttemptId, buf);
    }
    return blocks;
  }

  private ShuffleDataFlushEvent generateEvent(
      String appId,
      int shuffleId,
      int partitionId,
      int taskAttemptId,
      int blockSize,
      int blockNum) {
    ShufflePartitionedBlock[] blocks =
        generateBlocks(partitionId, taskAttemptId, blockNum, blockSize);
    long encodedLength =
        Arrays.stream(blocks).mapToLong(ShufflePartitionedBlock::getEncodedLength).sum();
    long dataLength = Arrays.stream(blocks).mapToLong(ShufflePartitionedBlock::getDataLength).sum();
    ShufflePartitionedData shufflePartitionedData =
        new ShufflePartitionedData(partitionId, encodedLength, dataLength, blocks);
    ShuffleBuffer shuffleBuffer = shuffleBuffers.get(partitionId);
    shuffleBuffer.append(shufflePartitionedData);
    return shuffleBuffer.toFlushEvent(appId, shuffleId, partitionId, partitionId, null, null);
  }

  private static void printHelp() {
    System.out.println("Usage: ShuffleServerBenchmark [options]");
    System.out.println("Options:");
    System.out.println("  -c, --conf <file>        Config file");
    System.out.println("  -b, --blockSize <size>   Block size (e.g., 1M, 1G)");
    System.out.println("  -n, --blockNum <num>     Number of blocks");
    System.out.println("  -e, --eventNum <num>     Number of events");
    System.out.println("  -p, --partitionNum <num> Number of partitions (required)");
    System.out.println("  -h, --help               Display this help message");
  }

  public static void main(String[] args) {
    try {
      BenchArgumentVO arguments = new BenchArgumentVO();
      new CommandLine(arguments).parseArgs(args);

      if (arguments.isHelp()) {
        printHelp();
        return;
      }

      String configFile = arguments.getConfigFile();
      ShuffleServerConf shuffleServerConf = new ShuffleServerConf(configFile);
      shuffleServerConf.setLong(
          ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT.key(), Long.MAX_VALUE);
      shuffleServerConf.setLong(ShuffleServerConf.SERVER_HEARTBEAT_INTERVAL.key(), Long.MAX_VALUE);

      // start shuffle server
      ShuffleServer shuffleServer = new ShuffleServer(shuffleServerConf);
      shuffleServer.start();

      runBench(arguments, shuffleServerConf, shuffleServer, false, TimeUnit.DAYS.toMillis(1));
      System.exit(0);
    } catch (Exception e) {
      System.err.printf(
          "%s Error during test: %s%n",
          LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME), e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  public void close() throws Exception {
    if (blockData != null) {
      blockData.release();
    }
    if (shuffleBuffers != null) {
      for (ShuffleBuffer shuffleBuffer : shuffleBuffers.values()) {
        shuffleBuffer.release();
      }
      shuffleBuffers.clear();
    }
  }
}
