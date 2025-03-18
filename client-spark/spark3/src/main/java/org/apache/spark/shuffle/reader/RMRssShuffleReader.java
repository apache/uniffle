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

package org.apache.spark.shuffle.reader;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import scala.Product2;
import scala.Tuple2;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.ClassUtils;
import org.apache.spark.Aggregator;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.ShuffleReader;
import org.apache.spark.shuffle.SparkCombiner;
import org.apache.spark.util.CompletionIterator;
import org.apache.spark.util.CompletionIterator$;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.record.Record;
import org.apache.uniffle.client.record.reader.RMRecordsReader;
import org.apache.uniffle.client.record.writer.Combiner;
import org.apache.uniffle.client.util.DefaultIdHelper;
import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.BlockIdLayout;

public class RMRssShuffleReader<K, C> implements ShuffleReader<K, C> {
  private static final Logger LOG = LoggerFactory.getLogger(RMRssShuffleReader.class);
  private final Map<Integer, List<ShuffleServerInfo>> partitionToShuffleServers;

  private final String appId;
  private final int shuffleId;
  private final int startPartition;
  private final int endPartition;
  private final TaskContext context;
  private final ShuffleDependency<K, ?, C> shuffleDependency;
  private final int numMaps;
  private final String taskId;
  private final ShuffleWriteClient shuffleWriteClient;
  private final Map<Integer, List<ShuffleServerInfo>> partitionToServerInfos;
  private final Map<Integer, Roaring64NavigableMap> partitionToExpectBlocks;
  private final Roaring64NavigableMap taskIdBitmap;
  private final ShuffleReadMetrics readMetrics;
  private final Supplier<ShuffleManagerClient> managerClientSupplier;

  private final RssConf rssConf;
  private final String clientType;

  private final Class keyClass;
  private final Class valueClass;
  private final boolean isMapCombine;
  private Comparator comparator = null;
  private Combiner combiner = null;
  private DefaultIdHelper idHelper;

  private Object metricsLock = new Object();

  public RMRssShuffleReader(
      int startPartition,
      int endPartition,
      TaskContext context,
      RssShuffleHandle<K, ?, C> rssShuffleHandle,
      ShuffleWriteClient shuffleWriteClient,
      Map<Integer, List<ShuffleServerInfo>> partitionToServerInfos,
      Map<Integer, Roaring64NavigableMap> partitionToExpectBlocks,
      Roaring64NavigableMap taskIdBitmap,
      ShuffleReadMetrics readMetrics,
      Supplier<ShuffleManagerClient> managerClientSupplier,
      RssConf rssConf,
      String clientType) {
    this.appId = rssShuffleHandle.getAppId();
    this.startPartition = startPartition;
    this.endPartition = endPartition;
    this.context = context;
    this.numMaps = rssShuffleHandle.getNumMaps();
    this.shuffleDependency = rssShuffleHandle.getDependency();
    this.shuffleId = shuffleDependency.shuffleId();
    this.taskId = "" + context.taskAttemptId() + "_" + context.attemptNumber();
    this.shuffleWriteClient = shuffleWriteClient;
    this.partitionToServerInfos = partitionToServerInfos;
    this.partitionToExpectBlocks = partitionToExpectBlocks;
    this.taskIdBitmap = taskIdBitmap;
    this.readMetrics = readMetrics;
    this.managerClientSupplier = managerClientSupplier;
    this.partitionToShuffleServers = rssShuffleHandle.getPartitionToServers();
    this.rssConf = rssConf;
    this.clientType = clientType;
    this.idHelper = new DefaultIdHelper(BlockIdLayout.from(rssConf));
    try {
      this.keyClass = ClassUtils.getClass(rssShuffleHandle.getDependency().keyClassName());
      this.isMapCombine = rssShuffleHandle.getDependency().mapSideCombine();
      this.valueClass =
          isMapCombine
              ? ClassUtils.getClass(
                  rssShuffleHandle
                      .getDependency()
                      .combinerClassName()
                      .getOrElse(
                          () -> {
                            throw new RssException(
                                "Can not find combine class even though map combine is enabled!");
                          }))
              : ClassUtils.getClass(rssShuffleHandle.getDependency().valueClassName());
      comparator = rssShuffleHandle.getDependency().keyOrdering().getOrElse(() -> null);
      Aggregator agg = rssShuffleHandle.getDependency().aggregator().getOrElse(() -> null);
      if (agg != null) {
        combiner = new SparkCombiner(agg);
      }
    } catch (ClassNotFoundException e) {
      throw new RssException(e);
    }
  }

  private void reportUniqueBlocks(Set<Integer> partitionIds) {
    for (int partitionId : partitionIds) {
      Roaring64NavigableMap blockIdBitmap = partitionToExpectBlocks.get(partitionId);
      Roaring64NavigableMap uniqueBlockIdBitMap = Roaring64NavigableMap.bitmapOf();
      blockIdBitmap.forEach(
          blockId -> {
            long taId = idHelper.getTaskAttemptId(blockId);
            if (taskIdBitmap.contains(taId)) {
              uniqueBlockIdBitMap.add(blockId);
            }
          });
      shuffleWriteClient.startSortMerge(
          new HashSet<>(partitionToServerInfos.get(partitionId)),
          appId,
          shuffleId,
          partitionId,
          uniqueBlockIdBitMap);
    }
  }

  @Override
  public Iterator<Product2<K, C>> read() {
    LOG.info("Shuffle read started:" + getReadInfo());

    Iterator<Product2<K, C>> resultIter = new MultiPartitionIterator<K, C>();
    resultIter = new InterruptibleIterator<Product2<K, C>>(context, resultIter);

    // resubmit stage and shuffle manager server port are both set
    if (rssConf.getBoolean(RssClientConfig.RSS_RESUBMIT_STAGE, false)
        && rssConf.getInteger(RssClientConf.SHUFFLE_MANAGER_GRPC_PORT, 0) > 0) {
      resultIter =
          RssFetchFailedIterator.newBuilder()
              .appId(appId)
              .shuffleId(shuffleId)
              .partitionId(startPartition)
              .stageAttemptId(context.stageAttemptNumber())
              .managerClientSupplier(managerClientSupplier)
              .build(resultIter);
    }
    return resultIter;
  }

  private String getReadInfo() {
    return "appId="
        + appId
        + ", shuffleId="
        + shuffleId
        + ",taskId="
        + taskId
        + ", partitions: ["
        + startPartition
        + ", "
        + endPartition
        + ")";
  }

  class MultiPartitionIterator<K, C> extends AbstractIterator<Product2<K, C>> {

    CompletionIterator<Record<K, C>, RMRssShuffleDataIterator<K, C>> dataIterator;

    MultiPartitionIterator() {
      if (numMaps <= 0) {
        return;
      }
      Set<Integer> partitionIds = new HashSet<>();
      for (int partition = startPartition; partition < endPartition; partition++) {
        if (partitionToExpectBlocks.get(partition).isEmpty()) {
          LOG.info("{} partition is empty partition", partition);
          continue;
        }
        partitionIds.add(partition);
      }
      if (partitionIds.size() == 0) {
        return;
      }
      // report unique blockIds
      reportUniqueBlocks(partitionIds);
      RMRecordsReader reader = createRMRecordsReader(partitionIds, partitionToShuffleServers);
      reader.start();
      RMRssShuffleDataIterator iter = new RMRssShuffleDataIterator<>(reader);
      this.dataIterator =
          CompletionIterator$.MODULE$.apply(
              iter,
              () -> {
                context.taskMetrics().mergeShuffleReadMetrics();
                return iter.cleanup();
              });
      context.addTaskCompletionListener(
          (taskContext) -> {
            if (dataIterator != null) {
              dataIterator.completion();
            }
          });
    }

    @Override
    public boolean hasNext() {
      if (dataIterator == null) {
        return false;
      }
      return dataIterator.hasNext();
    }

    @Override
    public Product2<K, C> next() {
      Record<K, C> record = dataIterator.next();
      Product2<K, C> result = new Tuple2<K, C>(record.getKey(), record.getValue());
      return result;
    }
  }

  @VisibleForTesting
  public RMRecordsReader createRMRecordsReader(
      Set<Integer> partitionIds, Map<Integer, List<ShuffleServerInfo>> serverInfoMap) {
    return new RMRecordsReader(
        appId,
        shuffleId,
        partitionIds,
        serverInfoMap,
        rssConf,
        keyClass,
        valueClass,
        comparator,
        false,
        combiner,
        isMapCombine,
        inc -> {
          // ShuffleReadMetrics is not thread-safe. Many Fetcher thread will update this value.
          synchronized (metricsLock) {
            readMetrics.incRecordsRead(inc);
          }
        },
        clientType);
  }
}
