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

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import scala.Function0;
import scala.Function1;
import scala.Option;
import scala.Product2;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Aggregator;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.FunctionUtils;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.ShuffleReader;
import org.apache.spark.util.CompletionIterator;
import org.apache.spark.util.CompletionIterator$;
import org.apache.spark.util.collection.ExternalSorter;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_RESUBMIT_STAGE_WITH_FETCH_FAILURE_ENABLED;

public class RssShuffleReader<K, C> implements ShuffleReader<K, C> {
  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleReader.class);
  private final Map<Integer, List<ShuffleServerInfo>> partitionToShuffleServers;

  private String appId;
  private int shuffleId;
  private int startPartition;
  private int endPartition;
  private TaskContext context;
  private ShuffleDependency<K, ?, C> shuffleDependency;
  private int numMaps;
  private Serializer serializer;
  private String taskId;
  private String basePath;
  private int partitionNum;
  private Map<Integer, Roaring64NavigableMap> partitionToExpectBlocks;
  private Roaring64NavigableMap taskIdBitmap;
  private Configuration hadoopConf;
  private int mapStartIndex;
  private int mapEndIndex;
  private ShuffleReadMetrics readMetrics;
  private RssConf rssConf;
  private ShuffleDataDistributionType dataDistributionType;
  private Supplier<ShuffleManagerClient> managerClientSupplier;

  public RssShuffleReader(
      int startPartition,
      int endPartition,
      int mapStartIndex,
      int mapEndIndex,
      TaskContext context,
      RssShuffleHandle<K, ?, C> rssShuffleHandle,
      String basePath,
      Configuration hadoopConf,
      int partitionNum,
      Map<Integer, Roaring64NavigableMap> partitionToExpectBlocks,
      Roaring64NavigableMap taskIdBitmap,
      ShuffleReadMetrics readMetrics,
      Supplier<ShuffleManagerClient> managerClientSupplier,
      RssConf rssConf,
      ShuffleDataDistributionType dataDistributionType,
      Map<Integer, List<ShuffleServerInfo>> allPartitionToServers) {
    this.appId = rssShuffleHandle.getAppId();
    this.startPartition = startPartition;
    this.endPartition = endPartition;
    this.mapStartIndex = mapStartIndex;
    this.mapEndIndex = mapEndIndex;
    this.context = context;
    this.numMaps = rssShuffleHandle.getNumMaps();
    this.shuffleDependency = rssShuffleHandle.getDependency();
    this.shuffleId = shuffleDependency.shuffleId();
    this.serializer = rssShuffleHandle.getDependency().serializer();
    this.taskId = "" + context.taskAttemptId() + "_" + context.attemptNumber();
    this.basePath = basePath;
    this.partitionNum = partitionNum;
    this.partitionToExpectBlocks = partitionToExpectBlocks;
    this.taskIdBitmap = taskIdBitmap;
    this.hadoopConf = hadoopConf;
    this.readMetrics = readMetrics;
    this.partitionToShuffleServers = allPartitionToServers;
    this.rssConf = rssConf;
    this.dataDistributionType = dataDistributionType;
    this.managerClientSupplier = managerClientSupplier;
  }

  @Override
  public Iterator<Product2<K, C>> read() {
    LOG.info("Shuffle read started:" + getReadInfo());

    Iterator<Product2<K, C>> resultIter;
    MultiPartitionIterator rssShuffleDataIterator = new MultiPartitionIterator<K, C>();

    if (shuffleDependency.keyOrdering().isDefined()) {
      // Create an ExternalSorter to sort the data
      Option<Aggregator<K, Object, C>> aggregator = Option.empty();
      if (shuffleDependency.aggregator().isDefined()) {
        if (shuffleDependency.mapSideCombine()) {
          aggregator =
              Option.apply(
                  (Aggregator<K, Object, C>)
                      new Aggregator<K, C, C>(
                          x -> x,
                          shuffleDependency.aggregator().get().mergeCombiners(),
                          shuffleDependency.aggregator().get().mergeCombiners()));
        } else {
          aggregator =
              Option.apply((Aggregator<K, Object, C>) shuffleDependency.aggregator().get());
        }
      }
      ExternalSorter<K, Object, C> sorter =
          new ExternalSorter<>(
              context, aggregator, Option.empty(), shuffleDependency.keyOrdering(), serializer);
      LOG.info("Inserting aggregated records to sorter");
      long startTime = System.currentTimeMillis();
      sorter.insertAll(rssShuffleDataIterator);
      LOG.info(
          "Inserted aggregated records to sorter: millis:"
              + (System.currentTimeMillis() - startTime));
      context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled());
      context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes());
      context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled());
      Function0<BoxedUnit> fn0 =
          new AbstractFunction0<BoxedUnit>() {
            @Override
            public BoxedUnit apply() {
              sorter.stop();
              return BoxedUnit.UNIT;
            }
          };
      Function1<TaskContext, Void> fn1 =
          new AbstractFunction1<TaskContext, Void>() {
            public Void apply(TaskContext context) {
              sorter.stop();
              return null;
            }
          };
      context.addTaskCompletionListener(fn1);
      resultIter = CompletionIterator$.MODULE$.apply(sorter.iterator(), fn0);
    } else if (shuffleDependency.aggregator().isDefined()) {
      Aggregator<K, Object, C> aggregator =
          (Aggregator<K, Object, C>) shuffleDependency.aggregator().get();
      if (shuffleDependency.mapSideCombine()) {
        resultIter = aggregator.combineCombinersByKey(rssShuffleDataIterator, context);
      } else {
        resultIter = aggregator.combineValuesByKey(rssShuffleDataIterator, context);
      }
    } else {
      resultIter = rssShuffleDataIterator;
    }

    if (!(resultIter instanceof InterruptibleIterator)) {
      resultIter = new InterruptibleIterator<>(context, resultIter);
    }
    // resubmit stage and shuffle manager server port are both set
    if (rssConf.getBoolean(RSS_RESUBMIT_STAGE_WITH_FETCH_FAILURE_ENABLED)
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
        + ")"
        + ", maps: ["
        + mapStartIndex
        + ", "
        + mapEndIndex
        + ")";
  }

  @VisibleForTesting
  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  class MultiPartitionIterator<K, C> extends AbstractIterator<Product2<K, C>> {
    java.util.Iterator<CompletionIterator<Product2<K, C>, RssShuffleDataIterator<K, C>>> iterator;
    CompletionIterator<Product2<K, C>, RssShuffleDataIterator<K, C>> dataIterator;

    MultiPartitionIterator() {
      List<CompletionIterator<Product2<K, C>, RssShuffleDataIterator<K, C>>> iterators =
          Lists.newArrayList();
      if (numMaps <= 0) {
        return;
      }
      for (int partition = startPartition; partition < endPartition; partition++) {
        if (partitionToExpectBlocks.get(partition).isEmpty()) {
          LOG.info("{} partition is empty partition", partition);
          continue;
        }
        List<ShuffleServerInfo> shuffleServerInfoList = partitionToShuffleServers.get(partition);
        // This mechanism of expectedTaskIdsBitmap filter is to filter out the most of data.
        // especially for AQE skew optimization
        boolean expectedTaskIdsBitmapFilterEnable =
            !(mapStartIndex == 0 && mapEndIndex == Integer.MAX_VALUE)
                || shuffleServerInfoList.size() > 1;
        int retryMax =
            rssConf.getInteger(
                RssClientConfig.RSS_CLIENT_RETRY_MAX,
                RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE);
        long retryIntervalMax =
            rssConf.getLong(
                RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
                RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE);
        ShuffleReadClient shuffleReadClient =
            ShuffleClientFactory.getInstance()
                .createShuffleReadClient(
                    ShuffleClientFactory.newReadBuilder()
                        .appId(appId)
                        .shuffleId(shuffleId)
                        .partitionId(partition)
                        .basePath(basePath)
                        .partitionNumPerRange(1)
                        .partitionNum(partitionNum)
                        .blockIdBitmap(partitionToExpectBlocks.get(partition))
                        .taskIdBitmap(taskIdBitmap)
                        .shuffleServerInfoList(shuffleServerInfoList)
                        .hadoopConf(hadoopConf)
                        .shuffleDataDistributionType(dataDistributionType)
                        .expectedTaskIdsBitmapFilterEnable(expectedTaskIdsBitmapFilterEnable)
                        .retryMax(retryMax)
                        .retryIntervalMax(retryIntervalMax)
                        .rssConf(rssConf));
        RssShuffleDataIterator<K, C> iterator =
            new RssShuffleDataIterator<>(
                shuffleDependency.serializer(), shuffleReadClient, readMetrics, rssConf);
        CompletionIterator<Product2<K, C>, RssShuffleDataIterator<K, C>> completionIterator =
            CompletionIterator$.MODULE$.apply(
                iterator,
                FunctionUtils.once(
                    () -> {
                      context.taskMetrics().mergeShuffleReadMetrics();
                      return iterator.cleanup();
                    }));
        iterators.add(completionIterator);
      }
      iterator = iterators.iterator();
      if (iterator.hasNext()) {
        dataIterator = iterator.next();
        iterator.remove();
      }
      context.addTaskCompletionListener(
          (taskContext) -> {
            if (dataIterator != null) {
              dataIterator.completion();
            }
            iterator.forEachRemaining(CompletionIterator::completion);
          });
    }

    @Override
    public boolean hasNext() {
      if (dataIterator == null) {
        return false;
      }
      while (!dataIterator.hasNext()) {
        if (!iterator.hasNext()) {
          return false;
        }
        dataIterator = iterator.next();
        iterator.remove();
      }
      return dataIterator.hasNext();
    }

    @Override
    public Product2<K, C> next() {
      Product2<K, C> result = dataIterator.next();
      return result;
    }
  }
}
