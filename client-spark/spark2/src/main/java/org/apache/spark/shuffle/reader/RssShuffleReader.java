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
import scala.Function2;
import scala.Option;
import scala.Product2;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Aggregator;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.executor.TempShuffleReadMetrics;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.ShuffleReader;
import org.apache.spark.util.CompletionIterator;
import org.apache.spark.util.CompletionIterator$;
import org.apache.spark.util.TaskCompletionListener;
import org.apache.spark.util.collection.ExternalSorter;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_RESUBMIT_STAGE_WITH_FETCH_FAILURE_ENABLED;

public class RssShuffleReader<K, C> implements ShuffleReader<K, C> {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleReader.class);
  private final boolean expectedTaskIdsBitmapFilterEnable;

  private String appId;
  private int shuffleId;
  private int startPartition;
  private int endPartition;
  private TaskContext context;
  private ShuffleDependency<K, C, ?> shuffleDependency;
  private Serializer serializer;
  private String taskId;
  private String basePath;
  private int partitionNumPerRange;
  private int partitionNum;
  private Roaring64NavigableMap blockIdBitmap;
  private Roaring64NavigableMap taskIdBitmap;
  private List<ShuffleServerInfo> shuffleServerInfoList;
  private Configuration hadoopConf;
  private RssConf rssConf;
  private Supplier<ShuffleManagerClient> managerClientSupplier;

  public RssShuffleReader(
      int startPartition,
      int endPartition,
      TaskContext context,
      RssShuffleHandle<K, C, ?> rssShuffleHandle,
      String basePath,
      Configuration hadoopConf,
      int partitionNumPerRange,
      int partitionNum,
      Roaring64NavigableMap blockIdBitmap,
      Roaring64NavigableMap taskIdBitmap,
      RssConf rssConf,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      Supplier<ShuffleManagerClient> managerClientSupplier) {
    this.appId = rssShuffleHandle.getAppId();
    this.startPartition = startPartition;
    this.endPartition = endPartition;
    this.context = context;
    this.shuffleDependency = rssShuffleHandle.getDependency();
    this.shuffleId = shuffleDependency.shuffleId();
    this.serializer = rssShuffleHandle.getDependency().serializer();
    this.taskId = "" + context.taskAttemptId() + "_" + context.attemptNumber();
    this.basePath = basePath;
    this.partitionNumPerRange = partitionNumPerRange;
    this.partitionNum = partitionNum;
    this.blockIdBitmap = blockIdBitmap;
    this.taskIdBitmap = taskIdBitmap;
    this.hadoopConf = hadoopConf;
    this.shuffleServerInfoList = (List<ShuffleServerInfo>) (partitionToServers.get(startPartition));
    this.rssConf = rssConf;
    this.managerClientSupplier = managerClientSupplier;
    expectedTaskIdsBitmapFilterEnable = shuffleServerInfoList.size() > 1;
  }

  @Override
  public Iterator<Product2<K, C>> read() {
    LOG.info("Shuffle read started:" + getReadInfo());
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
                    .partitionId(startPartition)
                    .basePath(basePath)
                    .partitionNumPerRange(partitionNumPerRange)
                    .partitionNum(partitionNum)
                    .blockIdBitmap(blockIdBitmap)
                    .taskIdBitmap(taskIdBitmap)
                    .shuffleServerInfoList(shuffleServerInfoList)
                    .hadoopConf(hadoopConf)
                    .expectedTaskIdsBitmapFilterEnable(expectedTaskIdsBitmapFilterEnable)
                    .retryMax(retryMax)
                    .retryIntervalMax(retryIntervalMax)
                    .rssConf(rssConf));
    RssShuffleDataIterator rssShuffleDataIterator =
        new RssShuffleDataIterator<K, C>(
            shuffleDependency.serializer(),
            shuffleReadClient,
            new ReadMetrics(context.taskMetrics().createTempShuffleReadMetrics()),
            rssConf);
    CompletionIterator completionIterator =
        CompletionIterator$.MODULE$.apply(
            rssShuffleDataIterator,
            new AbstractFunction0<BoxedUnit>() {
              @Override
              public BoxedUnit apply() {
                context.taskMetrics().mergeShuffleReadMetrics();
                return rssShuffleDataIterator.cleanup();
              }
            });
    context.addTaskCompletionListener(
        context -> {
          completionIterator.completion();
        });

    Iterator<Product2<K, C>> resultIter;

    if (shuffleDependency.keyOrdering().isDefined()) {
      // Create an ExternalSorter to sort the data
      Option<Aggregator<K, Object, C>> aggregator = Option.empty();
      if (shuffleDependency.aggregator().isDefined()) {
        if (shuffleDependency.mapSideCombine()) {
          aggregator =
              Option.apply(
                  (Aggregator<K, Object, C>)
                      new Aggregator<K, C, C>(
                          new AbstractFunction1<C, C>() {
                            @Override
                            public C apply(C x) {
                              return x;
                            }
                          },
                          (Function2<C, C, C>)
                              shuffleDependency.aggregator().get().mergeCombiners(),
                          (Function2<C, C, C>)
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
      context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled());
      context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes());

      // Use completion callback to stop sorter if task was finished/cancelled.
      context.addTaskCompletionListener(
          new TaskCompletionListener() {
            public void onTaskCompletion(TaskContext context) {
              sorter.stop();
            }
          });

      Function0<BoxedUnit> fn0 =
          new AbstractFunction0<BoxedUnit>() {
            @Override
            public BoxedUnit apply() {
              sorter.stop();
              return BoxedUnit.UNIT;
            }
          };
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

    // stage re-compute and shuffle manager server port are both set
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
        + ")";
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  static class ReadMetrics extends ShuffleReadMetrics {
    private TempShuffleReadMetrics tempShuffleReadMetrics;

    ReadMetrics(TempShuffleReadMetrics tempShuffleReadMetric) {
      this.tempShuffleReadMetrics = tempShuffleReadMetric;
    }

    @Override
    public void incRemoteBytesRead(long v) {
      tempShuffleReadMetrics.incRemoteBytesRead(v);
    }

    @Override
    public void incFetchWaitTime(long v) {
      tempShuffleReadMetrics.incFetchWaitTime(v);
    }

    @Override
    public void incRecordsRead(long v) {
      tempShuffleReadMetrics.incRecordsRead(v);
    }
  }
}
