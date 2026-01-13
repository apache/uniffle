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

package org.apache.uniffle.server;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.prometheus.client.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.ThreadUtils;

public class TopNShuffleDataSizeOfAppCalcTask {
  private static final Logger LOG = LoggerFactory.getLogger(TopNShuffleDataSizeOfAppCalcTask.class);

  private final int topNShuffleDataNumber;
  private final int topNShuffleDataTaskRefreshInterval;

  private final Gauge gaugeTotalDataSize;
  private final Gauge gaugeInMemoryDataSize;
  private final Gauge gaugeOnLocalFileDataSize;
  private final Gauge gaugeOnHadoopDataSize;

  private final ShuffleTaskManager shuffleTaskManager;
  private final ScheduledExecutorService scheduler;

  public TopNShuffleDataSizeOfAppCalcTask(ShuffleTaskManager taskManager, ShuffleServerConf conf) {
    topNShuffleDataNumber = conf.getInteger(ShuffleServerConf.TOP_N_APP_SHUFFLE_DATA_SIZE_NUMBER);
    topNShuffleDataTaskRefreshInterval =
        conf.getInteger(ShuffleServerConf.TOP_N_APP_SHUFFLE_DATA_REFRESH_INTERVAL);
    shuffleTaskManager = taskManager;
    this.gaugeTotalDataSize = ShuffleServerMetrics.gaugeTotalDataSizeUsage;
    this.gaugeInMemoryDataSize = ShuffleServerMetrics.gaugeInMemoryDataSizeUsage;
    this.gaugeOnLocalFileDataSize = ShuffleServerMetrics.gaugeOnDiskDataSizeUsage;
    this.gaugeOnHadoopDataSize = ShuffleServerMetrics.gaugeOnHadoopDataSizeUsage;
    this.scheduler =
        ThreadUtils.getDaemonSingleThreadScheduledExecutor(
            ThreadUtils.getExceptionCatchingThreadFactory("topN-shuffleDataSize-calc"));
  }

  private void calcTopNShuffleDataSize() {
    List<Map.Entry<String, ShuffleTaskInfo>> topNTaskInfo = calcTopNTotalDataSizeTaskInfo();
    gaugeTotalDataSize.clear();
    for (Map.Entry<String, ShuffleTaskInfo> taskInfo : topNTaskInfo) {
      gaugeTotalDataSize.labels(taskInfo.getKey()).set(taskInfo.getValue().getTotalDataSize());
    }

    topNTaskInfo = calcTopNInMemoryDataSizeTaskInfo();
    gaugeInMemoryDataSize.clear();
    for (Map.Entry<String, ShuffleTaskInfo> taskInfo : topNTaskInfo) {
      gaugeInMemoryDataSize
          .labels(taskInfo.getKey())
          .set(taskInfo.getValue().getInMemoryDataSize());
    }

    topNTaskInfo = calcTopNOnLocalFileDataSizeTaskInfo();
    gaugeOnLocalFileDataSize.clear();
    for (Map.Entry<String, ShuffleTaskInfo> taskInfo : topNTaskInfo) {
      gaugeOnLocalFileDataSize
          .labels(taskInfo.getKey())
          .set(taskInfo.getValue().getOnLocalFileDataSize());
    }

    topNTaskInfo = calcTopNOnHadoopDataSizeTaskInfo();
    gaugeOnHadoopDataSize.clear();
    for (Map.Entry<String, ShuffleTaskInfo> taskInfo : topNTaskInfo) {
      gaugeOnHadoopDataSize
          .labels(taskInfo.getKey())
          .set(taskInfo.getValue().getOnHadoopDataSize());
    }
  }

  public List<Map.Entry<String, ShuffleTaskInfo>> calcTopNTotalDataSizeTaskInfo() {
    return shuffleTaskManager.getShuffleTaskInfos().entrySet().stream()
        .sorted(
            (e1, e2) ->
                Long.compare(e2.getValue().getTotalDataSize(), e1.getValue().getTotalDataSize()))
        .limit(topNShuffleDataNumber)
        .collect(Collectors.toList());
  }

  public List<Map.Entry<String, ShuffleTaskInfo>> calcTopNInMemoryDataSizeTaskInfo() {
    return shuffleTaskManager.getShuffleTaskInfos().entrySet().stream()
        .sorted(
            (e1, e2) ->
                Long.compare(
                    e2.getValue().getInMemoryDataSize(), e1.getValue().getInMemoryDataSize()))
        .limit(topNShuffleDataNumber)
        .collect(Collectors.toList());
  }

  public List<Map.Entry<String, ShuffleTaskInfo>> calcTopNOnLocalFileDataSizeTaskInfo() {
    return shuffleTaskManager.getShuffleTaskInfos().entrySet().stream()
        .sorted(
            (e1, e2) ->
                Long.compare(
                    e2.getValue().getOnLocalFileDataSize(), e1.getValue().getOnLocalFileDataSize()))
        .limit(topNShuffleDataNumber)
        .collect(Collectors.toList());
  }

  public List<Map.Entry<String, ShuffleTaskInfo>> calcTopNOnHadoopDataSizeTaskInfo() {
    return shuffleTaskManager.getShuffleTaskInfos().entrySet().stream()
        .sorted(
            (e1, e2) ->
                Long.compare(
                    e2.getValue().getOnHadoopDataSize(), e1.getValue().getOnHadoopDataSize()))
        .limit(topNShuffleDataNumber)
        .collect(Collectors.toList());
  }

  public void start() {
    LOG.info("TopNShuffleDataSizeOfAppCalcTask start schedule.");
    this.scheduler.scheduleWithFixedDelay(
        this::calcTopNShuffleDataSize,
        0,
        topNShuffleDataTaskRefreshInterval,
        TimeUnit.MILLISECONDS);
  }

  public void stop() {
    this.scheduler.shutdown();
    try {
      this.scheduler.awaitTermination(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
