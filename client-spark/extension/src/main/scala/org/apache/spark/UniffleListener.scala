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

package org.apache.spark

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerTaskEnd}
import org.apache.spark.shuffle.events.{ShuffleAssignmentInfoEvent, TaskShuffleReadInfoEvent, TaskShuffleWriteInfoEvent}
import org.apache.spark.status.ElementTrackingStore

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters.mapAsScalaMapConverter

class UniffleListener(conf: SparkConf, kvstore: ElementTrackingStore)
  extends SparkListener with Logging {

  private val aggregatedShuffleWriteMetric = new ConcurrentHashMap[String, AggregatedShuffleWriteMetric]
  private val aggregatedShuffleReadMetric = new ConcurrentHashMap[String, AggregatedShuffleReadMetric]

  private val totalTaskCpuTime = new AtomicLong(0)
  private val totalShuffleReadTime = new AtomicLong(0)
  private val totalShuffleWriteTime = new AtomicLong(0)
  private val totalShuffleBytes = new AtomicLong(0)

  private val updateIntervalMillis = 5000
  private var updateLastTimeMillis: Long = -1

  // Using the async interval update to reduce state store pressure
  private def mayUpdate(force: Boolean): Unit = {
    val now = System.currentTimeMillis()
    if (force || now - updateLastTimeMillis > updateIntervalMillis) {
      updateLastTimeMillis = now
      kvstore.write(
        new AggregatedShuffleWriteMetricsUIData(this.aggregatedShuffleWriteMetric)
      )
      kvstore.write(
        new AggregatedShuffleReadMetricsUIData(this.aggregatedShuffleReadMetric)
      )
      kvstore.write(
        AggregatedTaskInfoUIData(totalTaskCpuTime.get(), totalShuffleWriteTime.get(), totalShuffleReadTime.get(), totalShuffleBytes.get())
      )
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    this.mayUpdate(false)
    if (taskEnd.taskMetrics.shuffleReadMetrics.recordsRead > 0
      || taskEnd.taskMetrics.shuffleWriteMetrics.recordsWritten > 0) {
      totalTaskCpuTime.addAndGet(taskEnd.taskInfo.duration)
      totalShuffleWriteTime.addAndGet(taskEnd.taskMetrics.shuffleWriteMetrics.writeTime / 1000000)
      totalShuffleReadTime.addAndGet(taskEnd.taskMetrics.shuffleReadMetrics.fetchWaitTime)
      totalShuffleBytes.addAndGet(taskEnd.taskMetrics.shuffleWriteMetrics.bytesWritten)
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val rssConf = conf.getAll.filter(x => x._1.startsWith("spark.rss."))
    kvstore.write(new UniffleProperties(rssConf))
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    this.mayUpdate(true)
  }

  private def onBuildInfo(event: BuildInfoEvent): Unit = {
    val uiData = new BuildInfoUIData(event.info.toSeq.sortBy(_._1))
    kvstore.write(uiData)
  }

  private def onAssignmentInfo(info: ShuffleAssignmentInfoEvent): Unit = {
    kvstore.write(
      new ShuffleAssignmentUIData(
        info.getShuffleId,
        info.getAssignedServers
      )
    )
  }

  private def onTaskShuffleWriteInfo(event: TaskShuffleWriteInfoEvent): Unit = {
    val metrics = event.getMetrics
    for (metric <- metrics.asScala) {
      val id = metric._1
      val agg_metric = this.aggregatedShuffleWriteMetric.computeIfAbsent(id, _ => new AggregatedShuffleWriteMetric(0, 0))
      agg_metric.byteSize += metric._2.getByteSize
      agg_metric.durationMillis += metric._2.getDurationMillis
    }
  }

  private def onTaskShuffleReadInfo(event: TaskShuffleReadInfoEvent): Unit = {
    val metrics = event.getMetrics
    for (metric <- metrics.asScala) {
      val id = metric._1
      val agg_metric = this.aggregatedShuffleReadMetric.computeIfAbsent(id, _ => new AggregatedShuffleReadMetric(0, 0))
      agg_metric.byteSize += metric._2.getByteSize
      agg_metric.durationMillis += metric._2.getDurationMillis
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: BuildInfoEvent => onBuildInfo(e)
    case e: TaskShuffleWriteInfoEvent => onTaskShuffleWriteInfo(e)
    case e: TaskShuffleReadInfoEvent => onTaskShuffleReadInfo(e)
    case e: ShuffleAssignmentInfoEvent => onAssignmentInfo(e)
    case _ => // Ignore
  }
}

object UniffleListener {
  def register(ctx: SparkContext): Unit = {
    val kvStore = ctx.statusStore.store.asInstanceOf[ElementTrackingStore]
    val listener = new UniffleListener(ctx.conf, kvStore)
    ctx.listenerBus.addToStatusQueue(listener)
  }
}