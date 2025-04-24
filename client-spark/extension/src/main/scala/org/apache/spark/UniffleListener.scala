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
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.shuffle.events.{ShuffleAssignmentInfoEvent, TaskShuffleReadInfoEvent, TaskShuffleWriteInfoEvent}
import org.apache.spark.status.ElementTrackingStore

class UniffleListener(conf: SparkConf, kvstore: ElementTrackingStore)
  extends SparkListener with Logging {

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
    kvstore.write(
      new TaskShuffleWriteMetricUIData(
        event.getStageId,
        event.getShuffleId,
        event.getTaskId,
        event.getMetrics
      )
    )
  }

  private def onTaskShuffleReadInfo(event: TaskShuffleReadInfoEvent): Unit = {
    kvstore.write(
      new TaskShuffleReadMetricUIData(
        event.getStageId,
        event.getShuffleId,
        event.getTaskId,
        event.getMetrics
      )
    )
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