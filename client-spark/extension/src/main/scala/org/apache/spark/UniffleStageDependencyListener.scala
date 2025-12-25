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
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerStageSubmitted}
import org.apache.uniffle.shuffle.manager.RssShuffleManagerBase

class UniffleStageDependencyListener extends SparkListener with Logging {

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stageInfo = stageSubmitted.stageInfo
    val stageId = stageInfo.stageId
    val shuffleIdFromProducer = stageInfo.shuffleDepId
    val parentStageIds = stageInfo.parentIds

    getShuffleManager().map(manager => {
      manager.getStageDependencyTracker.ifPresent(tracker => {
        // 1. if this is the mapStage, mark this stage as the producer stage
        shuffleIdFromProducer.map(shuffleId => tracker.markShuffleWriter(shuffleId, stageId))
        // 2. if the parent stages exist, mark them as the consumer
        parentStageIds.foreach(parentStageId => {
          val shuffleId = tracker.getShuffleIdByStageIdOfWriter(parentStageId)
          tracker.markShuffleReader(shuffleId, stageId)
        })
      })
    })
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    val stageId = stageInfo.stageId
    getShuffleManager().map(manager => {
      manager.getStageDependencyTracker.ifPresent(tracker => {
        tracker.removeStage(stageId)
      })
    })
  }

  def getShuffleManager(): Option[RssShuffleManagerBase] = {
    val shuffleMgr = SparkEnv.get.shuffleManager
    shuffleMgr match {
      case rssShuffleMgr: RssShuffleManagerBase => Some(rssShuffleMgr)
      case _ => None
    }
  }
}

