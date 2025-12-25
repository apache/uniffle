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

