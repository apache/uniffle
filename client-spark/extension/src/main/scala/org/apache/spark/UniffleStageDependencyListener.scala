package org.apache.spark

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerStageSubmitted}
import org.apache.uniffle.shuffle.manager.RssShuffleManagerBase
import scala.collection.JavaConverters._

class UniffleStageDependencyListener extends SparkListener with Logging {

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stageInfo = stageSubmitted.stageInfo
    val stageId = stageInfo.stageId
    val parentStageIds = stageInfo.parentIds

    val shuffleMgr = SparkEnv.get.shuffleManager.asInstanceOf[RssShuffleManagerBase]
    shuffleMgr.addStageDependency(stageId, parentStageIds.map(x => new Integer(x)).toSet.asJava)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    val stageId = stageInfo.stageId

    val shuffleMgr = SparkEnv.get.shuffleManager.asInstanceOf[RssShuffleManagerBase]
    shuffleMgr.removeStageDependency(stageId)
  }
}

