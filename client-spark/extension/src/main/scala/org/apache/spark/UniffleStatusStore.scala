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

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.shuffle.events.{ShuffleReadMetric, ShuffleWriteMetric}
import org.apache.spark.status.KVUtils.KVIndexParam
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.{KVIndex, KVStore, KVStoreView}

import scala.collection.JavaConverters.asScalaIteratorConverter

class UniffleStatusStore(store: KVStore) {
  private def viewToSeq[T](view: KVStoreView[T]): Seq[T] = {
    Utils.tryWithResource(view.closeableIterator())(iter => iter.asScala.toList)
  }

  def buildInfo(): BuildInfoUIData = {
    val kClass = classOf[BuildInfoUIData]
    store.read(kClass, kClass.getName)
  }

  def taskShuffleReadMetrics(): Seq[TaskShuffleReadMetricUIData] = {
    viewToSeq(store.view(classOf[TaskShuffleReadMetricUIData]))
  }

  def taskShuffleWriteMetrics(): Seq[TaskShuffleWriteMetricUIData] = {
    viewToSeq(store.view(classOf[TaskShuffleWriteMetricUIData]))
  }

  def assignmentInfos(): Seq[ShuffleAssignmentUIData] = {
    viewToSeq(store.view(classOf[ShuffleAssignmentUIData]))
  }
}

class BuildInfoUIData(val info: Seq[(String, String)]) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[BuildInfoUIData].getName()
}

class TaskShuffleWriteMetricUIData(val stageId: Int,
                                   val shuffleId: Int,
                                   @KVIndexParam val taskId: Long,
                                   val metrics: java.util.Map[String, ShuffleWriteMetric])

class TaskShuffleReadMetricUIData(val stageId: Int,
                                  val shuffleId: Int,
                                  @KVIndexParam val taskId: Long,
                                  val metrics: java.util.Map[String, ShuffleReadMetric])
class ShuffleAssignmentUIData(@KVIndexParam val shuffleId: Int,
                              val shuffleServerIdList: java.util.List[String])