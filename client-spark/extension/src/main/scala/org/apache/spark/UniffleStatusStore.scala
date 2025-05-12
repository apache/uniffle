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
import org.apache.spark.status.KVUtils.KVIndexParam
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.{KVIndex, KVStore, KVStoreView}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.asScalaIteratorConverter

class UniffleStatusStore(store: KVStore) {
  private def viewToSeq[T](view: KVStoreView[T]): Seq[T] = {
    Utils.tryWithResource(view.closeableIterator())(iter => iter.asScala.toList)
  }

  def uniffleProperties(): UniffleProperties = {
    val kClass = classOf[UniffleProperties]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException => new UniffleProperties(Seq.empty)
    }
  }

  def buildInfo(): BuildInfoUIData = {
    val kClass = classOf[BuildInfoUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException => new BuildInfoUIData(Seq.empty)
    }
  }

  def assignmentInfos(): Seq[ShuffleAssignmentUIData] = {
    viewToSeq(store.view(classOf[ShuffleAssignmentUIData]))
  }

  def aggregatedShuffleWriteMetrics(): AggregatedShuffleWriteMetricsUIData = {
    val kClass = classOf[AggregatedShuffleWriteMetricsUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException =>
        new AggregatedShuffleWriteMetricsUIData(new ConcurrentHashMap[String, AggregatedShuffleWriteMetric]())
    }
  }

  def aggregatedShuffleReadMetrics(): AggregatedShuffleReadMetricsUIData = {
    val kClass = classOf[AggregatedShuffleReadMetricsUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException =>
        new AggregatedShuffleReadMetricsUIData(new ConcurrentHashMap[String, AggregatedShuffleReadMetric]())
    }
  }

  def aggregatedTaskInfo(): AggregatedTaskInfoUIData = {
    val kClass = classOf[AggregatedTaskInfoUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: Exception => AggregatedTaskInfoUIData(0, 0, 0, 0)
    }
  }
}

class UniffleProperties(val info: Seq[(String, String)]) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[UniffleProperties].getName()
}

class BuildInfoUIData(val info: Seq[(String, String)]) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[BuildInfoUIData].getName()
}

class ShuffleAssignmentUIData(@KVIndexParam val shuffleId: Int,
                              val shuffleServerIdList: java.util.List[String])

// Aggregated shuffle write/read metrics
class AggregatedShuffleMetric(var durationMillis: Long, var byteSize: Long)

class AggregatedShuffleWriteMetricsUIData(val metrics: ConcurrentHashMap[String, AggregatedShuffleWriteMetric]) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[AggregatedShuffleWriteMetricsUIData].getName()
}
class AggregatedShuffleWriteMetric(durationMillis: Long,
                                   byteSize: Long,
                                   var requireBufferFailureNumber: Long,
                                   var pushFailureNumber: Long,
                                   var lastPushFailureReason: String)
  extends AggregatedShuffleMetric(durationMillis, byteSize)

class AggregatedShuffleReadMetricsUIData(val metrics: ConcurrentHashMap[String, AggregatedShuffleReadMetric]) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[AggregatedShuffleReadMetricsUIData].getName()
}

class AggregatedShuffleReadMetric(durationMillis: Long,
                                  byteSize: Long,
                                  var memoryDurationMills: Long,
                                  var memoryByteSize: Long,
                                  var localfileDurationMillis: Long,
                                  var localfileByteSize: Long,
                                  var hadoopDurationMillis: Long,
                                  var hadoopByteSize: Long)
  extends AggregatedShuffleMetric(durationMillis, byteSize)

// task total cpu time
case class AggregatedTaskInfoUIData(cpuTimeMillis: Long,
                                    shuffleWriteMillis: Long,
                                    shuffleReadMillis: Long,
                                    shuffleBytes: Long) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[AggregatedTaskInfoUIData].getName()
}