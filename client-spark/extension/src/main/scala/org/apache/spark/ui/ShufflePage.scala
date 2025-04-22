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

package org.apache.spark.ui

import org.apache.spark.TaskShuffleMetricUIData
import org.apache.spark.internal.Logging

import javax.servlet.http.HttpServletRequest
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.xml.{Node, NodeSeq}

class ShufflePage(parent: ShuffleTab) extends WebUIPage("") with Logging {
  private val runtimeStatusStore = parent.store

  private def propertyHeader = Seq("Name", "Value")

  private def propertyRow(kv: (String, String)) = <tr>
    <td>
      {kv._1}
    </td> <td>
      {kv._2}
    </td>
  </tr>

  private object ShuffleTrackerType extends Enumeration {
    val Write, Read = Value
  }

  private def shuffleStatisticsCalculate(shuffleMetrics: Seq[TaskShuffleMetricUIData], trackerType: ShuffleTrackerType.Value): (Seq[Long], Seq[String]) = {
    val trackerData = trackerType match {
      case ShuffleTrackerType.Write => shuffleMetrics.flatMap(f => f.shuffleServerWriteTracker.asScala)
      case ShuffleTrackerType.Read => shuffleMetrics.flatMap(f => f.shuffleServerReadTracker.asScala)
    }

    val groupedAndSortedMetrics = trackerData
      .groupBy(_._1)
      .map {
        case (key, metrics) =>
          val totalByteSize = metrics.map(_._2.byteSize).sum
          val totalDuration = metrics.map(_._2.duration).sum
          (key, totalByteSize, totalDuration, totalByteSize / totalDuration)
      }
      .toSeq
      .sortBy(_._4)

    val minMetric = groupedAndSortedMetrics.head
    val maxMetric = groupedAndSortedMetrics.last
    val p25Metric = groupedAndSortedMetrics((groupedAndSortedMetrics.size * 0.25).toInt)
    val p50Metric = groupedAndSortedMetrics(groupedAndSortedMetrics.size / 2)
    val p75Metric = groupedAndSortedMetrics((groupedAndSortedMetrics.size * 0.75).toInt)

    val speeds = Seq(minMetric, p25Metric, p50Metric, p75Metric, maxMetric).map(_._4)
    val shuffleServerIds = Seq(minMetric, p25Metric, p50Metric, p75Metric, maxMetric).map(_._1)

    (speeds, shuffleServerIds)
  }

  def createShuffleMetricsRows(shuffleWriteMetrics: (Seq[Long], Seq[String]), shuffleReadMetrics: (Seq[Long], Seq[String])): Seq[scala.xml.Elem] = {
    val (writeSpeeds, writeServerIds) = shuffleWriteMetrics
    val (readSpeeds, readServerIds) = shuffleReadMetrics

    def createSpeedRow(metricType: String, speeds: Seq[Long]) = <tr>
      <td>
        {metricType}
      </td>{speeds.map(speed => <td>
        {f"$speed%.2f"}
      </td>)}
    </tr>

    def createServerIdRow(metricType: String, serverIds: Seq[String]) = <tr>
      <td>
        {metricType}
      </td>{serverIds.map(serverId => <td>
        {serverId}
      </td>)}
    </tr>

    Seq(
      createSpeedRow("Write Speed (bytes/sec)", writeSpeeds),
      createServerIdRow("Shuffle Write Server ID", writeServerIds),
      createSpeedRow("Read Speed (bytes/sec)", readSpeeds),
      createServerIdRow("Shuffle Read Server ID", readServerIds)
    )
  }

  override def render(request: HttpServletRequest): Seq[Node] = {
    // render build info
    val buildInfo = runtimeStatusStore.buildInfo()
    val buildInfoTableUI = UIUtils.listingTable(
      propertyHeader,
      propertyRow,
      buildInfo.info,
      fixedWidth = true
    )

    // render shuffle-servers write+read statistics
    val shuffleMetrics = runtimeStatusStore.taskShuffleMetrics()
    val shuffleWriteMetrics = shuffleStatisticsCalculate(shuffleMetrics, ShuffleTrackerType.Write)
    val shuffleReadMetrics = shuffleStatisticsCalculate(shuffleMetrics, ShuffleTrackerType.Read)
    val shuffleHeader = Seq("Min", "P25", "P50", "P75", "Max")
    val shuffleMetricsRows = createShuffleMetricsRows(shuffleWriteMetrics, shuffleReadMetrics)
    val shuffleMetricsTableUI =
      <table class="table table-bordered table-condensed table-striped table-head-clickable">
        <thead>
          <tr>
            {("Metric" +: shuffleHeader).map(header => <th>
            {header}
          </th>)}
          </tr>
        </thead>
        <tbody>
          {shuffleMetricsRows}
        </tbody>
      </table>

    val summary: NodeSeq =
      <div>
        <div>
          <span class="collapse-sql-properties collapse-table"
                onClick="collapseTable('collapse-sql-properties', 'sql-properties')">
            <h4>
              <span class="collapse-table-arrow arrow-closed"></span>
              <a>Uniffle Build Information</a>
            </h4>
          </span>
          <div class="sql-properties collapsible-table collapsed">
            {buildInfoTableUI}
          </div>
        </div>

        <div>
          <span class="collapse-sql-properties collapse-table"
                onClick="collapseTable('collapse-sql-properties', 'sql-properties')">
            <h4>
              <span class="collapse-table-arrow arrow-closed"></span>
              <a>Shuffle Server Write/Read Statistics</a>
            </h4>
          </span>
          <div class="sql-properties collapsible-table collapsed">
            {shuffleMetricsTableUI}
          </div>
        </div>
      </div>

    UIUtils.headerSparkPage(request, "Uniffle", summary, parent)
  }
}
