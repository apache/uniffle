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

import org.apache.spark.{AggregatedShuffleMetric, AggregatedShuffleReadMetric, AggregatedShuffleWriteMetric}
import org.apache.spark.internal.Logging

import java.util.concurrent.ConcurrentHashMap
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

  private def allServerRow(kv: (String, Long, Long, Long, Long, Long, Long)) = <tr>
    <td>{kv._1}</td>
    <td>{kv._2}</td>
    <td>{kv._3}</td>
    <td>{kv._4}</td>
    <td>{kv._5}</td>
    <td>{kv._6}</td>
  </tr>

  private def createShuffleMetricsRows(shuffleWriteMetrics: (Seq[Long], Seq[String]), shuffleReadMetrics: (Seq[Long], Seq[String])): Seq[scala.xml.Elem] = {
    val (writeSpeeds, writeServerIds) = if (shuffleWriteMetrics != null) shuffleWriteMetrics else (Seq.empty, Seq.empty)
    val (readSpeeds, readServerIds) = if (shuffleReadMetrics != null) shuffleReadMetrics else (Seq.empty, Seq.empty)

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

    val writeSpeedRow = if (writeSpeeds.nonEmpty) Some(createSpeedRow("Write Speed (bytes/sec)", writeSpeeds)) else None
    val writeServerIdRow = if (writeServerIds.nonEmpty) Some(createServerIdRow("Shuffle Write Server ID", writeServerIds)) else None
    val readSpeedRow = if (readSpeeds.nonEmpty) Some(createSpeedRow("Read Speed (bytes/sec)", readSpeeds)) else None
    val readServerIdRow = if (readServerIds.nonEmpty) Some(createServerIdRow("Shuffle Read Server ID", readServerIds)) else None

    Seq(writeSpeedRow, writeServerIdRow, readSpeedRow, readServerIdRow).flatten
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
    val shuffleWriteMetrics = shuffleSpeedStatistics(runtimeStatusStore.aggregatedShuffleWriteMetrics().metrics.asScala.toSeq)
    val shuffleReadMetrics = shuffleSpeedStatistics(runtimeStatusStore.aggregatedShuffleReadMetrics().metrics.asScala.toSeq)
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

    // render all assigned shuffle-servers
    val allServers = unionByServerId(
      runtimeStatusStore.aggregatedShuffleWriteMetrics().metrics,
      runtimeStatusStore.aggregatedShuffleReadMetrics().metrics
    )
    val allServersTableUI = UIUtils.listingTable(
      Seq("Shuffle Server ID", "Write Bytes", "Write Duration", "Write Speed", "Read Bytes", "Read Duration", "Read Speed"),
      allServerRow,
      allServers,
      fixedWidth = true
    )

    // render assignment info
    val assignmentInfos = runtimeStatusStore.assignmentInfos
    val assignmentTableUI = UIUtils.listingTable(
      Seq("Shuffle ID", "Assigned Server Number"),
      propertyRow,
      assignmentInfos.map(x => (x.shuffleId.toString, x.shuffleServerIdList.size().toString)),
      fixedWidth = true
    )

    val summary: NodeSeq =
      <div>
        <div>
          <span class="collapse-sql-properties collapse-table"
                onClick="collapseTable('build-info-table')">
            <h4>
              <span class="collapse-table-arrow arrow-closed"></span>
              <a>Uniffle Build Information</a>
            </h4>
          </span>
          <div class="build-info-table collapsible-table">
            {buildInfoTableUI}
          </div>
        </div>

        <div>
          <span class="collapse-sql-properties collapse-table"
                onClick="collapseTable('statistics-table')">
            <h4>
              <span class="collapse-table-arrow arrow-closed"></span>
              <a>Shuffle Throughput Statistics</a>
            </h4>
            <div class="statistics-table collapsible-table">
              {shuffleMetricsTableUI}
            </div>
          </span>
        </div>

        <div>
          <span class="collapse-table" onClick="collapseTable('all-servers-table')">
            <h4>
              <span class="collapse-table-arrow"></span>
              <a>Shuffle Server</a>
            </h4>
            <div class="all-servers-table collapsed">
              {allServersTableUI}
            </div>
          </span>
        </div>

        <div>
          <span class="collapse-sql-properties collapse-table"
                onClick="collapseTable('assignment-table')">
            <h4>
              <span class="collapse-table-arrow arrow-closed"></span>
              <a>Assignment</a>
            </h4>
          </span>
          <div class="assignment-table collapsible-table">
            {assignmentTableUI}
          </div>
        </div>
      </div>

    UIUtils.headerSparkPage(request, "Uniffle", summary, parent)
  }

  private def unionByServerId(write: ConcurrentHashMap[String, AggregatedShuffleWriteMetric],
                              read: ConcurrentHashMap[String, AggregatedShuffleReadMetric]): Seq[(String, Long, Long, Long, Long, Long, Long)] = {
    val writeMetrics = write.asScala
    val readMetrics = read.asScala
    val allServerIds = writeMetrics.keySet ++ readMetrics.keySet

    val writeMetricsToMap =
      writeMetrics
        .mapValues {
          metrics =>
            (metrics.byteSize, metrics.durationMillis, metrics.byteSize / metrics.durationMillis)
        }
        .toMap
    val readMetricsToMap =
      readMetrics
        .mapValues {
          metrics =>
            (metrics.byteSize, metrics.durationMillis, metrics.byteSize / metrics.durationMillis)
        }
        .toMap

    val unionMetrics = allServerIds.toSeq.map { serverId =>
      val writeMetric = writeMetricsToMap.getOrElse(serverId, (0L, 0L, 0L))
      val readMetric = readMetricsToMap.getOrElse(serverId, (0L, 0L, 0L))
      (serverId, writeMetric._1, writeMetric._2, writeMetric._3, readMetric._1, readMetric._2, readMetric._3)
    }
    unionMetrics
  }

  private def shuffleSpeedStatistics(metrics: Seq[(String, AggregatedShuffleMetric)]): (Seq[Long], Seq[String]) = {
    if (metrics.isEmpty) {
      return (Seq.empty, Seq.empty)
    }
    val sorted =
      metrics
        .map(x => {
          (x._1, x._2.byteSize, x._2.durationMillis, x._2.byteSize / x._2.durationMillis)
        })
        .sortBy(_._4)

    val minMetric = sorted.head
    val maxMetric = sorted.last
    val p25Metric = sorted((sorted.size * 0.25).toInt)
    val p50Metric = sorted(sorted.size / 2)
    val p75Metric = sorted((sorted.size * 0.75).toInt)

    val speeds = Seq(minMetric, p25Metric, p50Metric, p75Metric, maxMetric).map(_._4)
    val shuffleServerIds = Seq(minMetric, p25Metric, p50Metric, p75Metric, maxMetric).map(_._1)

    (speeds, shuffleServerIds)
  }
}
