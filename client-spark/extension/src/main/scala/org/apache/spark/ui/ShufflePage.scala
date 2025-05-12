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

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils
import org.apache.spark.{AggregatedShuffleMetric, AggregatedShuffleReadMetric, AggregatedShuffleWriteMetric, AggregatedTaskInfoUIData}

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

  private def allServerRow(kv: (String, String, String, Double, Long, Long, String, String, String, Double)) = <tr>
    <td>{kv._1}</td>
    <td>{kv._2}</td>
    <td>{kv._3}</td>
    <td>{kv._4}</td>
    <td>{kv._5}</td>
    <td>{kv._6}</td>
    <td>{kv._7}</td>
    <td>{kv._8}</td>
    <td>{kv._9}</td>
    <td>{kv._10}</td>
  </tr>

  private def createShuffleMetricsRows(shuffleWriteMetrics: (Seq[Double], Seq[String]), shuffleReadMetrics: (Seq[Double], Seq[String])): Seq[scala.xml.Elem] = {
    val (writeSpeeds, writeServerIds) = if (shuffleWriteMetrics != null) shuffleWriteMetrics else (Seq.empty, Seq.empty)
    val (readSpeeds, readServerIds) = if (shuffleReadMetrics != null) shuffleReadMetrics else (Seq.empty, Seq.empty)

    def createSpeedRow(metricType: String, speeds: Seq[Double]) = <tr>
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

    val writeSpeedRow = if (writeSpeeds.nonEmpty) Some(createSpeedRow("Write Speed (MB/sec)", writeSpeeds)) else None
    val writeServerIdRow = if (writeServerIds.nonEmpty) Some(createServerIdRow("Write Shuffle Server ID", writeServerIds)) else None
    val readSpeedRow = if (readSpeeds.nonEmpty) Some(createSpeedRow("Read Speed (MB/sec)", readSpeeds)) else None
    val readServerIdRow = if (readServerIds.nonEmpty) Some(createServerIdRow("Read Shuffle Server ID", readServerIds)) else None

    Seq(writeSpeedRow, writeServerIdRow, readSpeedRow, readServerIdRow).flatten
  }

  override def render(request: HttpServletRequest): Seq[Node] = {
    val originWriteMetric = runtimeStatusStore.aggregatedShuffleWriteMetrics()
    val originReadMetric = runtimeStatusStore.aggregatedShuffleReadMetrics()

    // render header
    val aggTaskInfo = runtimeStatusStore.aggregatedTaskInfo
    val taskInfo =
      if (aggTaskInfo == null)
        AggregatedTaskInfoUIData(0, 0, 0, 0)
      else
        aggTaskInfo
    val percent =
      if (taskInfo.cpuTimeMillis == 0)
        0
      else
        (taskInfo.shuffleWriteMillis + taskInfo.shuffleReadMillis).toDouble / taskInfo.cpuTimeMillis

    // render build info
    val buildInfo = runtimeStatusStore.buildInfo()
    val buildInfoTableUI = UIUtils.listingTable(
      propertyHeader,
      propertyRow,
      buildInfo.info,
      fixedWidth = true
    )

    // render uniffle configs
    val rssConf = runtimeStatusStore.uniffleProperties()
    val rssConfTableUI = UIUtils.listingTable(
      propertyHeader,
      propertyRow,
      rssConf.info,
      fixedWidth = true
    )

    // render shuffle-servers write+read statistics
    val shuffleWriteMetrics = shuffleSpeedStatistics(originWriteMetric.metrics.asScala.toSeq)
    val shuffleReadMetrics = shuffleSpeedStatistics(originReadMetric.metrics.asScala.toSeq)
    val shuffleHeader = Seq("Avg", "Min", "P25", "P50", "P75", "Max")
    val shuffleMetricsRows = createShuffleMetricsRows(shuffleWriteMetrics, shuffleReadMetrics)
    val shuffleMetricsTableUI =
      <table class="table table-bordered table-sm table-striped">
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
      originWriteMetric.metrics,
      originReadMetric.metrics
    )
    val allServersTableUI = UIUtils.listingTable(
      Seq(
        "Shuffle Server ID",
        "Write Bytes",
        "Write Duration",
        "Write Speed (MB/sec)",
        "Require Buffer Failures",
        "Push Failures",
        "Last push failure reason",
        "Read Bytes",
        "Read Duration",
        "Read Speed (MB/sec)"
      ),
      allServerRow,
      allServers,
      fixedWidth = true
    )

    // render reading hybrid storage statistics
    val readMetrics = originReadMetric.metrics
    val aggregatedByStorage = readMetrics.asScala.values
      .flatMap { metric =>
        Seq(
          ("MEMORY", metric.memoryByteSize, metric.memoryDurationMills),
          ("LOCALFILE", metric.localfileByteSize, metric.localfileDurationMillis),
          ("HADOOP", metric.hadoopByteSize, metric.hadoopDurationMillis)
        )
      }
      .groupBy(_._1)
      .mapValues { values =>
        val totalBytes = values.map(_._2).sum
        val totalTime = values.map(_._3).sum
        val speed = if (totalTime != 0) totalBytes.toDouble / totalTime / 1000 else 0L
        (totalBytes, totalTime, speed)
      }
      .toSeq
    val readTableUI = UIUtils.listingTable(
      Seq("Storage Type", "Read Bytes", "Read Time", "Read Speed (MB/sec)"),
      { row: (String, Long, Long, Double) =>
        <tr>
          <td>{row._1}</td>
          <td>{Utils.bytesToString(row._2)}</td>
          <td>{UIUtils.formatDuration(row._3)}</td>
          <td>{roundToTwoDecimals(row._4)}</td>
        </tr>
      },
      aggregatedByStorage.map { case (storageType, (bytes, time, speed)) =>
        (storageType, bytes, time, speed)
      },
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

    val summary: NodeSeq = {
      <div>
        <div>
          <ul class="list-unstyled">
            <li id="completed-summary" data-relingo-block="true">
              <a>
                <strong>Total shuffle bytes:</strong>
              </a>
              {Utils.bytesToString(taskInfo.shuffleBytes)}
            </li><li data-relingo-block="true">
            <a>
              <strong>Shuffle Duration (write+read) / Task Duration:</strong>
            </a>
            {UIUtils.formatDuration(taskInfo.shuffleWriteMillis + taskInfo.shuffleReadMillis)}
            ({UIUtils.formatDuration(taskInfo.shuffleWriteMillis)}+{UIUtils.formatDuration(taskInfo.shuffleReadMillis)})
            / {UIUtils.formatDuration(taskInfo.cpuTimeMillis)} = {roundToTwoDecimals(percent)}
          </li>
          </ul>
        </div>

        <div>
          <span class="collapse-build-info-properties collapse-table"
                onClick="collapseTable('collapse-build-info-properties', 'build-info-table')">
            <h4>
              <span class="collapse-table-arrow arrow-closed"></span>
              <a>Uniffle Build Information</a>
            </h4>
          </span>
          <div class="build-info-table collapsible-table collapsed">
            {buildInfoTableUI}
          </div>
        </div>

        <div>
          <span class="collapse-uniffle-config-properties collapse-table"
                onClick="collapseTable('collapse-uniffle-config-properties', 'uniffle-config-table')">
            <h4>
              <span class="collapse-table-arrow arrow-closed"></span>
              <a>Uniffle Properties</a>
            </h4>
          </span>
          <div class="uniffle-config-table collapsible-table collapsed">
            {rssConfTableUI}
          </div>
        </div>

        <div>
          <span class="collapse-throughput-properties collapse-table"
                onClick="collapseTable('collapse-throughput-properties', 'statistics-table')">
            <h4>
              <span class="collapse-table-arrow arrow-closed"></span>
              <a>Shuffle Throughput Statistics</a>
            </h4>
          </span>
          <div class="statistics-table collapsible-table collapsed">
            {shuffleMetricsTableUI}
          </div>
        </div>

        <div>
          <span class="collapse-read-throughput-properties collapse-table"
                onClick="collapseTable('collapse-read-throughput-properties', 'read-statistics-table')">
            <h4>
              <span class="collapse-table-arrow arrow-closed"></span>
              <a>Hybrid Storage Read Statistics</a>
            </h4>
          </span>
          <div class="read-statistics-table collapsible-table collapsed">
            {readTableUI}
          </div>
        </div>

        <div>
          <span class="collapse-server-properties collapse-table"
                onClick="collapseTable('collapse-server-properties', 'all-servers-table')">
            <h4>
              <span class="collapse-table-arrow arrow-closed"></span>
              <a>Shuffle Server ({allServers.length})</a>
            </h4>
          </span>
          <div class="all-servers-table collapsible-table collapsed">
            {allServersTableUI}
          </div>
        </div>

        <div>
          <span class="collapse-assignment-properties collapse-table"
                onClick="collapseTable('collapse-assignment-properties', 'assignment-table')">
            <h4>
              <span class="collapse-table-arrow arrow-closed"></span>
              <a>Assignment ({assignmentInfos.length})</a>
            </h4>
          </span>
          <div class="assignment-table collapsible-table collapsed">
            {assignmentTableUI}
          </div>
        </div>
      </div>
    }

    UIUtils.headerSparkPage(request, "Uniffle", summary, parent)
  }

  private def roundToTwoDecimals(value: Double): Double = {
    BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  private def unionByServerId(write: ConcurrentHashMap[String, AggregatedShuffleWriteMetric],
                              read: ConcurrentHashMap[String, AggregatedShuffleReadMetric]): Seq[(String, String, String, Double, Long, Long, String, String, String, Double)] = {
    val writeMetrics = write.asScala
    val readMetrics = read.asScala
    val allServerIds = writeMetrics.keySet ++ readMetrics.keySet

    val writeMetricsToMap =
      writeMetrics
        .mapValues {
          metrics =>
            (metrics.byteSize, metrics.durationMillis, (metrics.byteSize.toDouble / metrics.durationMillis) / 1000.00,
              metrics.requireBufferFailureNumber, metrics.pushFailureNumber, metrics.lastPushFailureReason)
        }
        .toMap
    val readMetricsToMap =
      readMetrics
        .mapValues {
          metrics =>
            (metrics.byteSize, metrics.durationMillis, (metrics.byteSize.toDouble / metrics.durationMillis) / 1000.00)
        }
        .toMap

    val unionMetrics = allServerIds.toSeq.map { serverId =>
      val writeMetric = writeMetricsToMap.getOrElse(serverId, (0L, 0L, 0.00, 0L, 0L, ""))
      val readMetric = readMetricsToMap.getOrElse(serverId, (0L, 0L, 0.00))
      (
        serverId,
        Utils.bytesToString(writeMetric._1),
        UIUtils.formatDuration(writeMetric._2),
        roundToTwoDecimals(writeMetric._3),
        writeMetric._4,
        writeMetric._5,
        writeMetric._6,
        Utils.bytesToString(readMetric._1),
        UIUtils.formatDuration(readMetric._2),
        roundToTwoDecimals(readMetric._3)
      )
    }
    unionMetrics
  }

  private def shuffleSpeedStatistics(metrics: Seq[(String, AggregatedShuffleMetric)]): (Seq[Double], Seq[String]) = {
    if (metrics.isEmpty) {
      return (Seq.empty, Seq.empty)
    }
    val sorted =
      metrics
        .map(x => {
          (x._1, x._2.byteSize, x._2.durationMillis, x._2.byteSize.toDouble / x._2.durationMillis / 1000.00)
        })
        .sortBy(_._4)

    val totalBytes = sorted.map(_._2).sum.toDouble // Sum of all byte sizes
    val totalDuration = sorted.map(_._3).sum.toDouble // Sum of all durations in milliseconds
    val avgMetric = if (totalDuration != 0) totalBytes / totalDuration / 1000.00 else 0.0

    val minMetric = sorted.head
    val maxMetric = sorted.last
    val p25Metric = sorted((sorted.size * 0.25).toInt)
    val p50Metric = sorted(sorted.size / 2)
    val p75Metric = sorted((sorted.size * 0.75).toInt)

    val speeds = avgMetric +: Seq(minMetric, p25Metric, p50Metric, p75Metric, maxMetric).map(_._4)
    val shuffleServerIds = "N/A" +: Seq(minMetric, p25Metric, p50Metric, p75Metric, maxMetric).map(_._1)

    (speeds, shuffleServerIds)
  }
}
