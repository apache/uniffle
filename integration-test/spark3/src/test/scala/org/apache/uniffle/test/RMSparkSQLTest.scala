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

package org.apache.uniffle.test

import com.google.common.collect.Maps
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.shuffle.RssSparkConfig
import org.apache.spark.sql.{Dataset, Row, SparkSession}
//import org.apache.uniffle.common.config.RssBaseConf.RSS_KRYO_REGISTRATION_CLASSES;
import org.apache.uniffle.common.rpc.ServerType
import org.apache.uniffle.coordinator.CoordinatorConf
import org.apache.uniffle.server.ShuffleServerConf
import org.apache.uniffle.server.buffer.ShuffleBufferType
import org.apache.uniffle.storage.util.StorageType
import org.apache.uniffle.test.IntegrationTestBase._
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{BeforeAll, Test}

import java.io.{File, FileWriter, PrintWriter}
import java.util
import java.util.Map

object RMSparkSQLTest {

  @BeforeAll
  @throws[Exception]
  def setupServers(): Unit = {
    val coordinatorConf: CoordinatorConf = getCoordinatorConf
    val dynamicConf: util.HashMap[String, String] = new util.HashMap[String, String]()
    dynamicConf.put(RssSparkConfig.RSS_STORAGE_TYPE.key, StorageType.MEMORY_LOCALFILE.name)
    // dynamicConf.put(RSS_KRYO_REGISTRATION_CLASSES.key(),
    //   "org.apache.spark.sql.execution.RowKey,org.apache.spark.sql.execution.IntKey")
    addDynamicConf(coordinatorConf, dynamicConf)
    createCoordinatorServer(coordinatorConf)
    val grpcShuffleServerConf = getShuffleServerConf(ServerType.GRPC)
    val nettyShuffleServerConf = getShuffleServerConf(ServerType.GRPC_NETTY)
    grpcShuffleServerConf.setBoolean(ShuffleServerConf.SERVER_MERGE_ENABLE, true)
    grpcShuffleServerConf.set(ShuffleServerConf.SERVER_SHUFFLE_BUFFER_TYPE, ShuffleBufferType.SKIP_LIST)
    // grpcShuffleServerConf.set(RSS_KRYO_REGISTRATION_CLASSES,
    //   "org.apache.spark.sql.execution.RowKey,org.apache.spark.sql.execution.IntKey")
    nettyShuffleServerConf.setBoolean(ShuffleServerConf.SERVER_MERGE_ENABLE, true)
    nettyShuffleServerConf.set(ShuffleServerConf.SERVER_SHUFFLE_BUFFER_TYPE, ShuffleBufferType.SKIP_LIST)
    // nettyShuffleServerConf.set(RSS_KRYO_REGISTRATION_CLASSES,
    //   "org.apache.spark.sql.execution.RowKey,org.apache.spark.sql.execution.IntKey")
    createShuffleServer(grpcShuffleServerConf)
    createShuffleServer(nettyShuffleServerConf)
    startServers()
  }
}

class RMSparkSQLTest extends SparkIntegrationTestBase {

  @Test
  @throws[Exception]
  def sparkSQLTest(): Unit = {
    run()
  }

  override def updateSparkConfCustomer(sparkConf: SparkConf): Unit = {
    sparkConf.set("spark.sql.shuffle.partitions", "4")
  }

  @throws[Exception]
  override def run(): Unit = {
    val fileName = generateTestFile
    val sparkConf = createSparkConf
    // lz4 conflict, so use snappy here
    sparkConf.set("spark.io.compression.codec", "snappy")
    // sparkConf.set("spark.sql.execution.sortedShuffle.enabled", "true")
    // 1 Run spark with remote sort rss
    // 1.1 GRPC
    val sparkConfWithRemoteSortRss = sparkConf.clone
    updateSparkConfWithRssGrpc(sparkConfWithRemoteSortRss)
    updateSparkConfCustomer(sparkConfWithRemoteSortRss)
    sparkConfWithRemoteSortRss.set(RssSparkConfig.RSS_REMOTE_MERGE_ENABLE.key, "true")
    val rssResult = runSparkApp(sparkConfWithRemoteSortRss, fileName)
    // 1.2 GRPC_NETTY
    val sparkConfWithRemoteSortRssNetty = sparkConf.clone
    updateSparkConfWithRssGrpc(sparkConfWithRemoteSortRssNetty)
    updateSparkConfCustomer(sparkConfWithRemoteSortRssNetty)
    sparkConfWithRemoteSortRssNetty.set(RssSparkConfig.RSS_REMOTE_MERGE_ENABLE.key, "true")
    sparkConfWithRemoteSortRssNetty.set(RssSparkConfig.RSS_CLIENT_TYPE.key, "GRPC_NETTY")
    val rssResultNetty = runSparkApp(sparkConfWithRemoteSortRssNetty, fileName)

    // 2 Run original spark
    val sparkConfOriginal = sparkConf.clone
    val originalResult = runSparkApp(sparkConfOriginal, fileName)

    // 3 verify
    assertEquals(originalResult.size(), rssResult.size())
    assertEquals(originalResult.size(), rssResultNetty.size())
    import scala.collection.JavaConverters._
    for ((k, v) <- originalResult.asScala.toMap) {
      assertEquals(v, rssResult.get(k))
      assertEquals(v, rssResultNetty.get(k))
    }
  }

  @throws[Exception]
  override def generateTestFile: String = generateCsvFile

  @throws[Exception]
  protected def generateCsvFile: String = {
    val rows = 1000
    val file = new File(IntegrationTestBase.tempDir, "test.csv")
    file.createNewFile
    file.deleteOnExit()
    try {
      val writer = new PrintWriter(new FileWriter(file))
      try for (i <- 0 until rows) {
        writer.println(generateRecord)
      }
      finally if (writer != null) writer.close()
    }
    file.getAbsolutePath
  }

  private def generateRecord = {
    val random = new java.util.Random
    val ch = ('a' + random.nextInt(26)).toChar
    val repeats = random.nextInt(10)
    StringUtils.repeat(ch, repeats) + "," + random.nextInt(100)
  }

  override def runTest(spark: SparkSession, fileName: String): util.Map[String, Long] = {
    val df = spark.read.schema("name STRING, age INT").csv(fileName)
    df.createOrReplaceTempView("people")
    val queryResult: Dataset[Row] =
      spark.sql("SELECT name, count(age) FROM people group by name order by name");
    val result:Map[String, Long] = Maps.newHashMap[String, Long]
    queryResult.rdd.collect().foreach(
      row => result.put(row.getString(0), row.getLong(1))
    )
    result
  }
}