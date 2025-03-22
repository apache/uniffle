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

package org.apache.uniffle.test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import scala.Option;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ClientType;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class SparkIntegrationTestBase extends IntegrationTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(SparkIntegrationTestBase.class);

  abstract Map runTest(SparkSession spark, String fileName) throws Exception;

  public String generateTestFile() throws Exception {
    return null;
  }

  public void updateSparkConfCustomer(SparkConf sparkConf) {}

  public void run() throws Exception {

    String fileName = generateTestFile();
    SparkConf sparkConf = createSparkConf();

    long start = System.currentTimeMillis();
    updateCommonSparkConf(sparkConf);
    final Map resultWithoutRss = runSparkApp(sparkConf, fileName);
    final long durationWithoutRss = System.currentTimeMillis() - start;

    Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    updateSparkConfWithRssGrpc(sparkConf);
    updateSparkConfCustomer(sparkConf);
    start = System.currentTimeMillis();
    Map resultWithRssGrpc = runSparkApp(sparkConf, fileName);
    final long durationWithRssGrpc = System.currentTimeMillis() - start;
    verifyTestResult(resultWithoutRss, resultWithRssGrpc);

    updateSparkConfWithRssNetty(sparkConf);
    updateSparkConfCustomer(sparkConf);
    start = System.currentTimeMillis();
    Map resultWithRssNetty = runSparkApp(sparkConf, fileName);
    final long durationWithRssNetty = System.currentTimeMillis() - start;
    verifyTestResult(resultWithoutRss, resultWithRssNetty);

    updateSparkConfWithBlockIdSelfManaged(sparkConf);
    start = System.currentTimeMillis();
    Map resultWithBlockIdSelfManaged = runSparkApp(sparkConf, fileName);
    final long durationWithBlockIdSelfManaged = System.currentTimeMillis() - start;
    verifyTestResult(resultWithoutRss, resultWithBlockIdSelfManaged);

    LOG.info(
        "Test: durationWithoutRss["
            + durationWithoutRss
            + "], durationWithRssGrpc["
            + durationWithRssGrpc
            + "], durationWithRssNetty["
            + durationWithRssNetty
            + "], durationWithBlockIdSelfManaged["
            + durationWithBlockIdSelfManaged
            + "]");
  }

  public void updateCommonSparkConf(SparkConf sparkConf) {}

  private static <T> T getIfExists(Option<T> o) {
    return o.isDefined() ? o.get() : null;
  }

  protected Map runSparkApp(SparkConf sparkConf, String testFileName) throws Exception {
    SparkSession spark = getIfExists(SparkSession.getActiveSession());
    if (spark != null) {
      spark.close();
    }
    spark = SparkSession.builder().config(sparkConf).getOrCreate();
    Map result = runTest(spark, testFileName);
    spark.stop();
    return result;
  }

  protected SparkConf createSparkConf() {
    return new SparkConf()
        .setAppName(this.getClass().getSimpleName())
        .setMaster("local[4]")
        .set("spark.ui.enabled", "false");
  }

  public void updateSparkConfWithRssGrpc(SparkConf sparkConf) {
    sparkConf.set("spark.shuffle.manager", "org.apache.spark.shuffle.RssShuffleManager");
    sparkConf.set(
        "spark.shuffle.sort.io.plugin.class", "org.apache.spark.shuffle.RssShuffleDataIo");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set(RssSparkConfig.RSS_WRITER_BUFFER_SIZE.key(), "4m");
    sparkConf.set(RssSparkConfig.RSS_WRITER_BUFFER_SPILL_SIZE.key(), "32m");
    sparkConf.set(RssSparkConfig.RSS_CLIENT_READ_BUFFER_SIZE.key(), "2m");
    sparkConf.set(RssSparkConfig.RSS_WRITER_SERIALIZER_BUFFER_SIZE.key(), "128k");
    sparkConf.set(RssSparkConfig.RSS_WRITER_BUFFER_SEGMENT_SIZE.key(), "256k");
    sparkConf.set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), getQuorum());
    sparkConf.set(RssSparkConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS.key(), "30000");
    sparkConf.set(RssSparkConfig.RSS_CLIENT_RETRY_MAX.key(), "10");
    sparkConf.set(RssSparkConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS.key(), "1000");
    sparkConf.set(RssSparkConfig.RSS_CLIENT_RETRY_INTERVAL_MAX.key(), "1000");
    sparkConf.set(RssSparkConfig.RSS_INDEX_READ_LIMIT.key(), "100");
    sparkConf.set(RssSparkConfig.RSS_CLIENT_READ_BUFFER_SIZE.key(), "1m");
    sparkConf.set(RssSparkConfig.RSS_HEARTBEAT_INTERVAL.key(), "2000");
    sparkConf.set(RssSparkConfig.RSS_TEST_MODE_ENABLE.key(), "true");
    sparkConf.set(RssSparkConfig.RSS_CLIENT_TYPE, ClientType.GRPC.name());
  }

  public void updateSparkConfWithRssNetty(SparkConf sparkConf) {
    sparkConf.set("spark.shuffle.manager", "org.apache.spark.shuffle.RssShuffleManager");
    sparkConf.set(
        "spark.shuffle.sort.io.plugin.class", "org.apache.spark.shuffle.RssShuffleDataIo");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set(RssSparkConfig.RSS_WRITER_BUFFER_SIZE.key(), "4m");
    sparkConf.set(RssSparkConfig.RSS_WRITER_BUFFER_SPILL_SIZE.key(), "32m");
    sparkConf.set(RssSparkConfig.RSS_CLIENT_READ_BUFFER_SIZE.key(), "2m");
    sparkConf.set(RssSparkConfig.RSS_WRITER_SERIALIZER_BUFFER_SIZE.key(), "128k");
    sparkConf.set(RssSparkConfig.RSS_WRITER_BUFFER_SEGMENT_SIZE.key(), "256k");
    sparkConf.set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), getQuorum());
    sparkConf.set(RssSparkConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS.key(), "30000");
    sparkConf.set(RssSparkConfig.RSS_CLIENT_RETRY_MAX.key(), "10");
    sparkConf.set(RssSparkConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS.key(), "1000");
    sparkConf.set(RssSparkConfig.RSS_CLIENT_RETRY_INTERVAL_MAX.key(), "1000");
    sparkConf.set(RssSparkConfig.RSS_INDEX_READ_LIMIT.key(), "100");
    sparkConf.set(RssSparkConfig.RSS_CLIENT_READ_BUFFER_SIZE.key(), "1m");
    sparkConf.set(RssSparkConfig.RSS_HEARTBEAT_INTERVAL.key(), "2000");
    sparkConf.set(RssSparkConfig.RSS_TEST_MODE_ENABLE.key(), "true");
    sparkConf.set(RssSparkConfig.RSS_CLIENT_TYPE, ClientType.GRPC_NETTY.name());
  }

  public void updateSparkConfWithBlockIdSelfManaged(SparkConf sparkConf) {
    sparkConf.set(RssSparkConfig.RSS_CLIENT_TYPE, ClientType.GRPC.name());
    sparkConf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX
            + RssSparkConfig.RSS_BLOCK_ID_SELF_MANAGEMENT_ENABLED.key(),
        "true");
  }

  protected void verifyTestResult(Map expected, Map actual) {
    assertEquals(expected.size(), actual.size());
    for (Object expectedKey : expected.keySet()) {
      assertEquals(expected.get(expectedKey), actual.get(expectedKey));
    }
  }
}
