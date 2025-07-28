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

package org.apache.spark.shuffle;

import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_RESUBMIT_STAGE_WITH_FETCH_FAILURE_ENABLED;
import static org.apache.spark.shuffle.RssSparkConfig.RSS_SHUFFLE_MANAGER_GRPC_PORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RssShuffleManagerTest extends RssShuffleManagerTestBase {
  private static final String SPARK_ADAPTIVE_EXECUTION_ENABLED_KEY = "spark.sql.adaptive.enabled";

  @Test
  public void testGetDataDistributionType() {
    // case1
    SparkConf sparkConf = new SparkConf();
    sparkConf.set(SPARK_ADAPTIVE_EXECUTION_ENABLED_KEY, "true");
    assertEquals(
        ShuffleDataDistributionType.LOCAL_ORDER,
        RssShuffleManager.getDataDistributionType(sparkConf));

    // case2
    sparkConf = new SparkConf();
    sparkConf.set(SPARK_ADAPTIVE_EXECUTION_ENABLED_KEY, "false");
    assertEquals(
        RssClientConf.DATA_DISTRIBUTION_TYPE.defaultValue(),
        RssShuffleManager.getDataDistributionType(sparkConf));

    // case3
    sparkConf = new SparkConf();
    sparkConf.set(SPARK_ADAPTIVE_EXECUTION_ENABLED_KEY, "true");
    sparkConf.set(
        "spark." + RssClientConf.DATA_DISTRIBUTION_TYPE.key(),
        ShuffleDataDistributionType.NORMAL.name());
    assertEquals(
        ShuffleDataDistributionType.NORMAL, RssShuffleManager.getDataDistributionType(sparkConf));

    // case4
    sparkConf = new SparkConf();
    sparkConf.set(SPARK_ADAPTIVE_EXECUTION_ENABLED_KEY, "true");
    sparkConf.set(
        "spark." + RssClientConf.DATA_DISTRIBUTION_TYPE.key(),
        ShuffleDataDistributionType.LOCAL_ORDER.name());
    assertEquals(
        ShuffleDataDistributionType.LOCAL_ORDER,
        RssShuffleManager.getDataDistributionType(sparkConf));

    // case5
    sparkConf = new SparkConf();
    boolean aqeEnable = (boolean) sparkConf.get(SQLConf.ADAPTIVE_EXECUTION_ENABLED());
    if (aqeEnable) {
      assertEquals(
          ShuffleDataDistributionType.LOCAL_ORDER,
          RssShuffleManager.getDataDistributionType(sparkConf));
    } else {
      assertEquals(
          RssClientConf.DATA_DISTRIBUTION_TYPE.defaultValue(),
          RssShuffleManager.getDataDistributionType(sparkConf));
    }
  }

  @Test
  public void testGetRemoteStorageInfo() {
    setupMockedRssShuffleUtils(StatusCode.SUCCESS);

    SparkConf conf = new SparkConf();
    conf.set(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED.key(), "false");
    conf.set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), "m1:8001,m2:8002");
    conf.set("spark.driver.host", "localhost");
    conf.set("spark.rss.storage.type", StorageType.LOCALFILE.name());
    conf.set(RssSparkConfig.RSS_TEST_MODE_ENABLE, true);

    // inject some hadoop configs
    conf.set("spark.rss.hadoop.k1", "v1");
    conf.set("spark.rss.hadoop.k2", "v2");

    RssShuffleManager shuffleManager = new RssShuffleManager(conf, true);
    RemoteStorageInfo remoteStorageInfo = shuffleManager.getRemoteStorageInfo();
    assertEquals(2, remoteStorageInfo.getConfItems().size());
  }

  @Test
  public void testCreateShuffleManagerServer() {
    setupMockedRssShuffleUtils(StatusCode.SUCCESS);

    SparkConf conf = new SparkConf();
    conf.set(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED.key(), "false");
    conf.set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), "m1:8001,m2:8002");
    conf.set("spark.driver.host", "localhost");
    conf.set("spark.rss.storage.type", StorageType.LOCALFILE.name());
    conf.set(RssSparkConfig.RSS_TEST_MODE_ENABLE, true);
    // enable stage recompute
    conf.set("spark." + RssClientConfig.RSS_RESUBMIT_STAGE, "true");

    RssShuffleManager shuffleManager = new RssShuffleManager(conf, true);

    ConfigOption<Boolean> a = RSS_RESUBMIT_STAGE_WITH_FETCH_FAILURE_ENABLED;

    assertTrue(conf.get(RSS_SHUFFLE_MANAGER_GRPC_PORT) > 0);
  }

  @Test
  public void testRssShuffleManagerInterface() throws Exception {
    setupMockedRssShuffleUtils(StatusCode.SUCCESS);

    SparkConf conf = new SparkConf();
    conf.set(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED.key(), "false");
    conf.set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), "m1:8001,m2:8002");
    conf.set("spark.rss.storage.type", StorageType.LOCALFILE.name());
    conf.set(RssSparkConfig.RSS_TEST_MODE_ENABLE, true);

    conf.set("spark.task.maxFailures", "3");
    RssShuffleManager shuffleManager = new RssShuffleManager(conf, true);
    assertEquals(shuffleManager.getMaxFetchFailures(), 2);
    // by default, the appId is null
    assertNull(shuffleManager.getAppId());
  }

  @ParameterizedTest
  @ValueSource(ints = {16, 20, 24})
  public void testRssShuffleManagerRegisterShuffle(int partitionIdBits) {
    BlockIdLayout layout =
        BlockIdLayout.from(
            63 - partitionIdBits - partitionIdBits - 2, partitionIdBits, partitionIdBits + 2);

    SparkConf conf = new SparkConf();
    conf.set(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED.key(), "false");
    conf.set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), "m1:8001,m2:8002");
    conf.set("spark.rss.storage.type", StorageType.LOCALFILE.name());
    conf.set(RssSparkConfig.RSS_TEST_MODE_ENABLE, true);
    conf.set("spark.task.maxFailures", "4");

    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_SEQUENCE_NO_BITS.key(),
        String.valueOf(layout.sequenceNoBits));
    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_PARTITION_ID_BITS.key(),
        String.valueOf(layout.partitionIdBits));
    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS.key(),
        String.valueOf(layout.taskAttemptIdBits));

    // register a shuffle with too many partitions should fail
    Partitioner mockPartitioner = mock(Partitioner.class);
    when(mockPartitioner.numPartitions()).thenReturn(layout.maxNumPartitions + 1);
    ShuffleDependency<String, String, String> mockDependency = mock(ShuffleDependency.class);
    when(mockDependency.partitioner()).thenReturn(mockPartitioner);

    RssShuffleManager shuffleManager = new RssShuffleManager(conf, true);

    RssException e =
        assertThrowsExactly(
            RssException.class, () -> shuffleManager.registerShuffle(0, mockDependency));
    assertEquals(
        "Cannot register shuffle with "
            + (layout.maxNumPartitions + 1)
            + " partitions because the configured block id layout supports at most "
            + layout.maxNumPartitions
            + " partitions.",
        e.getMessage());
  }

  @Test
  public void testWithStageRetry() {
    // case1: disable the stage retry
    SparkConf conf = createSparkConf();
    RssShuffleManager shuffleManager = new RssShuffleManager(conf, true);
    assertFalse(shuffleManager.isRssStageRetryEnabled());
    assertFalse(shuffleManager.isRssStageRetryForFetchFailureEnabled());
    assertFalse(shuffleManager.isRssStageRetryForWriteFailureEnabled());
    shuffleManager.stop();

    // case2: enable the stage retry
    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssSparkConfig.RSS_RESUBMIT_STAGE_ENABLED.key(),
        "true");
    shuffleManager = new RssShuffleManager(conf, true);
    assertTrue(shuffleManager.isRssStageRetryEnabled());
    assertTrue(shuffleManager.isRssStageRetryForFetchFailureEnabled());
    assertTrue(shuffleManager.isRssStageRetryForWriteFailureEnabled());
    shuffleManager.stop();

    // case3: overwrite the stage retry
    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX
            + RSS_RESUBMIT_STAGE_WITH_FETCH_FAILURE_ENABLED.key(),
        "false");
    shuffleManager = new RssShuffleManager(conf, true);
    assertTrue(shuffleManager.isRssStageRetryEnabled());
    assertFalse(shuffleManager.isRssStageRetryForFetchFailureEnabled());
    assertTrue(shuffleManager.isRssStageRetryForWriteFailureEnabled());
    shuffleManager.stop();

    // case4: enable the partial stage retry of fetch failure
    conf = createSparkConf();
    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX
            + RSS_RESUBMIT_STAGE_WITH_FETCH_FAILURE_ENABLED.key(),
        "true");
    shuffleManager = new RssShuffleManager(conf, true);
    assertTrue(shuffleManager.isRssStageRetryEnabled());
    assertTrue(shuffleManager.isRssStageRetryForFetchFailureEnabled());
    assertFalse(shuffleManager.isRssStageRetryForWriteFailureEnabled());
    shuffleManager.stop();
  }

  private SparkConf createSparkConf() {
    SparkConf conf = new SparkConf();
    conf.set(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED.key(), "false");
    conf.set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), "m1:8001,m2:8002");
    conf.set("spark.rss.storage.type", StorageType.LOCALFILE.name());
    conf.set(RssSparkConfig.RSS_TEST_MODE_ENABLE, true);
    conf.set("spark.task.maxFailures", "4");
    conf.set("spark.driver.host", "localhost");
    return conf;
  }
}
