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

package org.apache.uniffle.shuffle.manager;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssSparkConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import org.apache.uniffle.client.api.CoordinatorClient;
import org.apache.uniffle.client.factory.CoordinatorClientFactory;
import org.apache.uniffle.client.impl.grpc.CoordinatorGrpcRetryableClient;
import org.apache.uniffle.client.request.RssFetchClientConfRequest;
import org.apache.uniffle.client.response.RssFetchClientConfResponse;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;

import static org.apache.uniffle.common.rpc.StatusCode.INVALID_REQUEST;
import static org.apache.uniffle.common.rpc.StatusCode.SUCCESS;
import static org.apache.uniffle.shuffle.manager.RssShuffleManagerBase.getTaskAttemptIdForBlockId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RssShuffleManagerBaseTest {

  @Test
  public void testGetDefaultRemoteStorageInfo() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set(
        "spark." + RssClientConf.RSS_CLIENT_REMOTE_STORAGE_USE_LOCAL_CONF_ENABLED.key(), "false");
    RemoteStorageInfo remoteStorageInfo =
        RssShuffleManagerBase.getDefaultRemoteStorageInfo(sparkConf);
    assertTrue(remoteStorageInfo.getConfItems().isEmpty());

    sparkConf.set("spark.rss.hadoop.fs.defaultFs", "hdfs://rbf-xxx/foo");
    remoteStorageInfo = RssShuffleManagerBase.getDefaultRemoteStorageInfo(sparkConf);
    assertEquals(remoteStorageInfo.getConfItems().size(), 1);
    assertEquals(remoteStorageInfo.getConfItems().get("fs.defaultFs"), "hdfs://rbf-xxx/foo");
  }

  private static Stream<Arguments> testConfigureBlockIdLayoutSource() {
    // test arguments are
    // - maxPartitions
    // - maxFailure
    // - speculation
    // - expected maxPartitions
    // - expected sequence number bits
    // - expected partition id bits
    // - expected task attempt id bits
    return Stream.of(
        // default config values
        Arguments.of(null, 4, false, "1048576", 21, 20, 22),

        // without speculation
        Arguments.of("2", 0, false, "65536", 31, 16, 16),
        Arguments.of("2147483647", 0, false, "2147483647", 1, 31, 31),
        Arguments.of("1024", 3, false, "32768", 31, 15, 17),
        Arguments.of("131072", 3, false, "131072", 27, 17, 19),
        Arguments.of("1048576", 3, false, "1048576", 21, 20, 22),
        Arguments.of("1048577", 3, false, "1048577", 19, 21, 23),
        Arguments.of("1024", 4, false, "32768", 31, 15, 17),
        Arguments.of("131072", 4, false, "131072", 27, 17, 19),
        Arguments.of("1048576", 4, false, "1048576", 21, 20, 22),
        Arguments.of("1048577", 4, false, "1048577", 19, 21, 23),
        Arguments.of("1024", 5, false, "32768", 30, 15, 18),
        Arguments.of("131072", 5, false, "131072", 26, 17, 20),
        Arguments.of("1048576", 5, false, "1048576", 20, 20, 23),
        Arguments.of("1048577", 5, false, "1048577", 18, 21, 24),
        Arguments.of("2", 1073741824, false, "2", 31, 1, 31),

        // with speculation
        Arguments.of("2", 0, true, "65536", 30, 16, 17),
        Arguments.of("1073741824", 0, true, "1073741824", 2, 30, 31),
        Arguments.of("1024", 3, true, "32768", 31, 15, 17),
        Arguments.of("131072", 3, true, "131072", 27, 17, 19),
        Arguments.of("1048576", 3, true, "1048576", 21, 20, 22),
        Arguments.of("1048577", 3, true, "1048577", 19, 21, 23),
        Arguments.of("1024", 4, true, "32768", 30, 15, 18),
        Arguments.of("131072", 4, true, "131072", 26, 17, 20),
        Arguments.of("1048576", 4, true, "1048576", 20, 20, 23),
        Arguments.of("1048577", 4, true, "1048577", 18, 21, 24),
        Arguments.of("1024", 5, true, "32768", 30, 15, 18),
        Arguments.of("131072", 5, true, "131072", 26, 17, 20),
        Arguments.of("1048576", 5, true, "1048576", 20, 20, 23),
        Arguments.of("1048577", 5, true, "1048577", 18, 21, 24),
        Arguments.of("2", 1073741823, true, "2", 31, 1, 31));
  }

  @ParameterizedTest
  @MethodSource("testConfigureBlockIdLayoutSource")
  public void testConfigureBlockIdLayout(
      String setMaxPartitions,
      Integer setMaxFailure,
      Boolean setSpeculation,
      String expectedMaxPartitions,
      int expectedSequenceNoBits,
      int expectedPartitionIdBits,
      int expectedTaskAttemptIdBits) {
    SparkConf sparkConf = new SparkConf();
    if (setMaxPartitions != null) {
      sparkConf.set(RssSparkConfig.RSS_MAX_PARTITIONS.key(), setMaxPartitions);
    }
    RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);

    RssShuffleManagerBase.configureBlockIdLayout(sparkConf, rssConf, setMaxFailure, setSpeculation);

    if (expectedMaxPartitions == null) {
      assertFalse(sparkConf.contains(RssSparkConfig.RSS_MAX_PARTITIONS.key()));
    } else {
      assertTrue(sparkConf.contains(RssSparkConfig.RSS_MAX_PARTITIONS.key()));
      assertEquals(expectedMaxPartitions, sparkConf.get(RssSparkConfig.RSS_MAX_PARTITIONS.key()));
    }

    String key;
    key = RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_SEQUENCE_NO_BITS.key();
    assertTrue(sparkConf.contains(key));
    assertEquals(String.valueOf(expectedSequenceNoBits), sparkConf.get(key));
    assertEquals(expectedSequenceNoBits, rssConf.get(RssClientConf.BLOCKID_SEQUENCE_NO_BITS));

    key = RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_PARTITION_ID_BITS.key();
    assertTrue(sparkConf.contains(key));
    assertEquals(String.valueOf(expectedPartitionIdBits), sparkConf.get(key));
    assertEquals(expectedPartitionIdBits, rssConf.get(RssClientConf.BLOCKID_PARTITION_ID_BITS));

    key = RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS.key();
    assertTrue(sparkConf.contains(key));
    assertEquals(String.valueOf(expectedTaskAttemptIdBits), sparkConf.get(key));
    assertEquals(
        expectedTaskAttemptIdBits, rssConf.get(RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS));
  }

  @Test
  public void testConfigureBlockIdLayoutOverrides() {
    SparkConf sparkConf = new SparkConf();
    RssConf rssConf = new RssConf();
    int maxFailures = 4;
    boolean speculation = false;

    String sparkPrefix = RssSparkConfig.SPARK_RSS_CONFIG_PREFIX;
    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    String sparkSeqNoBitsKey = sparkPrefix + RssClientConf.BLOCKID_SEQUENCE_NO_BITS.key();
    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    String sparkPartIdBitsKey = sparkPrefix + RssClientConf.BLOCKID_PARTITION_ID_BITS.key();
    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    String sparkTaskIdBitsKey = sparkPrefix + RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS.key();

    // SparkConf populates RssConf
    sparkConf.set(RssSparkConfig.RSS_MAX_PARTITIONS.key(), "131072");
    RssShuffleManagerBase.configureBlockIdLayout(sparkConf, rssConf, maxFailures, speculation);
    assertEquals(27, rssConf.get(RssClientConf.BLOCKID_SEQUENCE_NO_BITS));
    assertEquals(17, rssConf.get(RssClientConf.BLOCKID_PARTITION_ID_BITS));
    assertEquals(19, rssConf.get(RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS));
    assertEquals(131072, sparkConf.getInt(RssSparkConfig.RSS_MAX_PARTITIONS.key(), -1));
    assertEquals(27, sparkConf.getInt(sparkSeqNoBitsKey, -1));
    assertEquals(17, sparkConf.getInt(sparkPartIdBitsKey, -1));
    assertEquals(19, sparkConf.getInt(sparkTaskIdBitsKey, -1));

    // SparkConf overrides RssConf
    sparkConf.set(RssSparkConfig.RSS_MAX_PARTITIONS.key(), "131073");
    RssShuffleManagerBase.configureBlockIdLayout(sparkConf, rssConf, maxFailures, speculation);
    assertEquals(25, rssConf.get(RssClientConf.BLOCKID_SEQUENCE_NO_BITS));
    assertEquals(18, rssConf.get(RssClientConf.BLOCKID_PARTITION_ID_BITS));
    assertEquals(20, rssConf.get(RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS));
    assertEquals(131073, sparkConf.getInt(RssSparkConfig.RSS_MAX_PARTITIONS.key(), -1));
    assertEquals(25, sparkConf.getInt(sparkSeqNoBitsKey, -1));
    assertEquals(18, sparkConf.getInt(sparkPartIdBitsKey, -1));
    assertEquals(20, sparkConf.getInt(sparkTaskIdBitsKey, -1));

    // SparkConf block id config overrides RssConf
    sparkConf.remove(RssSparkConfig.RSS_MAX_PARTITIONS.key());
    sparkConf.set(sparkSeqNoBitsKey, "22");
    sparkConf.set(sparkPartIdBitsKey, "21");
    sparkConf.set(sparkTaskIdBitsKey, "20");
    RssShuffleManagerBase.configureBlockIdLayout(sparkConf, rssConf, maxFailures, speculation);
    assertEquals(22, rssConf.get(RssClientConf.BLOCKID_SEQUENCE_NO_BITS));
    assertEquals(21, rssConf.get(RssClientConf.BLOCKID_PARTITION_ID_BITS));
    assertEquals(20, rssConf.get(RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS));
    assertFalse(sparkConf.contains(RssSparkConfig.RSS_MAX_PARTITIONS.key()));
    assertEquals(22, sparkConf.getInt(sparkSeqNoBitsKey, -1));
    assertEquals(21, sparkConf.getInt(sparkPartIdBitsKey, -1));
    assertEquals(20, sparkConf.getInt(sparkTaskIdBitsKey, -1));

    // empty SparkConf preserves RssConf
    sparkConf = new SparkConf();
    RssShuffleManagerBase.configureBlockIdLayout(sparkConf, rssConf, maxFailures, speculation);
    assertEquals(22, rssConf.get(RssClientConf.BLOCKID_SEQUENCE_NO_BITS));
    assertEquals(21, rssConf.get(RssClientConf.BLOCKID_PARTITION_ID_BITS));
    assertEquals(20, rssConf.get(RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS));
    assertFalse(sparkConf.contains(RssSparkConfig.RSS_MAX_PARTITIONS.key()));
    assertEquals(22, sparkConf.getInt(sparkSeqNoBitsKey, -1));
    assertEquals(21, sparkConf.getInt(sparkPartIdBitsKey, -1));
    assertEquals(20, sparkConf.getInt(sparkTaskIdBitsKey, -1));
  }

  private static Stream<Arguments> testConfigureBlockIdLayoutMaxPartitionsValueExceptionSource() {
    // test arguments are
    // - maxPartitions
    // - maxFailure
    // - speculation
    return Stream.of(
        // without speculation
        Arguments.of("-1", 4, false),
        Arguments.of("0", 4, false),

        // with speculation
        Arguments.of("-1", 4, true),
        Arguments.of("0", 4, true),
        Arguments.of("1", 4, true));
  }

  @ParameterizedTest
  @MethodSource("testConfigureBlockIdLayoutMaxPartitionsValueExceptionSource")
  public void testConfigureBlockIdLayoutMaxPartitionsValueException(
      String setMaxPartitions, int setMaxFailure, boolean setSpeculation) {
    SparkConf sparkConf = new SparkConf();
    if (setMaxPartitions != null) {
      sparkConf.set(RssSparkConfig.RSS_MAX_PARTITIONS.key(), setMaxPartitions);
    }
    RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);

    Executable call =
        () ->
            RssShuffleManagerBase.configureBlockIdLayout(
                sparkConf, rssConf, setMaxFailure, setSpeculation);
    Exception e = assertThrowsExactly(IllegalArgumentException.class, call);

    String expectedMessage =
        "Value of spark.rss.blockId.maxPartitions must be larger than 1: " + setMaxPartitions;
    assertEquals(expectedMessage, e.getMessage());
  }

  private static Stream<Arguments> testConfigureBlockIdLayoutUnsupportedMaxPartitionsSource() {
    // test arguments are
    // - maxPartitions
    // - maxFailure
    // - speculation
    // - expected message
    return Stream.of(
        // without speculation
        Arguments.of("2097152", 2048, false, "1048576"),
        Arguments.of("536870913", 3, false, "536870912"),
        Arguments.of("1073741825", 2, false, "1073741824"),

        // with speculation
        Arguments.of("2097152", 2048, true, "524288"),
        Arguments.of("536870913", 3, true, "536870912"),
        Arguments.of("1073741824", 2, true, "536870912"));
  }

  @ParameterizedTest
  @MethodSource("testConfigureBlockIdLayoutUnsupportedMaxPartitionsSource")
  public void testConfigureBlockIdLayoutUnsupportedMaxPartitions(
      String setMaxPartitions, int setMaxFailure, boolean setSpeculation, String atMost) {
    SparkConf sparkConf = new SparkConf();
    if (setMaxPartitions != null) {
      sparkConf.set(RssSparkConfig.RSS_MAX_PARTITIONS.key(), setMaxPartitions);
    }
    RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);

    String expectedMessage =
        "Cannot support spark.rss.blockId.maxPartitions="
            + setMaxPartitions
            + " partitions, as this would require to reserve more than 31 bits in the block id for task attempt ids. With spark.maxFailures="
            + setMaxFailure
            + " and spark.speculation="
            + setSpeculation
            + " at most "
            + atMost
            + " partitions can be supported.";
    Executable call =
        () ->
            RssShuffleManagerBase.configureBlockIdLayout(
                sparkConf, rssConf, setMaxFailure, setSpeculation);
    Exception e = assertThrowsExactly(IllegalArgumentException.class, call);
    assertEquals(expectedMessage, e.getMessage());
  }

  private static Stream<Arguments> testConfigureBlockIdLayoutInsufficientConfigExceptionSource() {
    // test arguments are
    // - sequenceNoBits
    // - partitionIdBits
    // - taskAttemptIdBits
    // - config
    return Stream.of("spark", "rss")
        .flatMap(
            config ->
                Stream.of(
                    Arguments.of(null, 21, 22, config),
                    Arguments.of(20, null, 22, config),
                    Arguments.of(20, 21, null, config)));
  }

  @ParameterizedTest
  @MethodSource("testConfigureBlockIdLayoutInsufficientConfigExceptionSource")
  public void testConfigureBlockIdLayoutInsufficientConfigException(
      Integer sequenceNoBits, Integer partitionIdBits, Integer taskAttemptIdBits, String config) {
    SparkConf sparkConf = new SparkConf();
    RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);

    if (config.equals("spark")) {
      String sparkPrefix = RssSparkConfig.SPARK_RSS_CONFIG_PREFIX;
      String sparkSeqNoBitsKey = sparkPrefix + RssClientConf.BLOCKID_SEQUENCE_NO_BITS.key();
      String sparkPartIdBitsKey = sparkPrefix + RssClientConf.BLOCKID_PARTITION_ID_BITS.key();
      String sparkTaskIdBitsKey = sparkPrefix + RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS.key();

      if (sequenceNoBits != null) {
        sparkConf.set(sparkSeqNoBitsKey, sequenceNoBits.toString());
      }
      if (partitionIdBits != null) {
        sparkConf.set(sparkPartIdBitsKey, partitionIdBits.toString());
      }
      if (taskAttemptIdBits != null) {
        sparkConf.set(sparkTaskIdBitsKey, taskAttemptIdBits.toString());
      }
    } else if (config.equals("rss")) {
      if (sequenceNoBits != null) {
        rssConf.set(RssClientConf.BLOCKID_SEQUENCE_NO_BITS, sequenceNoBits);
      }
      if (partitionIdBits != null) {
        rssConf.set(RssClientConf.BLOCKID_PARTITION_ID_BITS, partitionIdBits);
      }
      if (taskAttemptIdBits != null) {
        rssConf.set(RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS, taskAttemptIdBits);
      }
    } else {
      throw new IllegalArgumentException(config);
    }

    Executable call =
        () -> RssShuffleManagerBase.configureBlockIdLayout(sparkConf, rssConf, 4, false);
    Exception e = assertThrowsExactly(IllegalArgumentException.class, call);

    assertTrue(e.getMessage().startsWith("All block id bit config keys must be provided "));
  }

  private long bits(String string) {
    return Long.parseLong(string.replaceAll("[|]", ""), 2);
  }

  @Test
  public void testGetTaskAttemptIdWithoutSpeculation() {
    // the expected bits("xy|z") represents the expected Long in bit notation where | is used to
    // separate map index from attempt number, so merely for visualization purposes

    // maxFailures < 1 not allowed, we fall back to maxFailures=1 to be robust
    for (int maxFailures : Arrays.asList(-1, 0, 1)) {
      assertEquals(
          bits("0000|"),
          getTaskAttemptIdForBlockId(0, 0, maxFailures, false, 10),
          String.valueOf(maxFailures));
      assertEquals(
          bits("0001|"),
          getTaskAttemptIdForBlockId(1, 0, maxFailures, false, 10),
          String.valueOf(maxFailures));
      assertEquals(
          bits("0010|"),
          getTaskAttemptIdForBlockId(2, 0, maxFailures, false, 10),
          String.valueOf(maxFailures));
    }

    // maxFailures of 2
    assertEquals(bits("000|0"), getTaskAttemptIdForBlockId(0, 0, 2, false, 10));
    assertEquals(bits("000|1"), getTaskAttemptIdForBlockId(0, 1, 2, false, 10));
    assertEquals(bits("001|0"), getTaskAttemptIdForBlockId(1, 0, 2, false, 10));
    assertEquals(bits("001|1"), getTaskAttemptIdForBlockId(1, 1, 2, false, 10));
    assertEquals(bits("010|0"), getTaskAttemptIdForBlockId(2, 0, 2, false, 10));
    assertEquals(bits("010|1"), getTaskAttemptIdForBlockId(2, 1, 2, false, 10));
    assertEquals(bits("011|0"), getTaskAttemptIdForBlockId(3, 0, 2, false, 10));
    assertEquals(bits("011|1"), getTaskAttemptIdForBlockId(3, 1, 2, false, 10));

    // maxFailures of 3
    assertEquals(bits("00|00"), getTaskAttemptIdForBlockId(0, 0, 3, false, 10));
    assertEquals(bits("00|01"), getTaskAttemptIdForBlockId(0, 1, 3, false, 10));
    assertEquals(bits("00|10"), getTaskAttemptIdForBlockId(0, 2, 3, false, 10));
    assertEquals(bits("01|00"), getTaskAttemptIdForBlockId(1, 0, 3, false, 10));
    assertEquals(bits("01|01"), getTaskAttemptIdForBlockId(1, 1, 3, false, 10));
    assertEquals(bits("01|10"), getTaskAttemptIdForBlockId(1, 2, 3, false, 10));
    assertEquals(bits("10|00"), getTaskAttemptIdForBlockId(2, 0, 3, false, 10));
    assertEquals(bits("10|01"), getTaskAttemptIdForBlockId(2, 1, 3, false, 10));
    assertEquals(bits("10|10"), getTaskAttemptIdForBlockId(2, 2, 3, false, 10));
    assertEquals(bits("11|00"), getTaskAttemptIdForBlockId(3, 0, 3, false, 10));
    assertEquals(bits("11|01"), getTaskAttemptIdForBlockId(3, 1, 3, false, 10));
    assertEquals(bits("11|10"), getTaskAttemptIdForBlockId(3, 2, 3, false, 10));

    // maxFailures of 4
    assertEquals(bits("00|00"), getTaskAttemptIdForBlockId(0, 0, 4, false, 10));
    assertEquals(bits("00|01"), getTaskAttemptIdForBlockId(0, 1, 4, false, 10));
    assertEquals(bits("00|10"), getTaskAttemptIdForBlockId(0, 2, 4, false, 10));
    assertEquals(bits("00|11"), getTaskAttemptIdForBlockId(0, 3, 4, false, 10));
    assertEquals(bits("01|00"), getTaskAttemptIdForBlockId(1, 0, 4, false, 10));
    assertEquals(bits("01|01"), getTaskAttemptIdForBlockId(1, 1, 4, false, 10));
    assertEquals(bits("01|10"), getTaskAttemptIdForBlockId(1, 2, 4, false, 10));
    assertEquals(bits("01|11"), getTaskAttemptIdForBlockId(1, 3, 4, false, 10));
    assertEquals(bits("10|00"), getTaskAttemptIdForBlockId(2, 0, 4, false, 10));
    assertEquals(bits("10|01"), getTaskAttemptIdForBlockId(2, 1, 4, false, 10));
    assertEquals(bits("10|10"), getTaskAttemptIdForBlockId(2, 2, 4, false, 10));
    assertEquals(bits("10|11"), getTaskAttemptIdForBlockId(2, 3, 4, false, 10));
    assertEquals(bits("11|00"), getTaskAttemptIdForBlockId(3, 0, 4, false, 10));
    assertEquals(bits("11|01"), getTaskAttemptIdForBlockId(3, 1, 4, false, 10));
    assertEquals(bits("11|10"), getTaskAttemptIdForBlockId(3, 2, 4, false, 10));
    assertEquals(bits("11|11"), getTaskAttemptIdForBlockId(3, 3, 4, false, 10));

    // maxFailures of 5
    assertEquals(bits("0|000"), getTaskAttemptIdForBlockId(0, 0, 5, false, 10));
    assertEquals(bits("1|100"), getTaskAttemptIdForBlockId(1, 4, 5, false, 10));

    // test with ints that overflow into signed int and long
    assertEquals(Integer.MAX_VALUE, getTaskAttemptIdForBlockId(Integer.MAX_VALUE, 0, 1, false, 31));
    assertEquals(
        (long) Integer.MAX_VALUE << 1 | 1,
        getTaskAttemptIdForBlockId(Integer.MAX_VALUE, 1, 2, false, 32));
    assertEquals(
        (long) Integer.MAX_VALUE << 2 | 3,
        getTaskAttemptIdForBlockId(Integer.MAX_VALUE, 3, 4, false, 33));
    assertEquals(
        (long) Integer.MAX_VALUE << 3 | 7,
        getTaskAttemptIdForBlockId(Integer.MAX_VALUE, 7, 8, false, 34));

    // test with attemptNo >= maxFailures
    assertThrowsExactly(RssException.class, () -> getTaskAttemptIdForBlockId(0, 1, -1, false, 10));
    assertThrowsExactly(RssException.class, () -> getTaskAttemptIdForBlockId(0, 1, 0, false, 10));
    for (int maxFailures : Arrays.asList(1, 2, 3, 4, 8, 128)) {
      assertThrowsExactly(
          RssException.class,
          () -> getTaskAttemptIdForBlockId(0, maxFailures, maxFailures, false, 10),
          String.valueOf(maxFailures));
      assertThrowsExactly(
          RssException.class,
          () -> getTaskAttemptIdForBlockId(0, maxFailures + 1, maxFailures, false, 10),
          String.valueOf(maxFailures));
      assertThrowsExactly(
          RssException.class,
          () -> getTaskAttemptIdForBlockId(0, maxFailures + 2, maxFailures, false, 10),
          String.valueOf(maxFailures));
      Exception e =
          assertThrowsExactly(
              RssException.class,
              () -> getTaskAttemptIdForBlockId(0, maxFailures + 128, maxFailures, false, 10),
              String.valueOf(maxFailures));
      assertEquals(
          "Observing attempt number "
              + (maxFailures + 128)
              + " while maxFailures is set to "
              + maxFailures
              + ".",
          e.getMessage());
    }

    // test with mapIndex that would require more than maxTaskAttemptBits
    Exception e =
        assertThrowsExactly(
            RssException.class, () -> getTaskAttemptIdForBlockId(256, 0, 3, true, 10));
    assertEquals(
        "Observing mapIndex[256] that would produce a taskAttemptId with 11 bits "
            + "which is larger than the allowed 10 bits (maxFailures[3], speculation[true]). "
            + "Please consider providing more bits for taskAttemptIds.",
        e.getMessage());
    // check that a lower mapIndex works as expected
    assertEquals(bits("11111111|00"), getTaskAttemptIdForBlockId(255, 0, 3, true, 10));
  }

  @Test
  public void testGetTaskAttemptIdWithSpeculation() {
    // with speculation, we expect maxFailures+1 attempts

    // the expected bits("xy|z") represents the expected Long in bit notation where | is used to
    // separate map index from attempt number, so merely for visualization purposes

    // maxFailures < 1 not allowed, we fall back to maxFailures=1 to be robust
    for (int maxFailures : Arrays.asList(-1, 0, 1)) {
      for (int attemptNo : Arrays.asList(0, 1)) {
        assertEquals(
            bits("0000|" + attemptNo),
            getTaskAttemptIdForBlockId(0, attemptNo, maxFailures, true, 10),
            "maxFailures=" + maxFailures + ", attemptNo=" + attemptNo);
        assertEquals(
            bits("0001|" + attemptNo),
            getTaskAttemptIdForBlockId(1, attemptNo, maxFailures, true, 10),
            "maxFailures=" + maxFailures + ", attemptNo=" + attemptNo);
        assertEquals(
            bits("0010|" + attemptNo),
            getTaskAttemptIdForBlockId(2, attemptNo, maxFailures, true, 10),
            "maxFailures=" + maxFailures + ", attemptNo=" + attemptNo);
      }
    }

    // maxFailures of 2
    assertEquals(bits("00|00"), getTaskAttemptIdForBlockId(0, 0, 2, true, 10));
    assertEquals(bits("00|01"), getTaskAttemptIdForBlockId(0, 1, 2, true, 10));
    assertEquals(bits("00|10"), getTaskAttemptIdForBlockId(0, 2, 2, true, 10));
    assertEquals(bits("01|00"), getTaskAttemptIdForBlockId(1, 0, 2, true, 10));
    assertEquals(bits("01|01"), getTaskAttemptIdForBlockId(1, 1, 2, true, 10));
    assertEquals(bits("01|10"), getTaskAttemptIdForBlockId(1, 2, 2, true, 10));
    assertEquals(bits("10|00"), getTaskAttemptIdForBlockId(2, 0, 2, true, 10));
    assertEquals(bits("10|01"), getTaskAttemptIdForBlockId(2, 1, 2, true, 10));
    assertEquals(bits("10|10"), getTaskAttemptIdForBlockId(2, 2, 2, true, 10));
    assertEquals(bits("11|00"), getTaskAttemptIdForBlockId(3, 0, 2, true, 10));
    assertEquals(bits("11|01"), getTaskAttemptIdForBlockId(3, 1, 2, true, 10));
    assertEquals(bits("11|10"), getTaskAttemptIdForBlockId(3, 2, 2, true, 10));

    // maxFailures of 3
    assertEquals(bits("00|00"), getTaskAttemptIdForBlockId(0, 0, 3, true, 10));
    assertEquals(bits("00|01"), getTaskAttemptIdForBlockId(0, 1, 3, true, 10));
    assertEquals(bits("00|10"), getTaskAttemptIdForBlockId(0, 2, 3, true, 10));
    assertEquals(bits("00|11"), getTaskAttemptIdForBlockId(0, 3, 3, true, 10));
    assertEquals(bits("01|00"), getTaskAttemptIdForBlockId(1, 0, 3, true, 10));
    assertEquals(bits("01|01"), getTaskAttemptIdForBlockId(1, 1, 3, true, 10));
    assertEquals(bits("01|10"), getTaskAttemptIdForBlockId(1, 2, 3, true, 10));
    assertEquals(bits("01|11"), getTaskAttemptIdForBlockId(1, 3, 3, true, 10));
    assertEquals(bits("10|00"), getTaskAttemptIdForBlockId(2, 0, 3, true, 10));
    assertEquals(bits("10|01"), getTaskAttemptIdForBlockId(2, 1, 3, true, 10));
    assertEquals(bits("10|10"), getTaskAttemptIdForBlockId(2, 2, 3, true, 10));
    assertEquals(bits("10|11"), getTaskAttemptIdForBlockId(2, 3, 3, true, 10));
    assertEquals(bits("11|00"), getTaskAttemptIdForBlockId(3, 0, 3, true, 10));
    assertEquals(bits("11|01"), getTaskAttemptIdForBlockId(3, 1, 3, true, 10));
    assertEquals(bits("11|10"), getTaskAttemptIdForBlockId(3, 2, 3, true, 10));
    assertEquals(bits("11|11"), getTaskAttemptIdForBlockId(3, 3, 3, true, 10));

    // maxFailures of 4
    assertEquals(bits("0|000"), getTaskAttemptIdForBlockId(0, 0, 4, true, 10));
    assertEquals(bits("1|100"), getTaskAttemptIdForBlockId(1, 4, 4, true, 10));

    // test with ints that overflow into signed int and long
    assertEquals(
        (long) Integer.MAX_VALUE << 1,
        getTaskAttemptIdForBlockId(Integer.MAX_VALUE, 0, 1, true, 32));
    assertEquals(
        (long) Integer.MAX_VALUE << 1 | 1,
        getTaskAttemptIdForBlockId(Integer.MAX_VALUE, 1, 1, true, 32));
    assertEquals(
        (long) Integer.MAX_VALUE << 2 | 3,
        getTaskAttemptIdForBlockId(Integer.MAX_VALUE, 3, 3, true, 33));
    assertEquals(
        (long) Integer.MAX_VALUE << 3 | 7,
        getTaskAttemptIdForBlockId(Integer.MAX_VALUE, 7, 7, true, 34));

    // test with attemptNo > maxFailures (attemptNo == maxFailures allowed for speculation enabled)
    assertThrowsExactly(RssException.class, () -> getTaskAttemptIdForBlockId(0, 2, -1, true, 10));
    assertThrowsExactly(RssException.class, () -> getTaskAttemptIdForBlockId(0, 2, 0, true, 10));
    for (int maxFailures : Arrays.asList(1, 2, 3, 4, 8, 128)) {
      assertThrowsExactly(
          RssException.class,
          () -> getTaskAttemptIdForBlockId(0, maxFailures + 1, maxFailures, true, 10),
          String.valueOf(maxFailures));
      assertThrowsExactly(
          RssException.class,
          () -> getTaskAttemptIdForBlockId(0, maxFailures + 2, maxFailures, true, 10),
          String.valueOf(maxFailures));
      Exception e =
          assertThrowsExactly(
              RssException.class,
              () -> getTaskAttemptIdForBlockId(0, maxFailures + 128, maxFailures, true, 10),
              String.valueOf(maxFailures));
      assertEquals(
          "Observing attempt number "
              + (maxFailures + 128)
              + " while maxFailures is set to "
              + maxFailures
              + " with speculation enabled.",
          e.getMessage());
    }

    // test with mapIndex that would require more than maxTaskAttemptBits
    Exception e =
        assertThrowsExactly(
            RssException.class, () -> getTaskAttemptIdForBlockId(256, 0, 4, false, 10));
    assertEquals(
        "Observing mapIndex[256] that would produce a taskAttemptId with 11 bits "
            + "which is larger than the allowed 10 bits (maxFailures[4], speculation[false]). "
            + "Please consider providing more bits for taskAttemptIds.",
        e.getMessage());
    // check that a lower mapIndex works as expected
    assertEquals(bits("11111111|00"), getTaskAttemptIdForBlockId(255, 0, 4, false, 10));
  }

  @Test
  void testFetchAndApplyDynamicConf() {
    ClientType clientType = ClientType.GRPC;
    String coordinators = "host1,host2,host3";
    int timeout = RssSparkConfig.RSS_ACCESS_TIMEOUT_MS.defaultValue().get() / 10;

    SparkConf conf = new SparkConf();
    conf.set(RssSparkConfig.RSS_CLIENT_TYPE, clientType.toString());
    conf.set(RssSparkConfig.RSS_COORDINATOR_QUORUM, coordinators);
    conf.set(RssSparkConfig.RSS_ACCESS_TIMEOUT_MS, timeout);

    CoordinatorClientFactory mockFactoryInstance = mock(CoordinatorClientFactory.class);
    CoordinatorClient mockClient1 = mock(CoordinatorClient.class);
    CoordinatorClient mockClient2 = mock(CoordinatorClient.class);
    CoordinatorClient mockClient3 = mock(CoordinatorClient.class);

    Map<String, String> clientConf1 = ImmutableMap.of("rss.config.from", "client1");
    Map<String, String> clientConf2 = ImmutableMap.of("rss.config.from", "client2");
    Map<String, String> clientConf3 = ImmutableMap.of("rss.config.from", "client3");

    when(mockClient1.fetchClientConf(any(RssFetchClientConfRequest.class)))
        .thenReturn(new RssFetchClientConfResponse(INVALID_REQUEST, "error", clientConf1));
    when(mockClient2.fetchClientConf(any(RssFetchClientConfRequest.class)))
        .thenReturn(new RssFetchClientConfResponse(SUCCESS, "success", clientConf2));
    when(mockClient3.fetchClientConf(any(RssFetchClientConfRequest.class)))
        .thenReturn(new RssFetchClientConfResponse(SUCCESS, "success", clientConf3));

    List<CoordinatorClient> mockClients = Arrays.asList(mockClient1, mockClient2, mockClient3);
    when(mockFactoryInstance.createCoordinatorClient(clientType, coordinators))
        .thenReturn(mockClients);

    assertFalse(conf.contains("rss.config.from"));
    assertFalse(conf.contains("spark.rss.config.from"));

    try (MockedStatic<CoordinatorClientFactory> mockFactoryStatic =
        mockStatic(CoordinatorClientFactory.class)) {
      mockFactoryStatic.when(CoordinatorClientFactory::getInstance).thenReturn(mockFactoryInstance);
      long interval = conf.get(RssSparkConfig.RSS_CLIENT_RETRY_INTERVAL_MAX);
      int retry = conf.get(RssSparkConfig.RSS_CLIENT_RETRY_MAX);
      int num = conf.get(RssSparkConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM);
      when(mockFactoryInstance.createCoordinatorClient(
              clientType, coordinators, interval, retry, num))
          .thenReturn(new CoordinatorGrpcRetryableClient(mockClients, interval, retry, num));
      RssShuffleManagerBase.fetchAndApplyDynamicConf(conf);
    }

    assertFalse(conf.contains("rss.config.from"));
    assertTrue(conf.contains("spark.rss.config.from"));
    assertEquals("client2", conf.get("spark.rss.config.from"));

    ArgumentCaptor<RssFetchClientConfRequest> request1 =
        ArgumentCaptor.forClass(RssFetchClientConfRequest.class);
    ArgumentCaptor<RssFetchClientConfRequest> request2 =
        ArgumentCaptor.forClass(RssFetchClientConfRequest.class);
    ArgumentCaptor<RssFetchClientConfRequest> request3 =
        ArgumentCaptor.forClass(RssFetchClientConfRequest.class);

    verify(mockClient1, times(1)).fetchClientConf(request1.capture());
    verify(mockClient2, times(1)).fetchClientConf(request2.capture());
    verify(mockClient3, never()).fetchClientConf(request3.capture());

    assertEquals(timeout, request1.getValue().getTimeoutMs());
    assertEquals(timeout, request2.getValue().getTimeoutMs());

    verify(mockClient1, times(1)).close();
    verify(mockClient2, times(1)).close();
    verify(mockClient3, times(1)).close();
  }
}
