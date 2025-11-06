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

package org.apache.uniffle.shuffle;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.config.RssConf;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_CLIENT_INTEGRITY_VALIDATION_STATS_COMPRESSION_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShuffleWriteTaskStatsTest {

  private static Stream<Arguments> codecType() {
    RssConf conf1 = new RssConf();
    conf1.set(RSS_CLIENT_INTEGRITY_VALIDATION_STATS_COMPRESSION_TYPE, Codec.Type.LZ4);
    RssConf conf2 = new RssConf();
    RssConf conf3 = new RssConf();
    conf3.set(RSS_CLIENT_INTEGRITY_VALIDATION_STATS_COMPRESSION_TYPE, Codec.Type.NONE);
    return Stream.of(Arguments.of(conf1), Arguments.of(conf2), Arguments.of(conf3));
  }

  @ParameterizedTest
  @MethodSource("codecType")
  public void testValidValidationInfo(RssConf rssConf) {
    long taskId = 10;
    long taskAttemptId = 12345L;
    ShuffleWriteTaskStats stats = new ShuffleWriteTaskStats(rssConf, 2, taskAttemptId, taskId);
    stats.incPartitionRecord(0);
    stats.incPartitionRecord(1);

    stats.incPartitionBlock(0);
    stats.incPartitionBlock(1);

    String encoded = stats.encode();
    System.out.println("Encoded length: " + encoded.length());
    ShuffleWriteTaskStats decoded = ShuffleWriteTaskStats.decode(rssConf, encoded);

    assertEquals(10, stats.getTaskId());

    assertEquals(taskAttemptId, decoded.getTaskAttemptId());
    assertEquals(1, decoded.getRecordsWritten(0));
    assertEquals(1, decoded.getRecordsWritten(1));
  }
}
