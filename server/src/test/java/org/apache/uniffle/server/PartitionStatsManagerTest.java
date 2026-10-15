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

package org.apache.uniffle.server;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.request.RssReportShuffleResultRequest;
import org.apache.uniffle.proto.RssProtos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionStatsManagerTest {
  @Test
  public void partitionStatsTest() {
    PartitionStatsManager manager = new PartitionStatsManager();
    RssReportShuffleResultRequest report =
        new RssReportShuffleResultRequest(
            "partitionStatsTest",
            0,
            1L,
            Collections.emptyMap(),
            1,
            Collections.singletonMap(1, 10L));
    manager.add(0, report.toProto().getPartitionStatsList());
    manager.add(0, report.toProto().getPartitionStatsList());
    manager.add(1, report.toProto().getPartitionStatsList());
    final RssProtos.PartitionStats snapshot = manager.get(0, Collections.singleton(1)).get(0);

    report =
        new RssReportShuffleResultRequest(
            "partitionStatsTest",
            0,
            1L,
            Collections.emptyMap(),
            1,
            Collections.singletonMap(1, 15L));
    manager.add(0, report.toProto().getPartitionStatsList());

    report =
        new RssReportShuffleResultRequest(
            "partitionStatsTest",
            0,
            2L,
            Collections.emptyMap(),
            1,
            Collections.singletonMap(1, 20L));
    manager.add(0, report.toProto().getPartitionStatsList());
    report =
        new RssReportShuffleResultRequest(
            "partitionStatsTest",
            0,
            1L,
            Collections.emptyMap(),
            1,
            Collections.singletonMap(2, 0L));
    manager.add(0, report.toProto().getPartitionStatsList());
    assertEquals(1, snapshot.getTaskAttemptIdToRecordsCount());
    Map<Long, Long> records =
        manager.get(0, Collections.singleton(1)).get(0).getTaskAttemptIdToRecordsList().stream()
            .collect(
                Collectors.toMap(
                    RssProtos.TaskAttemptIdToRecords::getTaskAttemptId,
                    RssProtos.TaskAttemptIdToRecords::getRecordNumber));
    assertEquals(2, records.size());
    assertEquals(15L, records.get(1L).longValue());
    assertEquals(20L, records.get(2L).longValue());
    assertEquals(
        0L,
        manager
            .get(0, Collections.singleton(2))
            .get(0)
            .getTaskAttemptIdToRecords(0)
            .getRecordNumber());
    assertTrue(manager.get(0, Collections.singleton(3)).isEmpty());
    assertTrue(manager.get(2, Collections.singleton(1)).isEmpty());
    assertTrue(manager.get(0, Collections.emptySet()).isEmpty());

    manager.remove(0);
    assertTrue(manager.get(0, Collections.singleton(1)).isEmpty());
    assertEquals(Arrays.asList(snapshot), manager.get(1, Collections.singleton(1)));
  }

  @Test
  public void partitionStatsConcurrentTest() {
    PartitionStatsManager manager = new PartitionStatsManager();
    IntStream.rangeClosed(1, 100)
        .parallel()
        .forEach(
            task -> {
              RssReportShuffleResultRequest report =
                  new RssReportShuffleResultRequest(
                      "partitionStatsConcurrentTest",
                      0,
                      task,
                      Collections.emptyMap(),
                      1,
                      Collections.singletonMap(1, (long) task));
              manager.add(0, report.toProto().getPartitionStatsList());
              manager.add(0, report.toProto().getPartitionStatsList());
            });
    RssProtos.PartitionStats stats = manager.get(0, Collections.singleton(1)).get(0);
    assertEquals(100, stats.getTaskAttemptIdToRecordsCount());
    assertEquals(
        5050L,
        stats.getTaskAttemptIdToRecordsList().stream()
            .mapToLong(RssProtos.TaskAttemptIdToRecords::getRecordNumber)
            .sum());
  }
}
