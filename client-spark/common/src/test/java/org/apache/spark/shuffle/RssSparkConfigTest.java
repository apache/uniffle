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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.writer.BufferManagerOptions;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.ConfigUtils;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RssSparkConfigTest {
  @Test
  void testConfigurationKeysAreDefinedOnce() {
    Set<String> keys = new HashSet<>();
    for (Class<?> configClass :
        new Class<?>[] {RssBaseConf.class, RssClientConf.class, RssSparkConfig.class}) {
      for (ConfigOption<?> option : ConfigUtils.getAllConfigOptions(configClass)) {
        assertTrue(keys.add(option.key()), option.key());
      }
    }
  }

  @Test
  void testSparkKeysAndDefaults() {
    SparkConf sparkConf = new SparkConf(false);
    RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);
    for (ConfigOption<?> option :
        Stream.concat(
                ConfigUtils.getAllConfigOptions(RssClientConf.class).stream(),
                ConfigUtils.getAllConfigOptions(RssSparkConfig.class).stream())
            .collect(Collectors.toList())) {
      assertTrue(option.key().startsWith("rss."), option.key());
      assertFalse(rssConf.contains(option));
      assertEquals(option.defaultValue(), rssConf.get(option));
    }
    assertEquals(0, sparkConf.getAll().length);
    assertNull(RssSparkShuffleUtils.getCoordinatorQuorumStr(sparkConf));
    assertThrows(
        RssException.class,
        () -> RssSparkShuffleUtils.createCoordinatorClientsWithoutHeartbeat(sparkConf));
    assertNull(rssConf.get(RssClientConf.SHUFFLE_MANAGER_GRPC_PORT));

    // Use literal public keys so that a change to key generation cannot hide a regression.
    sparkConf.set("spark.rss.client.retry.max", "7");
    sparkConf.set("spark.rss.heartbeat.interval", "12000");
    sparkConf.set("spark.rss.enabled", "true");
    sparkConf.set("spark.rss.test.mode.enable", "true");
    sparkConf.set("spark.rss.estimate.task.concurrency.dynamic.factor", "0.25");
    sparkConf.set("spark.rss.client.type", "GRPC");
    sparkConf.set("spark.driver.host", "localhost");
    sparkConf.set("rss.client.retry.max", "99");
    rssConf = RssSparkConfig.toRssConf(sparkConf);
    assertEquals(7, rssConf.get(RssClientConf.RSS_CLIENT_RETRY_MAX));
    assertEquals(12000L, rssConf.get(RssClientConf.RSS_HEARTBEAT_INTERVAL));
    assertTrue(rssConf.get(RssSparkConfig.RSS_ENABLED));
    assertTrue(rssConf.get(RssBaseConf.RSS_TEST_MODE_ENABLE));
    assertEquals(0.25, rssConf.get(RssClientConf.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR));
    assertEquals("GRPC", rssConf.get(RssClientConf.RSS_CLIENT_TYPE).name());
    assertEquals("localhost", rssConf.getString("driver.host", ""));
    assertFalse(rssConf.containsKey("spark.rss.client.retry.max"));
  }

  @Test
  void testBufferSizes() {
    SparkConf sparkConf = new SparkConf(false);
    BufferManagerOptions defaults = new BufferManagerOptions(sparkConf);
    assertEquals(3 * 1024 * 1024, defaults.getBufferSize());
    assertEquals(3 * 1024, defaults.getSerializerBufferSize());
    assertEquals(3 * 1024, defaults.getBufferSegmentSize());
    assertEquals(128 * 1024 * 1024, defaults.getBufferSpillThreshold());
    assertEquals(16 * 1024 * 1024, defaults.getPreAllocatedBufferSize());
    sparkConf.set("spark.rss.writer.buffer.size", "4m");
    sparkConf.set("spark.rss.writer.serializer.buffer.size", "128k");
    sparkConf.set("spark.rss.writer.buffer.segment.size", "256k");
    sparkConf.set("spark.rss.writer.buffer.spill.size", "32m");
    sparkConf.set("spark.rss.writer.pre.allocated.buffer.size", "1g");
    sparkConf.set("spark.rss.client.memory.spill.ratio", "0.5");
    BufferManagerOptions options = new BufferManagerOptions(sparkConf);
    assertEquals(4 * 1024 * 1024, options.getBufferSize());
    assertEquals(128 * 1024, options.getSerializerBufferSize());
    assertEquals(256 * 1024, options.getBufferSegmentSize());
    assertEquals(32 * 1024 * 1024, options.getBufferSpillThreshold());
    assertEquals(1024 * 1024 * 1024, options.getPreAllocatedBufferSize());
    assertEquals(0.5, options.getBufferSpillPercent());
    sparkConf.set("spark.rss.writer.buffer.size", "4g");
    assertEquals(Integer.MAX_VALUE, new BufferManagerOptions(sparkConf).getBufferSize());
    sparkConf.set("spark.rss.writer.buffer.size", "invalid");
    assertThrows(IllegalArgumentException.class, () -> new BufferManagerOptions(sparkConf));
  }

  @Test
  void testConversionPreservesLiteralValues() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set("spark.rss.client.retry.max", "7");
    sparkConf.set("spark.rss.access.id", "${spark.rss.client.retry.max}");
    RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);
    assertEquals(7, rssConf.get(RssClientConf.RSS_CLIENT_RETRY_MAX));
    assertEquals("${spark.rss.client.retry.max}", rssConf.get(RssSparkConfig.RSS_ACCESS_ID));
  }

  @Test
  void testDynamicConfigAndExecutorPropagation() {
    SparkConf driverConf = new SparkConf(false);
    driverConf.set("spark.rss.storage.type", "MEMORY_LOCALFILE");
    driverConf.set("spark.rss.remote.storage.path", "hdfs://client/path");
    driverConf.set("spark.rss.client.retry.max", "3");
    Map<String, String> dynamicConf = new HashMap<>();
    dynamicConf.put("rss.storage.type", "MEMORY_HDFS");
    dynamicConf.put("spark.rss.remote.storage.path", "hdfs://cluster/path");
    dynamicConf.put("rss.client.retry.max", "6");
    RssSparkShuffleUtils.applyDynamicClientConf(driverConf, dynamicConf);
    driverConf.set(RssSparkConfig.toSparkConfKey(RssSparkConfig.RSS_ENABLED), "true");
    driverConf.set(RssSparkConfig.toSparkConfKey(RssClientConf.SHUFFLE_MANAGER_GRPC_PORT), "12345");
    assertEquals("12345", driverConf.get("spark.rss.shuffle.manager.grpc.port"));
    SparkConf executorConf = driverConf.clone();
    RssConf rssConf = RssSparkConfig.toRssConf(executorConf);
    assertEquals("MEMORY_HDFS", rssConf.get(RssBaseConf.RSS_STORAGE_TYPE).name());
    assertEquals("hdfs://cluster/path", rssConf.get(RssClientConf.RSS_REMOTE_STORAGE_PATH));
    assertEquals(3, rssConf.get(RssClientConf.RSS_CLIENT_RETRY_MAX));
    assertEquals(12345, rssConf.get(RssClientConf.SHUFFLE_MANAGER_GRPC_PORT));
    assertTrue(rssConf.get(RssSparkConfig.RSS_ENABLED));
  }
}
