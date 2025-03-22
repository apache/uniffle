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

package org.apache.uniffle.common.executor;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.prometheus.client.CollectorRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.metrics.CommonMetrics;
import org.apache.uniffle.common.metrics.MetricsManager;
import org.apache.uniffle.common.util.Constants;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test ThreadPoolManager. */
public class ThreadPoolManagerTest {
  @BeforeAll
  public static void beforeAll() {
    Map<String, String> labels = Maps.newHashMap();
    labels.put(Constants.METRICS_TAG_LABEL_NAME, "test");
    MetricsManager metricsManager = new MetricsManager(CollectorRegistry.defaultRegistry, labels);
    CommonMetrics.register(metricsManager.getCollectorRegistry(), "test");
  }

  @BeforeEach
  public void before() {
    ThreadPoolManager.clear();
  }

  @AfterAll
  public static void cleanup() {
    CommonMetrics.clear();
  }

  @Test
  public void test0() {
    ThreadPoolExecutor threadPool =
        new ThreadPoolExecutor(1, 4, 60_000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    ThreadPoolManager.registerThreadPool("test", 1, 4, 60_000L, threadPool);
    testInternal(threadPool);
  }

  @Test
  public void test1() {
    ThreadPoolExecutor threadPool =
        ThreadPoolManager.newThreadPool(
            "test",
            1,
            4,
            60_000L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder().setDaemon(false).setNameFormat("test-thread-pool").build());
    testInternal(threadPool);
  }

  @Test
  public void test2() {
    ThreadPoolExecutor threadPool =
        ThreadPoolManager.newThreadPool(
            "test",
            1,
            4,
            60_000L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder().setDaemon(false).setNameFormat("test-thread-pool").build(),
            new ThreadPoolExecutor.CallerRunsPolicy());
    testInternal(threadPool);
  }

  @Test
  public void testReject() {
    ThreadPoolExecutor threadPool =
        ThreadPoolManager.newThreadPool(
            "test",
            1,
            1,
            60_000L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(1),
            new ThreadFactoryBuilder().setDaemon(false).setNameFormat("test-thread-pool").build(),
            new ThreadPoolExecutor.AbortPolicy());
    try {
      int rejectedCount = 0;
      for (int i = 0; i < 10; i++) {
        try {
          threadPool.submit(
              () -> {
                try {
                  Thread.sleep(1000L);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              });
        } catch (RejectedExecutionException e) {
          rejectedCount++;
        }
      }
      assertEquals(8, rejectedCount);
      assertEquals(
          rejectedCount,
          CommonMetrics.getCollectorRegistry()
              .getSampleValue("test_RejectCount", new String[] {"tags"}, new String[] {"test"})
              .intValue());
    } finally {
      ThreadPoolManager.unregister(threadPool);
      threadPool.shutdown();
    }
  }

  private void testInternal(ThreadPoolExecutor threadPool) {
    try {
      assertTrue(ThreadPoolManager.exists(threadPool));
      assertEquals(4, threadPool.getMaximumPoolSize());
      assertEquals(60_000L, threadPool.getKeepAliveTime(TimeUnit.MILLISECONDS));
      Map<Object, ThreadPoolManager.MeasurableThreadPoolExecutor> map =
          ThreadPoolManager.getThreadPoolMap();
      assertEquals(1, map.size());
      Map.Entry<Object, ThreadPoolManager.MeasurableThreadPoolExecutor> first =
          map.entrySet().iterator().next();
      assertEquals("test", first.getValue().getName());
      assertEquals(4, ((ThreadPoolExecutor) (first.getKey())).getMaximumPoolSize());
      assertEquals(1, ((ThreadPoolExecutor) (first.getKey())).getCorePoolSize());
    } finally {
      ThreadPoolManager.unregister(threadPool);
      assertTrue(!ThreadPoolManager.exists(threadPool));
      threadPool.shutdown();
    }
  }
}
