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

import java.io.Closeable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ReconfigurableRegistry;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.metrics.CommonMetrics;

/** The threadPool manager which represents a manager to handle all thread pool executors. */
public class ThreadPoolManager {
  private static final Logger LOG = LoggerFactory.getLogger(ThreadPoolManager.class);

  private static final Map<Object, MeasurableThreadPoolExecutor> THREAD_POOL_MAP =
      new ConcurrentHashMap<>();

  /**
   * Add a thread pool.
   *
   * @param name the name of the thread pool
   * @param corePoolSize the core pool size
   * @param maximumPoolSize the maximum pool size
   * @param keepAliveTime the keep alive time
   * @param unit the unit
   * @param workQueue the work queue
   * @param threadFactory the thread factory
   * @return the registered thread pool
   */
  @VisibleForTesting
  public static ThreadPoolExecutor newThreadPool(
      String name,
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory) {
    return newThreadPool(
        name,
        () -> corePoolSize,
        () -> maximumPoolSize,
        () -> keepAliveTime,
        unit,
        workQueue,
        threadFactory);
  }

  /**
   * Add a thread pool.
   *
   * @param name the name of the thread pool
   * @param corePoolSize the core pool size
   * @param maximumPoolSize the maximum pool size
   * @param keepAliveTime the keep alive time
   * @param unit the unit
   * @param workQueue the work queue
   * @param threadFactory the thread factory
   * @param handler the handler to use when execution is blocked because the thread bounds and queue
   *     capacities are reached
   * @return the registered thread pool
   */
  @VisibleForTesting
  public static ThreadPoolExecutor newThreadPool(
      String name,
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory,
      RejectedExecutionHandler handler) {
    return newThreadPool(
        name,
        () -> corePoolSize,
        () -> maximumPoolSize,
        () -> keepAliveTime,
        unit,
        workQueue,
        threadFactory,
        handler);
  }

  /**
   * Add a thread pool.
   *
   * @param name the name of the thread pool
   * @param corePoolSizeSupplier the core pool size supplier
   * @param maximumPoolSizeSupplier the maximum pool size supplier
   * @param keepAliveTimeSupplier the keep alive time supplier
   * @param unit the unit
   * @param workQueue the work queue
   * @param threadFactory the thread factory
   * @return the registered thread pool
   */
  public static ThreadPoolExecutor newThreadPool(
      String name,
      Supplier<Integer> corePoolSizeSupplier,
      Supplier<Integer> maximumPoolSizeSupplier,
      Supplier<Long> keepAliveTimeSupplier,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory) {
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(
            corePoolSizeSupplier.get(),
            maximumPoolSizeSupplier.get(),
            keepAliveTimeSupplier.get(),
            unit,
            workQueue,
            threadFactory);
    registerThreadPool(
        name,
        corePoolSizeSupplier,
        maximumPoolSizeSupplier,
        () -> keepAliveTimeSupplier.get() * unit.toMillis(1),
        threadPoolExecutor);
    return threadPoolExecutor;
  }

  /**
   * Add a thread pool.
   *
   * @param name the name of the thread pool
   * @param corePoolSizeSupplier the core pool size supplier
   * @param maximumPoolSizeSupplier the maximum pool size supplier
   * @param keepAliveTimeSupplier the keep alive time supplier
   * @param unit the unit
   * @param workQueue the work queue
   * @param threadFactory the thread factory
   * @param handler the handler to use when execution is blocked because the thread bounds and queue
   *     capacities are reached
   * @return the registered thread pool
   */
  public static ThreadPoolExecutor newThreadPool(
      String name,
      Supplier<Integer> corePoolSizeSupplier,
      Supplier<Integer> maximumPoolSizeSupplier,
      Supplier<Long> keepAliveTimeSupplier,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory,
      RejectedExecutionHandler handler) {
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(
            corePoolSizeSupplier.get(),
            maximumPoolSizeSupplier.get(),
            keepAliveTimeSupplier.get(),
            unit,
            workQueue,
            threadFactory,
            handler);
    registerThreadPool(
        name,
        corePoolSizeSupplier,
        maximumPoolSizeSupplier,
        keepAliveTimeSupplier,
        threadPoolExecutor);
    return threadPoolExecutor;
  }

  /**
   * Add a thread pool.
   *
   * @param name the name of the thread pool
   * @param corePoolSize the core pool size
   * @param maximumPoolSize the maximum pool size
   * @param keepAliveTime the keep alive time
   * @param threadPoolExecutor the thread pool which will be registered
   * @return the registered thread pool
   */
  @VisibleForTesting
  public static void registerThreadPool(
      String name,
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      ThreadPoolExecutor threadPoolExecutor) {
    registerThreadPool(
        name, () -> corePoolSize, () -> maximumPoolSize, () -> keepAliveTime, threadPoolExecutor);
  }

  /**
   * Register a thread pool to THREAD_POOL_MAP.
   *
   * @param name the name of the thread pool
   * @param corePoolSizeSupplier the core pool size supplier
   * @param maximumPoolSizeSupplier the maximum pool size supplier
   * @param keepAliveTimeSupplier the keep alive time supplier
   * @param threadPoolExecutor the thread pool which will be registered
   */
  public static void registerThreadPool(
      String name,
      Supplier<Integer> corePoolSizeSupplier,
      Supplier<Integer> maximumPoolSizeSupplier,
      Supplier<Long> keepAliveTimeSupplier,
      ThreadPoolExecutor threadPoolExecutor) {
    THREAD_POOL_MAP.put(
        threadPoolExecutor,
        new MeasurableThreadPoolExecutor(
            name,
            threadPoolExecutor,
            corePoolSizeSupplier,
            maximumPoolSizeSupplier,
            keepAliveTimeSupplier));
    LOG.info(
        "{} thread pool, core size:{}, max size:{}, keep alive time:{}",
        name,
        corePoolSizeSupplier,
        maximumPoolSizeSupplier,
        keepAliveTimeSupplier);
  }

  /**
   * Unregister the thread pool executor related to the given key.
   *
   * @param key the key of thread pool executor to unregister
   */
  public static void unregister(Object key) {
    MeasurableThreadPoolExecutor measurableThreadPoolExecutor = THREAD_POOL_MAP.remove(key);
    if (measurableThreadPoolExecutor != null) {
      measurableThreadPoolExecutor.close();
    }
  }

  public static boolean exists(Object key) {
    return THREAD_POOL_MAP.containsKey(key);
  }

  @VisibleForTesting
  public static Map<Object, MeasurableThreadPoolExecutor> getThreadPoolMap() {
    return Collections.unmodifiableMap(THREAD_POOL_MAP);
  }

  @VisibleForTesting
  public static void clear() {
    for (MeasurableThreadPoolExecutor executor : THREAD_POOL_MAP.values()) {
      executor.close();
    }
    THREAD_POOL_MAP.clear();
  }

  @VisibleForTesting
  public static class MeasurableThreadPoolExecutor
      implements Closeable, ReconfigurableRegistry.ReconfigureListener {
    private final String name;
    private final ThreadPoolExecutor threadPoolExecutor;
    private final Supplier<Integer> corePoolSizeSupplier;
    private final Supplier<Integer> maximumPoolSizeSupplier;
    private final Supplier<Long> keepAliveTimeSupplier;

    MeasurableThreadPoolExecutor(
        String name,
        ThreadPoolExecutor threadPoolExecutor,
        Supplier<Integer> corePoolSizeSupplier,
        Supplier<Integer> maximumPoolSizeSupplier,
        Supplier<Long> keepAliveTimeSupplier) {
      this.name = name;
      this.threadPoolExecutor = threadPoolExecutor;
      this.corePoolSizeSupplier = corePoolSizeSupplier;
      this.maximumPoolSizeSupplier = maximumPoolSizeSupplier;
      this.keepAliveTimeSupplier = keepAliveTimeSupplier;

      MeasurableRejectedExecutionHandler measurableRejectedExecutionHandler =
          new MeasurableRejectedExecutionHandler(threadPoolExecutor.getRejectedExecutionHandler());
      threadPoolExecutor.setRejectedExecutionHandler(measurableRejectedExecutionHandler);
      CommonMetrics.addLabeledGauge(
          name + "_ThreadActiveCount", threadPoolExecutor::getActiveCount);
      CommonMetrics.addLabeledGauge(name + "_ThreadCurrentCount", threadPoolExecutor::getPoolSize);
      CommonMetrics.addLabeledGauge(
          name + "_ThreadMaxCount", threadPoolExecutor::getMaximumPoolSize);
      CommonMetrics.addLabeledGauge(name + "_ThreadMinCount", threadPoolExecutor::getCorePoolSize);
      CommonMetrics.addLabeledGauge(
          name + "_CompleteTaskCount", threadPoolExecutor::getCompletedTaskCount);
      CommonMetrics.addLabeledGauge(
          name + "_ThreadQueueWaitingTaskCount", threadPoolExecutor.getQueue()::size);
      CommonMetrics.addLabeledGauge(
          name + "_RejectCount", measurableRejectedExecutionHandler::getCount);

      ReconfigurableRegistry.register(this);
    }

    @VisibleForTesting
    public String getName() {
      return name;
    }

    @Override
    public void close() {
      ReconfigurableRegistry.unregister(this);
      CommonMetrics.unregisterSupplierGauge(name + "_ThreadActiveCount");
      CommonMetrics.unregisterSupplierGauge(name + "_ThreadCurrentCount");
      CommonMetrics.unregisterSupplierGauge(name + "_ThreadMaxCount");
      CommonMetrics.unregisterSupplierGauge(name + "_ThreadMinCount");
      CommonMetrics.unregisterSupplierGauge(name + "_CompleteTaskCount");
      CommonMetrics.unregisterSupplierGauge(name + "_ThreadQueueWaitingTaskCount");
      CommonMetrics.unregisterSupplierGauge(name + "_RejectCount");
    }

    @Override
    public void update(RssConf conf, Set<String> changedProperties) {
      int newCorePoolSize = this.corePoolSizeSupplier.get();
      int newMaximumPoolSize = this.maximumPoolSizeSupplier.get();
      if (this.keepAliveTimeSupplier != null) {
        long keepAliveTime = keepAliveTimeSupplier.get();
        if (keepAliveTime > 0
            && keepAliveTime != threadPoolExecutor.getKeepAliveTime(TimeUnit.MILLISECONDS)) {
          LOG.info(
              "Updated thread pool {} keep alive time from {} to {}",
              name,
              threadPoolExecutor.getKeepAliveTime(TimeUnit.MILLISECONDS),
              keepAliveTime);
          threadPoolExecutor.setKeepAliveTime(keepAliveTime, TimeUnit.MILLISECONDS);
        }
      }
      if (newCorePoolSize != threadPoolExecutor.getPoolSize()
          && newMaximumPoolSize != threadPoolExecutor.getMaximumPoolSize()) {
        LOG.info(
            "Updated thread pool {} MaximumPoolSize from {} to {}",
            name,
            threadPoolExecutor.getMaximumPoolSize(),
            newMaximumPoolSize);
        LOG.info(
            "Updated thread pool {} CorePoolSize from {} to {}",
            name,
            threadPoolExecutor.getCorePoolSize(),
            newCorePoolSize);
        if (newCorePoolSize > threadPoolExecutor.getMaximumPoolSize()) {
          threadPoolExecutor.setMaximumPoolSize(newMaximumPoolSize);
          threadPoolExecutor.setCorePoolSize(newCorePoolSize);
        } else {
          threadPoolExecutor.setCorePoolSize(newCorePoolSize);
          threadPoolExecutor.setMaximumPoolSize(newMaximumPoolSize);
        }
      } else if (newMaximumPoolSize != threadPoolExecutor.getMaximumPoolSize()) {
        LOG.info(
            "Updated thread pool {} MaximumPoolSize from {} to {}",
            name,
            threadPoolExecutor.getMaximumPoolSize(),
            newMaximumPoolSize);
        threadPoolExecutor.setMaximumPoolSize(newMaximumPoolSize);
      } else if (newCorePoolSize != threadPoolExecutor.getCorePoolSize()) {
        LOG.info(
            "Updated thread pool {} CorePoolSize from {} to {}",
            name,
            threadPoolExecutor.getCorePoolSize(),
            newCorePoolSize);
        threadPoolExecutor.setCorePoolSize(newCorePoolSize);
      }
    }
  }
}
