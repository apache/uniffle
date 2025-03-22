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

package org.apache.uniffle.common.util;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadUtils.class);
  private static final ThreadMXBean THREAD_BEAN = ManagementFactory.getThreadMXBean();

  /** Provide a general method to create a thread factory to make the code more standardized */
  public static ThreadFactory getThreadFactory(String factoryName) {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(factoryName + "-%d").build();
  }

  /** Creates a new ThreadFactory which prefixes each thread with the given name. */
  public static ThreadFactory getNettyThreadFactory(String threadPoolPrefix) {
    return new DefaultThreadFactory(threadPoolPrefix, true);
  }

  /**
   * Encapsulation of the ScheduledExecutorService
   *
   * @param factoryName Prefix name of each thread from this threadPool
   * @return ScheduledExecutorService
   */
  public static ScheduledExecutorService getDaemonSingleThreadScheduledExecutor(
      String factoryName) {
    ScheduledThreadPoolExecutor executor =
        new ScheduledThreadPoolExecutor(1, getThreadFactory(factoryName));
    executor.setRemoveOnCancelPolicy(true);
    return executor;
  }

  /**
   * Encapsulation of the newFixedThreadPool
   *
   * @param threadNum Number of core threads
   * @param factoryName Prefix name of each thread from this threadPool
   * @return ExecutorService
   */
  public static ExecutorService getDaemonFixedThreadPool(int threadNum, String factoryName) {
    return Executors.newFixedThreadPool(threadNum, getThreadFactory(factoryName));
  }

  /** Encapsulation of the newSingleThreadExecutor */
  public static ExecutorService getDaemonSingleThreadExecutor(String factoryName) {
    return Executors.newSingleThreadExecutor(getThreadFactory(factoryName));
  }

  /** Encapsulation of the newCachedThreadPool */
  public static ExecutorService getDaemonCachedThreadPool(String factoryName) {
    return Executors.newCachedThreadPool(getThreadFactory(factoryName));
  }

  public static void shutdownThreadPool(ExecutorService threadPool, int waitSec)
      throws InterruptedException {
    if (threadPool == null) {
      return;
    }
    threadPool.shutdown();
    if (!threadPool.awaitTermination(waitSec, TimeUnit.SECONDS)) {
      threadPool.shutdownNow();
      if (!threadPool.awaitTermination(waitSec, TimeUnit.SECONDS)) {
        LOGGER.warn("Thread pool don't stop gracefully.");
      }
    }
  }

  public static <T, R> List<R> executeTasks(
      ExecutorService executorService,
      Collection<T> items,
      Function<T, R> task,
      long timeoutMs,
      String taskMsg,
      String timeoutMsg,
      Function<Future<R>, R> futureHandler) {
    List<Callable<R>> callableList =
        items.stream()
            .map(item -> (Callable<R>) () -> task.apply(item))
            .collect(Collectors.toList());
    try {
      List<Future<R>> futures =
          executorService.invokeAll(callableList, timeoutMs, TimeUnit.MILLISECONDS);
      AtomicInteger cancelled = new AtomicInteger();
      List<R> result =
          futures.stream()
              .map(
                  future -> {
                    // api doc says all futures are done, but better be safe here
                    if (!future.isDone()) {
                      future.cancel(true);
                    }
                    // detect cancelled tasks (timeout)
                    if (future.isCancelled()) {
                      cancelled.incrementAndGet();
                    }
                    // do not replace this map with peek as peek is for debug purposes and may be
                    // optimized away
                    return future;
                  })
              .map(futureHandler)
              .collect(Collectors.toList());
      if (cancelled.get() > 0) {
        if (timeoutMsg != null) {
          timeoutMsg = " " + timeoutMsg;
        } else {
          timeoutMsg = "";
        }
        LOGGER.warn(
            "Executing {} observed timeout of {}ms, {} out of {} tasks cancelled.{}",
            taskMsg,
            timeoutMs,
            cancelled.get(),
            items.size(),
            timeoutMsg);
      }
      return result;
    } catch (InterruptedException ie) {
      LOGGER.warn("Execute " + taskMsg + " is interrupted", ie);
      return Collections.emptyList();
    }
  }

  public static <T, R> List<R> executeTasks(
      ExecutorService executorService,
      Collection<T> items,
      Function<T, R> task,
      long timeoutMs,
      String taskMsg,
      Function<Future<R>, R> futureHandler) {
    return executeTasks(executorService, items, task, timeoutMs, taskMsg, null, futureHandler);
  }

  public static <T, R> List<R> executeTasks(
      ExecutorService executorService,
      Collection<T> items,
      Function<T, R> task,
      long timeoutMs,
      String taskMsg,
      String timeoutMsg) {
    return executeTasks(
        executorService, items, task, timeoutMs, taskMsg, timeoutMsg, future -> null);
  }

  public static <T, R> List<R> executeTasks(
      ExecutorService executorService,
      Collection<T> items,
      Function<T, R> task,
      long timeoutMs,
      String taskMsg) {
    return executeTasks(executorService, items, task, timeoutMs, taskMsg, future -> null);
  }

  public static synchronized void printThreadInfo(StringBuilder builder, String title) {
    builder.append("Process Thread Dump: " + title + "\n");
    builder.append(THREAD_BEAN.getThreadCount() + " active threads\n");
    long[] threadIds = THREAD_BEAN.getAllThreadIds();
    for (long id : threadIds) {
      ThreadInfo info = THREAD_BEAN.getThreadInfo(id, Integer.MAX_VALUE);
      if (info == null) {
        // The thread is no longer active, ignore
        continue;
      }
      builder.append(info + "\n");
    }
  }
}
