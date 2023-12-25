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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadUtils.class);

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
      Function<Future<R>, R> futureHandler) {
    List<Callable<R>> callableList =
        items.stream()
            .map(item -> (Callable<R>) () -> task.apply(item))
            .collect(Collectors.toList());
    try {
      List<Future<R>> futures =
          executorService.invokeAll(callableList, timeoutMs, TimeUnit.MILLISECONDS);
      return futures.stream().map(futureHandler).collect(Collectors.toList());
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
      String taskMsg) {
    return executeTasks(
        executorService,
        items,
        task,
        timeoutMs,
        taskMsg,
        future -> {
          try {
            if (future.isDone()) {
              return future.get(timeoutMs, TimeUnit.MILLISECONDS);
            } else {
              future.cancel(true);
              return null;
            }
          } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOGGER.warn("Error getting future result", e);
            return null;
          }
        });
  }
}
