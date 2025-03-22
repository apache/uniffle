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

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Supplier for T cacheable and autocloseable with delay by using ExpiringCloseableSupplier to
 * obtain an object, manual closure may not be necessary.
 */
public class ExpiringCloseableSupplier<T extends StatefulCloseable>
    implements Supplier<T>, Serializable {
  private static final long serialVersionUID = 0;
  private static final Logger LOG = LoggerFactory.getLogger(ExpiringCloseableSupplier.class);
  private static final int DEFAULT_DELAY_CLOSE_INTERVAL = 60000;
  private static final ScheduledExecutorService executor =
      ThreadUtils.getDaemonSingleThreadScheduledExecutor("ExpiringCloseableSupplier");

  private final Supplier<T> delegate;
  private final long delayCloseInterval;

  private transient volatile ScheduledFuture<?> future;

  @SuppressFBWarnings("SE_TRANSIENT_FIELD_NOT_RESTORED")
  private transient volatile long accessTime = System.currentTimeMillis();

  private transient volatile T t;

  private ExpiringCloseableSupplier(Supplier<T> delegate, long delayCloseInterval) {
    this.delegate = delegate;
    this.delayCloseInterval = delayCloseInterval;
  }

  public synchronized T get() {
    accessTime = System.currentTimeMillis();
    if (t == null || t.isClosed()) {
      this.t = delegate.get();
      ensureCloseFutureScheduled();
    }
    return t;
  }

  public synchronized void close() {
    try {
      if (t != null && !t.isClosed()) {
        t.close();
      }
    } catch (IOException ioe) {
      LOG.warn("Failed to close {} the resource", t.getClass().getName(), ioe);
    } finally {
      this.t = null;
      this.accessTime = System.currentTimeMillis();
      cancelCloseFuture();
    }
  }

  private void tryClose() {
    if (System.currentTimeMillis() - accessTime > delayCloseInterval) {
      close();
    }
  }

  private void ensureCloseFutureScheduled() {
    cancelCloseFuture();
    this.future =
        executor.scheduleAtFixedRate(
            this::tryClose, delayCloseInterval, delayCloseInterval, TimeUnit.MILLISECONDS);
  }

  private void cancelCloseFuture() {
    if (future != null && !future.isDone()) {
      future.cancel(false);
      this.future = null;
    }
  }

  public static <T extends StatefulCloseable> ExpiringCloseableSupplier<T> of(
      Supplier<T> delegate) {
    return new ExpiringCloseableSupplier<>(delegate, DEFAULT_DELAY_CLOSE_INTERVAL);
  }

  public static <T extends StatefulCloseable> ExpiringCloseableSupplier<T> of(
      Supplier<T> delegate, long delayCloseInterval) {
    return new ExpiringCloseableSupplier<>(delegate, delayCloseInterval);
  }
}
