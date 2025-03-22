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

package org.apache.uniffle.common.future;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.ThreadUtils;

public class CompletableFutureUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompletableFutureUtils.class);

  public static <T> T withTimeoutCancel(Supplier<T> supplier, long timeoutMills) throws Exception {
    return withTimeoutCancel(supplier, timeoutMills, "");
  }

  public static <T> T withTimeoutCancel(
      Supplier<T> supplier, long timeoutMills, String operationName) throws Exception {
    CompletableFuture<T> future =
        CompletableFuture.supplyAsync(
            supplier,
            Executors.newSingleThreadExecutor(ThreadUtils.getThreadFactory(operationName)));
    future.exceptionally(
        throwable -> {
          throw new RssException(throwable);
        });

    CompletableFuture<T> extended =
        CompletableFutureExtension.orTimeout(future, timeoutMills, TimeUnit.MILLISECONDS);
    try {
      return extended.get();
    } catch (Exception e) {
      if (e instanceof ExecutionException) {
        Throwable internalThrowable = e.getCause();
        if (internalThrowable instanceof TimeoutException) {
          LOGGER.error(
              "The operation of [{}] haven't finished in the {}(millis). Drop this execution!",
              operationName,
              timeoutMills);
          throw new TimeoutException();
        }
        if (internalThrowable instanceof Exception) {
          throw (Exception) internalThrowable;
        }
      }
      throw e;
    }
  }
}
