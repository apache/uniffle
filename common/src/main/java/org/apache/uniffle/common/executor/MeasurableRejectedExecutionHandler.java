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

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/** A handler to measure reject count. */
public class MeasurableRejectedExecutionHandler implements RejectedExecutionHandler {
  private AtomicLong counter = new AtomicLong(0L);

  private final RejectedExecutionHandler handler;

  /**
   * Constructs a wrapped {@link RejectedExecutionHandler} to measure rejected count.
   *
   * @param handler the rejected execution handler
   */
  public MeasurableRejectedExecutionHandler(RejectedExecutionHandler handler) {
    this.handler = handler;
  }

  @Override
  public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
    counter.incrementAndGet();
    if (handler != null) {
      handler.rejectedExecution(r, executor);
    }
  }

  /** @return the rejected count */
  public long getCount() {
    return counter.get();
  }
}
