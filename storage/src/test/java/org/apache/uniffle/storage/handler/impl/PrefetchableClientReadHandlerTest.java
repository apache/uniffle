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

package org.apache.uniffle.storage.handler.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.exception.RssException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class PrefetchableClientReadHandlerTest {

  class MockedHandler extends PrefetchableClientReadHandler {
    private AtomicInteger maxReadLoopNum;
    private boolean markTimeout;
    private boolean markFetchFailure;
    private boolean markEmptyResult;

    MockedHandler(
        Optional<PrefetchOption> option,
        AtomicInteger maxReadLoopNum,
        boolean markTimeout,
        boolean markFetchFailure) {
      this(option, maxReadLoopNum, markTimeout, markFetchFailure, false);
    }

    MockedHandler(
        Optional<PrefetchOption> option,
        AtomicInteger maxReadLoopNum,
        boolean markTimeout,
        boolean markFetchFailure,
        boolean markEmptyResult) {
      super(option);
      this.maxReadLoopNum = maxReadLoopNum;
      this.markTimeout = markTimeout;
      this.markFetchFailure = markFetchFailure;
      this.markEmptyResult = markEmptyResult;
    }

    @Override
    protected ShuffleDataResult doReadShuffleData() {
      if (markEmptyResult) {
        maxReadLoopNum.decrementAndGet();
        return new ShuffleDataResult();
      }

      if (markFetchFailure) {
        throw new RssException("");
      }

      if (markTimeout) {
        try {
          Thread.sleep(2 * 1000L);
        } catch (Exception e) {
          // ignore
        }
      }
      if (maxReadLoopNum.get() > 0) {
        maxReadLoopNum.decrementAndGet();
        List<BufferSegment> segments = new ArrayList<>();
        segments.add(new BufferSegment(1, 1, 1, 1, 1, 1));
        return new ShuffleDataResult(new byte[10], segments);
      }
      return null;
    }
  }

  @Test
  public void testWithPrefetchWithEmptyResult() {
    AtomicInteger maxReadLoopNum = new AtomicInteger(10);
    PrefetchableClientReadHandler handler =
        new MockedHandler(
            Optional.of(new PrefetchableClientReadHandler.PrefetchOption(4, 10)),
            maxReadLoopNum,
            false,
            false,
            true);
    assertTrue(handler.readShuffleData().isEmpty());
    Awaitility.await().until(() -> handler.isFinished());
    assertEquals(9, maxReadLoopNum.get());
  }

  @Test
  public void testWithPrefetch() {
    PrefetchableClientReadHandler handler =
        new MockedHandler(
            Optional.of(new PrefetchableClientReadHandler.PrefetchOption(4, 10)),
            new AtomicInteger(10),
            false,
            false);
    int counter = 0;
    while (true) {
      if (handler.readShuffleData() != null) {
        counter += 1;
      } else {
        break;
      }
    }
    assertEquals(10, counter);
  }

  @Test
  public void testWithPrefetchButTimeout() {
    try {
      PrefetchableClientReadHandler handler =
          new MockedHandler(
              Optional.of(new PrefetchableClientReadHandler.PrefetchOption(4, 1)),
              new AtomicInteger(10),
              true,
              false);
      handler.readShuffleData();
      fail();
    } catch (Exception e) {
      // ignore
    }
  }

  @Test
  public void testWithPrefetchButFailure() {
    try {
      PrefetchableClientReadHandler handler =
          new MockedHandler(
              Optional.of(new PrefetchableClientReadHandler.PrefetchOption(4, 1)),
              new AtomicInteger(10),
              false,
              true);
      handler.readShuffleData();
      fail();
    } catch (Exception e) {
      // ignore
    }
  }

  @Test
  public void testWithoutPrefetch() {
    PrefetchableClientReadHandler handler =
        new MockedHandler(Optional.empty(), new AtomicInteger(10), true, false);
    handler.readShuffleData();
  }
}
