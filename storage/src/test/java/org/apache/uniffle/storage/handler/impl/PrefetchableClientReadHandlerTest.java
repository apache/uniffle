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

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.exception.RssException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class PrefetchableClientReadHandlerTest {

  class MockedHandler extends PrefetchableClientReadHandler {
    private AtomicInteger readNum;
    private boolean markTimeout;
    private boolean markFetchFailure;

    MockedHandler(
        Optional<PrefetchOption> option,
        int readNum,
        boolean markTimeout,
        boolean markFetchFailure) {
      super(option);
      this.readNum = new AtomicInteger(readNum);
      this.markTimeout = markTimeout;
      this.markFetchFailure = markFetchFailure;
    }

    @Override
    protected ShuffleDataResult doReadShuffleData() {
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
      if (readNum.get() > 0) {
        readNum.decrementAndGet();
        return new ShuffleDataResult();
      }
      return null;
    }
  }

  @Test
  public void test_with_prefetch() {
    PrefetchableClientReadHandler handler =
        new MockedHandler(
            Optional.of(new PrefetchableClientReadHandler.PrefetchOption(4, 10)), 10, false, false);
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
  public void test_with_timeout() {
    try {
      PrefetchableClientReadHandler handler =
          new MockedHandler(
              Optional.of(new PrefetchableClientReadHandler.PrefetchOption(4, 1)), 10, true, false);
      handler.readShuffleData();
      fail();
    } catch (Exception e) {
      // ignore
    }
  }

  @Test
  public void test_with_fetch_failure() {
    try {
      PrefetchableClientReadHandler handler =
          new MockedHandler(
              Optional.of(new PrefetchableClientReadHandler.PrefetchOption(4, 1)), 10, false, true);
      handler.readShuffleData();
      fail();
    } catch (Exception e) {
      // ignore
    }
  }

  @Test
  public void test_without_prefetch() {
    PrefetchableClientReadHandler handler = new MockedHandler(Optional.empty(), 10, true, false);
    handler.readShuffleData();
  }
}
