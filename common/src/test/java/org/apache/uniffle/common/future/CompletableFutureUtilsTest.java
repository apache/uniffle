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

import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.exception.RssException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class CompletableFutureUtilsTest {

  @Test
  public void timeoutTest() {
    // case1: legal operation
    Supplier<Integer> supplier = () -> 1;
    try {
      int result = CompletableFutureUtils.withTimeoutCancel(supplier, 100);
      assertEquals(1, result);
    } catch (Exception e) {
      fail();
    }

    // case2: illegal
    supplier =
        () -> {
          try {
            Thread.sleep(100000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          return 10;
        };
    try {
      int result = CompletableFutureUtils.withTimeoutCancel(supplier, 100);
      fail();
    } catch (Exception e) {
      if (!(e instanceof TimeoutException)) {
        fail();
      }
    }

    // case3: fast fail when internal supplier throw exception
    supplier =
        () -> {
          throw new RssException("Hello");
        };
    try {
      int result = CompletableFutureUtils.withTimeoutCancel(supplier, 100);
      fail();
    } catch (Exception e) {
      if (e instanceof RssException) {
        // ignore
      } else {
        fail();
      }
    }
  }
}
