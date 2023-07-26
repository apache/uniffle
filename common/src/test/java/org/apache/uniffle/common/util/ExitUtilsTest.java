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

import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.util.ExitUtils.ExitException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class ExitUtilsTest {

  @Test
  public void test() throws Exception {
    final int status = -1;
    final String testExitMessage = "testExitMessage";
    try {
      ExitUtils.disableSystemExit();
      ExitUtils.terminate(status, testExitMessage, null, null);
      fail();
    } catch (ExitException e) {
      assertEquals(status, e.getStatus());
      assertEquals(testExitMessage, e.getMessage());
    }

    final Thread t =
        new Thread(
            null,
            () -> {
              throw new AssertionError("TestUncaughtException");
            },
            "testThread");
    t.start();
    t.join();
  }
}
