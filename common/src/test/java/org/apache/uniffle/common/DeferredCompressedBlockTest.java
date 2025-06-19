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

package org.apache.uniffle.common;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeferredCompressedBlockTest {

  @Test
  public void testDeferredCompressedBlock() {
    AtomicBoolean isInitialized = new AtomicBoolean(false);
    DeferredCompressedBlock block =
        new DeferredCompressedBlock(
            1,
            1,
            1,
            null,
            0,
            1,
            1,
            null,
            deferredCompressedBlock -> {
              isInitialized.set(true);
              deferredCompressedBlock.reset(new byte[10], 10, 10);
              return deferredCompressedBlock;
            },
            10);

    // case1: some params accessing won't trigger initialization
    block.getBlockId();
    assertFalse(isInitialized.get());

    // case2
    block.getLength();
    assertTrue(isInitialized.get());
  }
}
