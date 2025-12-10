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

package org.apache.uniffle.shuffle;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BlockStatsTest {

  private static Set<Long> setsOf(long... vars) {
    Set<Long> set = new HashSet<>();
    for (long var : vars) {
      set.add(var);
    }
    return set;
  }

  @Test
  public void testConstructor() {
    BlockStats stats = new BlockStats();
    assertEquals(0, stats.getRecordNumber());
    assertTrue(stats.getBlockIds().isEmpty());

    BlockStats stats2 = new BlockStats(10, 100L);
    assertEquals(10, stats2.getRecordNumber());
    assertEquals(setsOf(100L), stats2.getBlockIds());
  }

  @Test
  public void testMerge() {
    BlockStats s1 = new BlockStats(10, 1L);
    BlockStats s2 = new BlockStats(5, 2L);

    s1.merge(s2);

    assertEquals(15, s1.getRecordNumber());
    assertEquals(setsOf(1L, 2L), s1.getBlockIds());
  }

  @Test
  public void testRemove() {
    BlockStats s1 = new BlockStats(20, 1L);
    BlockStats s2 = new BlockStats(5, 1L);

    s1.remove(s2);

    assertEquals(15, s1.getRecordNumber());
    assertTrue(s1.getBlockIds().isEmpty());
  }

  @Test
  public void testRemoveMultipleBlocks() {
    BlockStats base = new BlockStats(30, 1L);
    base.getBlockIds().add(2L);

    BlockStats toRemove = new BlockStats(10, 1L);

    base.remove(toRemove);

    assertEquals(20, base.getRecordNumber());
    assertEquals(setsOf(2L), base.getBlockIds());
  }
}
