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

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OutputUtilsTest {
  @Test
  public void test() {
    List<Integer> numbers =
        Arrays.asList(
            9527, 9528, 9529, 9530, 11375, 11376, 11377, 11378, 11379, 12000, 12001, 12002, 12003,
            12004);
    assertEquals("[9527~9530], [11375~11379], [12000~12004]", OutputUtils.listToSegment(numbers));
    assertEquals(
        "[9527~9530], [11375~11379], [12000~12004]", OutputUtils.listToSegment(numbers, 4));
    assertEquals(
        "[9527, 9528, 9529, 9530, 11375, 11376, 11377, 11378,"
            + " 11379, 12000, 12001, 12002, 12003, 12004]",
        OutputUtils.listToSegment(numbers, 20));
    // limit
    assertEquals(
        "[9527~9530], [11375~11379]...1 more ranges...", OutputUtils.listToSegment(numbers, 1, 2));
    assertEquals("[9527~9530]...2 more ranges...", OutputUtils.listToSegment(numbers, 1, 1));
    assertEquals("...3 more ranges...", OutputUtils.listToSegment(numbers, 1, 0));

    // corner case
    assertEquals("[9527]", OutputUtils.listToSegment(Arrays.asList(9527)));
    assertEquals("[]", OutputUtils.listToSegment(Arrays.asList()));
    assertEquals("[]", OutputUtils.listToSegment(null));
  }
}
