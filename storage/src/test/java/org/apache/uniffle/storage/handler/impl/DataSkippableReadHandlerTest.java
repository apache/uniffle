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

import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.ShuffleDataSegment;

import static org.apache.uniffle.storage.handler.impl.DataSkippableReadHandler.getNextSegments;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DataSkippableReadHandlerTest {

  private List<ShuffleDataSegment> createSegments(int count, int length) {
    List<ShuffleDataSegment> segments = new ArrayList<>();
    int offset = 0;
    for (int i = 0; i < count; i++) {
      segments.add(new ShuffleDataSegment(offset, length, -1, null));
      offset += length;
    }
    return segments;
  }

  @Test
  public void testGetNextSegments() {
    List<ShuffleDataSegment> segments = createSegments(10, 10);
    List<ShuffleDataSegment> nexts = getNextSegments(segments, 0, 2);
    assertEquals(2, nexts.size());

    segments = createSegments(10, 10);
    nexts = getNextSegments(segments, 10, 2);
    assertEquals(0, nexts.size());
  }
}
