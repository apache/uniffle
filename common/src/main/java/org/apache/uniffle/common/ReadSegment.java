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

import java.util.ArrayList;
import java.util.List;

public class ReadSegment {
  private final long offset;
  private final long length;

  public ReadSegment(long offset, long length) {
    this.offset = offset;
    this.length = length;
  }

  public long getOffset() {
    return offset;
  }

  public long getLength() {
    return length;
  }

  public static ReadSegment from(ShuffleDataSegment segment) {
    return new ReadSegment(segment.getOffset(), segment.getLength());
  }

  public static List<ReadSegment> from(List<ShuffleDataSegment> segments) {
    List<ReadSegment> readSegments = new ArrayList<ReadSegment>();
    for (ShuffleDataSegment segment : segments) {
      readSegments.add(ReadSegment.from(segment));
    }
    return readSegments;
  }
}
