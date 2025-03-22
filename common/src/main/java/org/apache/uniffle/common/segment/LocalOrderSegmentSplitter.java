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

package org.apache.uniffle.common.segment;

import java.util.List;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.ShuffleDataSegment;
import org.apache.uniffle.common.ShuffleIndexResult;

/**
 * {@class LocalOrderSegmentSplitter} will be initialized only when the {@class
 * ShuffleDataDistributionType} is LOCAL_ORDER, which means the index file will be split into
 * several segments according to its locally ordered properties. And it will skip some blocks, but
 * the remaining blocks in a segment are continuous.
 *
 * <p>This strategy will be useful for Spark AQE skew optimization, it will split the single
 * partition into multiple shuffle readers, and each one will fetch partial single partition data
 * which is in the range of [StartMapIndex, endMapIndex). And so if one reader uses this, it will
 * skip lots of unnecessary blocks.
 *
 * <p>Last but not least, this split strategy depends on LOCAL_ORDER of index file, which must be
 * guaranteed by the shuffle server.
 */
public class LocalOrderSegmentSplitter extends AbstractSegmentSplitter {
  private Roaring64NavigableMap expectTaskIds;

  public LocalOrderSegmentSplitter(Roaring64NavigableMap expectTaskIds, int readBufferSize) {
    super(readBufferSize);
    this.expectTaskIds = expectTaskIds;
  }

  @Override
  public List<ShuffleDataSegment> split(ShuffleIndexResult shuffleIndexResult) {
    return splitCommon(shuffleIndexResult, taskAttemptId -> expectTaskIds.contains(taskAttemptId));
  }
}
