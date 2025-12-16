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

/** Multi blocks stats */
public class BlockStats {
  private long recordNumber;
  private Set<Long> blockIds;

  public BlockStats() {
    this.recordNumber = 0;
    this.blockIds = new HashSet<>();
  }

  public BlockStats(long recordNumber, long blockId) {
    this.recordNumber = recordNumber;
    this.blockIds = new HashSet<>(1);
    this.blockIds.add(blockId);
  }

  public long getRecordNumber() {
    return recordNumber;
  }

  public Set<Long> getBlockIds() {
    return blockIds;
  }

  public void merge(BlockStats other) {
    recordNumber += other.recordNumber;
    blockIds.addAll(other.blockIds);
  }

  public void remove(BlockStats other) {
    if (blockIds.removeAll(other.blockIds)) {
      recordNumber -= other.recordNumber;
    }
  }
}
