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

package org.apache.spark.shuffle.writer;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleBlockInfo;

public class PartitionLengthStatistic {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionLengthStatistic.class);
  private final AtomicLong[] partitionLens;

  public PartitionLengthStatistic(int numPartitions) {
    this.partitionLens = new AtomicLong[numPartitions];
    Arrays.fill(partitionLens, new AtomicLong(0));
  }

  public void inc(ShuffleBlockInfo block) {
    partitionLens[block.getPartitionId()].addAndGet(block.getLength());
  }

  public long[] toArray() {
    return Arrays.stream(this.partitionLens).mapToLong(x -> x.get()).toArray();
  }
}
