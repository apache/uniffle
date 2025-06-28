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

package org.apache.uniffle.common.compression;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatisticsCodec extends Codec {
  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsCodec.class);

  private final Codec codec;
  // todo: decompression could be involved in cost tracking.
  private List<CodecCost> compressCosts;

  StatisticsCodec(Codec codec) {
    LOGGER.info("Statistic codec is enabled");
    this.codec = codec;
    this.compressCosts = new ArrayList<>();
  }

  @Override
  public void decompress(ByteBuffer src, int uncompressedLen, ByteBuffer dest, int destOffset) {
    this.codec.decompress(src, uncompressedLen, dest, destOffset);
  }

  @Override
  public byte[] compress(byte[] src) {
    long start = System.currentTimeMillis();
    byte[] result = this.codec.compress(src);
    compressCosts.add(new CodecCost(src.length, result.length, System.currentTimeMillis() - start));
    return result;
  }

  @Override
  public int compress(ByteBuffer src, ByteBuffer dest) {
    return this.codec.compress(src, dest);
  }

  @Override
  public int maxCompressedLength(int sourceLength) {
    return this.codec.maxCompressedLength(sourceLength);
  }

  public void statistics() {
    if (compressCosts.isEmpty()) {
      return;
    }

    // Sort by sourceByteSize
    compressCosts.sort(Comparator.comparingInt(c -> c.sourceByteSize));

    LOGGER.info(
        "Statistics of compression({}): \n"
            + "-------------------------------------------"
            + "\nMinimum: {} \nP25: {} \nMedian: {} \nP75: {} \nP90: {} \nMaximum: {}\n"
            + "-------------------------------------------",
        compressCosts.size(),
        compressCosts.get(0),
        percentile(compressCosts, 0.25),
        percentile(compressCosts, 0.50),
        percentile(compressCosts, 0.75),
        percentile(compressCosts, 0.90),
        compressCosts.get(compressCosts.size() - 1));
  }

  private CodecCost percentile(List<CodecCost> values, double percentile) {
    if (values.isEmpty()) {
      return null;
    }
    int index = (int) Math.ceil(percentile * values.size()) - 1;
    index = Math.min(Math.max(index, 0), values.size() - 1);
    return values.get(index);
  }

  static class CodecCost {
    private int sourceByteSize;
    private int targetByteSize;
    private long duration;

    CodecCost(int sourceByteSize, int targetByteSize, long duration) {
      this.sourceByteSize = sourceByteSize;
      this.targetByteSize = targetByteSize;
      this.duration = duration;
    }

    @Override
    public String toString() {
      return "CodecCost{"
          + "sourceByteSize="
          + sourceByteSize
          + ", targetByteSize="
          + targetByteSize
          + ", durationMillis="
          + duration
          + '}';
    }
  }
}
