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

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssSparkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;

import static org.apache.spark.shuffle.RssSparkConfig.toSparkConfKey;

public class BufferManagerOptions {

  private static final Logger LOG = LoggerFactory.getLogger(BufferManagerOptions.class);

  private long bufferSize;
  private long serializerBufferSize;
  private long bufferSegmentSize;
  private long bufferSpillThreshold;
  private long preAllocatedBufferSize;
  private long requireMemoryInterval;
  private int requireMemoryRetryMax;
  private double bufferSpillPercent;

  public BufferManagerOptions(SparkConf sparkConf) {
    RssConf rssConf = RssSparkConfig.toRssConf(sparkConf);
    bufferSize =
        rssConf.getSizeInBytes(
            RssClientConf.RSS_WRITER_BUFFER_SIZE.key(),
            RssClientConf.RSS_WRITER_BUFFER_SIZE.defaultValue());
    serializerBufferSize =
        rssConf.getSizeAsBytes(
            RssSparkConfig.RSS_WRITER_SERIALIZER_BUFFER_SIZE.key(),
            RssSparkConfig.RSS_WRITER_SERIALIZER_BUFFER_SIZE.defaultValue());
    bufferSegmentSize =
        rssConf.getSizeAsBytes(
            RssSparkConfig.RSS_WRITER_BUFFER_SEGMENT_SIZE.key(),
            RssSparkConfig.RSS_WRITER_BUFFER_SEGMENT_SIZE.defaultValue());
    bufferSpillThreshold =
        rssConf.getSizeAsBytes(
            RssSparkConfig.RSS_WRITER_BUFFER_SPILL_SIZE.key(),
            RssSparkConfig.RSS_WRITER_BUFFER_SPILL_SIZE.defaultValue());
    bufferSpillPercent = rssConf.get(RssSparkConfig.RSS_MEMORY_SPILL_RATIO);
    preAllocatedBufferSize =
        rssConf.getSizeAsBytes(
            RssSparkConfig.RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE.key(),
            RssSparkConfig.RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE.defaultValue());
    requireMemoryInterval = rssConf.get(RssSparkConfig.RSS_WRITER_REQUIRE_MEMORY_INTERVAL);
    requireMemoryRetryMax = rssConf.get(RssSparkConfig.RSS_WRITER_REQUIRE_MEMORY_RETRY_MAX);
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "New buffer manager options, bufferSize: {}, bufferSpillThreshold: {}, preAllocatedBufferSize: {}",
          bufferSize,
          bufferSpillThreshold,
          preAllocatedBufferSize);
    }
    checkBufferSize();
  }

  private void checkBufferSize() {
    if (bufferSize < 0) {
      throw new RssException(
          "Unexpected value of "
              + toSparkConfKey(RssClientConf.RSS_WRITER_BUFFER_SIZE)
              + "="
              + bufferSize);
    }
    if (bufferSpillThreshold < 0) {
      throw new RssException(
          "Unexpected value of "
              + toSparkConfKey(RssSparkConfig.RSS_WRITER_BUFFER_SPILL_SIZE)
              + "="
              + bufferSpillThreshold);
    }
    if (bufferSegmentSize > bufferSize) {
      LOG.warn(
          toSparkConfKey(RssSparkConfig.RSS_WRITER_BUFFER_SEGMENT_SIZE)
              + "["
              + bufferSegmentSize
              + "] should be less than "
              + toSparkConfKey(RssClientConf.RSS_WRITER_BUFFER_SIZE)
              + "["
              + bufferSize
              + "]");
    }
  }

  // limit of buffer size is 2G
  public int getBufferSize() {
    return parseToInt(bufferSize);
  }

  public int getSerializerBufferSize() {
    return parseToInt(serializerBufferSize);
  }

  public int getBufferSegmentSize() {
    return parseToInt(bufferSegmentSize);
  }

  private int parseToInt(long value) {
    if (value > Integer.MAX_VALUE) {
      value = Integer.MAX_VALUE;
    }
    return (int) value;
  }

  public long getPreAllocatedBufferSize() {
    return preAllocatedBufferSize;
  }

  public long getBufferSpillThreshold() {
    return bufferSpillThreshold;
  }

  public double getBufferSpillPercent() {
    return bufferSpillPercent;
  }

  public long getRequireMemoryInterval() {
    return requireMemoryInterval;
  }

  public int getRequireMemoryRetryMax() {
    return requireMemoryRetryMax;
  }
}
