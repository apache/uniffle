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

package org.apache.spark.shuffle.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

import scala.Product2;
import scala.Tuple2;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;
import scala.runtime.BoxedUnit;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.RssSparkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.common.ShuffleReadTimes;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.shuffle.ShuffleReadTaskStats;

public class RssShuffleDataIterator<K, C> extends AbstractIterator<Product2<K, C>> {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleDataIterator.class);

  private Iterator<Tuple2<Object, Object>> recordsIterator = null;
  private SerializerInstance serializerInstance;
  private ShuffleReadClient shuffleReadClient;
  private ShuffleReadMetrics shuffleReadMetrics;
  private long serializeTime = 0;
  private DeserializationStream deserializationStream = null;
  private ByteBufInputStream byteBufInputStream = null;
  private long totalRawBytesLength = 0;
  private Optional<Codec> codec;
  private DecodedShuffleBlockIterator decodedBlockIterator;
  private DecodedShuffleBlock currentDecodedBlock;

  private final int partitionId;
  private Optional<ShuffleReadTaskStats> shuffleReadTaskStats;
  private long currentBlockTaskAttemptId = -1L;

  // only for tests
  @VisibleForTesting
  public RssShuffleDataIterator(
      Serializer serializer,
      ShuffleReadClient shuffleReadClient,
      ShuffleReadMetrics shuffleReadMetrics,
      RssConf rssConf) {
    this(
        serializer,
        shuffleReadClient,
        shuffleReadMetrics,
        rssConf,
        codecFromConf(rssConf),
        Optional.empty(),
        0);
  }

  public RssShuffleDataIterator(
      Serializer serializer,
      ShuffleReadClient shuffleReadClient,
      ShuffleReadMetrics shuffleReadMetrics,
      RssConf rssConf,
      Optional<Codec> codec,
      Optional<ShuffleReadTaskStats> shuffleReadTaskStats,
      int partitionId) {
    this.serializerInstance = serializer.newInstance();
    this.shuffleReadClient = shuffleReadClient;
    this.shuffleReadMetrics = shuffleReadMetrics;
    this.codec = codec;
    this.shuffleReadTaskStats = shuffleReadTaskStats;
    this.partitionId = partitionId;
    boolean overlappingDecompression =
        codec.isPresent() && rssConf.get(RssSparkConfig.RSS_READ_OVERLAPPING_DECOMPRESSION_ENABLED);
    this.decodedBlockIterator =
        overlappingDecompression
            ? new AsyncDecodedShuffleBlockIterator(shuffleReadClient, codec, rssConf)
            : new SyncDecodedShuffleBlockIterator(shuffleReadClient, codec);
  }

  public Iterator<Tuple2<Object, Object>> createKVIterator(ByteBuffer data) {
    clearDeserializationStream();
    // Unpooled.wrapperBuffer will return a ByteBuf, but this ByteBuf won't release direct/heap
    // memory
    // when the ByteBuf is released. This is because the UnpooledDirectByteBuf's doFree is false
    // when it is constructed from user provided ByteBuffer.
    // The `releaseOnClose` parameter doesn't take effect, we would release the data ByteBuffer
    // manually.
    byteBufInputStream = new ByteBufInputStream(Unpooled.wrappedBuffer(data), true);
    deserializationStream = serializerInstance.deserializeStream(byteBufInputStream);
    return deserializationStream.asKeyValueIterator();
  }

  private void clearDeserializationStream() {
    if (byteBufInputStream != null) {
      try {
        byteBufInputStream.close();
      } catch (IOException e) {
        LOG.warn("Can't close ByteBufInputStream, memory may be leaked.");
      }
    }

    if (deserializationStream != null) {
      deserializationStream.close();
    }
    deserializationStream = null;
    byteBufInputStream = null;
  }

  @Override
  public boolean hasNext() {
    if (recordsIterator == null || !recordsIterator.hasNext()) {
      clearCurrentBlock();
      if (decodedBlockIterator.hasNext()) {
        DecodedShuffleBlock decodedBlock = decodedBlockIterator.next();
        this.currentDecodedBlock = decodedBlock;
        this.currentBlockTaskAttemptId = decodedBlock.taskAttemptId();
        shuffleReadTaskStats.ifPresent(
            stats -> stats.incPartitionBlock(partitionId, decodedBlock.taskAttemptId()));
        totalRawBytesLength += decodedBlock.compressedLength();
        shuffleReadMetrics.incRemoteBytesRead(decodedBlock.compressedLength());

        long startSerialization = System.currentTimeMillis();
        recordsIterator = createKVIterator(decodedBlock.data());
        long serializationDuration = System.currentTimeMillis() - startSerialization;
        shuffleReadMetrics.incFetchWaitTime(
            serializationDuration + decodedBlock.decodeWaitMillis() + decodedBlock.readMillis());
        serializeTime += serializationDuration;
      } else {
        // finish reading records, check data consistent
        shuffleReadClient.checkProcessedBlockIds();
        shuffleReadClient.logStatics();
        shuffleReadClient.getShuffleReadTimes();
        String decInfo =
            !codec.isPresent()
                ? "."
                : (", "
                    + decodedBlockIterator.foregroundDecompressionMillis()
                    + " ms to decompress with unCompressionLength["
                    + decodedBlockIterator.uncompressedBytes()
                    + "]");
        LOG.info(
            "Fetched {} bytes cost {} ms and {} ms to serialize{}",
            totalRawBytesLength,
            decodedBlockIterator.readMillis(),
            serializeTime,
            decInfo);
        return false;
      }
    }
    return recordsIterator.hasNext();
  }

  @Override
  public Product2<K, C> next() {
    shuffleReadMetrics.incRecordsRead(1L);
    shuffleReadTaskStats.ifPresent(
        x -> x.incPartitionRecord(partitionId, currentBlockTaskAttemptId));
    return (Product2<K, C>) recordsIterator.next();
  }

  public BoxedUnit cleanup() {
    clearCurrentBlock();
    if (decodedBlockIterator != null) {
      decodedBlockIterator.close();
    }
    if (shuffleReadClient != null) {
      shuffleReadClient.close();
    }
    shuffleReadClient = null;
    return BoxedUnit.UNIT;
  }

  @VisibleForTesting
  protected ShuffleReadMetrics getShuffleReadMetrics() {
    return shuffleReadMetrics;
  }

  public ShuffleReadTimes getReadTimes() {
    ShuffleReadTimes times = shuffleReadClient.getShuffleReadTimes();
    times.withDecompressed(decodedBlockIterator.foregroundDecompressionMillis());
    times.withBackgroundDecompressed(decodedBlockIterator.backgroundDecompressionMillis());
    times.withDeserialized(serializeTime);
    return times;
  }

  private void clearCurrentBlock() {
    clearDeserializationStream();
    if (currentDecodedBlock != null) {
      currentDecodedBlock.close();
      currentDecodedBlock = null;
    }
  }

  private static Optional<Codec> codecFromConf(RssConf rssConf) {
    boolean compress =
        rssConf.getBoolean(
            RssSparkConfig.SPARK_SHUFFLE_COMPRESS_KEY.substring(
                RssSparkConfig.SPARK_RSS_CONFIG_PREFIX.length()),
            RssSparkConfig.SPARK_SHUFFLE_COMPRESS_DEFAULT);
    return compress ? Codec.newInstance(rssConf) : Optional.empty();
  }
}
