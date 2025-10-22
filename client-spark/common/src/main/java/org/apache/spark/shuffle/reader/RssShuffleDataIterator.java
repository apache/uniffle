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
import org.apache.uniffle.client.response.CompressedShuffleBlock;
import org.apache.uniffle.client.response.ShuffleBlock;
import org.apache.uniffle.common.ShuffleReadTimes;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.RssUtils;

public class RssShuffleDataIterator<K, C> extends AbstractIterator<Product2<K, C>> {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleDataIterator.class);

  private Iterator<Tuple2<Object, Object>> recordsIterator = null;
  private SerializerInstance serializerInstance;
  private ShuffleReadClient shuffleReadClient;
  private ShuffleReadMetrics shuffleReadMetrics;
  private long readTime = 0;
  private long serializeTime = 0;
  private long decompressTime = 0;
  private DeserializationStream deserializationStream = null;
  private ByteBufInputStream byteBufInputStream = null;
  private long totalRawBytesLength = 0;
  private long unCompressedBytesLength = 0;
  private ByteBuffer uncompressedData;
  private Optional<Codec> codec;

  // only for tests
  @VisibleForTesting
  public RssShuffleDataIterator(
      Serializer serializer,
      ShuffleReadClient shuffleReadClient,
      ShuffleReadMetrics shuffleReadMetrics,
      RssConf rssConf) {
    this(serializer, shuffleReadClient, shuffleReadMetrics, rssConf, Optional.empty());
    boolean compress =
        rssConf.getBoolean(
            RssSparkConfig.SPARK_SHUFFLE_COMPRESS_KEY.substring(
                RssSparkConfig.SPARK_RSS_CONFIG_PREFIX.length()),
            RssSparkConfig.SPARK_SHUFFLE_COMPRESS_DEFAULT);
    this.codec = compress ? Codec.newInstance(rssConf) : Optional.empty();
  }

  public RssShuffleDataIterator(
      Serializer serializer,
      ShuffleReadClient shuffleReadClient,
      ShuffleReadMetrics shuffleReadMetrics,
      RssConf rssConf,
      Optional<Codec> codec) {
    this.serializerInstance = serializer.newInstance();
    this.shuffleReadClient = shuffleReadClient;
    this.shuffleReadMetrics = shuffleReadMetrics;
    this.codec = codec;
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
      // read next segment
      long startFetch = System.currentTimeMillis();
      ShuffleBlock shuffleBlock = shuffleReadClient.readShuffleBlockData();
      long fetchDuration = System.currentTimeMillis() - startFetch;

      // get the buffer, if the block is the DecompressedShuffleBlock,
      // the duration is the block wait decompression time.
      long getBuffer = System.currentTimeMillis();
      ByteBuffer rawData = shuffleBlock != null ? shuffleBlock.getByteBuffer() : null;
      long getBufferDuration = System.currentTimeMillis() - getBuffer;

      if (rawData != null) {
        // collect metrics from raw data
        totalRawBytesLength += shuffleBlock.getCompressedLength();
        shuffleReadMetrics.incRemoteBytesRead(shuffleBlock.getCompressedLength());

        long startUncompression = System.currentTimeMillis();
        // get initial data
        ByteBuffer decompressed = null;
        if (shuffleBlock instanceof CompressedShuffleBlock) {
          uncompress(shuffleBlock, rawData);
          decompressed = uncompressedData;
        } else {
          decompressed = shuffleBlock.getByteBuffer();
          unCompressedBytesLength += shuffleBlock.getUncompressLength();
          decompressTime += getBufferDuration;
        }
        long uncompressionDuration = System.currentTimeMillis() - startUncompression;
        uncompressionDuration += getBufferDuration;

        // create new iterator for shuffle data
        long startSerialization = System.currentTimeMillis();
        recordsIterator = createKVIterator(decompressed);
        long serializationDuration = System.currentTimeMillis() - startSerialization;
        shuffleReadMetrics.incFetchWaitTime(
            serializationDuration + uncompressionDuration + fetchDuration);
        readTime += fetchDuration;
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
                    + decompressTime
                    + " ms to decompress with unCompressionLength["
                    + unCompressedBytesLength
                    + "]");
        LOG.info(
            "Fetch {} bytes cost {} ms and {} ms to serialize{}",
            totalRawBytesLength,
            readTime,
            serializeTime,
            decInfo);
        return false;
      }
    }
    return recordsIterator.hasNext();
  }

  private boolean isSameMemoryType(ByteBuffer left, ByteBuffer right) {
    return left.isDirect() == right.isDirect();
  }

  private int uncompress(ShuffleBlock rawBlock, ByteBuffer rawData) {
    int uncompressedLen = rawBlock.getUncompressLength();
    if (uncompressedLen < 0) {
      LOG.error("Uncompressed length is negative: {}", uncompressedLen);
      throw new IllegalArgumentException("Uncompressed length is negative: " + uncompressedLen);
    }
    if (codec.isPresent()) {
      if (uncompressedData == null
          || uncompressedData.capacity() < uncompressedLen
          || !isSameMemoryType(uncompressedData, rawData)) {

        if (LOG.isDebugEnabled()) {
          if (uncompressedData != null && !isSameMemoryType(uncompressedData, rawData)) {
            LOG.debug(
                "This should not happen that the temporary uncompressed data's memory type(isDirect:{}) "
                    + "is not same with fetched data buffer(isDirect:{})",
                uncompressedData.isDirect(),
                rawData.isDirect());
          }
        }

        if (uncompressedData != null) {
          RssUtils.releaseByteBuffer(uncompressedData);
        }
        uncompressedData =
            rawData.isDirect()
                ? ByteBuffer.allocateDirect(uncompressedLen)
                : ByteBuffer.allocate(uncompressedLen);
      }
      uncompressedData.clear();
      long startDecompress = System.currentTimeMillis();
      codec.get().decompress(rawData, uncompressedLen, uncompressedData, 0);
      unCompressedBytesLength += uncompressedLen;
      long decompressDuration = System.currentTimeMillis() - startDecompress;
      decompressTime += decompressDuration;

      // uncompressedData's limit is not updated by `codec.decompress`, however this information is
      // used by `createKVIterator`. update position and limit
      uncompressedData.position(0);
      uncompressedData.limit(uncompressedLen);
    } else {
      uncompressedData = rawData;
    }
    return uncompressedLen;
  }

  @Override
  public Product2<K, C> next() {
    shuffleReadMetrics.incRecordsRead(1L);
    return (Product2<K, C>) recordsIterator.next();
  }

  public BoxedUnit cleanup() {
    clearDeserializationStream();
    // Uncompressed data is released in this class, Compressed data is release in the class
    // ShuffleReadClientImpl
    // So if codec is null, we don't release the data when the stream is closed
    if (codec.isPresent()) {
      RssUtils.releaseByteBuffer(uncompressedData);
    }
    if (shuffleReadClient != null) {
      shuffleReadClient.close();
    }
    shuffleReadClient = null;
    uncompressedData = null;
    return BoxedUnit.UNIT;
  }

  @VisibleForTesting
  protected ShuffleReadMetrics getShuffleReadMetrics() {
    return shuffleReadMetrics;
  }

  public ShuffleReadTimes getReadTimes() {
    ShuffleReadTimes times = shuffleReadClient.getShuffleReadTimes();
    times.withDecompressed(decompressTime);
    times.withDeserialized(serializeTime);
    return times;
  }
}
