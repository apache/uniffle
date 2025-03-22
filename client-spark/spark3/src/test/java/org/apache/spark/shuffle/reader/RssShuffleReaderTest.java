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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Option;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.ExpiringCloseableSupplier;
import org.apache.uniffle.storage.handler.impl.HadoopShuffleWriteHandler;
import org.apache.uniffle.storage.util.StorageType;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class RssShuffleReaderTest extends AbstractRssReaderTest {

  private static final Serializer KRYO_SERIALIZER = new KryoSerializer(new SparkConf(false));

  @Test
  public void readTest() throws Exception {
    ShuffleServerInfo ssi = new ShuffleServerInfo("127.0.0.1", 0);
    String basePath = HDFS_URI + "readTest1";
    HadoopShuffleWriteHandler writeHandler =
        new HadoopShuffleWriteHandler("appId", 0, 0, 0, basePath, ssi.getId(), conf);
    final HadoopShuffleWriteHandler writeHandler1 =
        new HadoopShuffleWriteHandler("appId", 0, 1, 1, basePath, ssi.getId(), conf);

    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    final Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    Map<String, String> expectedData = Maps.newHashMap();
    final Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();
    writeTestData(writeHandler, 2, 5, expectedData, blockIdBitmap, "key", KRYO_SERIALIZER, 0);

    RssShuffleHandle<String, String, String> handleMock = mock(RssShuffleHandle.class);
    ShuffleDependency<String, String, String> dependencyMock = mock(ShuffleDependency.class);
    when(handleMock.getAppId()).thenReturn("appId");
    when(handleMock.getDependency()).thenReturn(dependencyMock);
    when(handleMock.getShuffleId()).thenReturn(1);
    when(handleMock.getNumMaps()).thenReturn(1);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    partitionToServers.put(0, Lists.newArrayList(ssi));
    partitionToServers.put(1, Lists.newArrayList(ssi));
    when(handleMock.getPartitionToServers()).thenReturn(partitionToServers);
    when(dependencyMock.serializer()).thenReturn(KRYO_SERIALIZER);
    TaskContext contextMock = mock(TaskContext.class);
    when(contextMock.attemptNumber()).thenReturn(1);
    when(contextMock.taskAttemptId()).thenReturn(1L);
    when(contextMock.taskMetrics()).thenReturn(new TaskMetrics());
    doNothing().when(contextMock).killTaskIfInterrupted();
    when(dependencyMock.aggregator()).thenReturn(Option.empty());
    when(dependencyMock.keyOrdering()).thenReturn(Option.empty());
    when(dependencyMock.mapSideCombine()).thenReturn(false);

    Map<Integer, Roaring64NavigableMap> partitionToExpectBlocks = Maps.newHashMap();
    partitionToExpectBlocks.put(0, blockIdBitmap);
    RssConf rssConf = new RssConf();
    rssConf.set(RssClientConf.RSS_STORAGE_TYPE, StorageType.HDFS.name());
    rssConf.set(RssClientConf.RSS_INDEX_READ_LIMIT, 1000);
    rssConf.set(RssClientConf.RSS_CLIENT_READ_BUFFER_SIZE, "1000");
    ShuffleManagerClient mockShuffleManagerClient = mock(ShuffleManagerClient.class);
    RssShuffleReader<String, String> rssShuffleReaderSpy =
        spy(
            new RssShuffleReader<>(
                0,
                1,
                0,
                Integer.MAX_VALUE,
                contextMock,
                handleMock,
                basePath,
                conf,
                1,
                partitionToExpectBlocks,
                taskIdBitmap,
                new ShuffleReadMetrics(),
                ExpiringCloseableSupplier.of(() -> mockShuffleManagerClient),
                rssConf,
                ShuffleDataDistributionType.NORMAL,
                partitionToServers));
    validateResult(rssShuffleReaderSpy.read(), expectedData, 10);

    writeTestData(
        writeHandler1, 2, 4, expectedData, blockIdBitmap1, "another_key", KRYO_SERIALIZER, 1);
    partitionToExpectBlocks.put(1, blockIdBitmap1);
    RssShuffleReader<String, String> rssShuffleReaderSpy1 =
        spy(
            new RssShuffleReader<>(
                0,
                2,
                0,
                Integer.MAX_VALUE,
                contextMock,
                handleMock,
                basePath,
                conf,
                2,
                partitionToExpectBlocks,
                taskIdBitmap,
                new ShuffleReadMetrics(),
                ExpiringCloseableSupplier.of(() -> mockShuffleManagerClient),
                rssConf,
                ShuffleDataDistributionType.NORMAL,
                partitionToServers));
    validateResult(rssShuffleReaderSpy1.read(), expectedData, 18);

    RssShuffleReader<String, String> rssShuffleReaderSpy2 =
        spy(
            new RssShuffleReader<>(
                0,
                2,
                0,
                Integer.MAX_VALUE,
                contextMock,
                handleMock,
                basePath,
                conf,
                2,
                partitionToExpectBlocks,
                Roaring64NavigableMap.bitmapOf(),
                new ShuffleReadMetrics(),
                ExpiringCloseableSupplier.of(() -> mockShuffleManagerClient),
                rssConf,
                ShuffleDataDistributionType.NORMAL,
                partitionToServers));
    validateResult(rssShuffleReaderSpy2.read(), Maps.newHashMap(), 0);
  }
}
