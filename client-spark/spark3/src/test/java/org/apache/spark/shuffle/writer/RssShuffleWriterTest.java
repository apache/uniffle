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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import scala.Product2;
import scala.Tuple2;
import scala.collection.immutable.Nil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.RssShuffleManager;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.shuffle.TestUtils;
import org.apache.spark.shuffle.handle.MutableShuffleHandleInfo;
import org.apache.spark.shuffle.handle.SimpleShuffleHandleInfo;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.impl.FailedBlockSendTracker;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.ExpiringCloseableSupplier;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.storage.util.StorageType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class RssShuffleWriterTest {

  private scala.collection.immutable.List<Product2<String, String>> createMockRecords() {
    return Nil.$colon$colon(new Tuple2<>("testKey2", "testValue2"))
        .$colon$colon(new Tuple2<>("testKey3", "testValue3"))
        .$colon$colon(new Tuple2<>("testKey4", "testValue4"))
        .$colon$colon(new Tuple2<>("testKey6", "testValue6"))
        .$colon$colon(new Tuple2<>("testKey1", "testValue1"))
        .$colon$colon(new Tuple2<>("testKey5", "testValue5"));
  }

  private MutableShuffleHandleInfo createMutableShuffleHandle() {
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = Maps.newHashMap();
    List<ShuffleServerInfo> ssi12 =
        Arrays.asList(
            new ShuffleServerInfo("id1", "0.0.0.1", 100),
            new ShuffleServerInfo("id2", "0.0.0.2", 100));
    partitionToServers.put(0, ssi12);
    List<ShuffleServerInfo> ssi34 =
        Arrays.asList(
            new ShuffleServerInfo("id3", "0.0.0.3", 100),
            new ShuffleServerInfo("id4", "0.0.0.4", 100));
    partitionToServers.put(1, ssi34);
    List<ShuffleServerInfo> ssi56 =
        Arrays.asList(
            new ShuffleServerInfo("id5", "0.0.0.5", 100),
            new ShuffleServerInfo("id6", "0.0.0.6", 100));
    partitionToServers.put(2, ssi56);

    MutableShuffleHandleInfo shuffleHandleInfo =
        new MutableShuffleHandleInfo(0, partitionToServers, RemoteStorageInfo.EMPTY_REMOTE_STORAGE);
    return shuffleHandleInfo;
  }

  private RssShuffleWriter createMockWriter(MutableShuffleHandleInfo shuffleHandle, String taskId) {
    SparkConf conf = new SparkConf();
    conf.setAppName("testApp")
        .setMaster("local[2]")
        .set(RssSparkConfig.RSS_WRITER_SERIALIZER_BUFFER_SIZE.key(), "32")
        .set(RssSparkConfig.RSS_WRITER_BUFFER_SIZE.key(), "32")
        .set(RssSparkConfig.RSS_TEST_FLAG.key(), "true")
        .set(RssSparkConfig.RSS_TEST_MODE_ENABLE.key(), "true")
        .set(RssSparkConfig.RSS_WRITER_BUFFER_SEGMENT_SIZE.key(), "64")
        .set(RssSparkConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS.key(), "1000")
        .set(RssSparkConfig.RSS_WRITER_BUFFER_SPILL_SIZE.key(), "128")
        .set(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE.name());

    Map<String, Set<Long>> successBlockIds = JavaUtils.newConcurrentMap();
    Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker = JavaUtils.newConcurrentMap();
    taskToFailedBlockSendTracker.put(taskId, new FailedBlockSendTracker());

    FakedDataPusher dataPusher = null;
    final RssShuffleManager manager =
        TestUtils.createShuffleManager(
            conf, false, dataPusher, successBlockIds, taskToFailedBlockSendTracker);
    Serializer kryoSerializer = new KryoSerializer(conf);
    Partitioner mockPartitioner = mock(Partitioner.class);
    final ShuffleWriteClient mockShuffleWriteClient = mock(ShuffleWriteClient.class);
    final ShuffleManagerClient mockShuffleManagerClient = mock(ShuffleManagerClient.class);
    ShuffleDependency<String, String, String> mockDependency = mock(ShuffleDependency.class);
    RssShuffleHandle<String, String, String> mockHandle = mock(RssShuffleHandle.class);
    when(mockHandle.getDependency()).thenReturn(mockDependency);
    when(mockDependency.serializer()).thenReturn(kryoSerializer);
    when(mockDependency.partitioner()).thenReturn(mockPartitioner);
    when(mockPartitioner.numPartitions()).thenReturn(3);

    when(mockPartitioner.getPartition("testKey1")).thenReturn(0);
    when(mockPartitioner.getPartition("testKey2")).thenReturn(1);
    when(mockPartitioner.getPartition("testKey4")).thenReturn(0);
    when(mockPartitioner.getPartition("testKey5")).thenReturn(1);
    when(mockPartitioner.getPartition("testKey3")).thenReturn(2);
    when(mockPartitioner.getPartition("testKey7")).thenReturn(0);
    when(mockPartitioner.getPartition("testKey8")).thenReturn(1);
    when(mockPartitioner.getPartition("testKey9")).thenReturn(2);
    when(mockPartitioner.getPartition("testKey6")).thenReturn(2);

    TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);

    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);
    ShuffleWriteMetrics shuffleWriteMetrics = new ShuffleWriteMetrics();
    WriteBufferManager bufferManager =
        new WriteBufferManager(
            0,
            0,
            bufferOptions,
            kryoSerializer,
            shuffleHandle.getAvailablePartitionServersForWriter(),
            mockTaskMemoryManager,
            shuffleWriteMetrics,
            RssSparkConfig.toRssConf(conf));
    bufferManager.setTaskId(taskId);

    WriteBufferManager bufferManagerSpy = spy(bufferManager);
    TaskContext contextMock = mock(TaskContext.class);
    RssShuffleWriter<String, String, String> rssShuffleWriter =
        new RssShuffleWriter<>(
            "appId",
            0,
            taskId,
            1L,
            bufferManagerSpy,
            shuffleWriteMetrics,
            manager,
            conf,
            mockShuffleWriteClient,
            ExpiringCloseableSupplier.of(() -> mockShuffleManagerClient),
            mockHandle,
            shuffleHandle,
            contextMock);
    rssShuffleWriter.enableBlockFailSentRetry();
    doReturn(100000L).when(bufferManagerSpy).acquireMemory(anyLong());

    RssShuffleWriter<String, String, String> rssShuffleWriterSpy = spy(rssShuffleWriter);
    doNothing().when(rssShuffleWriterSpy).sendCommit();

    return rssShuffleWriterSpy;
  }

  private void updateShuffleHandleAssignment(
      MutableShuffleHandleInfo handle,
      Set<Integer> partitionIds,
      String receivingFailureServerId,
      Set<ShuffleServerInfo> replacements) {
    for (int partitionId : partitionIds) {
      handle.updateAssignment(partitionId, receivingFailureServerId, replacements);
    }
  }

  /** Test the reassign multi times for one partitionId. */
  @Test
  public void reassignMultiTimesForOnePartitionIdTest() {
    String taskId = "taskId";
    MutableShuffleHandleInfo shuffleHandle = createMutableShuffleHandle();
    RssShuffleWriter writer = createMockWriter(shuffleHandle, taskId);
    writer.setBlockFailSentRetryMaxTimes(10);

    // Make the id1 + id10 + id11 broken, and then finally, it will use the id12 successfully
    AtomicInteger failureCnt = new AtomicInteger();
    RssShuffleManager shuffleManager = writer.getShuffleManager();
    Map<String, Set<Long>> taskToSuccessBlockIds = shuffleManager.getTaskToSuccessBlockIds();
    Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker =
        shuffleManager.getTaskToFailedBlockSendTracker();
    TaskAttemptAssignment taskAssignment = writer.getTaskAttemptAssignment();
    FakedDataPusher pusher =
        new FakedDataPusher(
            addBlockEvent -> {
              List<ShuffleBlockInfo> blocks = addBlockEvent.getShuffleDataInfoList();
              for (ShuffleBlockInfo block : blocks) {
                ShuffleServerInfo server = block.getShuffleServerInfos().get(0);
                String serverId = server.getId();
                if (Arrays.asList("id1", "id10", "id11").contains(serverId)) {
                  taskToFailedBlockSendTracker
                      .computeIfAbsent(taskId, x -> new FailedBlockSendTracker())
                      .add(block, server, StatusCode.NO_BUFFER);
                  failureCnt.incrementAndGet();

                  // refresh the assignment to simulate the reassign rpc.
                  if (serverId.equals("id1")) {
                    ShuffleServerInfo replacement1 = new ShuffleServerInfo("id10", "0.0.0.10", 100);
                    updateShuffleHandleAssignment(
                        shuffleHandle,
                        Sets.newHashSet(0, 1, 2),
                        "id1",
                        Sets.newHashSet(replacement1));
                    taskAssignment.update(shuffleHandle);
                  } else if (serverId.equals("id10")) {
                    ShuffleServerInfo replacement2 = new ShuffleServerInfo("id11", "0.0.0.10", 100);
                    updateShuffleHandleAssignment(
                        shuffleHandle,
                        Sets.newHashSet(0, 1, 2),
                        "id10",
                        Sets.newHashSet(replacement2));
                    taskAssignment.update(shuffleHandle);
                  } else if (serverId.equals("id11")) {
                    ShuffleServerInfo replacement3 = new ShuffleServerInfo("id12", "0.0.0.10", 100);
                    updateShuffleHandleAssignment(
                        shuffleHandle,
                        Sets.newHashSet(0, 1, 2),
                        "id11",
                        Sets.newHashSet(replacement3));
                    taskAssignment.update(shuffleHandle);
                  }

                } else {
                  taskToSuccessBlockIds
                      .computeIfAbsent(taskId, x -> new HashSet<>())
                      .add(block.getBlockId());
                }
              }
              return new CompletableFuture<>();
            });
    shuffleManager.setDataPusher(pusher);

    writer
        .getBufferManager()
        .setPartitionAssignmentRetrieveFunc(
            partitionId -> writer.getPartitionAssignedServers(partitionId));

    // case1: the reassignment will refresh the following plan. So the failure will only occur one
    // time.
    scala.collection.immutable.List<Product2<String, String>> mockedData = createMockRecords();
    writer.write(mockedData.iterator());

    Awaitility.await()
        .timeout(Duration.ofSeconds(5))
        .until(() -> taskToSuccessBlockIds.get(taskId).size() == mockedData.size());
    assertEquals(3, failureCnt.get());
  }

  /** Once the reassignment occurs, the following AddBlockEvents will use the latest assignment. */
  @Test
  public void refreshAssignmentTest() {
    String taskId = "taskId";
    MutableShuffleHandleInfo shuffleHandle = createMutableShuffleHandle();
    RssShuffleWriter writer = createMockWriter(shuffleHandle, taskId);

    ShuffleServerInfo replacement = new ShuffleServerInfo("id10", "0.0.0.10", 100);
    updateShuffleHandleAssignment(
        shuffleHandle, Sets.newHashSet(0, 1, 2), "id1", Sets.newHashSet(replacement));

    AtomicInteger failureCnt = new AtomicInteger();
    RssShuffleManager shuffleManager = writer.getShuffleManager();
    Map<String, Set<Long>> taskToSuccessBlockIds = shuffleManager.getTaskToSuccessBlockIds();
    Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker =
        shuffleManager.getTaskToFailedBlockSendTracker();
    FakedDataPusher pusher =
        new FakedDataPusher(
            addBlockEvent -> {
              List<ShuffleBlockInfo> blocks = addBlockEvent.getShuffleDataInfoList();
              for (ShuffleBlockInfo block : blocks) {
                ShuffleServerInfo server = block.getShuffleServerInfos().get(0);
                if (server.getId().equals("id1")) {
                  taskToFailedBlockSendTracker
                      .computeIfAbsent(taskId, x -> new FailedBlockSendTracker())
                      .add(block, server, StatusCode.NO_BUFFER);
                  failureCnt.incrementAndGet();
                  // refresh the assignment to simulate the reassign rpc.
                  writer.getTaskAttemptAssignment().update(shuffleHandle);
                } else {
                  taskToSuccessBlockIds
                      .computeIfAbsent(taskId, x -> new HashSet<>())
                      .add(block.getBlockId());
                }
              }
              return new CompletableFuture<>();
            });
    shuffleManager.setDataPusher(pusher);

    writer
        .getBufferManager()
        .setPartitionAssignmentRetrieveFunc(
            partitionId -> writer.getPartitionAssignedServers(partitionId));

    // case1: the reassignment will refresh the following plan. So the failure will only occur one
    // time.
    scala.collection.immutable.List<Product2<String, String>> mockedData = createMockRecords();
    writer.write(mockedData.iterator());

    Awaitility.await()
        .timeout(Duration.ofSeconds(5))
        .until(() -> taskToSuccessBlockIds.get(taskId).size() == mockedData.size());
    assertEquals(1, failureCnt.get());
  }

  @Test
  public void blockFailureResendTest() {
    SparkConf conf = new SparkConf();
    conf.setAppName("testApp")
        .setMaster("local[2]")
        .set(RssSparkConfig.RSS_WRITER_SERIALIZER_BUFFER_SIZE.key(), "32")
        .set(RssSparkConfig.RSS_WRITER_BUFFER_SIZE.key(), "32")
        .set(RssSparkConfig.RSS_TEST_FLAG.key(), "true")
        .set(RssSparkConfig.RSS_TEST_MODE_ENABLE.key(), "true")
        .set(RssSparkConfig.RSS_WRITER_BUFFER_SEGMENT_SIZE.key(), "64")
        .set(RssSparkConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS.key(), "1000")
        .set(RssSparkConfig.RSS_WRITER_BUFFER_SPILL_SIZE.key(), "128")
        .set(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE.name());

    List<ShuffleBlockInfo> shuffleBlockInfos = Lists.newArrayList();
    Map<String, Set<Long>> successBlockIds = JavaUtils.newConcurrentMap();
    Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker = JavaUtils.newConcurrentMap();
    taskToFailedBlockSendTracker.put("taskId", new FailedBlockSendTracker());

    AtomicInteger sentFailureCnt = new AtomicInteger();
    FakedDataPusher dataPusher =
        new FakedDataPusher(
            event -> {
              assertEquals("taskId", event.getTaskId());
              FailedBlockSendTracker tracker = taskToFailedBlockSendTracker.get(event.getTaskId());
              for (ShuffleBlockInfo block : event.getShuffleDataInfoList()) {
                boolean isSuccessful = true;
                ShuffleServerInfo shuffleServer = block.getShuffleServerInfos().get(0);
                if (shuffleServer.getId().equals("id1") && block.getRetryCnt() == 0) {
                  tracker.add(block, shuffleServer, StatusCode.NO_BUFFER);
                  sentFailureCnt.addAndGet(1);
                  isSuccessful = false;
                } else {
                  successBlockIds.putIfAbsent(event.getTaskId(), Sets.newConcurrentHashSet());
                  successBlockIds.get(event.getTaskId()).add(block.getBlockId());
                  shuffleBlockInfos.add(block);
                }
                block.executeCompletionCallback(isSuccessful);
              }
              return new CompletableFuture<>();
            });

    final RssShuffleManager manager =
        TestUtils.createShuffleManager(
            conf, false, dataPusher, successBlockIds, taskToFailedBlockSendTracker);
    Serializer kryoSerializer = new KryoSerializer(conf);
    Partitioner mockPartitioner = mock(Partitioner.class);
    final ShuffleWriteClient mockShuffleWriteClient = mock(ShuffleWriteClient.class);
    final ShuffleManagerClient mockShuffleManagerClient = mock(ShuffleManagerClient.class);
    ShuffleDependency<String, String, String> mockDependency = mock(ShuffleDependency.class);
    RssShuffleHandle<String, String, String> mockHandle = mock(RssShuffleHandle.class);
    when(mockHandle.getDependency()).thenReturn(mockDependency);
    when(mockDependency.serializer()).thenReturn(kryoSerializer);
    when(mockDependency.partitioner()).thenReturn(mockPartitioner);
    when(mockPartitioner.numPartitions()).thenReturn(3);

    Map<Integer, List<ShuffleServerInfo>> partitionToServers = Maps.newHashMap();
    List<ShuffleServerInfo> ssi12 =
        Arrays.asList(
            new ShuffleServerInfo("id1", "0.0.0.1", 100),
            new ShuffleServerInfo("id2", "0.0.0.2", 100));
    partitionToServers.put(0, ssi12);
    List<ShuffleServerInfo> ssi34 =
        Arrays.asList(
            new ShuffleServerInfo("id3", "0.0.0.3", 100),
            new ShuffleServerInfo("id4", "0.0.0.4", 100));
    partitionToServers.put(1, ssi34);
    List<ShuffleServerInfo> ssi56 =
        Arrays.asList(
            new ShuffleServerInfo("id5", "0.0.0.5", 100),
            new ShuffleServerInfo("id6", "0.0.0.6", 100));
    partitionToServers.put(2, ssi56);

    when(mockPartitioner.getPartition("testKey1")).thenReturn(0);
    when(mockPartitioner.getPartition("testKey2")).thenReturn(1);
    when(mockPartitioner.getPartition("testKey4")).thenReturn(0);
    when(mockPartitioner.getPartition("testKey5")).thenReturn(1);
    when(mockPartitioner.getPartition("testKey3")).thenReturn(2);
    when(mockPartitioner.getPartition("testKey7")).thenReturn(0);
    when(mockPartitioner.getPartition("testKey8")).thenReturn(1);
    when(mockPartitioner.getPartition("testKey9")).thenReturn(2);
    when(mockPartitioner.getPartition("testKey6")).thenReturn(2);

    TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);

    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);
    ShuffleWriteMetrics shuffleWriteMetrics = new ShuffleWriteMetrics();
    WriteBufferManager bufferManager =
        new WriteBufferManager(
            0,
            0,
            bufferOptions,
            kryoSerializer,
            partitionToServers,
            mockTaskMemoryManager,
            shuffleWriteMetrics,
            RssSparkConfig.toRssConf(conf));
    bufferManager.setTaskId("taskId");

    WriteBufferManager bufferManagerSpy = spy(bufferManager);
    TaskContext contextMock = mock(TaskContext.class);
    MutableShuffleHandleInfo shuffleHandleInfo =
        new MutableShuffleHandleInfo(0, partitionToServers, RemoteStorageInfo.EMPTY_REMOTE_STORAGE);
    RssShuffleWriter<String, String, String> rssShuffleWriter =
        new RssShuffleWriter<>(
            "appId",
            0,
            "taskId",
            1L,
            bufferManagerSpy,
            shuffleWriteMetrics,
            manager,
            conf,
            mockShuffleWriteClient,
            ExpiringCloseableSupplier.of(() -> mockShuffleManagerClient),
            mockHandle,
            shuffleHandleInfo,
            contextMock);
    rssShuffleWriter.enableBlockFailSentRetry();
    doReturn(100000L).when(bufferManagerSpy).acquireMemory(anyLong());

    ShuffleServerInfo replacement = new ShuffleServerInfo("id10", "0.0.0.10", 100);
    shuffleHandleInfo.updateAssignment(0, "id1", Sets.newHashSet(replacement));
    shuffleHandleInfo.updateAssignment(1, "id1", Sets.newHashSet(replacement));
    shuffleHandleInfo.updateAssignment(2, "id1", Sets.newHashSet(replacement));

    rssShuffleWriter.getTaskAttemptAssignment().update(shuffleHandleInfo);

    RssShuffleWriter<String, String, String> rssShuffleWriterSpy = spy(rssShuffleWriter);
    doNothing().when(rssShuffleWriterSpy).sendCommit();

    // case 1. failed blocks will be resent
    scala.collection.immutable.List<Product2<String, String>> data = createMockRecords();
    rssShuffleWriterSpy.write(data.iterator());

    Awaitility.await()
        .timeout(Duration.ofSeconds(5))
        .until(() -> successBlockIds.get("taskId").size() == data.size());
    assertEquals(2, sentFailureCnt.get());
    assertEquals(0, taskToFailedBlockSendTracker.get("taskId").getFailedBlockIds().size());
    assertEquals(6, shuffleWriteMetrics.recordsWritten());
    assertEquals(
        shuffleBlockInfos.stream().mapToInt(ShuffleBlockInfo::getLength).sum(),
        shuffleWriteMetrics.bytesWritten());
    assertEquals(6, shuffleBlockInfos.size());

    assertEquals(0, bufferManagerSpy.getUsedBytes());
    assertEquals(0, bufferManagerSpy.getInSendListBytes());

    // check the blockId -> servers mapping.
    // server -> partitionId -> blockIds
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds =
        rssShuffleWriterSpy.getServerToPartitionToBlockIds();
    assertEquals(2, serverToPartitionToBlockIds.get(replacement).get(0).size());

    // case2. If exceeding the max retry times, it will fast fail.
    rssShuffleWriter.setBlockFailSentRetryMaxTimes(1);
    rssShuffleWriter.setTaskId("taskId2");
    rssShuffleWriter.getBufferManager().setTaskId("taskId2");
    taskToFailedBlockSendTracker.put("taskId2", new FailedBlockSendTracker());
    AtomicInteger rejectCnt = new AtomicInteger(0);
    FakedDataPusher alwaysFailedDataPusher =
        new FakedDataPusher(
            event -> {
              assertEquals("taskId2", event.getTaskId());
              FailedBlockSendTracker tracker = taskToFailedBlockSendTracker.get(event.getTaskId());
              for (ShuffleBlockInfo block : event.getShuffleDataInfoList()) {
                boolean isSuccessful = true;
                ShuffleServerInfo shuffleServer = block.getShuffleServerInfos().get(0);
                if (shuffleServer.getId().equals("id1") && rejectCnt.get() <= 3) {
                  tracker.add(block, shuffleServer, StatusCode.NO_BUFFER);
                  isSuccessful = false;
                  rejectCnt.incrementAndGet();
                } else {
                  successBlockIds.putIfAbsent(event.getTaskId(), Sets.newConcurrentHashSet());
                  successBlockIds.get(event.getTaskId()).add(block.getBlockId());
                }
                block.executeCompletionCallback(isSuccessful);
              }
              return new CompletableFuture<>();
            });
    manager.setDataPusher(alwaysFailedDataPusher);

    scala.collection.immutable.List<Product2<String, String>> mockedData = createMockRecords();

    try {
      rssShuffleWriter.write(mockedData.iterator());
      fail();
    } catch (Exception e) {
      // ignore
    }
    assertEquals(0, bufferManagerSpy.getUsedBytes());
    assertEquals(0, bufferManagerSpy.getInSendListBytes());
  }

  @Test
  public void checkBlockSendResultTest() {
    SparkConf conf = new SparkConf();
    conf.setAppName("testApp")
        .setMaster("local[2]")
        .set(RssSparkConfig.RSS_TEST_FLAG.key(), "true")
        .set(RssSparkConfig.RSS_TEST_MODE_ENABLE.key(), "true")
        .set(RssSparkConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS.key(), "10000")
        .set(RssSparkConfig.RSS_CLIENT_RETRY_MAX.key(), "10")
        .set(RssSparkConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS.key(), "1000")
        .set(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE.name())
        .set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), "127.0.0.1:12345,127.0.0.1:12346");
    Map<String, Set<Long>> successBlocks = JavaUtils.newConcurrentMap();
    Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker = JavaUtils.newConcurrentMap();
    Map<String, Map<Long, BlockingQueue<ShuffleServerInfo>>> taskToFailedBlockIdsAndServer =
        JavaUtils.newConcurrentMap();
    Serializer kryoSerializer = new KryoSerializer(conf);
    RssShuffleManager manager =
        TestUtils.createShuffleManager(
            conf, false, null, successBlocks, taskToFailedBlockSendTracker);

    ShuffleWriteClient mockShuffleWriteClient = mock(ShuffleWriteClient.class);
    ShuffleManagerClient mockShuffleManagerClient = mock(ShuffleManagerClient.class);
    Partitioner mockPartitioner = mock(Partitioner.class);
    RssShuffleHandle<String, String, String> mockHandle = mock(RssShuffleHandle.class);
    ShuffleDependency<String, String, String> mockDependency = mock(ShuffleDependency.class);
    when(mockHandle.getDependency()).thenReturn(mockDependency);
    when(mockPartitioner.numPartitions()).thenReturn(2);
    TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);
    when(mockHandle.getPartitionToServers()).thenReturn(Maps.newHashMap());
    when(mockDependency.partitioner()).thenReturn(mockPartitioner);

    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);
    WriteBufferManager bufferManager =
        new WriteBufferManager(
            0,
            0,
            bufferOptions,
            kryoSerializer,
            Maps.newHashMap(),
            mockTaskMemoryManager,
            new ShuffleWriteMetrics(),
            RssSparkConfig.toRssConf(conf));
    WriteBufferManager bufferManagerSpy = spy(bufferManager);

    TaskContext contextMock = mock(TaskContext.class);
    SimpleShuffleHandleInfo mockShuffleHandleInfo = mock(SimpleShuffleHandleInfo.class);
    RssShuffleWriter<String, String, String> rssShuffleWriter =
        new RssShuffleWriter<>(
            "appId",
            0,
            "taskId",
            1L,
            bufferManagerSpy,
            (new TaskMetrics()).shuffleWriteMetrics(),
            manager,
            conf,
            mockShuffleWriteClient,
            ExpiringCloseableSupplier.of(() -> mockShuffleManagerClient),
            mockHandle,
            mockShuffleHandleInfo,
            contextMock);
    doReturn(1000000L).when(bufferManagerSpy).acquireMemory(anyLong());

    // case 1: all blocks are sent successfully
    successBlocks.put("taskId", Sets.newHashSet(1L, 2L, 3L));
    rssShuffleWriter.checkBlockSendResult(Sets.newHashSet(1L, 2L, 3L));
    successBlocks.clear();

    // case 2: partial blocks aren't sent before spark.rss.client.send.check.timeout.ms,
    // Runtime exception will be thrown
    successBlocks.put("taskId", Sets.newHashSet(1L, 2L));
    Throwable e2 =
        assertThrows(
            RuntimeException.class,
            () -> rssShuffleWriter.checkBlockSendResult(Sets.newHashSet(1L, 2L, 3L)));
    assertTrue(e2.getMessage().startsWith("Timeout:"));
    successBlocks.clear();

    // case 3: partial blocks are sent failed, Runtime exception will be thrown
    successBlocks.put("taskId", Sets.newHashSet(1L, 2L));
    FailedBlockSendTracker failedBlockSendTracker = new FailedBlockSendTracker();
    taskToFailedBlockSendTracker.put("taskId", failedBlockSendTracker);
    ShuffleServerInfo shuffleServerInfo = new ShuffleServerInfo("127.0.0.1", 20001);
    failedBlockSendTracker.add(
        TestUtils.createMockBlockOnlyBlockId(3L), shuffleServerInfo, StatusCode.INTERNAL_ERROR);
    Throwable e3 =
        assertThrows(
            RuntimeException.class,
            () -> rssShuffleWriter.checkBlockSendResult(Sets.newHashSet(1L, 2L, 3L)));
    assertTrue(e3.getMessage().startsWith("Fail to send the block"));
    successBlocks.clear();
    taskToFailedBlockSendTracker.clear();
  }

  static class FakedDataPusher extends DataPusher {
    private final Function<AddBlockEvent, CompletableFuture<Long>> sendFunc;

    FakedDataPusher(Function<AddBlockEvent, CompletableFuture<Long>> sendFunc) {
      this(null, null, null, null, null, 1, 1, sendFunc);
    }

    private FakedDataPusher(
        ShuffleWriteClient shuffleWriteClient,
        Map<String, Set<Long>> taskToSuccessBlockIds,
        Map<String, Set<Long>> taskToFailedBlockIds,
        Map<String, FailedBlockSendTracker> failedBlockSendTracker,
        Set<String> failedTaskIds,
        int threadPoolSize,
        int threadKeepAliveTime,
        Function<AddBlockEvent, CompletableFuture<Long>> sendFunc) {
      super(
          shuffleWriteClient,
          taskToSuccessBlockIds,
          failedBlockSendTracker,
          failedTaskIds,
          threadPoolSize,
          threadKeepAliveTime);
      this.sendFunc = sendFunc;
    }

    @Override
    public CompletableFuture<Long> send(AddBlockEvent event) {
      return sendFunc.apply(event);
    }
  }

  @Test
  public void dataConsistencyWhenSpillTriggeredTest() throws Exception {
    SparkConf conf = new SparkConf();
    conf.set("spark.rss.client.memory.spill.enabled", "true");
    conf.setAppName("dataConsistencyWhenSpillTriggeredTest_app")
        .setMaster("local[2]")
        .set(RssSparkConfig.RSS_WRITER_SERIALIZER_BUFFER_SIZE.key(), "32")
        .set(RssSparkConfig.RSS_WRITER_BUFFER_SIZE.key(), "32")
        .set(RssSparkConfig.RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE.key(), "32")
        .set(RssSparkConfig.RSS_TEST_FLAG.key(), "true")
        .set(RssSparkConfig.RSS_TEST_MODE_ENABLE.key(), "true")
        .set(RssSparkConfig.RSS_WRITER_BUFFER_SEGMENT_SIZE.key(), "32")
        .set(RssSparkConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS.key(), "1000")
        .set(RssSparkConfig.RSS_WRITER_BUFFER_SPILL_SIZE.key(), "100000")
        .set(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.MEMORY.name())
        .set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), "127.0.0.1:12345,127.0.0.1:12346");

    Map<String, Set<Long>> successBlockIds = Maps.newConcurrentMap();

    List<Long> freeMemoryList = new ArrayList<>();
    FakedDataPusher dataPusher =
        new FakedDataPusher(
            event -> {
              event.getProcessedCallbackChain().stream().forEach(x -> x.run());
              long sum =
                  event.getShuffleDataInfoList().stream().mapToLong(x -> x.getFreeMemory()).sum();
              freeMemoryList.add(sum);
              successBlockIds.putIfAbsent(event.getTaskId(), new HashSet<>());
              successBlockIds
                  .get(event.getTaskId())
                  .add(event.getShuffleDataInfoList().get(0).getBlockId());
              return CompletableFuture.completedFuture(sum);
            });

    final RssShuffleManager manager =
        TestUtils.createShuffleManager(
            conf, false, dataPusher, successBlockIds, JavaUtils.newConcurrentMap());

    WriteBufferManagerTest.FakedTaskMemoryManager fakedTaskMemoryManager =
        new WriteBufferManagerTest.FakedTaskMemoryManager();
    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = Maps.newConcurrentMap();
    partitionToServers.put(0, Lists.newArrayList(new ShuffleServerInfo("127.0.0.1", 1111)));
    WriteBufferManager bufferManager =
        new WriteBufferManager(
            0,
            "taskId",
            0,
            bufferOptions,
            new KryoSerializer(conf),
            partitionToServers,
            fakedTaskMemoryManager,
            new ShuffleWriteMetrics(),
            RssSparkConfig.toRssConf(conf),
            null,
            0);

    Serializer kryoSerializer = new KryoSerializer(conf);
    Partitioner mockPartitioner = mock(Partitioner.class);
    final ShuffleWriteClient mockShuffleWriteClient = mock(ShuffleWriteClient.class);
    final ShuffleManagerClient mockShuffleManagerClient = mock(ShuffleManagerClient.class);
    ShuffleDependency<String, String, String> mockDependency = mock(ShuffleDependency.class);
    RssShuffleHandle<String, String, String> mockHandle = mock(RssShuffleHandle.class);
    when(mockHandle.getDependency()).thenReturn(mockDependency);
    when(mockDependency.serializer()).thenReturn(kryoSerializer);
    when(mockDependency.partitioner()).thenReturn(mockPartitioner);
    when(mockPartitioner.numPartitions()).thenReturn(1);
    TaskContext contextMock = mock(TaskContext.class);
    SimpleShuffleHandleInfo mockShuffleHandleInfo = mock(SimpleShuffleHandleInfo.class);

    RssShuffleWriter<String, String, String> rssShuffleWriter =
        new RssShuffleWriter<>(
            "appId",
            0,
            "taskId",
            1L,
            bufferManager,
            new ShuffleWriteMetrics(),
            manager,
            conf,
            mockShuffleWriteClient,
            ExpiringCloseableSupplier.of(() -> mockShuffleManagerClient),
            mockHandle,
            mockShuffleHandleInfo,
            contextMock);
    rssShuffleWriter.getBufferManager().setSpillFunc(rssShuffleWriter::processShuffleBlockInfos);

    scala.collection.immutable.List<Product2<String, String>> data =
        Nil
            // One record is 26 bytes
            .$colon$colon(new Tuple2<>("Key", "Value11111111111111"))
            .$colon$colon(new Tuple2<>("Key", "Value11111111111111"))
            .$colon$colon(new Tuple2<>("Key", "Value11111111111111"))
            .$colon$colon(new Tuple2<>("Key", "Value11111111111111"));

    // case1: all blocks are sent and pass the blocks check when spill is triggered
    rssShuffleWriter.write(data.iterator());
    assertEquals(4, successBlockIds.get("taskId").size());
    for (int i = 0; i < 4; i++) {
      assertEquals(32, freeMemoryList.get(i));
    }
  }

  @Test
  public void writeTest() throws Exception {
    SparkConf conf = new SparkConf();
    conf.setAppName("testApp")
        .setMaster("local[2]")
        .set(RssSparkConfig.RSS_WRITER_SERIALIZER_BUFFER_SIZE.key(), "32")
        .set(RssSparkConfig.RSS_WRITER_BUFFER_SIZE.key(), "32")
        .set(RssSparkConfig.RSS_TEST_FLAG.key(), "true")
        .set(RssSparkConfig.RSS_TEST_MODE_ENABLE.key(), "true")
        .set(RssSparkConfig.RSS_WRITER_BUFFER_SEGMENT_SIZE.key(), "64")
        .set(RssSparkConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS.key(), "1000")
        .set(RssSparkConfig.RSS_WRITER_BUFFER_SPILL_SIZE.key(), "128")
        .set(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE.name())
        .set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), "127.0.0.1:12345,127.0.0.1:12346");
    List<ShuffleBlockInfo> shuffleBlockInfos = Lists.newArrayList();
    Map<String, Set<Long>> successBlockIds = Maps.newConcurrentMap();

    FakedDataPusher dataPusher =
        new FakedDataPusher(
            event -> {
              assertEquals("taskId", event.getTaskId());
              shuffleBlockInfos.addAll(event.getShuffleDataInfoList());
              Set<Long> blockIds =
                  event
                      .getShuffleDataInfoList()
                      .parallelStream()
                      .map(sdi -> sdi.getBlockId())
                      .collect(Collectors.toSet());
              successBlockIds.putIfAbsent(event.getTaskId(), Sets.newConcurrentHashSet());
              successBlockIds.get(event.getTaskId()).addAll(blockIds);
              return new CompletableFuture<>();
            });

    final RssShuffleManager manager =
        TestUtils.createShuffleManager(
            conf, false, dataPusher, successBlockIds, JavaUtils.newConcurrentMap());
    Serializer kryoSerializer = new KryoSerializer(conf);
    Partitioner mockPartitioner = mock(Partitioner.class);
    final ShuffleWriteClient mockShuffleWriteClient = mock(ShuffleWriteClient.class);
    final ShuffleManagerClient mockShuffleManagerClient = mock(ShuffleManagerClient.class);
    ShuffleDependency<String, String, String> mockDependency = mock(ShuffleDependency.class);
    RssShuffleHandle<String, String, String> mockHandle = mock(RssShuffleHandle.class);
    when(mockHandle.getDependency()).thenReturn(mockDependency);
    when(mockDependency.serializer()).thenReturn(kryoSerializer);
    when(mockDependency.partitioner()).thenReturn(mockPartitioner);
    when(mockPartitioner.numPartitions()).thenReturn(3);

    Map<Integer, List<ShuffleServerInfo>> partitionToServers = Maps.newHashMap();
    List<ShuffleServerInfo> ssi34 =
        Arrays.asList(
            new ShuffleServerInfo("id3", "0.0.0.3", 100),
            new ShuffleServerInfo("id4", "0.0.0.4", 100));
    partitionToServers.put(1, ssi34);
    List<ShuffleServerInfo> ssi56 =
        Arrays.asList(
            new ShuffleServerInfo("id5", "0.0.0.5", 100),
            new ShuffleServerInfo("id6", "0.0.0.6", 100));
    partitionToServers.put(2, ssi56);
    List<ShuffleServerInfo> ssi12 =
        Arrays.asList(
            new ShuffleServerInfo("id1", "0.0.0.1", 100),
            new ShuffleServerInfo("id2", "0.0.0.2", 100));
    partitionToServers.put(0, ssi12);
    when(mockPartitioner.getPartition("testKey1")).thenReturn(0);
    when(mockPartitioner.getPartition("testKey2")).thenReturn(1);
    when(mockPartitioner.getPartition("testKey4")).thenReturn(0);
    when(mockPartitioner.getPartition("testKey5")).thenReturn(1);
    when(mockPartitioner.getPartition("testKey3")).thenReturn(2);
    when(mockPartitioner.getPartition("testKey7")).thenReturn(0);
    when(mockPartitioner.getPartition("testKey8")).thenReturn(1);
    when(mockPartitioner.getPartition("testKey9")).thenReturn(2);
    when(mockPartitioner.getPartition("testKey6")).thenReturn(2);

    TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);

    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);
    ShuffleWriteMetrics shuffleWriteMetrics = new ShuffleWriteMetrics();
    WriteBufferManager bufferManager =
        new WriteBufferManager(
            0,
            0,
            bufferOptions,
            kryoSerializer,
            partitionToServers,
            mockTaskMemoryManager,
            shuffleWriteMetrics,
            RssSparkConfig.toRssConf(conf));
    bufferManager.setTaskId("taskId");

    WriteBufferManager bufferManagerSpy = spy(bufferManager);
    TaskContext contextMock = mock(TaskContext.class);
    SimpleShuffleHandleInfo mockShuffleHandleInfo = mock(SimpleShuffleHandleInfo.class);
    RssShuffleWriter<String, String, String> rssShuffleWriter =
        new RssShuffleWriter<>(
            "appId",
            0,
            "taskId",
            1L,
            bufferManagerSpy,
            shuffleWriteMetrics,
            manager,
            conf,
            mockShuffleWriteClient,
            ExpiringCloseableSupplier.of(() -> mockShuffleManagerClient),
            mockHandle,
            mockShuffleHandleInfo,
            contextMock);
    doReturn(1000000L).when(bufferManagerSpy).acquireMemory(anyLong());

    RssShuffleWriter<String, String, String> rssShuffleWriterSpy = spy(rssShuffleWriter);
    doNothing().when(rssShuffleWriterSpy).sendCommit();

    // case 1
    scala.collection.immutable.List<Product2<String, String>> data =
        Nil.$colon$colon(new Tuple2<>("testKey2", "testValue2"))
            .$colon$colon(new Tuple2<>("testKey3", "testValue3"))
            .$colon$colon(new Tuple2<>("testKey4", "testValue4"))
            .$colon$colon(new Tuple2<>("testKey6", "testValue6"))
            .$colon$colon(new Tuple2<>("testKey1", "testValue1"))
            .$colon$colon(new Tuple2<>("testKey5", "testValue5"));
    rssShuffleWriterSpy.write(data.iterator());

    assertTrue(shuffleWriteMetrics.writeTime() > 0);
    assertEquals(6, shuffleWriteMetrics.recordsWritten());

    assertEquals(
        shuffleBlockInfos.stream().mapToInt(ShuffleBlockInfo::getLength).sum(),
        shuffleWriteMetrics.bytesWritten());

    assertEquals(6, shuffleBlockInfos.size());
    for (ShuffleBlockInfo shuffleBlockInfo : shuffleBlockInfos) {
      assertEquals(22, shuffleBlockInfo.getUncompressLength());
      assertEquals(0, shuffleBlockInfo.getShuffleId());
      if (shuffleBlockInfo.getPartitionId() == 0) {
        assertEquals(shuffleBlockInfo.getShuffleServerInfos(), ssi12);
      }
      if (shuffleBlockInfo.getPartitionId() == 1) {
        assertEquals(shuffleBlockInfo.getShuffleServerInfos(), ssi34);
      }
      if (shuffleBlockInfo.getPartitionId() == 2) {
        assertEquals(shuffleBlockInfo.getShuffleServerInfos(), ssi56);
      }
      if (shuffleBlockInfo.getPartitionId() < 0 || shuffleBlockInfo.getPartitionId() > 2) {
        throw new Exception("Shouldn't be here");
      }
    }
    Map<Integer, Set<Long>> partitionToBlockIds = rssShuffleWriterSpy.getPartitionToBlockIds();
    System.out.println(11111);
    assertEquals(2, partitionToBlockIds.get(1).size());
    assertEquals(2, partitionToBlockIds.get(0).size());
    assertEquals(2, partitionToBlockIds.get(2).size());
    partitionToBlockIds.clear();
  }

  @Test
  public void postBlockEventTest() throws Exception {
    SparkConf conf = new SparkConf();
    conf.set(
            RssSparkConfig.SPARK_RSS_CONFIG_PREFIX
                + RssSparkConfig.RSS_CLIENT_SEND_SIZE_LIMITATION.key(),
            "64")
        .set(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE.name());

    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);
    WriteBufferManager bufferManager =
        new WriteBufferManager(
            0,
            0,
            bufferOptions,
            new KryoSerializer(conf),
            Maps.newHashMap(),
            mock(TaskMemoryManager.class),
            new ShuffleWriteMetrics(),
            RssSparkConfig.toRssConf(conf));
    WriteBufferManager bufferManagerSpy = spy(bufferManager);

    ShuffleDependency<String, String, String> mockDependency = mock(ShuffleDependency.class);
    ShuffleWriteMetrics mockMetrics = mock(ShuffleWriteMetrics.class);
    Partitioner mockPartitioner = mock(Partitioner.class);
    when(mockDependency.partitioner()).thenReturn(mockPartitioner);
    SparkConf sparkConf = new SparkConf();
    when(mockPartitioner.numPartitions()).thenReturn(2);
    List<AddBlockEvent> events = Lists.newArrayList();

    FakedDataPusher dataPusher =
        new FakedDataPusher(
            event -> {
              events.add(event);
              return new CompletableFuture<>();
            });

    RssShuffleManager mockShuffleManager =
        spy(
            TestUtils.createShuffleManager(
                sparkConf,
                false,
                dataPusher,
                Maps.newConcurrentMap(),
                JavaUtils.newConcurrentMap()));

    RssShuffleHandle<String, String, String> mockHandle = mock(RssShuffleHandle.class);
    when(mockHandle.getDependency()).thenReturn(mockDependency);
    TaskContext contextMock = mock(TaskContext.class);
    SimpleShuffleHandleInfo mockShuffleHandleInfo = mock(SimpleShuffleHandleInfo.class);
    ShuffleWriteClient mockWriteClient = mock(ShuffleWriteClient.class);
    ShuffleManagerClient mockShuffleManagerClient = mock(ShuffleManagerClient.class);

    List<ShuffleBlockInfo> shuffleBlockInfoList = createShuffleBlockList(1, 31);
    RssShuffleWriter<String, String, String> writer =
        new RssShuffleWriter<>(
            "appId",
            0,
            "taskId",
            1L,
            bufferManagerSpy,
            mockMetrics,
            mockShuffleManager,
            conf,
            mockWriteClient,
            ExpiringCloseableSupplier.of(() -> mockShuffleManagerClient),
            mockHandle,
            mockShuffleHandleInfo,
            contextMock);
    writer.postBlockEvent(shuffleBlockInfoList);
    Awaitility.await().timeout(Duration.ofSeconds(1)).until(() -> events.size() == 1);
    assertEquals(1, events.get(0).getShuffleDataInfoList().size());
    events.clear();

    testSingleEvent(events, writer, 2, 15);

    testSingleEvent(events, writer, 1, 33);

    testSingleEvent(events, writer, 2, 16);

    testSingleEvent(events, writer, 2, 15);

    testSingleEvent(events, writer, 2, 17);

    testSingleEvent(events, writer, 2, 32);

    testTwoEvents(events, writer, 2, 33, 1, 1);

    testTwoEvents(events, writer, 3, 17, 2, 1);
  }

  private void testTwoEvents(
      List<AddBlockEvent> events,
      RssShuffleWriter<String, String, String> writer,
      int blockNum,
      int blockLength,
      int firstEventSize,
      int secondEventSize)
      throws InterruptedException {
    List<ShuffleBlockInfo> shuffleBlockInfoList;
    shuffleBlockInfoList = createShuffleBlockList(blockNum, blockLength);
    writer.postBlockEvent(shuffleBlockInfoList);
    Thread.sleep(500);
    assertEquals(2, events.size());
    assertEquals(firstEventSize, events.get(0).getShuffleDataInfoList().size());
    assertEquals(secondEventSize, events.get(1).getShuffleDataInfoList().size());
    events.clear();
  }

  private void testSingleEvent(
      List<AddBlockEvent> events,
      RssShuffleWriter<String, String, String> writer,
      int blockNum,
      int blockLength)
      throws InterruptedException {
    List<ShuffleBlockInfo> shuffleBlockInfoList;
    shuffleBlockInfoList = createShuffleBlockList(blockNum, blockLength);
    writer.postBlockEvent(shuffleBlockInfoList);
    Thread.sleep(500);
    assertEquals(1, events.size());
    assertEquals(blockNum, events.get(0).getShuffleDataInfoList().size());
    events.clear();
  }

  private List<ShuffleBlockInfo> createShuffleBlockList(int blockNum, int blockLength) {
    List<ShuffleServerInfo> shuffleServerInfoList =
        Lists.newArrayList(new ShuffleServerInfo("id", "host", 0));
    List<ShuffleBlockInfo> shuffleBlockInfoList = Lists.newArrayList();
    for (int i = 0; i < blockNum; i++) {
      shuffleBlockInfoList.add(
          new ShuffleBlockInfo(
              0,
              0,
              10,
              blockLength,
              10,
              new byte[] {1},
              shuffleServerInfoList,
              blockLength,
              10,
              0));
    }
    return shuffleBlockInfoList;
  }
}
