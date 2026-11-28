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

package org.apache.uniffle.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.common.netty.buffer.FileSegmentManagedBuffer;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.GrpcSharedBufferAllocator;
import org.apache.uniffle.proto.RssProtos.GetLocalShuffleIndexRequest;
import org.apache.uniffle.proto.RssProtos.GetLocalShuffleIndexResponse;
import org.apache.uniffle.proto.ShuffleServerGrpc;
import org.apache.uniffle.server.buffer.ShuffleBufferManager;
import org.apache.uniffle.server.storage.StorageManager;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ShuffleIndexCacheGrpcTest {
  @TempDir Path directory;
  private ShuffleIndexCache cache;
  private ShuffleServerGrpcService service;
  private ShuffleTaskManager taskManager;
  private ShuffleBufferManager buffers;
  private final GetLocalShuffleIndexRequest request =
      GetLocalShuffleIndexRequest.newBuilder()
          .setAppId("app")
          .setShuffleId(1)
          .setPartitionId(0)
          .setPartitionNumPerRange(1)
          .setPartitionNum(1)
          .build();

  @BeforeEach
  void setUp() throws Exception {
    ShuffleServerMetrics.clear();
    ShuffleServerMetrics.register();
    final Path file = Files.write(directory.resolve("index"), new byte[40]);
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.set(ShuffleServerConf.SERVER_READ_BUFFER_CAPACITY, 100_000L);
    conf.set(ShuffleServerConf.RPC_EXECUTOR_SIZE, 2);
    buffers = new ShuffleBufferManager(conf, mock(ShuffleFlushManager.class), false);
    cache = new ShuffleIndexCache(60_000, buffers::requireReadMemory, buffers::releaseReadMemory);
    taskManager = mock(ShuffleTaskManager.class);
    when(taskManager.getCachedShuffleIndex(anyString(), anyInt(), anyInt(), anyInt(), anyInt()))
        .thenAnswer(
            invocation ->
                cache.acquire(
                    "app",
                    1,
                    0,
                    new ShuffleIndexResult(
                        new FileSegmentManagedBuffer(file.toFile(), 0, 40), 100, "data", 7)));
    ShuffleServer server = mock(ShuffleServer.class);
    when(server.getShuffleServerConf()).thenReturn(conf);
    when(server.getShuffleTaskManager()).thenReturn(taskManager);
    when(server.getShuffleBufferManager()).thenReturn(buffers);
    when(server.getStorageManager()).thenReturn(mock(StorageManager.class));
    when(server.getGrpcMetrics()).thenReturn(mock(ShuffleServerGrpcMetrics.class));
    service = new ShuffleServerGrpcService(server);
  }

  @AfterEach
  void tearDown() {
    cache.close();
    assertEquals(0, ShuffleServerMetrics.gaugeReadBufferUsedSize.get());
    assertEquals(0, ShuffleServerMetrics.gaugeReadLocalIndexFileBufferSize.get());
  }

  @Test
  void realGrpcCallClosesLeaseAfterUnaryResponse() throws Exception {
    String name = InProcessServerBuilder.generateName();
    Server server =
        InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start();
    ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    try {
      GetLocalShuffleIndexResponse response =
          ShuffleServerGrpc.newBlockingStub(channel)
              .withDeadlineAfter(5, TimeUnit.SECONDS)
              .getLocalShuffleIndex(request);
      assertEquals(StatusCode.SUCCESS.toProto(), response.getStatus());
      assertEquals(40, response.getIndexData().size());
      cache.invalidate("app", null);
      assertEquals(0, cache.getReservedBytes());
    } finally {
      channel.shutdownNow();
      server.shutdownNow();
      channel.awaitTermination(5, TimeUnit.SECONDS);
      server.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void responseRemainsPinnedUntilTransportCloses() {
    Observer observer = new Observer();
    service.getLocalShuffleIndex(request, observer);
    assertTrue(observer.completed);
    assertEquals(StatusCode.SUCCESS.toProto(), observer.response.getStatus());
    assertEquals(40, observer.response.getIndexData().size());
    assertEquals(100, observer.response.getDataFileLen());
    assertEquals(7, observer.response.getStorageIds(0));
    assertEquals(40, ShuffleServerMetrics.gaugeReadBufferUsedSize.get());
    cache.invalidate("app", null);
    assertTrue(cache.getReservedBytes() > 0);
    assertNotNull(observer.onClose);
    observer.onClose.run();
    assertEquals(0, cache.getReservedBytes());
    assertEquals(0, ShuffleServerMetrics.gaugeReadBufferUsedSize.get());
    observer.onCancel.run();
    assertEquals(0, cache.getReservedBytes());
  }

  @Test
  void cancellationReleasesSnapshotAndCompletedCallbackCannotDoubleRelease() {
    Observer observer = new Observer();
    service.getLocalShuffleIndex(request, observer);
    cache.invalidate("app", null);
    observer.onCancel.run();
    assertEquals(0, cache.getReservedBytes());
    observer.onClose.run();
    assertEquals(0, cache.getReservedBytes());
  }

  @Test
  void transportFailureKeepsLeaseUntilCancellation() {
    Observer observer =
        new Observer() {
          @Override
          public void onNext(GetLocalShuffleIndexResponse value) {
            throw new IllegalStateException("transport failure");
          }
        };
    assertThrows(
        IllegalStateException.class, () -> service.getLocalShuffleIndex(request, observer));
    cache.invalidate("app", null);
    assertTrue(cache.getReservedBytes() > 0);
    observer.onCancel.run();
    assertEquals(0, cache.getReservedBytes());
    assertEquals(0, ShuffleServerMetrics.gaugeReadLocalIndexFileThreadNum.get());
  }

  @Test
  void exhaustedBudgetReturnsNoBufferWithoutReadingIndependently() throws Exception {
    cache.close();
    cache = new ShuffleIndexCache(1000, n -> false, n -> {});
    Observer observer = new Observer();
    service.getLocalShuffleIndex(request, observer);
    assertEquals(StatusCode.NO_BUFFER.toProto(), observer.response.getStatus());
    assertEquals(0, observer.response.getIndexData().size());
    assertEquals(0, cache.getReservedBytes());
  }

  @Test
  void readFailureReturnsInternalErrorAndBalancesThreadGauge() throws Exception {
    doThrow(new IOException("read failed"))
        .when(taskManager)
        .getCachedShuffleIndex(anyString(), anyInt(), anyInt(), anyInt(), anyInt());
    Observer observer = new Observer();
    service.getLocalShuffleIndex(request, observer);
    assertEquals(StatusCode.INTERNAL_ERROR.toProto(), observer.response.getStatus());
    assertEquals(0, ShuffleServerMetrics.gaugeReadLocalIndexFileThreadNum.get());
  }

  @Test
  void concurrentCacheHitsDoNotReservePrivateResponsesOrQueue() {
    Observer first = new Observer();
    service.getLocalShuffleIndex(request, first);
    long otherReads = buffers.getReadCapacity() - 41;
    assertTrue(buffers.requireReadMemory(otherReads));
    Observer second = new Observer();
    Observer third = new Observer();
    try {
      service.getLocalShuffleIndex(request, second);
      service.getLocalShuffleIndex(request, third);
      assertTrue(first.completed && second.completed && third.completed);
      assertEquals(3, cache.getReaderCount());
      assertEquals(otherReads + 40, ShuffleServerMetrics.gaugeReadBufferUsedSize.get());
      cache.invalidate("app", null);
      second.onCancel.run();
      assertEquals(40, cache.getReservedBytes());
      first.onClose.run();
      assertEquals(40, cache.getReservedBytes());
      third.onClose.run();
      assertEquals(0, cache.getReservedBytes());
    } finally {
      first.onClose.run();
      if (second.onClose != null) {
        second.onClose.run();
      }
      if (third.onClose != null) {
        third.onClose.run();
      }
      buffers.releaseReadMemory(otherReads);
    }
  }

  @Test
  void plainObserverFailureReturnsSharedLeaseInFinally() {
    StreamObserver<GetLocalShuffleIndexResponse> observer =
        new StreamObserver<GetLocalShuffleIndexResponse>() {
          @Override
          public void onNext(GetLocalShuffleIndexResponse response) {
            assertEquals(40, ShuffleServerMetrics.gaugeReadBufferUsedSize.get());
            throw new IllegalStateException("send failed");
          }

          @Override
          public void onError(Throwable error) {}

          @Override
          public void onCompleted() {}
        };
    assertThrows(
        IllegalStateException.class, () -> service.getLocalShuffleIndex(request, observer));
    assertEquals(40, ShuffleServerMetrics.gaugeReadBufferUsedSize.get());
    assertEquals(0, ShuffleServerMetrics.gaugeReadLocalIndexFileBufferSize.get());
  }

  @Test
  void nettyTransportSharesIndexAcrossConcurrentCallsWithoutQueueing() throws Exception {
    byte[] contents = new byte[2_000_000];
    new Random(7).nextBytes(contents);
    final Path file = Files.write(directory.resolve("large-index"), contents);
    ShuffleServerConf largeConf = new ShuffleServerConf();
    largeConf.set(ShuffleServerConf.SERVER_READ_BUFFER_CAPACITY, 3_000_000L);
    cache.close();
    buffers = new ShuffleBufferManager(largeConf, mock(ShuffleFlushManager.class), false);
    cache = new ShuffleIndexCache(60_000, buffers::requireReadMemory, buffers::releaseReadMemory);
    doAnswer(
            invocation ->
                cache.acquire(
                    "app",
                    1,
                    0,
                    new ShuffleIndexResult(
                        new FileSegmentManagedBuffer(file.toFile(), 0, contents.length),
                        100,
                        "data",
                        7)))
        .when(taskManager)
        .getCachedShuffleIndex(anyString(), anyInt(), anyInt(), anyInt(), anyInt());
    AtomicBoolean holdFirst = new AtomicBoolean(true);
    CountDownLatch sending = new CountDownLatch(1);
    CountDownLatch proceed = new CountDownLatch(1);
    ServerInterceptor holdSuccessfulResponse =
        new ServerInterceptor() {
          @Override
          public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
              ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
            return next.startCall(
                new SimpleForwardingServerCall<ReqT, RespT>(call) {
                  @Override
                  public void sendMessage(RespT message) {
                    if (((GetLocalShuffleIndexResponse) message).getStatus()
                            == StatusCode.SUCCESS.toProto()
                        && holdFirst.compareAndSet(true, false)) {
                      sending.countDown();
                      try {
                        if (!proceed.await(10, TimeUnit.SECONDS)) {
                          throw new IllegalStateException("Timed out waiting to send");
                        }
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                      }
                    }
                    super.sendMessage(message);
                  }
                },
                headers);
          }
        };
    GrpcSharedBufferAllocator allocator =
        new GrpcSharedBufferAllocator(PooledByteBufAllocator.DEFAULT);
    Server server =
        NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
            .withChildOption(ChannelOption.ALLOCATOR, allocator)
            .addService(ServerInterceptors.intercept(service, holdSuccessfulResponse))
            .build()
            .start();
    ManagedChannel channel =
        NettyChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
    try {
      final Future<GetLocalShuffleIndexResponse> first =
          ShuffleServerGrpc.newFutureStub(channel)
              .withDeadlineAfter(10, TimeUnit.SECONDS)
              .getLocalShuffleIndex(request);
      assertTrue(sending.await(5, TimeUnit.SECONDS));
      assertEquals(contents.length, ShuffleServerMetrics.gaugeReadBufferUsedSize.get());
      GetLocalShuffleIndexResponse second =
          ShuffleServerGrpc.newBlockingStub(channel)
              .withDeadlineAfter(5, TimeUnit.SECONDS)
              .getLocalShuffleIndex(request);
      assertArrayEquals(contents, second.getIndexData().toByteArray());
      // First call is still blocked. Second completed without a private 2 MB reservation or queue.
      assertFalse(first.isDone());
      proceed.countDown();
      assertArrayEquals(contents, first.get(5, TimeUnit.SECONDS).getIndexData().toByteArray());
      assertTrue(
          allocator.getSharedBytes() >= 2L * (contents.length - 4096),
          "Both responses must actually share their payload through the Netty allocator");
      Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> cache.getReaderCount() == 0);
      assertEquals(contents.length, ShuffleServerMetrics.gaugeReadBufferUsedSize.get());
    } finally {
      proceed.countDown();
      channel.shutdownNow();
      server.shutdownNow();
      channel.awaitTermination(5, TimeUnit.SECONDS);
      server.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void nettyCancellationReleasesSharedSnapshotWhileClientIsNotReading() throws Exception {
    final int size = 8_000_000;
    final Path file = Files.write(directory.resolve("cancel-index"), new byte[size]);
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.set(ShuffleServerConf.SERVER_READ_BUFFER_CAPACITY, 10_000_000L);
    cache.close();
    buffers = new ShuffleBufferManager(conf, mock(ShuffleFlushManager.class), false);
    cache = new ShuffleIndexCache(60_000, buffers::requireReadMemory, buffers::releaseReadMemory);
    doAnswer(
            invocation ->
                cache.acquire(
                    "app",
                    1,
                    0,
                    new ShuffleIndexResult(
                        new FileSegmentManagedBuffer(file.toFile(), 0, size), 100, "data", 7)))
        .when(taskManager)
        .getCachedShuffleIndex(anyString(), anyInt(), anyInt(), anyInt(), anyInt());
    final GrpcSharedBufferAllocator allocator =
        new GrpcSharedBufferAllocator(PooledByteBufAllocator.DEFAULT);
    Server server =
        NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
            .withChildOption(ChannelOption.ALLOCATOR, allocator)
            .addService(service)
            .build()
            .start();
    ManagedChannel channel =
        NettyChannelBuilder.forAddress("127.0.0.1", server.getPort())
            .flowControlWindow(65535)
            .maxInboundMessageSize(size + 1024)
            .usePlaintext()
            .build();
    ClientCall<GetLocalShuffleIndexRequest, GetLocalShuffleIndexResponse> call =
        channel.newCall(
            ShuffleServerGrpc.getGetLocalShuffleIndexMethod(),
            CallOptions.DEFAULT.withDeadlineAfter(10, TimeUnit.SECONDS));
    try {
      // Do not request response messages: the server must retain flow-controlled send views.
      call.start(new ClientCall.Listener<GetLocalShuffleIndexResponse>() {}, new Metadata());
      call.sendMessage(request);
      call.halfClose();
      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .until(() -> allocator.getSharedBytes() >= size - 4096);
      assertEquals(1, cache.getReaderCount());
      cache.invalidate("app", null);
      assertEquals(size, cache.getReservedBytes());
      call.cancel("test cancellation", null);
      Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> cache.getReservedBytes() == 0);
      assertEquals(0, ShuffleServerMetrics.gaugeReadBufferUsedSize.get());
    } finally {
      call.cancel("cleanup", null);
      channel.shutdownNow();
      server.shutdownNow();
      channel.awaitTermination(5, TimeUnit.SECONDS);
      server.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static class Observer extends ServerCallStreamObserver<GetLocalShuffleIndexResponse> {
    private Runnable onClose;
    private Runnable onCancel;
    private volatile GetLocalShuffleIndexResponse response;
    private volatile boolean completed;
    private volatile Throwable error;

    @Override
    public void onNext(GetLocalShuffleIndexResponse value) {
      response = value;
    }

    @Override
    public void onError(Throwable error) {
      this.error = error;
    }

    @Override
    public void onCompleted() {
      completed = true;
    }

    @Override
    public void setOnCloseHandler(Runnable handler) {
      onClose = handler;
    }

    @Override
    public void setOnCancelHandler(Runnable handler) {
      onCancel = handler;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public void setCompression(String compression) {}

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void setOnReadyHandler(Runnable handler) {}

    @Override
    public void disableAutoInboundFlowControl() {}

    @Override
    public void request(int count) {}

    @Override
    public void setMessageCompression(boolean enabled) {}
  }
}
