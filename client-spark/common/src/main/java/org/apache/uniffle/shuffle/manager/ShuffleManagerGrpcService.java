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

package org.apache.uniffle.shuffle.manager;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.protobuf.UnsafeByteOperations;
import io.grpc.stub.StreamObserver;
import org.apache.spark.SparkException;
import org.apache.spark.shuffle.RssSparkShuffleUtils;
import org.apache.spark.shuffle.events.ShuffleReadMetric;
import org.apache.spark.shuffle.events.ShuffleWriteMetric;
import org.apache.spark.shuffle.events.TaskShuffleReadInfoEvent;
import org.apache.spark.shuffle.events.TaskShuffleWriteInfoEvent;
import org.apache.spark.shuffle.handle.MutableShuffleHandleInfo;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ReceivingFailureServer;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.proto.ShuffleManagerGrpc.ShuffleManagerImplBase;
import org.apache.uniffle.shuffle.BlockIdManager;

public class ShuffleManagerGrpcService extends ShuffleManagerImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleManagerGrpcService.class);
  private final Map<Integer, RssShuffleStatus> shuffleStatus = JavaUtils.newConcurrentMap();
  private final RssShuffleManagerInterface shuffleManager;

  public ShuffleManagerGrpcService(RssShuffleManagerInterface shuffleManager) {
    this.shuffleManager = shuffleManager;
  }

  @Override
  public void reportShuffleFetchFailure(
      RssProtos.ReportShuffleFetchFailureRequest request,
      StreamObserver<RssProtos.ReportShuffleFetchFailureResponse> responseObserver) {
    String appId = request.getAppId();
    int shuffleId = request.getShuffleId();
    int uniffleShuffleId = request.getUniffleShuffleId();
    int stageAttemptId = request.getStageAttemptId();
    int stageAttemptNumber = request.getStageAttemptNumber();
    int partitionId = request.getPartitionId();
    RssProtos.StatusCode code;
    boolean reSubmitWholeStage;
    String msg;
    if (!appId.equals(shuffleManager.getAppId())) {
      msg =
          String.format(
              "got a wrong shuffle fetch failure report from appId: %s, expected appId: %s",
              appId, shuffleManager.getAppId());
      LOG.warn(msg);
      code = RssProtos.StatusCode.INVALID_REQUEST;
      reSubmitWholeStage = false;
    } else {
      RssShuffleStatus status =
          shuffleStatus.computeIfAbsent(
              request.getShuffleId(),
              key -> {
                int partitionNum = shuffleManager.getPartitionNum(key);
                return new RssShuffleStatus(partitionNum, stageAttemptNumber);
              });
      int c = status.resetStageAttemptIfNecessary(stageAttemptNumber);
      if (c < 0) {
        msg =
            String.format(
                "got an old stage(%d vs %d) shuffle fetch failure report, which should be impossible.",
                status.getStageAttempt(), stageAttemptNumber);
        LOG.warn(msg);
        code = RssProtos.StatusCode.INVALID_REQUEST;
        reSubmitWholeStage = false;
      } else { // update the stage partition fetch failure count
        synchronized (status) {
          code = RssProtos.StatusCode.SUCCESS;
          status.incPartitionFetchFailure(stageAttemptNumber, partitionId);
          if (status.currentPartitionIsFetchFailed(
              stageAttemptNumber, partitionId, shuffleManager)) {
            reSubmitWholeStage = true;
            if (!status.hasClearedMapTrackerBlock()) {
              try {
                // Clear the metadata of the completed task, after the upstream ShuffleId is
                // cleared, the write Stage can be triggered again.
                shuffleManager.unregisterAllMapOutput(shuffleId);
                status.clearedMapTrackerBlock();
                LOG.info(
                    "Clear shuffle result in shuffleId:{}, uniffleShuffleId:{}, stageId:{}, stageAttemptNumber:{} in the write failure phase.",
                    shuffleId,
                    uniffleShuffleId,
                    stageAttemptId,
                    stageAttemptNumber);
              } catch (SparkException e) {
                LOG.error(
                    "Clear MapoutTracker Meta failed in shuffleId:{}, uniffleShuffleId:{}, stageId:{}, stageAttemptNumber:{} in the write failure phase.",
                    shuffleId,
                    uniffleShuffleId,
                    stageAttemptId,
                    stageAttemptNumber);
                throw new RssException("Clear MapoutTracker Meta failed!", e);
              }
            }
            msg =
                String.format(
                    "report shuffle fetch failure as maximum number(%d) of shuffle fetch is occurred",
                    shuffleManager.getMaxFetchFailures());
          } else {
            reSubmitWholeStage = false;
            msg = "don't report shuffle fetch failure";
          }
        }
      }
    }

    RssProtos.ReportShuffleFetchFailureResponse reply =
        RssProtos.ReportShuffleFetchFailureResponse.newBuilder()
            .setStatus(code)
            .setReSubmitWholeStage(reSubmitWholeStage)
            .setMsg(msg)
            .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void reassignOnBlockSendFailure(
      org.apache.uniffle.proto.RssProtos.RssReassignOnBlockSendFailureRequest request,
      io.grpc.stub.StreamObserver<
              org.apache.uniffle.proto.RssProtos.ReassignOnBlockSendFailureResponse>
          responseObserver) {
    RssProtos.StatusCode code = RssProtos.StatusCode.INTERNAL_ERROR;
    RssProtos.ReassignOnBlockSendFailureResponse reply;
    try {
      LOG.info(
          "Accepted reassign request on block sent failure for shuffleId: {}, stageId: {}, stageAttemptNumber: {} from taskAttemptId: {} on executorId: {} while partition split:{}",
          request.getShuffleId(),
          request.getStageId(),
          request.getStageAttemptNumber(),
          request.getTaskAttemptId(),
          request.getExecutorId(),
          request.getPartitionSplit());
      MutableShuffleHandleInfo handle =
          shuffleManager.reassignOnBlockSendFailure(
              request.getStageId(),
              request.getStageAttemptNumber(),
              request.getShuffleId(),
              request.getFailurePartitionToServerIdsMap().entrySet().stream()
                  .collect(
                      Collectors.toMap(
                          Map.Entry::getKey, x -> ReceivingFailureServer.fromProto(x.getValue()))),
              request.getPartitionSplit());
      code = RssProtos.StatusCode.SUCCESS;
      reply =
          RssProtos.ReassignOnBlockSendFailureResponse.newBuilder()
              .setStatus(code)
              .setHandle(MutableShuffleHandleInfo.toProto(handle))
              .build();
    } catch (Exception e) {
      LOG.error("Errors on reassigning when block send failure.", e);
      reply =
          RssProtos.ReassignOnBlockSendFailureResponse.newBuilder()
              .setStatus(code)
              .setMsg(e.getMessage())
              .build();
    }
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  /**
   * Remove the no longer used shuffle id's rss shuffle status. This is called when ShuffleManager
   * unregisters the corresponding shuffle id.
   *
   * @param shuffleId the shuffle id to unregister.
   */
  public void unregisterShuffle(int shuffleId) {
    shuffleStatus.remove(shuffleId);
  }

  private static class RssShuffleStatus {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    private final int[] partitions;
    private int stageAttempt;
    // Whether the Shuffle result has been cleared for the current number of attempts.
    private boolean hasClearedMapTrackerBlock;

    private RssShuffleStatus(int partitionNum, int stageAttempt) {
      this.stageAttempt = stageAttempt;
      this.partitions = new int[partitionNum];
      this.hasClearedMapTrackerBlock = false;
    }

    private <T> T withReadLock(Supplier<T> fn) {
      readLock.lock();
      try {
        return fn.get();
      } finally {
        readLock.unlock();
      }
    }

    private <T> T withWriteLock(Supplier<T> fn) {
      writeLock.lock();
      try {
        return fn.get();
      } finally {
        writeLock.unlock();
      }
    }

    // todo: maybe it's more performant to just use synchronized method here.
    public int getStageAttempt() {
      return withReadLock(() -> this.stageAttempt);
    }

    /**
     * Check whether the input stage attempt is a new stage or not. If a new stage attempt is
     * requested, reset partitions.
     *
     * @param stageAttempt the incoming stage attempt number
     * @return 0 if stageAttempt == this.stageAttempt 1 if stageAttempt > this.stageAttempt -1 if
     *     stateAttempt < this.stageAttempt which means nothing happens
     */
    public int resetStageAttemptIfNecessary(int stageAttempt) {
      return withWriteLock(
          () -> {
            if (this.stageAttempt < stageAttempt) {
              // a new stage attempt is issued. the partitions array should be clear and reset.
              Arrays.fill(this.partitions, 0);
              this.stageAttempt = stageAttempt;
              return 1;
            } else if (this.stageAttempt > stageAttempt) {
              return -1;
            }
            return 0;
          });
    }

    public void incPartitionFetchFailure(int stageAttempt, int partition) {
      withWriteLock(
          () -> {
            if (this.stageAttempt != stageAttempt) {
              // do nothing here
            } else {
              this.partitions[partition] = this.partitions[partition] + 1;
            }
            return null;
          });
    }

    public boolean currentPartitionIsFetchFailed(
        int stageAttempt, int partition, RssShuffleManagerInterface shuffleManager) {
      return withReadLock(
          () -> {
            if (this.stageAttempt != stageAttempt) {
              return false;
            } else {
              return this.partitions[partition] >= shuffleManager.getMaxFetchFailures();
            }
          });
    }

    public void clearedMapTrackerBlock() {
      withWriteLock(
          () -> {
            this.hasClearedMapTrackerBlock = true;
            return null;
          });
    }

    public boolean hasClearedMapTrackerBlock() {
      return withReadLock(
          () -> {
            return hasClearedMapTrackerBlock;
          });
    }
  }

  @Override
  public void getShuffleResult(
      RssProtos.GetShuffleResultRequest request,
      StreamObserver<RssProtos.GetShuffleResultResponse> responseObserver) {
    String appId = request.getAppId();
    if (!appId.equals(shuffleManager.getAppId())) {
      RssProtos.GetShuffleResultResponse reply =
          RssProtos.GetShuffleResultResponse.newBuilder()
              .setStatus(RssProtos.StatusCode.ACCESS_DENIED)
              .setRetMsg("Illegal appId: " + appId)
              .build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
      return;
    }

    int shuffleId = request.getShuffleId();
    int partitionId = request.getPartitionId();

    BlockIdManager blockIdManager = shuffleManager.getBlockIdManager();
    Roaring64NavigableMap blockIdBitmap = blockIdManager.get(shuffleId, partitionId);
    RssProtos.GetShuffleResultResponse reply;
    try {
      byte[] serializeBitmap = RssUtils.serializeBitMap(blockIdBitmap);
      reply =
          RssProtos.GetShuffleResultResponse.newBuilder()
              .setStatus(RssProtos.StatusCode.SUCCESS)
              .setSerializedBitmap(UnsafeByteOperations.unsafeWrap(serializeBitmap))
              .build();
    } catch (Exception exception) {
      LOG.error("Errors on getting the blockId bitmap.", exception);
      reply =
          RssProtos.GetShuffleResultResponse.newBuilder()
              .setStatus(RssProtos.StatusCode.INTERNAL_ERROR)
              .build();
    }
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void getShuffleResultForMultiPart(
      RssProtos.GetShuffleResultForMultiPartRequest request,
      StreamObserver<RssProtos.GetShuffleResultForMultiPartResponse> responseObserver) {
    String appId = request.getAppId();
    if (!appId.equals(shuffleManager.getAppId())) {
      RssProtos.GetShuffleResultForMultiPartResponse reply =
          RssProtos.GetShuffleResultForMultiPartResponse.newBuilder()
              .setStatus(RssProtos.StatusCode.ACCESS_DENIED)
              .setRetMsg("Illegal appId: " + appId)
              .build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
      return;
    }

    BlockIdManager blockIdManager = shuffleManager.getBlockIdManager();
    int shuffleId = request.getShuffleId();
    List<Integer> partitionIds = request.getPartitionsList();

    Roaring64NavigableMap blockIdBitmapCollection = Roaring64NavigableMap.bitmapOf();
    for (int partitionId : partitionIds) {
      Roaring64NavigableMap blockIds = blockIdManager.get(shuffleId, partitionId);
      blockIds.forEach(x -> blockIdBitmapCollection.add(x));
    }

    RssProtos.GetShuffleResultForMultiPartResponse reply;
    try {
      byte[] serializeBitmap = RssUtils.serializeBitMap(blockIdBitmapCollection);
      reply =
          RssProtos.GetShuffleResultForMultiPartResponse.newBuilder()
              .setStatus(RssProtos.StatusCode.SUCCESS)
              .setSerializedBitmap(UnsafeByteOperations.unsafeWrap(serializeBitmap))
              .build();
    } catch (Exception exception) {
      LOG.error("Errors on getting the blockId bitmap.", exception);
      reply =
          RssProtos.GetShuffleResultForMultiPartResponse.newBuilder()
              .setStatus(RssProtos.StatusCode.INTERNAL_ERROR)
              .build();
    }
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void reportShuffleResult(
      RssProtos.ReportShuffleResultRequest request,
      StreamObserver<RssProtos.ReportShuffleResultResponse> responseObserver) {
    String appId = request.getAppId();
    if (!appId.equals(shuffleManager.getAppId())) {
      RssProtos.ReportShuffleResultResponse reply =
          RssProtos.ReportShuffleResultResponse.newBuilder()
              .setStatus(RssProtos.StatusCode.ACCESS_DENIED)
              .setRetMsg("Illegal appId: " + appId)
              .build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
      return;
    }

    BlockIdManager blockIdManager = shuffleManager.getBlockIdManager();
    int shuffleId = request.getShuffleId();

    for (RssProtos.PartitionToBlockIds partitionToBlockIds : request.getPartitionToBlockIdsList()) {
      int partitionId = partitionToBlockIds.getPartitionId();
      List<Long> blockIds = partitionToBlockIds.getBlockIdsList();
      blockIdManager.add(shuffleId, partitionId, blockIds);
    }

    RssProtos.ReportShuffleResultResponse reply =
        RssProtos.ReportShuffleResultResponse.newBuilder()
            .setStatus(RssProtos.StatusCode.SUCCESS)
            .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void reportShuffleWriteMetric(
      RssProtos.ReportShuffleWriteMetricRequest request,
      StreamObserver<RssProtos.ReportShuffleWriteMetricResponse> responseObserver) {
    TaskShuffleWriteInfoEvent event =
        new TaskShuffleWriteInfoEvent(
            request.getStageId(),
            request.getShuffleId(),
            request.getTaskId(),
            request.getMetricsMap().entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        x ->
                            new ShuffleWriteMetric(
                                x.getValue().getDurationMillis(), x.getValue().getByteSize()))));
    RssSparkShuffleUtils.getActiveSparkContext().listenerBus().post(event);
    RssProtos.ReportShuffleWriteMetricResponse reply =
        RssProtos.ReportShuffleWriteMetricResponse.newBuilder()
            .setStatus(RssProtos.StatusCode.SUCCESS)
            .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void reportShuffleReadMetric(
      RssProtos.ReportShuffleReadMetricRequest request,
      StreamObserver<RssProtos.ReportShuffleReadMetricResponse> responseObserver) {
    TaskShuffleReadInfoEvent event =
        new TaskShuffleReadInfoEvent(
            request.getStageId(),
            request.getShuffleId(),
            request.getTaskId(),
            request.getMetricsMap().entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        x ->
                            new ShuffleReadMetric(
                                x.getValue().getDurationMillis(),
                                x.getValue().getByteSize(),
                                x.getValue().getMemoryDurationMillis(),
                                x.getValue().getMemoryByteSize(),
                                x.getValue().getLocalfileDurationMillis(),
                                x.getValue().getLocalfileByteSize(),
                                x.getValue().getHadoopDurationMillis(),
                                x.getValue().getHadoopByteSize()))));
    RssSparkShuffleUtils.getActiveSparkContext().listenerBus().post(event);
    RssProtos.ReportShuffleReadMetricResponse reply =
        RssProtos.ReportShuffleReadMetricResponse.newBuilder()
            .setStatus(RssProtos.StatusCode.SUCCESS)
            .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void getPartitionToShufflerServer(
      RssProtos.PartitionToShuffleServerRequest request,
      StreamObserver<RssProtos.ReassignOnBlockSendFailureResponse> responseObserver) {
    RssProtos.ReassignOnBlockSendFailureResponse reply;
    RssProtos.StatusCode code;
    int shuffleId = request.getShuffleId();
    MutableShuffleHandleInfo shuffleHandle =
        (MutableShuffleHandleInfo) shuffleManager.getShuffleHandleInfoByShuffleId(shuffleId);
    if (shuffleHandle != null) {
      code = RssProtos.StatusCode.SUCCESS;
      reply =
          RssProtos.ReassignOnBlockSendFailureResponse.newBuilder()
              .setStatus(code)
              .setHandle(MutableShuffleHandleInfo.toProto(shuffleHandle))
              .build();
    } else {
      code = RssProtos.StatusCode.INVALID_REQUEST;
      reply = RssProtos.ReassignOnBlockSendFailureResponse.newBuilder().setStatus(code).build();
    }
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void getUniffleShuffleId(
      RssProtos.AppUniffleShuffleIdRequest request,
      StreamObserver<RssProtos.AppUniffleShuffleIdResponse> responseObserver) {
    int shuffleId = request.getShuffleId();
    String appShuffleIdentifier = request.getAppShuffleIdentifier();
    boolean isWriter = request.getIsWriter();
    int uniffleShuffleId =
        shuffleManager.getUniffleShuffleId(shuffleId, appShuffleIdentifier, isWriter);
    RssProtos.AppUniffleShuffleIdResponse reply =
        RssProtos.AppUniffleShuffleIdResponse.newBuilder()
            .setGeneratorShuffleId(uniffleShuffleId)
            .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }
}
