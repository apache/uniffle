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

package org.apache.uniffle.coordinator;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.google.protobuf.Empty;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ReconfigurableRegistry;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ServerStatus;
import org.apache.uniffle.common.audit.RpcAuditContext;
import org.apache.uniffle.common.rpc.ClientContextServerInterceptor;
import org.apache.uniffle.common.storage.StorageInfoUtils;
import org.apache.uniffle.coordinator.access.AccessCheckResult;
import org.apache.uniffle.coordinator.access.AccessInfo;
import org.apache.uniffle.coordinator.audit.CoordinatorRpcAuditContext;
import org.apache.uniffle.coordinator.conf.RssClientConfFetchInfo;
import org.apache.uniffle.coordinator.strategy.assignment.PartitionRangeAssignment;
import org.apache.uniffle.coordinator.util.CoordinatorUtils;
import org.apache.uniffle.proto.CoordinatorServerGrpc;
import org.apache.uniffle.proto.RssProtos.AccessClusterRequest;
import org.apache.uniffle.proto.RssProtos.AccessClusterResponse;
import org.apache.uniffle.proto.RssProtos.AppHeartBeatRequest;
import org.apache.uniffle.proto.RssProtos.AppHeartBeatResponse;
import org.apache.uniffle.proto.RssProtos.ApplicationInfoRequest;
import org.apache.uniffle.proto.RssProtos.ApplicationInfoResponse;
import org.apache.uniffle.proto.RssProtos.CheckServiceAvailableResponse;
import org.apache.uniffle.proto.RssProtos.ClientConfItem;
import org.apache.uniffle.proto.RssProtos.FetchClientConfRequest;
import org.apache.uniffle.proto.RssProtos.FetchClientConfResponse;
import org.apache.uniffle.proto.RssProtos.FetchRemoteStorageRequest;
import org.apache.uniffle.proto.RssProtos.FetchRemoteStorageResponse;
import org.apache.uniffle.proto.RssProtos.GetShuffleAssignmentsResponse;
import org.apache.uniffle.proto.RssProtos.GetShuffleServerListResponse;
import org.apache.uniffle.proto.RssProtos.GetShuffleServerNumResponse;
import org.apache.uniffle.proto.RssProtos.GetShuffleServerRequest;
import org.apache.uniffle.proto.RssProtos.RemoteStorage;
import org.apache.uniffle.proto.RssProtos.RemoteStorageConfItem;
import org.apache.uniffle.proto.RssProtos.ReportShuffleClientOpRequest;
import org.apache.uniffle.proto.RssProtos.ReportShuffleClientOpResponse;
import org.apache.uniffle.proto.RssProtos.ShuffleServerHeartBeatRequest;
import org.apache.uniffle.proto.RssProtos.ShuffleServerHeartBeatResponse;
import org.apache.uniffle.proto.RssProtos.ShuffleServerId;
import org.apache.uniffle.proto.RssProtos.StatusCode;

/** Implementation class for services defined in protobuf */
public class CoordinatorGrpcService extends CoordinatorServerGrpc.CoordinatorServerImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorGrpcService.class);
  private static final Logger AUDIT_LOGGER = LoggerFactory.getLogger("COORDINATOR_RPC_AUDIT_LOG");

  private final CoordinatorServer coordinatorServer;
  private boolean isRpcAuditLogEnabled;
  private List<String> rpcAuditExcludeOpList;

  public CoordinatorGrpcService(CoordinatorServer coordinatorServer) {
    this.coordinatorServer = coordinatorServer;
    isRpcAuditLogEnabled =
        coordinatorServer
            .getCoordinatorConf()
            .getReconfigurableConf(CoordinatorConf.COORDINATOR_RPC_AUDIT_LOG_ENABLED)
            .get();
    rpcAuditExcludeOpList =
        coordinatorServer
            .getCoordinatorConf()
            .getReconfigurableConf(CoordinatorConf.COORDINATOR_RPC_AUDIT_LOG_EXCLUDE_LIST)
            .get();
    ReconfigurableRegistry.register(
        Sets.newHashSet(
            CoordinatorConf.COORDINATOR_RPC_AUDIT_LOG_ENABLED.key(),
            CoordinatorConf.COORDINATOR_RPC_AUDIT_LOG_EXCLUDE_LIST.key()),
        (conf, changedProperties) -> {
          if (changedProperties == null || conf == null) {
            return;
          }
          if (changedProperties.contains(CoordinatorConf.COORDINATOR_RPC_AUDIT_LOG_ENABLED.key())) {
            isRpcAuditLogEnabled =
                conf.getBoolean(CoordinatorConf.COORDINATOR_RPC_AUDIT_LOG_ENABLED);
          }
          if (changedProperties.contains(
              CoordinatorConf.COORDINATOR_RPC_AUDIT_LOG_EXCLUDE_LIST.key())) {
            rpcAuditExcludeOpList =
                conf.get(CoordinatorConf.COORDINATOR_RPC_AUDIT_LOG_EXCLUDE_LIST);
          }
        });
  }

  @Override
  public void getShuffleServerList(
      Empty request, StreamObserver<GetShuffleServerListResponse> responseObserver) {
    final GetShuffleServerListResponse response =
        GetShuffleServerListResponse.newBuilder()
            .addAllServers(
                coordinatorServer.getClusterManager().list().stream()
                    .map(ServerNode::convertToGrpcProto)
                    .collect(Collectors.toList()))
            .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void getShuffleServerNum(
      Empty request, StreamObserver<GetShuffleServerNumResponse> responseObserver) {
    final int num = coordinatorServer.getClusterManager().getNodesNum();
    final GetShuffleServerNumResponse response =
        GetShuffleServerNumResponse.newBuilder().setNum(num).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void getShuffleAssignments(
      GetShuffleServerRequest request,
      StreamObserver<GetShuffleAssignmentsResponse> responseObserver) {
    try (CoordinatorRpcAuditContext auditContext = createAuditContext("getShuffleAssignments")) {
      final String appId = request.getApplicationId();
      final int shuffleId = request.getShuffleId();
      final int partitionNum = request.getPartitionNum();
      final int partitionNumPerRange = request.getPartitionNumPerRange();
      final int replica = request.getDataReplica();
      final Set<String> requiredTags = Sets.newHashSet(request.getRequireTagsList());
      final int requiredShuffleServerNumber = request.getAssignmentShuffleServerNumber();
      final int estimateTaskConcurrency = request.getEstimateTaskConcurrency();
      final Set<String> faultyServerIds = new HashSet<>(request.getFaultyServerIdsList());

      auditContext.withAppId(appId);
      auditContext.withArgs(
          String.format(
              "shuffleId=%d, partitionNum=%d, partitionNumPerRange=%d, replica=%d, requiredTags=%s, "
                  + "requiredShuffleServerNumber=%d, faultyServerIds=%s, stageId=%d, stageAttemptNumber=%d, isReassign=%b",
              shuffleId,
              partitionNum,
              partitionNumPerRange,
              replica,
              requiredTags,
              requiredShuffleServerNumber,
              faultyServerIds,
              request.getStageId(),
              request.getStageAttemptNumber(),
              request.getReassign()));

      LOG.info(
          "Request of getShuffleAssignments for appId[{}], shuffleId[{}], partitionNum[{}],"
              + " partitionNumPerRange[{}], replica[{}], requiredTags[{}], requiredShuffleServerNumber[{}],"
              + " faultyServerIds[{}], stageId[{}], stageAttemptNumber[{}], isReassign[{}]",
          appId,
          shuffleId,
          partitionNum,
          partitionNumPerRange,
          replica,
          requiredTags,
          requiredShuffleServerNumber,
          faultyServerIds.size(),
          request.getStageId(),
          request.getStageAttemptNumber(),
          request.getReassign());

      GetShuffleAssignmentsResponse response = null;
      try {
        if (!coordinatorServer.getClusterManager().isReadyForServe()) {
          throw new Exception("Coordinator is out-of-service when in starting.");
        }

        final PartitionRangeAssignment pra =
            coordinatorServer
                .getAssignmentStrategy()
                .assign(
                    partitionNum,
                    partitionNumPerRange,
                    replica,
                    requiredTags,
                    requiredShuffleServerNumber,
                    estimateTaskConcurrency,
                    faultyServerIds);
        response = CoordinatorUtils.toGetShuffleAssignmentsResponse(pra);
        logAssignmentResult(appId, shuffleId, pra);
        responseObserver.onNext(response);
      } catch (Exception e) {
        LOG.error(
            "Errors on getting shuffle assignments for app: {}, shuffleId: {}, partitionNum: {}, "
                + "partitionNumPerRange: {}, replica: {}, requiredTags: {}",
            appId,
            shuffleId,
            partitionNum,
            partitionNumPerRange,
            replica,
            requiredTags,
            e);
        response =
            GetShuffleAssignmentsResponse.newBuilder()
                .setStatus(StatusCode.INTERNAL_ERROR)
                .setRetMsg(e.getMessage())
                .build();
        responseObserver.onNext(response);
      } finally {
        if (response != null) {
          auditContext.withStatusCode(response.getStatus());
        }
        responseObserver.onCompleted();
      }
    }
  }

  @Override
  public void heartbeat(
      ShuffleServerHeartBeatRequest request,
      StreamObserver<ShuffleServerHeartBeatResponse> responseObserver) {
    try (CoordinatorRpcAuditContext auditContext = createAuditContext("heartbeat")) {
      final ServerNode serverNode = toServerNode(request);
      auditContext.withArgs("serverNode=" + serverNode.getId());
      coordinatorServer.getClusterManager().add(serverNode);
      final ShuffleServerHeartBeatResponse response =
          ShuffleServerHeartBeatResponse.newBuilder()
              .setRetMsg("")
              .setStatus(StatusCode.SUCCESS)
              .build();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got heartbeat from {}", serverNode);
      }
      auditContext.withStatusCode(response.getStatus());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void checkServiceAvailable(
      Empty request, StreamObserver<CheckServiceAvailableResponse> responseObserver) {
    try (CoordinatorRpcAuditContext auditContext = createAuditContext("checkServiceAvailable")) {
      final CheckServiceAvailableResponse response =
          CheckServiceAvailableResponse.newBuilder()
              .setAvailable(coordinatorServer.getClusterManager().getNodesNum() > 0)
              .build();
      auditContext.withStatusCode(StatusCode.SUCCESS);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void reportClientOperation(
      ReportShuffleClientOpRequest request,
      StreamObserver<ReportShuffleClientOpResponse> responseObserver) {
    try (CoordinatorRpcAuditContext auditContext = createAuditContext("reportClientOperation")) {
      final String clientHost = request.getClientHost();
      final int clientPort = request.getClientPort();
      final ShuffleServerId shuffleServer = request.getServer();
      final String operation = request.getOperation();
      auditContext.withArgs(
          String.format("%s:%s->%s->%s", clientHost, clientPort, operation, shuffleServer));

      LOG.info(clientHost + ":" + clientPort + "->" + operation + "->" + shuffleServer);
      final ReportShuffleClientOpResponse response =
          ReportShuffleClientOpResponse.newBuilder()
              .setRetMsg("")
              .setStatus(StatusCode.SUCCESS)
              .build();
      auditContext.withStatusCode(response.getStatus());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void appHeartbeat(
      AppHeartBeatRequest request, StreamObserver<AppHeartBeatResponse> responseObserver) {
    try (CoordinatorRpcAuditContext auditContext = createAuditContext("appHeartbeat")) {
      String appId = request.getAppId();
      auditContext.withAppId(appId);
      coordinatorServer.getApplicationManager().refreshAppId(appId);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got heartbeat from application: {}", appId);
      }
      AppHeartBeatResponse response =
          AppHeartBeatResponse.newBuilder().setRetMsg("").setStatus(StatusCode.SUCCESS).build();

      if (Context.current().isCancelled()) {
        responseObserver.onError(
            Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
        auditContext.withStatusCode("CANCELLED");
        LOG.warn("Cancelled by client {} for after deadline.", appId);
        return;
      }

      auditContext.withStatusCode(response.getStatus());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void registerApplicationInfo(
      ApplicationInfoRequest request, StreamObserver<ApplicationInfoResponse> responseObserver) {
    try (CoordinatorRpcAuditContext auditContext = createAuditContext("registerApplicationInfo")) {
      String appId = request.getAppId();
      String user = request.getUser();
      auditContext.withAppId(appId).withArgs("user=" + user);
      coordinatorServer
          .getApplicationManager()
          .registerApplicationInfo(appId, user, request.getVersion(), request.getGitCommitId());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got a registered application info: {}", appId);
      }
      ApplicationInfoResponse response =
          ApplicationInfoResponse.newBuilder().setRetMsg("").setStatus(StatusCode.SUCCESS).build();

      if (Context.current().isCancelled()) {
        responseObserver.onError(
            Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
        auditContext.withStatusCode("CANCELLED");
        LOG.warn("Cancelled by client {} for after deadline.", appId);
        return;
      }

      auditContext.withStatusCode(response.getStatus());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void accessCluster(
      AccessClusterRequest request, StreamObserver<AccessClusterResponse> responseObserver) {
    try (CoordinatorRpcAuditContext auditContext = createAuditContext("accessCluster")) {
      StatusCode statusCode = StatusCode.SUCCESS;
      AccessClusterResponse response;
      AccessManager accessManager = coordinatorServer.getAccessManager();

      AccessInfo accessInfo =
          new AccessInfo(
              request.getAccessId(),
              Sets.newHashSet(request.getTagsList()),
              request.getExtraPropertiesMap(),
              request.getUser());

      auditContext.withArgs("accessInfo=" + accessInfo);

      AccessCheckResult result = accessManager.handleAccessRequest(accessInfo);
      if (!result.isSuccess()) {
        statusCode = StatusCode.ACCESS_DENIED;
      }

      response =
          AccessClusterResponse.newBuilder()
              .setStatus(statusCode)
              .setRetMsg(result.getMsg())
              .setUuid(result.getUuid())
              .build();

      if (Context.current().isCancelled()) {
        responseObserver.onError(
            Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
        auditContext.withStatusCode("CANCELLED");
        LOG.warn("Cancelled by client {} for after deadline.", accessInfo);
        return;
      }

      auditContext.withStatusCode(response.getStatus());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  /** To be compatible with the older client version. */
  @Override
  public void fetchClientConf(
      Empty empty, StreamObserver<FetchClientConfResponse> responseObserver) {
    try (CoordinatorRpcAuditContext auditContext = createAuditContext("fetchClientConf")) {
      fetchClientConfImpl(RssClientConfFetchInfo.EMPTY_CLIENT_CONF_FETCH_INFO, responseObserver);
      auditContext.withStatusCode(StatusCode.SUCCESS);
    }
  }

  @Override
  public void fetchClientConfV2(
      FetchClientConfRequest request, StreamObserver<FetchClientConfResponse> responseObserver) {
    try (CoordinatorRpcAuditContext auditContext = createAuditContext("fetchClientConfV2")) {
      fetchClientConfImpl(RssClientConfFetchInfo.fromProto(request), responseObserver);
      auditContext.withStatusCode(StatusCode.SUCCESS);
    }
  }

  private void fetchClientConfImpl(
      RssClientConfFetchInfo rssClientConfFetchInfo,
      StreamObserver<FetchClientConfResponse> responseObserver) {
    FetchClientConfResponse response;
    FetchClientConfResponse.Builder builder =
        FetchClientConfResponse.newBuilder().setStatus(StatusCode.SUCCESS);
    boolean dynamicConfEnabled =
        coordinatorServer
            .getCoordinatorConf()
            .getBoolean(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_ENABLED);
    if (dynamicConfEnabled) {
      Map<String, String> clientConfigs =
          coordinatorServer.getClientConfApplyManager().apply(rssClientConfFetchInfo);
      for (Map.Entry<String, String> kv : clientConfigs.entrySet()) {
        builder.addClientConf(
            ClientConfItem.newBuilder().setKey(kv.getKey()).setValue(kv.getValue()).build());
      }
    }
    response = builder.build();

    if (Context.current().isCancelled()) {
      responseObserver.onError(
          Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
      LOG.warn("Fetch client conf cancelled by client for after deadline.");
      return;
    }

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void fetchRemoteStorage(
      FetchRemoteStorageRequest request,
      StreamObserver<FetchRemoteStorageResponse> responseObserver) {
    try (CoordinatorRpcAuditContext auditContext = createAuditContext("fetchRemoteStorage")) {
      FetchRemoteStorageResponse response;
      StatusCode status = StatusCode.SUCCESS;
      String appId = request.getAppId();
      auditContext.withAppId(appId);
      try {
        RemoteStorage.Builder rsBuilder = RemoteStorage.newBuilder();
        RemoteStorageInfo rsInfo =
            coordinatorServer.getApplicationManager().pickRemoteStorage(appId);
        if (rsInfo == null) {
          LOG.error("Remote storage of {} do not exist.", appId);
        } else {
          rsBuilder.setPath(rsInfo.getPath());
          for (Map.Entry<String, String> entry : rsInfo.getConfItems().entrySet()) {
            rsBuilder.addRemoteStorageConf(
                RemoteStorageConfItem.newBuilder()
                    .setKey(entry.getKey())
                    .setValue(entry.getValue())
                    .build());
          }
        }
        response =
            FetchRemoteStorageResponse.newBuilder()
                .setStatus(status)
                .setRemoteStorage(rsBuilder.build())
                .build();
      } catch (Exception e) {
        status = StatusCode.INTERNAL_ERROR;
        response = FetchRemoteStorageResponse.newBuilder().setStatus(status).build();
        LOG.error("Error happened when get remote storage for appId[{}]", appId, e);
      }

      if (Context.current().isCancelled()) {
        responseObserver.onError(
            Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
        auditContext.withStatusCode("CANCELLED");
        LOG.warn("Fetch client conf cancelled by client for after deadline.");
        return;
      }

      auditContext.withStatusCode(response.getStatus());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  private void logAssignmentResult(String appId, int shuffleId, PartitionRangeAssignment pra) {
    SortedMap<PartitionRange, List<ServerNode>> assignments = pra.getAssignments();
    if (assignments != null) {
      Set<String> nodeIds = Sets.newHashSet();
      for (Map.Entry<PartitionRange, List<ServerNode>> entry : assignments.entrySet()) {
        for (ServerNode node : entry.getValue()) {
          nodeIds.add(node.getId());
        }
      }
      if (!nodeIds.isEmpty()) {
        LOG.info(
            "Shuffle Servers of assignment for appId[{}], shuffleId[{}] are {}",
            appId,
            shuffleId,
            nodeIds);
      }
    }
  }

  private ServerNode toServerNode(ShuffleServerHeartBeatRequest request) {
    ServerStatus serverStatus =
        request.hasStatus() ? ServerStatus.fromProto(request.getStatus()) : ServerStatus.ACTIVE;
    boolean isHealthy = true;
    if (request.hasIsHealthy()) {
      isHealthy = request.getIsHealthy().getValue();
      /** Compatible with older version */
      if (isHealthy) {
        serverStatus = ServerStatus.ACTIVE;
      } else {
        serverStatus = ServerStatus.UNHEALTHY;
      }
    }
    return new ServerNode(
        request.getServerId().getId(),
        request.getServerId().getIp(),
        request.getServerId().getPort(),
        request.getUsedMemory(),
        request.getPreAllocatedMemory(),
        request.getAvailableMemory(),
        request.getEventNumInFlush(),
        Sets.newHashSet(request.getTagsList()),
        serverStatus,
        StorageInfoUtils.fromProto(request.getStorageInfoMap()),
        request.getServerId().getNettyPort(),
        request.getServerId().getJettyPort(),
        request.getStartTimeMs(),
        request.getVersion(),
        request.getGitCommitId(),
        request.getApplicationInfoList(),
        request.getDisplayMetricsMap());
  }

  /**
   * Creates a {@link CoordinatorRpcAuditContext} instance.
   *
   * @param command the command to be logged by this {@link RpcAuditContext}
   * @return newly-created {@link CoordinatorRpcAuditContext} instance
   */
  private CoordinatorRpcAuditContext createAuditContext(String command) {
    // Audit log may be enabled during runtime
    Logger auditLogger = null;
    if (isRpcAuditLogEnabled && !rpcAuditExcludeOpList.contains(command)) {
      auditLogger = AUDIT_LOGGER;
    }
    CoordinatorRpcAuditContext auditContext = new CoordinatorRpcAuditContext(auditLogger);
    if (auditLogger != null) {
      auditContext
          .withCommand(command)
          .withFrom(ClientContextServerInterceptor.getIpAddress())
          .withCreationTimeNs(System.nanoTime());
    }
    return auditContext;
  }
}
