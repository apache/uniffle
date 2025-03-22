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

package org.apache.uniffle.coordinator.web.resource;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.ServletContext;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hbase.thirdparty.javax.ws.rs.Consumes;
import org.apache.hbase.thirdparty.javax.ws.rs.DELETE;
import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.POST;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.PathParam;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.QueryParam;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;

import org.apache.uniffle.common.Application;
import org.apache.uniffle.common.ServerStatus;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.web.resource.Authorization;
import org.apache.uniffle.common.web.resource.BaseResource;
import org.apache.uniffle.common.web.resource.Response;
import org.apache.uniffle.coordinator.ApplicationManager;
import org.apache.uniffle.coordinator.ClusterManager;
import org.apache.uniffle.coordinator.ServerNode;
import org.apache.uniffle.coordinator.web.request.ApplicationRequest;
import org.apache.uniffle.coordinator.web.request.CancelDecommissionRequest;
import org.apache.uniffle.coordinator.web.request.DecommissionRequest;

@Produces({MediaType.APPLICATION_JSON})
public class ServerResource extends BaseResource {
  @Context protected ServletContext servletContext;

  @GET
  @Path("/status")
  public Response<String> status() {
    return execute(() -> "success");
  }

  @GET
  @Path("/nodes/{id}")
  public Response<ServerNode> node(@PathParam("id") String id) {
    return execute(() -> getClusterManager().getServerNodeById(id));
  }

  @GET
  @Path("/nodes")
  public Response<List<ServerNode>> nodes(@QueryParam("status") String status) {
    ClusterManager clusterManager = getClusterManager();
    List<ServerNode> serverList;
    if (ServerStatus.UNHEALTHY.name().equalsIgnoreCase(status)) {
      serverList = clusterManager.getUnhealthyServerList();
    } else if (ServerStatus.LOST.name().equalsIgnoreCase(status)) {
      serverList = clusterManager.getLostServerList();
    } else if (ServerStatus.EXCLUDED.name().equalsIgnoreCase(status)) {
      serverList =
          clusterManager.list().stream()
              .filter(node -> clusterManager.getExcludedNodes().contains(node.getId()))
              .collect(Collectors.toList());
    } else {
      List<ServerNode> serverAllList = clusterManager.list();
      serverList =
          serverAllList.stream()
              .filter(node -> !clusterManager.getExcludedNodes().contains(node.getId()))
              .collect(Collectors.toList());
    }
    serverList =
        serverList.stream()
            .filter(
                server ->
                    status == null
                        || server.getStatus().name().equalsIgnoreCase(status)
                        || ServerStatus.EXCLUDED.name().equalsIgnoreCase(status))
            .collect(Collectors.toList());
    serverList.sort(Comparator.comparing(ServerNode::getId));
    return Response.success(serverList);
  }

  @Authorization
  @POST
  @Path("/cancelDecommission")
  public Response<Object> cancelDecommission(CancelDecommissionRequest params) {
    return execute(
        () -> {
          if (CollectionUtils.isEmpty(params.getServerIds())) {
            throw new RssException("Parameter[serverIds] should not be empty!");
          }
          params.getServerIds().forEach(getClusterManager()::cancelDecommission);
          return null;
        });
  }

  @Authorization
  @POST
  @Path("/{id}/cancelDecommission")
  public Response<Object> cancelDecommission(@PathParam("id") String serverId) {
    return execute(
        () -> {
          getClusterManager().cancelDecommission(serverId);
          return null;
        });
  }

  @Authorization
  @POST
  @Path("/decommission")
  public Response<Object> decommission(DecommissionRequest params) {
    return execute(
        () -> {
          if (CollectionUtils.isEmpty(params.getServerIds())) {
            throw new RssException("Parameter[serverIds] should not be empty!");
          }
          params.getServerIds().forEach(getClusterManager()::decommission);
          return null;
        });
  }

  @Authorization
  @POST
  @Path("/{id}/decommission")
  @Produces({MediaType.APPLICATION_JSON})
  public Response<Object> decommission(@PathParam("id") String serverId) {
    return execute(
        () -> {
          getClusterManager().decommission(serverId);
          return null;
        });
  }

  @POST
  @Path("/applications")
  @Produces({MediaType.APPLICATION_JSON})
  public Response<Object> application(ApplicationRequest params) {

    if (params == null) {
      return Response.fail("ApplicationRequest Is not null");
    }

    Set<String> filterApplications = new HashSet<>();
    if (CollectionUtils.isNotEmpty(params.getApplications())) {
      filterApplications = params.getApplications();
    }

    int currentPage = params.getCurrentPage();
    int pageSize = params.getPageSize();
    String startTime = params.getHeartBeatStartTime();
    String endTime = params.getHeartBeatEndTime();
    String appIdRegex = params.getAppIdRegex();

    try {
      ApplicationManager applicationManager = getApplicationManager();
      List<Application> applicationSet =
          applicationManager.getApplications(
              filterApplications, pageSize, currentPage, startTime, endTime, appIdRegex);
      return Response.success(applicationSet);
    } catch (Exception e) {
      return Response.fail(e.getMessage());
    }
  }

  @GET
  @Path("/nodes/summary")
  public Response<Map<String, Integer>> getNodeStatusTotal() {
    return execute(
        () -> {
          ClusterManager clusterManager = getClusterManager();
          List<ServerNode> serverAllList = clusterManager.list();
          List<ServerNode> excludeNodes =
              clusterManager.getExcludedNodes().stream()
                  .map(ServerNode::new)
                  .collect(Collectors.toList());
          List<ServerNode> activeServerList =
              serverAllList.stream()
                  .filter(node -> !clusterManager.getExcludedNodes().contains(node.getId()))
                  .collect(Collectors.toList());
          Map<String, Integer> serverStatusNum =
              Stream.of(
                      activeServerList,
                      clusterManager.getLostServerList(),
                      excludeNodes,
                      clusterManager.getUnhealthyServerList())
                  .flatMap(Collection::stream)
                  .distinct()
                  .collect(
                      Collectors.groupingBy(
                          n -> n.getStatus().name(), Collectors.reducing(0, n -> 1, Integer::sum)));
          return serverStatusNum;
        });
  }

  @DELETE
  @Path("/deleteServer")
  public Response<String> handleDeleteLostServerRequest(@QueryParam("serverId") String serverId) {
    ClusterManager clusterManager = getClusterManager();
    if (clusterManager.deleteLostServerById(serverId)) {
      return Response.success("success");
    }
    return Response.fail("fail");
  }

  @POST
  @Path("/addExcludeNodes")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response<String> handleAddExcludedNodesRequest(Map<String, List<String>> excludeNodes) {
    ClusterManager clusterManager = getClusterManager();
    if (clusterManager.addExcludedNodes(excludeNodes.get("excludeNodes"))) {
      return Response.success("success");
    }
    return Response.fail("fail");
  }

  @POST
  @Path("/removeExcludeNodes")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response<String> handleDeleteExcludeNodesRequest(Map<String, List<String>> excludeNodes) {
    ClusterManager clusterManager = getClusterManager();
    if (clusterManager.removeExcludedNodesFromFile(excludeNodes.get("excludeNodes"))) {
      return Response.success("success");
    }
    return Response.fail("fail");
  }

  private ClusterManager getClusterManager() {
    return (ClusterManager) servletContext.getAttribute(ClusterManager.class.getCanonicalName());
  }

  private ApplicationManager getApplicationManager() {
    return (ApplicationManager)
        servletContext.getAttribute(ApplicationManager.class.getCanonicalName());
  }
}
