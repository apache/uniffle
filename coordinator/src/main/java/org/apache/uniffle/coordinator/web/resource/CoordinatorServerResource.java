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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.ServletContext;

import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;

import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.common.web.resource.BaseResource;
import org.apache.uniffle.common.web.resource.ConfOpsResource;
import org.apache.uniffle.common.web.resource.MetricResource;
import org.apache.uniffle.common.web.resource.PrometheusMetricResource;
import org.apache.uniffle.common.web.resource.Response;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.CoordinatorServer;
import org.apache.uniffle.coordinator.util.CoordinatorUtils;
import org.apache.uniffle.coordinator.web.vo.CoordinatorConfVO;

import static org.apache.uniffle.common.config.RssBaseConf.JETTY_HTTP_PORT;

@Produces({MediaType.APPLICATION_JSON})
public class CoordinatorServerResource extends BaseResource {

  @Context protected ServletContext servletContext;

  @GET
  @Path("/conf")
  public Response<List<CoordinatorConfVO>> getCoordinatorConf() {
    return execute(
        () -> {
          CoordinatorConf coordinatorConf = getCoordinatorServer().getCoordinatorConf();
          Set<Map.Entry<String, Object>> allEntry = coordinatorConf.getAll();
          List<CoordinatorConfVO> coordinatorConfVOs = new ArrayList<>();
          for (Map.Entry<String, Object> stringObjectEntry : allEntry) {
            CoordinatorConfVO result =
                new CoordinatorConfVO(
                    stringObjectEntry.getKey(), String.valueOf(stringObjectEntry.getValue()));
            coordinatorConfVOs.add(result);
          }
          return coordinatorConfVOs;
        });
  }

  @GET
  @Path("/info")
  public Response<Map<String, Object>> getCoordinatorInfo() {
    return execute(
        () -> {
          final CoordinatorConf coordinatorConf = getCoordinatorServer().getCoordinatorConf();
          Map<String, Object> coordinatorServerInfo = new HashMap<>();
          coordinatorServerInfo.put(
              "coordinatorId", coordinatorConf.getString(CoordinatorUtils.COORDINATOR_ID, "none"));
          coordinatorServerInfo.put("serverIp", RssUtils.getHostIp());
          coordinatorServerInfo.put(
              "serverPort", String.valueOf(coordinatorConf.getInteger("rss.rpc.server.port", 0)));
          coordinatorServerInfo.put(
              "serverWebPort", String.valueOf(coordinatorConf.get(JETTY_HTTP_PORT)));
          coordinatorServerInfo.put("version", Constants.VERSION);
          coordinatorServerInfo.put("gitCommitId", Constants.REVISION_SHORT);
          coordinatorServerInfo.put("startTime", getCoordinatorServer().getStartTimeMs());
          return coordinatorServerInfo;
        });
  }

  private CoordinatorServer getCoordinatorServer() {
    return (CoordinatorServer)
        servletContext.getAttribute(CoordinatorServer.class.getCanonicalName());
  }

  @Path("/metrics")
  public Class<MetricResource> getMetricResource() {
    return MetricResource.class;
  }

  @Path("/prometheus/metrics")
  public Class<PrometheusMetricResource> getPrometheusMetricResource() {
    return PrometheusMetricResource.class;
  }

  @GET
  @Path("/stacks")
  public String getCoordinatorStacks() {
    StringBuilder builder = new StringBuilder();
    ThreadUtils.printThreadInfo(builder, "");
    return builder.toString();
  }

  @Path("/conf/ops")
  public Class<ConfOpsResource> getConfOps() {
    return ConfOpsResource.class;
  }

  @Path("/banned")
  public Class<BannedResource> getBannedResource() {
    return BannedResource.class;
  }
}
