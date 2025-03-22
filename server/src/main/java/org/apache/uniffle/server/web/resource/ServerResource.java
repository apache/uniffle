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

package org.apache.uniffle.server.web.resource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.ServletContext;

import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;

import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.common.web.resource.BaseResource;
import org.apache.uniffle.common.web.resource.ConfOpsResource;
import org.apache.uniffle.common.web.resource.MetricResource;
import org.apache.uniffle.common.web.resource.PrometheusMetricResource;
import org.apache.uniffle.common.web.resource.Response;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.web.vo.ServerConfVO;

@Path("/api/shuffleServer")
public class ServerResource extends BaseResource {
  @Context protected ServletContext servletContext;

  @GET
  @Path("/conf")
  public Response<List<ServerConfVO>> getShuffleServerConf() {
    return execute(
        () -> {
          ShuffleServerConf serverConf = getShuffleServer().getShuffleServerConf();
          Set<Map.Entry<String, Object>> allEntry = serverConf.getAll();
          List<ServerConfVO> serverConfVOs = new ArrayList<>();
          for (Map.Entry<String, Object> stringObjectEntry : allEntry) {
            ServerConfVO result =
                new ServerConfVO(
                    stringObjectEntry.getKey(), String.valueOf(stringObjectEntry.getValue()));
            serverConfVOs.add(result);
          }
          return serverConfVOs;
        });
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
  public String getShuffleServerStacks() {
    StringBuilder builder = new StringBuilder();
    ThreadUtils.printThreadInfo(builder, "");
    return builder.toString();
  }

  @Path("/conf/ops")
  public Class<ConfOpsResource> getConfOps() {
    return ConfOpsResource.class;
  }

  private ShuffleServer getShuffleServer() {
    return (ShuffleServer) servletContext.getAttribute(ShuffleServer.class.getCanonicalName());
  }
}
