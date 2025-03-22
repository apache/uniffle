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

package org.apache.uniffle.dashboard.web.resource;

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
import org.apache.uniffle.common.web.resource.BaseResource;
import org.apache.uniffle.common.web.resource.Response;
import org.apache.uniffle.dashboard.web.Dashboard;
import org.apache.uniffle.dashboard.web.config.DashboardConf;
import org.apache.uniffle.dashboard.web.vo.DashboardConfVO;

@Produces({MediaType.APPLICATION_JSON})
public class DashboardResource extends BaseResource {

  @Context protected ServletContext servletContext;

  @GET
  @Path("/conf")
  public Response<List<DashboardConfVO>> getDashboardConf() {
    return execute(
        () -> {
          DashboardConf conf = getDashboard().getConf();
          Set<Map.Entry<String, Object>> allEntry = conf.getAll();
          List<DashboardConfVO> dashboardConfVOs = new ArrayList<>();
          for (Map.Entry<String, Object> stringObjectEntry : allEntry) {
            DashboardConfVO result =
                new DashboardConfVO(
                    stringObjectEntry.getKey(), String.valueOf(stringObjectEntry.getValue()));
            dashboardConfVOs.add(result);
          }
          return dashboardConfVOs;
        });
  }

  @GET
  @Path("/info")
  public Response<Map<String, Object>> getDashboardInfo() {
    return execute(
        () -> {
          Map<String, Object> dashboardInfo = new HashMap<>();
          dashboardInfo.put("version", Constants.VERSION_AND_REVISION_SHORT);
          dashboardInfo.put("startTime", getDashboard().getStartTimeMs());
          return dashboardInfo;
        });
  }

  private Dashboard getDashboard() {
    return (Dashboard) servletContext.getAttribute(Dashboard.class.getCanonicalName());
  }
}
