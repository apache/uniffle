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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletContext;

import com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.web.resource.BaseResource;
import org.apache.uniffle.common.web.resource.Response;
import org.apache.uniffle.coordinator.AppInfo;
import org.apache.uniffle.coordinator.ApplicationManager;
import org.apache.uniffle.coordinator.CoordinatorServer;
import org.apache.uniffle.coordinator.ServerNode;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;
import org.apache.uniffle.coordinator.web.vo.AppInfoVO;
import org.apache.uniffle.coordinator.web.vo.UserAppNumVO;
import org.apache.uniffle.proto.RssProtos;

@Produces({MediaType.APPLICATION_JSON})
public class ApplicationResource extends BaseResource {
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationResource.class);
  @Context protected ServletContext servletContext;

  @GET
  @Path("/total")
  public Response<Map<String, Integer>> getAppTotality() {
    return execute(
        () -> {
          Map<String, Integer> appTotalityMap = Maps.newHashMap();
          appTotalityMap.put("appCurrent", (int) CoordinatorMetrics.gaugeRunningAppNum.get());
          appTotalityMap.put("appTotality", (int) CoordinatorMetrics.counterTotalAppNum.get());
          return appTotalityMap;
        });
  }

  @GET
  @Path("/userTotal")
  public Response<List<UserAppNumVO>> getUserApps() {
    return execute(
        () -> {
          Map<String, Map<String, AppInfo>> currentUserAndApp =
              getApplicationManager().getCurrentUserAndApp();
          List<UserAppNumVO> usercnt = new ArrayList<>();
          for (Map.Entry<String, Map<String, AppInfo>> stringMapEntry :
              currentUserAndApp.entrySet()) {
            String userName = stringMapEntry.getKey();
            usercnt.add(new UserAppNumVO(userName, stringMapEntry.getValue().size()));
          }
          // Display inverted by the number of user applications.
          usercnt.sort(Comparator.reverseOrder());
          return usercnt;
        });
  }

  @GET
  @Path("/appInfos")
  public Response<List<AppInfoVO>> getAppInfoList() {
    return execute(
        () -> {
          List<AppInfoVO> userToAppList = new ArrayList<>();
          Map<String, Map<String, AppInfo>> currentUserAndApp =
              getApplicationManager().getCurrentUserAndApp();
          List<ServerNode> serverNodes = getCoordinatorServer().getClusterManager().list();
          for (Map.Entry<String, Map<String, AppInfo>> userAppIdTimestampMap :
              currentUserAndApp.entrySet()) {
            for (Map.Entry<String, AppInfo> appIdTimestampMap :
                userAppIdTimestampMap.getValue().entrySet()) {
              String user = appIdTimestampMap.getKey();
              AppInfo appInfo = appIdTimestampMap.getValue();
              AppInfoVO appInfoVO =
                  new AppInfoVO(
                      user,
                      appInfo.getAppId(),
                      appInfo.getUpdateTime(),
                      appInfo.getRegistrationTime(),
                      appInfo.getVersion(),
                      appInfo.getGitCommitId(),
                      0,
                      0,
                      0,
                      0,
                      0,
                      0,
                      0);
              for (ServerNode server : serverNodes) {
                Map<String, RssProtos.ApplicationInfo> appIdToInfos = server.getAppIdToInfos();
                if (appIdToInfos.containsKey(appInfoVO.getAppId())) {
                  RssProtos.ApplicationInfo app = appIdToInfos.get(appInfoVO.getAppId());
                  appInfoVO.setPartitionNum(appInfoVO.getPartitionNum() + app.getPartitionNum());
                  appInfoVO.setMemorySize(appInfoVO.getMemorySize() + app.getMemorySize());
                  appInfoVO.setLocalFileNum(appInfoVO.getLocalFileNum() + app.getLocalFileNum());
                  appInfoVO.setLocalTotalSize(
                      appInfoVO.getLocalTotalSize() + app.getLocalTotalSize());
                  appInfoVO.setHadoopFileNum(appInfoVO.getHadoopFileNum() + app.getHadoopFileNum());
                  appInfoVO.setHadoopTotalSize(
                      appInfoVO.getHadoopTotalSize() + app.getHadoopTotalSize());
                  appInfoVO.setTotalSize(appInfoVO.getTotalSize() + app.getTotalSize());
                }
              }
              userToAppList.add(appInfoVO);
            }
          }
          // Display is inverted by the submission time of the application.
          userToAppList.sort(Comparator.reverseOrder());
          return userToAppList;
        });
  }

  private ApplicationManager getApplicationManager() {
    return (ApplicationManager)
        servletContext.getAttribute(ApplicationManager.class.getCanonicalName());
  }

  private CoordinatorServer getCoordinatorServer() {
    return (CoordinatorServer)
        servletContext.getAttribute(CoordinatorServer.class.getCanonicalName());
  }
}
