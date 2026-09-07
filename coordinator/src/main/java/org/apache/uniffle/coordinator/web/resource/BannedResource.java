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

import java.util.Set;
import javax.servlet.ServletContext;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hbase.thirdparty.javax.ws.rs.Consumes;
import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.POST;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.web.resource.BaseResource;
import org.apache.uniffle.common.web.resource.Response;
import org.apache.uniffle.coordinator.AccessManager;
import org.apache.uniffle.coordinator.BannedManager;
import org.apache.uniffle.coordinator.web.vo.BannedReloadVO;

@Path("/banned")
public class BannedResource extends BaseResource {
  private static final Logger LOG = LoggerFactory.getLogger(BannedResource.class);
  @Context protected ServletContext servletContext;

  @Consumes(MediaType.APPLICATION_JSON)
  @POST
  @Path("/reload")
  public Response<String> reload(BannedReloadVO bannedReloadVo) {
    BannedManager bannedManager = getAccessManager().getBannedManager();
    if (bannedManager != null && bannedReloadVo != null) {
      bannedManager.reloadBannedIdsFromRest(
          Pair.of(bannedReloadVo.getVersion(), bannedReloadVo.getIds()));
      LOG.info("reload {} banned ids.", bannedReloadVo.getIds().size());
      return Response.success("success");
    } else {
      return Response.fail("bannedManager is not initialized or bannedIds is null.");
    }
  }

  @GET
  @Path("version")
  public Response<String> version() {
    BannedManager bannedManager = getAccessManager().getBannedManager();
    if (bannedManager != null) {
      String version = bannedManager.getVersion();
      LOG.info("Get version of banned ids is {}.", version);
      return Response.success(version);
    } else {
      return Response.fail("bannedManager is not initialized.");
    }
  }

  @GET
  @Path("get")
  public Response<Pair<String, Set<String>>> get() {
    BannedManager bannedManager = getAccessManager().getBannedManager();
    if (bannedManager != null) {
      Pair<String, Set<String>> bannedInfo = bannedManager.getBannedInfo();
      LOG.info(
          "Get version:{} include {} bannedIds ",
          bannedInfo.getKey(),
          bannedInfo.getValue().size());
      return Response.success(bannedInfo);
    } else {
      return Response.fail("bannedManager is not initialized.");
    }
  }

  private AccessManager getAccessManager() {
    return (AccessManager) servletContext.getAttribute(AccessManager.class.getCanonicalName());
  }
}
