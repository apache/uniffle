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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.servlet.ServletContext;

import org.apache.hbase.thirdparty.javax.ws.rs.Consumes;
import org.apache.hbase.thirdparty.javax.ws.rs.GET;
import org.apache.hbase.thirdparty.javax.ws.rs.POST;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.QueryParam;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.web.resource.Authorization;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.bench.BenchHandle;
import org.apache.uniffle.server.bench.ShuffleServerBenchmark;
import org.apache.uniffle.server.web.vo.BenchArgumentVO;

@Path("/bench")
public class ServerBenchResource {
  private static final Logger LOG = LoggerFactory.getLogger(ServerBenchResource.class);
  @Context protected ServletContext servletContext;

  @Authorization
  @POST
  @Path("/run")
  @Consumes(MediaType.APPLICATION_JSON)
  public String run(BenchArgumentVO argument) throws Exception {
    String id =
        ShuffleServerBenchmark.runBench(
            argument, getShuffleServer().getShuffleServerConf(), getShuffleServer(), true, -1);
    LOG.info("Benchmark {} is started.", id);
    return id;
  }

  @GET
  @Path("/get")
  public Map<String, BenchHandle> getBenchHandles(@QueryParam("ids") Set<String> ids) {
    if (ids == null) {
      ids = Collections.emptySet();
    }
    return ShuffleServerBenchmark.getBenchHandle(ids);
  }

  @Authorization
  @POST
  @Path("/stop")
  public String stop(@QueryParam("id") String id) throws Exception {
    if (ShuffleServerBenchmark.stopBench(id)) {
      return "OK";
    } else {
      return "Unknown id: " + id;
    }
  }

  @Authorization
  @POST
  @Path("/remove")
  public String remove(@QueryParam("id") String id) throws Exception {
    if (ShuffleServerBenchmark.removeBench(id)) {
      return "OK";
    } else {
      return "Unknown id: " + id;
    }
  }

  private ShuffleServer getShuffleServer() {
    return (ShuffleServer) servletContext.getAttribute(ShuffleServer.class.getCanonicalName());
  }
}
