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

package org.apache.uniffle.common.web.resource;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.servlet.ServletContext;

import org.apache.hbase.thirdparty.javax.ws.rs.Consumes;
import org.apache.hbase.thirdparty.javax.ws.rs.POST;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ReconfigurableRegistry;
import org.apache.uniffle.common.config.RssConf;

@Path("/conf/ops")
public class ConfOpsResource {
  private static final Logger LOG = LoggerFactory.getLogger(ConfOpsResource.class);
  private static final String WARNING_MSG = "temporarily effective until restart";
  public static final String SERVLET_CONTEXT_ATTR_CONF = "_servlet_context_attr_conf_";

  @Context protected ServletContext servletContext;

  @Authorization
  @POST
  @Path("/temp/update")
  @Consumes(MediaType.APPLICATION_JSON)
  public String update(ConfVO updateConfigs) {
    LOG.info("Dynamic updating {}", updateConfigs);
    String ret = "Nothing changed";
    if (updateConfigs != null) {
      RssConf conf = (RssConf) servletContext.getAttribute(SERVLET_CONTEXT_ATTR_CONF);
      if (conf != null) {
        Set<String> changedProperties = new HashSet<>();
        for (Map.Entry<String, String> entry : updateConfigs.getUpdate().entrySet()) {
          conf.setString(entry.getKey(), entry.getValue());
          changedProperties.add(entry.getKey());
        }
        for (String key : updateConfigs.getDelete()) {
          conf.remove(key);
          changedProperties.add(key);
        }
        ReconfigurableRegistry.update(conf, changedProperties);
        ret = WARNING_MSG + ": Update successfully";
      }
    }
    return ret;
  }
}
