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

import java.io.IOException;
import javax.servlet.ServletContext;

import org.apache.hbase.thirdparty.javax.ws.rs.container.ContainerRequestContext;
import org.apache.hbase.thirdparty.javax.ws.rs.container.ContainerRequestFilter;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Response;
import org.apache.hbase.thirdparty.javax.ws.rs.ext.Provider;

import org.apache.uniffle.common.config.RssBaseConf;

@Provider
@Authorization
public class AuthorizationRequestFilter implements ContainerRequestFilter {
  @Context protected ServletContext servletContext;

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    Object credentials =
        servletContext.getAttribute(RssBaseConf.REST_AUTHORIZATION_CREDENTIALS.key());
    if (credentials == null) {
      return;
    }
    String authorization = requestContext.getHeaderString("Authorization");
    if (authorization == null
        || !authorization.startsWith("Basic ")
        || !authorization.substring(6).equals(credentials)) {
      requestContext.abortWith(
          Response.status(Response.Status.UNAUTHORIZED)
              .entity("Authentication Failed")
              .type(MediaType.TEXT_PLAIN)
              .build());
    }
  }
}
