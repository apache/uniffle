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

package org.apache.uniffle.dashboard.web.proxy;

import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Preconditions;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.proxy.ProxyServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebProxyServlet extends ProxyServlet {

  private static final Logger LOG = LoggerFactory.getLogger(WebProxyServlet.class);
  /** The key of the request header. */
  private static final String HEADER_TARGET_ADDRESS = "targetAddress";

  private static final String HEADER_REQUEST_SERVER_TYPE = "requestServerType";
  /** The value of the request header. */
  private static final String REQUEST_SERVER_TYPE_COORDINATOR = "coordinator";

  private static final String REQUEST_SERVER_TYPE_SERVER = "server";
  private Map<String, String> coordinatorServerAddressesMap;

  public WebProxyServlet(Map<String, String> coordinatorServerAddressesMap) {
    Preconditions.checkArgument(
        !coordinatorServerAddressesMap.isEmpty(), "No coordinator server address found.");
    this.coordinatorServerAddressesMap = coordinatorServerAddressesMap;
  }

  @Override
  protected String rewriteTarget(HttpServletRequest clientRequest) {
    if (!validateDestination(clientRequest.getServerName(), clientRequest.getServerPort())) {
      return null;
    }
    String targetAddress;
    String requestServerType =
        clientRequest.getHeader(HEADER_REQUEST_SERVER_TYPE) != null
                && REQUEST_SERVER_TYPE_COORDINATOR.equalsIgnoreCase(
                    clientRequest.getHeader(HEADER_REQUEST_SERVER_TYPE))
            ? REQUEST_SERVER_TYPE_COORDINATOR
            : REQUEST_SERVER_TYPE_SERVER;
    if (requestServerType.equalsIgnoreCase(REQUEST_SERVER_TYPE_COORDINATOR)) {
      targetAddress =
          coordinatorServerAddressesMap.get(clientRequest.getHeader(HEADER_TARGET_ADDRESS));
    } else {
      targetAddress = clientRequest.getHeader(HEADER_TARGET_ADDRESS);
    }
    StringBuilder target = new StringBuilder();
    target.append(targetAddress).append("/api").append(clientRequest.getPathInfo());
    String query = clientRequest.getQueryString();
    if (query != null) {
      target.append("?").append(query);
    }
    LOG.info(target.toString());
    return target.toString();
  }

  /**
   * If the proxy address fails to be requested, 403 is returned and the front-end handles the
   * exception.
   *
   * @param clientRequest
   * @param proxyResponse
   * @param serverResponse
   * @param failure
   */
  @Override
  protected void onProxyResponseFailure(
      HttpServletRequest clientRequest,
      HttpServletResponse proxyResponse,
      org.eclipse.jetty.client.api.Response serverResponse,
      Throwable failure) {
    sendProxyResponseError(clientRequest, proxyResponse, HttpStatus.FORBIDDEN_403);
  }

  /**
   * If the proxy address fails to be rewritten, 403 is returned and the front-end handles the
   * exception.
   *
   * @param clientRequest the client request
   * @param proxyResponse the client response
   */
  @Override
  protected void onProxyRewriteFailed(
      HttpServletRequest clientRequest, HttpServletResponse proxyResponse) {
    sendProxyResponseError(clientRequest, proxyResponse, HttpStatus.FORBIDDEN_403);
  }
}
