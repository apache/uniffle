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

package org.apache.uniffle.server.audit;

import org.slf4j.Logger;

import org.apache.uniffle.common.audit.RpcAuditContext;

/** An audit context for shuffle server rpc. */
public class ServerRpcAuditContext extends RpcAuditContext {
  private String appId = "N/A";
  private int shuffleId = -1;

  /**
   * Constructor of {@link ServerRpcAuditContext}.
   *
   * @param log the logger to log the audit information
   */
  public ServerRpcAuditContext(Logger log) {
    super(log);
  }

  @Override
  protected String content() {
    return String.format("appId=%s\tshuffleId=%s", appId, shuffleId);
  }

  public ServerRpcAuditContext withAppId(String appId) {
    this.appId = appId;
    return this;
  }

  public ServerRpcAuditContext withShuffleId(int shuffleId) {
    this.shuffleId = shuffleId;
    return this;
  }
}
