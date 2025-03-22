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

package org.apache.uniffle.coordinator.audit;

import org.slf4j.Logger;

import org.apache.uniffle.common.audit.RpcAuditContext;

/** An audit context for coordinator rpc. */
public class CoordinatorRpcAuditContext extends RpcAuditContext {
  private String appId = "N/A";

  /**
   * Constructor of {@link CoordinatorRpcAuditContext}.
   *
   * @param log the logger to log the audit information
   */
  public CoordinatorRpcAuditContext(Logger log) {
    super(log);
  }

  @Override
  protected String content() {
    return String.format("appId=%s", appId);
  }

  public CoordinatorRpcAuditContext withAppId(String appId) {
    this.appId = appId;
    return this;
  }
}
