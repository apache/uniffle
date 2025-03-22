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

package org.apache.uniffle.client.simulator;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

public class NMCallBackHandler extends NMClientAsync.AbstractCallbackHandler {
  @Override
  public void onContainerStarted(
      ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
    System.out.println("NM: container started, id=" + containerId.toString());
  }

  @Override
  public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
    System.out.println("NM: container status received, id=" + containerId.toString());
  }

  @Override
  public void onContainerStopped(ContainerId containerId) {
    System.out.println("NM: container stopped, id=" + containerId.toString());
  }

  @Override
  public void onStartContainerError(ContainerId containerId, Throwable t) {
    System.out.println("NM: start container error, id=" + containerId.toString());
  }

  @Override
  public void onContainerResourceIncreased(ContainerId containerId, Resource resource) {
    System.out.println("NM: container resource increased, id=" + containerId.toString());
  }

  public void onContainerResourceUpdated(ContainerId containerId, Resource resource) {
    System.out.println("NM: container resource updated, id=" + containerId.toString());
  }

  @Override
  public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
    System.out.println("NM: get container status error, id=" + containerId.toString());
  }

  @Override
  public void onIncreaseContainerResourceError(ContainerId containerId, Throwable t) {
    System.out.println("NM: increase container resource error, id=" + containerId.toString());
  }

  public void onUpdateContainerResourceError(ContainerId containerId, Throwable t) {
    System.out.println("NM: update container resource error, id=" + containerId.toString());
  }

  @Override
  public void onStopContainerError(ContainerId containerId, Throwable t) {
    System.out.println("NM: stop container error, id=" + containerId.toString());
  }
}
