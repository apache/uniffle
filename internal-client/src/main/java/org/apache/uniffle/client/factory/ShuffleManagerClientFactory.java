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

package org.apache.uniffle.client.factory;

import org.apache.uniffle.client.impl.grpc.ShuffleManagerGrpcClient;
import org.apache.uniffle.common.ClientType;

public class ShuffleManagerClientFactory {

  private static class LazyHolder {
    private static final ShuffleManagerClientFactory INSTANCE = new ShuffleManagerClientFactory();
  }

  public static ShuffleManagerClientFactory getInstance() {
    return LazyHolder.INSTANCE;
  }

  private ShuffleManagerClientFactory() {}

  public ShuffleManagerGrpcClient createShuffleManagerClient(
      ClientType clientType, String host, int port, long rpcTimeout) {
    if (ClientType.GRPC.equals(clientType)) {
      return new ShuffleManagerGrpcClient(host, port, rpcTimeout);
    } else {
      throw new UnsupportedOperationException("Unsupported client type " + clientType);
    }
  }
}
