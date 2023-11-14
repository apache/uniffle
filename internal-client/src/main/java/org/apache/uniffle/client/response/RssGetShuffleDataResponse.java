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

package org.apache.uniffle.client.response;

import java.nio.ByteBuffer;

import io.netty.buffer.Unpooled;

import org.apache.uniffle.common.netty.buffer.ManagedBuffer;
import org.apache.uniffle.common.netty.buffer.NettyManagedBuffer;
import org.apache.uniffle.common.rpc.StatusCode;

public class RssGetShuffleDataResponse extends ClientResponse {

  private final ManagedBuffer shuffleData;

  public RssGetShuffleDataResponse(StatusCode statusCode, ByteBuffer data) {
    this(statusCode, new NettyManagedBuffer(Unpooled.wrappedBuffer(data)));
  }

  public RssGetShuffleDataResponse(StatusCode statusCode, ManagedBuffer data) {
    super(statusCode);
    this.shuffleData = data;
  }

  public ManagedBuffer getShuffleData() {
    return shuffleData;
  }
}
