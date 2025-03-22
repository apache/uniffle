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

public class RssGetSortedShuffleDataResponse extends ClientResponse {

  private final ManagedBuffer data;
  private final long nextBlockId;
  private final int mergeState;

  public RssGetSortedShuffleDataResponse(
      StatusCode statusCode, String message, ByteBuffer data, long nextBlockId, int mergeState) {
    this(
        statusCode,
        message,
        new NettyManagedBuffer(Unpooled.wrappedBuffer(data)),
        nextBlockId,
        mergeState);
  }

  public RssGetSortedShuffleDataResponse(
      StatusCode statusCode, String message, ManagedBuffer data, long nextBlockId, int mergeState) {
    super(statusCode, message);
    this.data = data;
    this.nextBlockId = nextBlockId;
    this.mergeState = mergeState;
  }

  public ManagedBuffer getData() {
    return data;
  }

  public long getNextBlockId() {
    return nextBlockId;
  }

  public int getMergeState() {
    return mergeState;
  }
}
