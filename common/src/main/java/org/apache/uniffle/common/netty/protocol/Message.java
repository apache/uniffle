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

package org.apache.uniffle.common.netty.protocol;

import io.netty.buffer.ByteBuf;

import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.netty.buffer.ManagedBuffer;

public abstract class Message implements Encodable {

  private ManagedBuffer body;

  protected Message() {
    this(null);
  }

  protected Message(ManagedBuffer body) {
    this.body = body;
  }

  public abstract Type type();

  public ManagedBuffer body() {
    return body;
  }

  public enum Type implements Encodable {
    UNKNOWN_TYPE(-1),
    RPC_RESPONSE(0),
    SHUFFLE_REGISTER_REQUEST(1),
    SHUFFLE_UNREGISTER_REQUEST(2),
    SEND_SHUFFLE_DATA_REQUEST(3),
    GET_LOCAL_SHUFFLE_INDEX_REQUEST(4),
    GET_LOCAL_SHUFFLE_DATA_REQUEST(5),
    GET_MEMORY_SHUFFLE_DATA_REQUEST(6),
    SHUFFLE_COMMIT_REQUEST(7),
    REPORT_SHUFFLE_RESULT_REQUEST(8),
    GET_SHUFFLE_RESULT_REQUEST(9),
    GET_SHUFFLE_RESULT_FOR_MULTI_PART_REQUEST(10),
    FINISH_SHUFFLE_REQUEST(11),
    REQUIRE_BUFFER_REQUEST(12),
    APP_HEART_BEAT_REQUEST(13),
    GET_LOCAL_SHUFFLE_INDEX_RESPONSE(14),
    GET_LOCAL_SHUFFLE_DATA_RESPONSE(15),
    GET_MEMORY_SHUFFLE_DATA_RESPONSE(16),
    SHUFFLE_COMMIT_RESPONSE(17),
    GET_SHUFFLE_RESULT_RESPONSE(18),
    GET_SHUFFLE_RESULT_FOR_MULTI_PART_RESPONSE(19),
    REQUIRE_BUFFER_RESPONSE(20),
    GET_SORTED_SHUFFLE_DATA_REQUEST(21),
    GET_SORTED_SHUFFLE_DATA_RESPONSE(22),
    GET_LOCAL_SHUFFLE_INDEX_V2_RESPONSE(23),
    GET_LOCAL_SHUFFLE_DATA_V2_REQUEST(24),
    ;

    private final byte id;

    Type(int id) {
      if (id >= 128) {
        throw new RssException("Cannot have more than 128 message types");
      }
      this.id = (byte) id;
    }

    public byte id() {
      return id;
    }

    @Override
    public int encodedLength() {
      return 1;
    }

    @Override
    public void encode(ByteBuf buf) {
      buf.writeByte(id);
    }

    public static Type decode(ByteBuf buf) {
      byte id = buf.readByte();
      switch (id) {
        case 0:
          return RPC_RESPONSE;
        case 1:
          return SHUFFLE_REGISTER_REQUEST;
        case 2:
          return SHUFFLE_UNREGISTER_REQUEST;
        case 3:
          return SEND_SHUFFLE_DATA_REQUEST;
        case 4:
          return GET_LOCAL_SHUFFLE_INDEX_REQUEST;
        case 5:
          return GET_LOCAL_SHUFFLE_DATA_REQUEST;
        case 6:
          return GET_MEMORY_SHUFFLE_DATA_REQUEST;
        case 7:
          return SHUFFLE_COMMIT_REQUEST;
        case 8:
          return REPORT_SHUFFLE_RESULT_REQUEST;
        case 9:
          return GET_SHUFFLE_RESULT_REQUEST;
        case 10:
          return GET_SHUFFLE_RESULT_FOR_MULTI_PART_REQUEST;
        case 11:
          return FINISH_SHUFFLE_REQUEST;
        case 12:
          return REQUIRE_BUFFER_REQUEST;
        case 13:
          return APP_HEART_BEAT_REQUEST;
        case 14:
          return GET_LOCAL_SHUFFLE_INDEX_RESPONSE;
        case 15:
          return GET_LOCAL_SHUFFLE_DATA_RESPONSE;
        case 16:
          return GET_MEMORY_SHUFFLE_DATA_RESPONSE;
        case 17:
          return SHUFFLE_COMMIT_RESPONSE;
        case 18:
          return GET_SHUFFLE_RESULT_RESPONSE;
        case 19:
          return GET_SHUFFLE_RESULT_FOR_MULTI_PART_RESPONSE;
        case 20:
          return REQUIRE_BUFFER_RESPONSE;
        case 21:
          return GET_SORTED_SHUFFLE_DATA_REQUEST;
        case 22:
          return GET_SORTED_SHUFFLE_DATA_RESPONSE;
        case 23:
          return GET_LOCAL_SHUFFLE_INDEX_V2_RESPONSE;
        case 24:
          return GET_LOCAL_SHUFFLE_DATA_V2_REQUEST;
        case -1:
          throw new IllegalArgumentException("User type messages cannot be decoded.");
        default:
          throw new IllegalArgumentException("Unknown message type: " + id);
      }
    }
  }

  public static Message decode(Type msgType, ByteBuf in) {
    switch (msgType) {
      case RPC_RESPONSE:
        return RpcResponse.decode(in, false);
      case SEND_SHUFFLE_DATA_REQUEST:
        return SendShuffleDataRequestV1.decode(in);
      case GET_LOCAL_SHUFFLE_DATA_REQUEST:
        return GetLocalShuffleDataRequest.decode(in);
      case GET_LOCAL_SHUFFLE_DATA_V2_REQUEST:
        return GetLocalShuffleDataV2Request.decode(in);
      case GET_LOCAL_SHUFFLE_DATA_RESPONSE:
        return GetLocalShuffleDataResponse.decode(in, true);
      case GET_LOCAL_SHUFFLE_INDEX_REQUEST:
        return GetLocalShuffleIndexRequest.decode(in);
      case GET_LOCAL_SHUFFLE_INDEX_RESPONSE:
        return GetLocalShuffleIndexResponse.decode(in, true);
      case GET_LOCAL_SHUFFLE_INDEX_V2_RESPONSE:
        return GetLocalShuffleIndexV2Response.decode(in, true);
      case GET_MEMORY_SHUFFLE_DATA_REQUEST:
        return GetMemoryShuffleDataRequest.decode(in);
      case GET_MEMORY_SHUFFLE_DATA_RESPONSE:
        return GetMemoryShuffleDataResponse.decode(in, true);
      case GET_SORTED_SHUFFLE_DATA_REQUEST:
        return GetSortedShuffleDataRequest.decode(in);
      case GET_SORTED_SHUFFLE_DATA_RESPONSE:
        return GetSortedShuffleDataResponse.decode(in, true);
      default:
        throw new IllegalArgumentException("Unexpected message type: " + msgType);
    }
  }

  public abstract long getRequestId();
}
