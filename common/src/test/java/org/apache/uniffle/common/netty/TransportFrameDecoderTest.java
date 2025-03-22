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

package org.apache.uniffle.common.netty;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.netty.buffer.ManagedBuffer;
import org.apache.uniffle.common.netty.buffer.NettyManagedBuffer;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleDataResponse;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleIndexRequest;
import org.apache.uniffle.common.netty.protocol.GetLocalShuffleIndexResponse;
import org.apache.uniffle.common.netty.protocol.GetMemoryShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.GetMemoryShuffleDataResponse;
import org.apache.uniffle.common.netty.protocol.GetSortedShuffleDataRequest;
import org.apache.uniffle.common.netty.protocol.GetSortedShuffleDataResponse;
import org.apache.uniffle.common.netty.protocol.Message;
import org.apache.uniffle.common.netty.protocol.RpcResponse;
import org.apache.uniffle.common.netty.protocol.SendShuffleDataRequest;
import org.apache.uniffle.common.rpc.StatusCode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TransportFrameDecoderTest {

  /** test if the RPC response should be released after decoding */
  @Test
  public void testShouldRpcResponsesToBeReleased() {
    RpcResponse rpcResponse1 = generateRpcResponse();
    int length1 = rpcResponse1.encodedLength();
    ByteBuf byteBuf1 = Unpooled.buffer(length1);
    rpcResponse1.encode(byteBuf1);
    assertEquals(byteBuf1.readableBytes(), length1);
    Message message1 = Message.decode(rpcResponse1.type(), byteBuf1);
    assertTrue(TransportFrameDecoder.shouldRelease(message1));
    byteBuf1.release();

    GetLocalShuffleDataResponse rpcResponse2 = generateGetLocalShuffleDataResponse();
    int length2 = rpcResponse2.encodedLength();
    byte[] body2 = generateBody();
    ByteBuf byteBuf2 = Unpooled.buffer(length2 + body2.length);
    rpcResponse2.encode(byteBuf2);
    assertEquals(byteBuf2.readableBytes(), length2);
    byteBuf2.writeBytes(body2);
    Message message2 = Message.decode(rpcResponse2.type(), byteBuf2);
    assertFalse(TransportFrameDecoder.shouldRelease(message2));
    // after processing some business logic in the code, and finally release the body buffer
    message2.body().release();

    GetLocalShuffleIndexResponse rpcResponse3 = generateGetLocalShuffleIndexResponse();
    int length3 = rpcResponse3.encodedLength();
    byte[] body3 = generateBody();
    ByteBuf byteBuf3 = Unpooled.buffer(length3 + body3.length);
    rpcResponse3.encode(byteBuf3);
    assertEquals(byteBuf3.readableBytes(), length3);
    byteBuf3.writeBytes(body3);
    Message message3 = Message.decode(rpcResponse3.type(), byteBuf3);
    assertFalse(TransportFrameDecoder.shouldRelease(message3));
    // after processing some business logic in the code, and finally release the body buffer
    message3.body().release();

    GetMemoryShuffleDataResponse rpcResponse4 = generateGetMemoryShuffleDataResponse();
    int length4 = rpcResponse4.encodedLength();
    byte[] body4 = generateBody();
    ByteBuf byteBuf4 = Unpooled.buffer(length4 + body4.length);
    rpcResponse4.encode(byteBuf4);
    assertEquals(byteBuf4.readableBytes(), length4);
    byteBuf4.writeBytes(body4);
    Message message4 = Message.decode(rpcResponse4.type(), byteBuf4);
    assertFalse(TransportFrameDecoder.shouldRelease(message4));
    // after processing some business logic in the code, and finally release the body buffer
    message4.body().release();

    GetSortedShuffleDataResponse rpcResponse5 = generateGetSortedShuffleDataResponse();
    int length5 = rpcResponse5.encodedLength();
    byte[] body5 = generateBody();
    ByteBuf byteBuf5 = Unpooled.buffer(length5 + body5.length);
    rpcResponse5.encode(byteBuf5);
    assertEquals(byteBuf5.readableBytes(), length5);
    byteBuf5.writeBytes(body5);
    Message message5 = Message.decode(rpcResponse5.type(), byteBuf5);
    assertFalse(TransportFrameDecoder.shouldRelease(message5));
    // after processing some business logic in the code, and finally release the body buffer
    message5.body().release();
  }

  /** test if the RPC request should be released after decoding */
  @Test
  public void testShouldRpcRequestsToBeReleased() {
    SendShuffleDataRequest rpcRequest1 = generateShuffleDataRequest();
    int length1 = rpcRequest1.encodedLength();
    ByteBuf byteBuf1 = Unpooled.buffer(length1);
    rpcRequest1.encode(byteBuf1);
    assertEquals(byteBuf1.readableBytes(), length1);
    Message message1 = Message.decode(rpcRequest1.type(), byteBuf1);
    assertTrue(TransportFrameDecoder.shouldRelease(message1));
    byteBuf1.release();

    GetLocalShuffleDataRequest rpcRequest2 = generateGetLocalShuffleDataRequest();
    int length2 = rpcRequest2.encodedLength();
    ByteBuf byteBuf2 = Unpooled.buffer(length2);
    rpcRequest2.encode(byteBuf2);
    assertEquals(byteBuf2.readableBytes(), length2);
    Message message2 = Message.decode(rpcRequest2.type(), byteBuf2);
    assertTrue(TransportFrameDecoder.shouldRelease(message2));
    byteBuf2.release();

    GetLocalShuffleIndexRequest rpcRequest3 = generateGetLocalShuffleIndexRequest();
    int length3 = rpcRequest3.encodedLength();
    ByteBuf byteBuf3 = Unpooled.buffer(length3);
    rpcRequest3.encode(byteBuf3);
    assertEquals(byteBuf3.readableBytes(), length3);
    Message message3 = Message.decode(rpcRequest3.type(), byteBuf3);
    assertTrue(TransportFrameDecoder.shouldRelease(message3));
    byteBuf3.release();

    GetMemoryShuffleDataRequest rpcRequest4 = generateGetMemoryShuffleDataRequest();
    int length4 = rpcRequest4.encodedLength();
    ByteBuf byteBuf4 = Unpooled.buffer(length4);
    rpcRequest4.encode(byteBuf4);
    assertEquals(byteBuf4.readableBytes(), length4);
    Message message4 = Message.decode(rpcRequest4.type(), byteBuf4);
    assertTrue(TransportFrameDecoder.shouldRelease(message4));
    byteBuf4.release();

    GetSortedShuffleDataRequest rpcRequest5 = generateGetSortedShuffleDataRequest();
    int length5 = rpcRequest5.encodedLength();
    ByteBuf byteBuf5 = Unpooled.buffer(length5);
    rpcRequest5.encode(byteBuf5);
    assertEquals(byteBuf5.readableBytes(), length5);
    Message message5 = Message.decode(rpcRequest5.type(), byteBuf5);
    assertTrue(TransportFrameDecoder.shouldRelease(message5));
    byteBuf5.release();
  }

  private byte[] generateBody() {
    return new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  }

  private RpcResponse generateRpcResponse() {
    RpcResponse rpcResponse1 = new RpcResponse(1, StatusCode.SUCCESS, "test_message");
    return rpcResponse1;
  }

  private GetLocalShuffleDataResponse generateGetLocalShuffleDataResponse() {
    byte[] data2 = new byte[] {1, 2, 3};
    GetLocalShuffleDataResponse rpcResponse2 =
        new GetLocalShuffleDataResponse(
            1,
            StatusCode.SUCCESS,
            "",
            new NettyManagedBuffer(Unpooled.wrappedBuffer(data2).retain()));
    return rpcResponse2;
  }

  private GetLocalShuffleIndexResponse generateGetLocalShuffleIndexResponse() {
    byte[] data3 = new byte[] {1, 2, 3};
    GetLocalShuffleIndexResponse rpcResponse3 =
        new GetLocalShuffleIndexResponse(
            1, StatusCode.SUCCESS, "", Unpooled.wrappedBuffer(data3).retain(), 23);
    return rpcResponse3;
  }

  private GetMemoryShuffleDataResponse generateGetMemoryShuffleDataResponse() {
    byte[] data4 = new byte[] {1, 2, 3, 4, 5};
    List<BufferSegment> bufferSegments =
        Lists.newArrayList(
            new BufferSegment(1, 0, 5, 10, 123, 1), new BufferSegment(1, 0, 5, 10, 345, 1));
    GetMemoryShuffleDataResponse rpcResponse4 =
        new GetMemoryShuffleDataResponse(
            1, StatusCode.SUCCESS, "", bufferSegments, Unpooled.wrappedBuffer(data4).retain());
    return rpcResponse4;
  }

  private SendShuffleDataRequest generateShuffleDataRequest() {
    String appId = "test_app";
    byte[] data = new byte[] {1, 2, 3};
    List<ShuffleServerInfo> shuffleServerInfoList =
        Arrays.asList(new ShuffleServerInfo("aaa", 1), new ShuffleServerInfo("bbb", 2));
    List<ShuffleBlockInfo> shuffleBlockInfoList1 =
        Arrays.asList(
            new ShuffleBlockInfo(
                1,
                1,
                1,
                data.length,
                123,
                Unpooled.wrappedBuffer(data).retain(),
                shuffleServerInfoList,
                5,
                0,
                1),
            new ShuffleBlockInfo(
                1,
                1,
                1,
                data.length,
                123,
                Unpooled.wrappedBuffer(data).retain(),
                shuffleServerInfoList,
                5,
                0,
                1));
    List<ShuffleBlockInfo> shuffleBlockInfoList2 =
        Arrays.asList(
            new ShuffleBlockInfo(
                1,
                2,
                1,
                data.length,
                123,
                Unpooled.wrappedBuffer(data).retain(),
                shuffleServerInfoList,
                5,
                0,
                1),
            new ShuffleBlockInfo(
                1,
                1,
                2,
                data.length,
                123,
                Unpooled.wrappedBuffer(data).retain(),
                shuffleServerInfoList,
                5,
                0,
                1));
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(1, shuffleBlockInfoList1);
    partitionToBlocks.put(2, shuffleBlockInfoList2);
    return new SendShuffleDataRequest(1L, appId, 1, 1, partitionToBlocks, 12345);
  }

  private GetLocalShuffleDataRequest generateGetLocalShuffleDataRequest() {
    return new GetLocalShuffleDataRequest(
        1, "test_app", 1, 1, 1, 100, 0, 200, System.currentTimeMillis());
  }

  private GetLocalShuffleIndexRequest generateGetLocalShuffleIndexRequest() {
    return new GetLocalShuffleIndexRequest(1, "test_app", 1, 1, 1, 100);
  }

  private GetMemoryShuffleDataRequest generateGetMemoryShuffleDataRequest() {
    Roaring64NavigableMap expectedTaskIdsBitmap = Roaring64NavigableMap.bitmapOf(1, 2, 3, 4, 5);
    return new GetMemoryShuffleDataRequest(
        1, "test_app", 1, 1, 1, 64, System.currentTimeMillis(), expectedTaskIdsBitmap);
  }

  private GetSortedShuffleDataRequest generateGetSortedShuffleDataRequest() {
    return new GetSortedShuffleDataRequest(1, "test_app", 2, 3, 4, 100, System.currentTimeMillis());
  }

  private GetSortedShuffleDataResponse generateGetSortedShuffleDataResponse() {
    byte[] data = new byte[] {1, 2, 3, 4, 5};
    ByteBuf byteBuf = Unpooled.wrappedBuffer(data);
    ManagedBuffer managedBuffer = new NettyManagedBuffer(byteBuf);
    return new GetSortedShuffleDataResponse(1, StatusCode.SUCCESS, "OK", 5L, 0, managedBuffer);
  }
}
