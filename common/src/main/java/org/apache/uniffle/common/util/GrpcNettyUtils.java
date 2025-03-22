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

package org.apache.uniffle.common.util;

import io.grpc.netty.shaded.io.netty.buffer.ByteBufAllocator;
import io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator;
import io.grpc.netty.shaded.io.netty.util.internal.PlatformDependent;

public class GrpcNettyUtils {

  private static volatile PooledByteBufAllocator allocator;

  public static PooledByteBufAllocator createPooledByteBufAllocator(
      boolean preferDirect, int numCores, int pageSize, int maxOrder, int smallCacheSize) {
    return createPooledByteBufAllocator(
        preferDirect, true, numCores, pageSize, maxOrder, smallCacheSize, 0);
  }

  private static PooledByteBufAllocator createPooledByteBufAllocator(
      boolean preferDirect,
      boolean allowCache,
      int numCores,
      int pageSize,
      int maxOrder,
      int smallCacheSize,
      int normalCacheSize) {
    numCores = NettyUtils.defaultNumThreads(numCores);
    if (pageSize == 0) {
      pageSize = PooledByteBufAllocator.defaultPageSize();
    }
    if (maxOrder == 0) {
      maxOrder = PooledByteBufAllocator.defaultMaxOrder();
    }
    if (smallCacheSize == 0) {
      smallCacheSize = PooledByteBufAllocator.defaultSmallCacheSize();
    }
    if (normalCacheSize == 0) {
      normalCacheSize = PooledByteBufAllocator.defaultNormalCacheSize();
    }
    return new PooledByteBufAllocator(
        preferDirect && PlatformDependent.directBufferPreferred(),
        Math.min(PooledByteBufAllocator.defaultNumHeapArena(), numCores),
        Math.min(PooledByteBufAllocator.defaultNumDirectArena(), preferDirect ? numCores : 0),
        pageSize,
        maxOrder,
        allowCache && smallCacheSize != -1 ? smallCacheSize : 0,
        allowCache && normalCacheSize != -1 ? normalCacheSize : 0,
        allowCache && PooledByteBufAllocator.defaultUseCacheForAllThreads());
  }

  public static PooledByteBufAllocator createPooledByteBufAllocatorWithSmallCacheOnly(
      boolean preferDirect, int numCores, int pageSize, int maxOrder, int smallCacheSize) {
    return createPooledByteBufAllocator(
        preferDirect, true, numCores, pageSize, maxOrder, smallCacheSize, -1);
  }

  public static synchronized ByteBufAllocator getSharedPooledByteBufAllocator(
      int pageSize, int maxOrder, int smallCacheSize) {
    if (allocator == null) {
      allocator = createPooledByteBufAllocator(true, 0, pageSize, maxOrder, smallCacheSize);
    }
    return allocator;
  }
}
