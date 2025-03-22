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

package org.apache.uniffle.storage.common;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.storage.handler.api.ServerReadHandler;
import org.apache.uniffle.storage.handler.api.ShuffleWriteHandler;
import org.apache.uniffle.storage.handler.api.ShuffleWriteHandlerWrapper;
import org.apache.uniffle.storage.request.CreateShuffleReadHandlerRequest;
import org.apache.uniffle.storage.request.CreateShuffleWriteHandlerRequest;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;

public abstract class AbstractStorage implements Storage {

  // appId -> partitionKey -> ShuffleWriteHandler
  private Map<String, Map<String, ShuffleWriteHandler>> writerHandlers =
      JavaUtils.newConcurrentMap();
  // appId -> partitionKey -> CreateShuffleWriteHandlerRequest
  private Map<String, Map<String, CreateShuffleWriteHandlerRequest>> requests =
      JavaUtils.newConcurrentMap();
  // appId -> partitionKey -> ServerReadHandler
  private Map<String, Map<String, ServerReadHandler>> readerHandlers = JavaUtils.newConcurrentMap();

  abstract ShuffleWriteHandler newWriteHandler(CreateShuffleWriteHandlerRequest request);

  @Override
  public ShuffleWriteHandlerWrapper getOrCreateWriteHandler(
      CreateShuffleWriteHandlerRequest request) {
    writerHandlers.computeIfAbsent(request.getAppId(), key -> JavaUtils.newConcurrentMap());
    requests.computeIfAbsent(request.getAppId(), key -> JavaUtils.newConcurrentMap());
    Map<String, ShuffleWriteHandler> map = writerHandlers.get(request.getAppId());
    String partitionKeyExceptAppId =
        generatePartitionKeyExceptAppId(request.getShuffleId(), request.getStartPartition());
    boolean isNewlyCreated = false;
    if (!map.containsKey(partitionKeyExceptAppId)) {
      isNewlyCreated = true;
    }
    map.computeIfAbsent(partitionKeyExceptAppId, key -> newWriteHandler(request));
    Map<String, CreateShuffleWriteHandlerRequest> requestMap = requests.get(request.getAppId());
    requestMap.putIfAbsent(partitionKeyExceptAppId, request);
    return new ShuffleWriteHandlerWrapper(map.get(partitionKeyExceptAppId), isNewlyCreated);
  }

  @Override
  public ServerReadHandler getOrCreateReadHandler(CreateShuffleReadHandlerRequest request) {
    readerHandlers.computeIfAbsent(request.getAppId(), key -> JavaUtils.newConcurrentMap());
    Map<String, ServerReadHandler> map = readerHandlers.get(request.getAppId());
    int[] range =
        ShuffleStorageUtils.getPartitionRange(
            request.getPartitionId(), request.getPartitionNumPerRange(), request.getPartitionNum());
    String partitionKeyExceptAppId =
        generatePartitionKeyExceptAppId(request.getShuffleId(), range[0]);
    map.computeIfAbsent(partitionKeyExceptAppId, key -> newReadHandler(request));
    return map.get(partitionKeyExceptAppId);
  }

  protected abstract ServerReadHandler newReadHandler(CreateShuffleReadHandlerRequest request);

  @Override
  public boolean containsWriteHandler(String appId) {
    return writerHandlers.containsKey(appId);
  }

  public boolean containsWriteHandler(String appId, int shuffleId, int partition) {
    Map<String, ShuffleWriteHandler> map = writerHandlers.get(appId);
    if (map == null || map.isEmpty()) {
      return false;
    }
    String partitionKeyExceptAppId = generatePartitionKeyExceptAppId(shuffleId, partition);
    return map.containsKey(partitionKeyExceptAppId);
  }

  public static String generatePartitionKeyExceptAppId(Integer shuffleId, Integer partition) {
    return String.join(
        Constants.KEY_SPLIT_CHAR, String.valueOf(shuffleId), String.valueOf(partition));
  }

  @Override
  public void removeHandlers(String appId) {
    writerHandlers.remove(appId);
    readerHandlers.remove(appId);
    requests.remove(appId);
  }

  @VisibleForTesting
  public int getHandlerSize() {
    return writerHandlers.size();
  }
}
