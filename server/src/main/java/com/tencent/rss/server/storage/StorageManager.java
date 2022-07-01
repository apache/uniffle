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

package com.tencent.rss.server.storage;

import java.util.Set;

import com.tencent.rss.common.RemoteStorageInfo;
import com.tencent.rss.server.Checker;
import com.tencent.rss.server.ShuffleDataFlushEvent;
import com.tencent.rss.server.ShuffleDataReadEvent;
import com.tencent.rss.storage.common.Storage;
import com.tencent.rss.storage.handler.api.ShuffleWriteHandler;


public interface StorageManager {

  Storage selectStorage(ShuffleDataFlushEvent event);

  Storage selectStorage(ShuffleDataReadEvent event);

  boolean write(Storage storage, ShuffleWriteHandler handler, ShuffleDataFlushEvent event);

  void updateWriteMetrics(ShuffleDataFlushEvent event, long writeTime);

  // todo: add an interface for updateReadMetrics

  void removeResources(String appId, Set<Integer> shuffleSet);

  void start();

  void stop();

  void registerRemoteStorage(String appId, RemoteStorageInfo remoteStorageInfo);

  Checker getStorageChecker();

  // todo: add an interface that check storage isHealthy
}
