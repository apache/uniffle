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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.FileNotFoundException;
import org.apache.uniffle.storage.handler.api.ServerReadHandler;
import org.apache.uniffle.storage.handler.api.ShuffleWriteHandler;
import org.apache.uniffle.storage.handler.impl.CompositeLocalFileServerReadHandler;
import org.apache.uniffle.storage.request.CreateShuffleReadHandlerRequest;
import org.apache.uniffle.storage.request.CreateShuffleWriteHandlerRequest;

public class CompositeReadingViewStorage extends AbstractStorage {

  private static final Logger LOG = LoggerFactory.getLogger(CompositeReadingViewStorage.class);
  private final List<LocalStorage> localStorages;

  public CompositeReadingViewStorage(List<LocalStorage> localStorages) {
    super();
    this.localStorages = localStorages;
  }

  @Override
  ShuffleWriteHandler newWriteHandler(CreateShuffleWriteHandlerRequest request) {
    return null;
  }

  public ServerReadHandler getOrCreateReadHandler(CreateShuffleReadHandlerRequest request) {
    // Do not cache it since this class is just a wrapper
    return newReadHandler(request);
  }

  @Override
  protected ServerReadHandler newReadHandler(CreateShuffleReadHandlerRequest request) {
    List<ServerReadHandler> handlers = new ArrayList<>();
    for (LocalStorage storage : localStorages) {
      try {
        handlers.add(storage.getOrCreateReadHandler(request));
      } catch (FileNotFoundException e) {
        // ignore it
      } catch (Exception e) {
        LOG.error("Failed to create read handler for storage: " + storage, e);
      }
    }
    return new CompositeLocalFileServerReadHandler(handlers);
  }

  @Override
  public boolean canWrite() {
    return false;
  }

  @Override
  public void updateWriteMetrics(StorageWriteMetrics metrics) {}

  @Override
  public void updateReadMetrics(StorageReadMetrics metrics) {}

  @Override
  public void createMetadataIfNotExist(String shuffleKey) {}

  @Override
  public String getStoragePath() {
    return null;
  }

  @Override
  public String getStorageHost() {
    return null;
  }
}
