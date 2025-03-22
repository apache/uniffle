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

package org.apache.uniffle.storage.handler.impl;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShuffleIndexResult;
import org.apache.uniffle.common.netty.buffer.ManagedBuffer;
import org.apache.uniffle.common.netty.buffer.MultiFileSegmentManagedBuffer;
import org.apache.uniffle.storage.handler.api.ServerReadHandler;

public class CompositeLocalFileServerReadHandler implements ServerReadHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(CompositeLocalFileServerReadHandler.class);
  private final List<ServerReadHandler> handlers;

  public CompositeLocalFileServerReadHandler(List<ServerReadHandler> handlers) {
    this.handlers = handlers;
  }

  @Override
  public ShuffleDataResult getShuffleData(long offset, int length) {
    return null;
  }

  @Override
  public ShuffleIndexResult getShuffleIndex() {
    if (handlers.size() == 0) {
      // caller should handle the null return
      return null;
    }
    int[] storageIds = new int[handlers.size()];
    List<ManagedBuffer> managedBuffers = new ArrayList<>(handlers.size());
    String dataFileName = "";
    long length = 0;
    for (int i = 0; i < handlers.size(); i++) {
      ServerReadHandler handler = handlers.get(i);
      storageIds[i] = handler.getStorageId();
      ShuffleIndexResult result = handler.getShuffleIndex();
      length += result.getDataFileLen();
      managedBuffers.add(result.getManagedBuffer());
      if (i == 0) {
        // Use the first data file name as the data file name of the combined result.
        // TODO: This cannot work for remote merge feature.
        dataFileName = result.getDataFileName();
      }
    }
    MultiFileSegmentManagedBuffer mergedManagedBuffer =
        new MultiFileSegmentManagedBuffer(managedBuffers);
    return new ShuffleIndexResult(mergedManagedBuffer, length, dataFileName, storageIds);
  }

  @Override
  public int getStorageId() {
    return 0;
  }
}
