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

import java.io.File;
import java.util.Arrays;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.storage.handler.AsynDeletionEvent;
import org.apache.uniffle.storage.handler.api.ShuffleDeleteHandler;
import org.apache.uniffle.storage.util.StorageType;

public class LocalFileAsyncDeleteHandler implements ShuffleDeleteHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LocalFileAsyncDeleteHandler.class);
  private AsynDeletionEventManager asynDeletionEventManager;

  public LocalFileAsyncDeleteHandler(AsynDeletionEventManager asynDeletionEventManager) {
    this.asynDeletionEventManager = asynDeletionEventManager;
  }

  /** Rename the file and then delete it asynchronously. */
  @Override
  public boolean delete(String[] storageBasePaths, String appId, String user) {
    AsynDeletionEvent asynDeletionEvent =
        new AsynDeletionEvent(
            appId, user, null, null, Arrays.asList(storageBasePaths), StorageType.LOCALFILE.name());
    for (Map.Entry<String, String> appIdNeedDeletePaths :
        asynDeletionEvent.getNeedDeletePathAndRenamePath().entrySet()) {
      String shufflePath = appIdNeedDeletePaths.getKey();
      String breakdownShufflePath = appIdNeedDeletePaths.getValue();
      boolean isExists;
      boolean isSuccess = false;
      long start = System.currentTimeMillis();
      try {
        File baseFolder = new File(shufflePath);
        isExists = baseFolder.exists();
        File breakdownBaseFolder = new File(breakdownShufflePath);
        if (isExists) {
          isSuccess = baseFolder.renameTo(breakdownBaseFolder);
        }
        if (isExists) {
          if (isSuccess) {
            LOG.info(
                "Rename shuffle data for appId[{}] with {} to {} cost {} ms",
                appId,
                shufflePath,
                breakdownShufflePath,
                (System.currentTimeMillis() - start));
          } else {
            LOG.warn(
                "Can't Rename shuffle data for appId[{}] with {} to {}",
                appId,
                shufflePath,
                breakdownShufflePath);
          }
        } else {
          LOG.info("Rename shuffle data for appId[{}],[{}] is not exists", appId, shufflePath);
        }
      } catch (Exception e) {
        LOG.error(
            "Can't Rename shuffle data for appId[{}] with {} to {}",
            appId,
            shufflePath,
            breakdownShufflePath,
            e);
      }
    }
    if (!asynDeletionEventManager.handlerAsynDelete(asynDeletionEvent)) {
      LOG.warn(
          "Remove the case where the renameAndDeletionEventQueue queue is full and cannot accept elements.");
      return false;
    }
    return true;
  }
}
