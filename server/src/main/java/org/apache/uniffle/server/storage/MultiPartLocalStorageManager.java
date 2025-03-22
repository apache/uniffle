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

package org.apache.uniffle.server.storage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleDataReadEvent;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.common.CompositeReadingViewStorage;
import org.apache.uniffle.storage.common.LocalStorage;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;

public class MultiPartLocalStorageManager extends LocalStorageManager {
  private static final Logger LOG = LoggerFactory.getLogger(MultiPartLocalStorageManager.class);
  // id -> storage
  private final Map<Integer, LocalStorage> idToStorages;

  private final CompositeReadingViewStorage compositeStorage;

  public MultiPartLocalStorageManager(ShuffleServerConf conf) {
    super(conf);
    idToStorages = new ConcurrentSkipListMap<>();
    for (LocalStorage storage : getStorages()) {
      idToStorages.put(storage.getId(), storage);
    }

    compositeStorage = new CompositeReadingViewStorage(getStorages());
  }

  @Override
  public Storage selectStorage(ShuffleDataFlushEvent event) {
    if (getStorages().size() == 1) {
      if (event.getUnderStorage() == null) {
        event.setUnderStorage(getStorages().get(0));
      }
      return getStorages().get(0);
    }
    String appId = event.getAppId();
    int shuffleId = event.getShuffleId();
    int partitionId = event.getStartPartition();

    // TODO(baoloongmao): extend to support select storage by free space
    // eventId is a non-negative long.
    LocalStorage storage = getStorages().get((int) (event.getEventId() % getStorages().size()));
    if (storage != null) {
      if (storage.isCorrupted()) {
        if (storage.containsWriteHandler(appId, shuffleId, partitionId)) {
          LOG.error(
              "LocalStorage: {} is corrupted. Switching another storage for event: {}, some data will be lost",
              storage.getBasePath(),
              event);
        }
      } else {
        if (event.getUnderStorage() == null) {
          event.setUnderStorage(storage);
        }
        return storage;
      }
    }

    // TODO(baoloongmao): update health storages and store it as member of this class.
    List<LocalStorage> candidates =
        getStorages().stream()
            .filter(x -> x.canWrite() && !x.isCorrupted())
            .collect(Collectors.toList());

    if (candidates.size() == 0) {
      return null;
    }
    final LocalStorage selectedStorage =
        candidates.get(
            ShuffleStorageUtils.getStorageIndex(candidates.size(), appId, shuffleId, partitionId));
    if (storage == null || storage.isCorrupted() || event.getUnderStorage() == null) {
      event.setUnderStorage(selectedStorage);
      return selectedStorage;
    }
    return storage;
  }

  @Override
  public Storage selectStorage(ShuffleDataReadEvent event) {
    if (getStorages().size() == 1) {
      return getStorages().get(0);
    }

    int storageId = event.getStorageId();
    // TODO(baoloongmao): check AOOB exception
    return idToStorages.get(storageId);
  }

  @Override
  public Storage selectStorageById(ShuffleDataReadEvent event) {
    return compositeStorage;
  }
}
