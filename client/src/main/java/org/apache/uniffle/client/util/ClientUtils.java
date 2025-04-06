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

package org.apache.uniffle.client.util;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.storage.util.StorageType;

public class ClientUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ClientUtils.class);

  public static RemoteStorageInfo fetchRemoteStorage(
      String appId,
      RemoteStorageInfo defaultRemoteStorage,
      boolean dynamicConfEnabled,
      String storageType,
      ShuffleWriteClient shuffleWriteClient) {
    RemoteStorageInfo remoteStorage = defaultRemoteStorage;
    if (storageType != null && StorageType.withRemoteStorage(StorageType.valueOf(storageType))) {
      if (remoteStorage.isEmpty() && dynamicConfEnabled) {
        // fallback to dynamic conf on coordinator
        remoteStorage = shuffleWriteClient.fetchRemoteStorage(appId);
      }
      if (remoteStorage.isEmpty()) {
        throw new IllegalStateException(
            "Can't find remoteStorage: with storageType[" + storageType + "]");
      }
    }
    return remoteStorage;
  }

  @SuppressWarnings("rawtypes")
  public static boolean waitUntilDoneOrFail(List<Future<Boolean>> list, boolean allowFastFail) {
    int expected = list.size();
    int failed = 0;
    int finished = 0;
    List<Future<Boolean>> futures = new LinkedList<>(list);

    while (true) {
      Iterator<Future<Boolean>> iterator = futures.iterator();
      while (iterator.hasNext()) {
        Future<Boolean> future = iterator.next();
        if (future.isDone()) {
          iterator.remove();
          try {
            if (!future.get()) {
              failed++;
            } else {
              finished++;
            }
          } catch (Exception e) {
            // cancel or execution exception
            failed++;
          }
        }
      }

      if (expected == finished || futures.isEmpty()) {
        return failed <= 0;
      }

      if (failed > 0 && allowFastFail) {
        futures.forEach(x -> x.cancel(true));
        return false;
      }
      try {
        futures.get(0).get(10, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        futures.forEach(x -> x.cancel(true));
        Thread.currentThread().interrupt();
        return false;
      } catch (TimeoutException e) {
        // ignore
      } catch (Exception e) {
        LOG.warn("Exception in waitUntilDoneOrFail", e);
        // ignore timeout or execution err
      }
    }
  }

  public static void validateTestModeConf(boolean testMode, String storageType) {
    if (!testMode
        && (StorageType.LOCALFILE.name().equals(storageType)
            || (StorageType.HDFS.name()).equals(storageType))) {
      throw new IllegalArgumentException(
          "LOCALFILE or HADOOP storage type should be used in test mode only, "
              + "because of the poor performance of these two types.");
    }
  }

  public static void validateClientType(String clientType) {
    Set<String> types =
        Arrays.stream(ClientType.values()).map(Enum::name).collect(Collectors.toSet());
    if (!types.contains(clientType)) {
      throw new IllegalArgumentException(
          String.format("The value of %s should be one of %s", clientType, types));
    }
  }

  public static int getMaxAttemptNo(int maxFailures, boolean speculation) {
    // attempt number is zero based: 0, 1, …, maxFailures-1
    // max maxFailures < 1 is not allowed but for safety, we interpret that as maxFailures == 1
    int maxAttemptNo = maxFailures < 1 ? 0 : maxFailures - 1;

    // with speculative execution enabled we could observe +1 attempts
    if (speculation) {
      maxAttemptNo++;
    }

    return maxAttemptNo;
  }

  public static int getNumberOfSignificantBits(int number) {
    return 32 - Integer.numberOfLeadingZeros(number);
  }
}
