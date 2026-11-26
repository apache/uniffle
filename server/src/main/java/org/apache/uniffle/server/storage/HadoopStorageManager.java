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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.AuditType;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.exception.InvalidRequestException;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.filesystem.HadoopFilesystemProvider;
import org.apache.uniffle.common.storage.StorageInfo;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.server.Checker;
import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.ShuffleDataReadEvent;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;
import org.apache.uniffle.server.event.AppPurgeEvent;
import org.apache.uniffle.server.event.PurgeEvent;
import org.apache.uniffle.storage.common.HadoopStorage;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.factory.ShuffleHandlerFactory;
import org.apache.uniffle.storage.handler.api.ShuffleDeleteHandler;
import org.apache.uniffle.storage.request.CreateShuffleDeleteHandlerRequest;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;
import org.apache.uniffle.storage.util.StorageType;

public class HadoopStorageManager extends SingleStorageManager {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopStorageManager.class);
  private static final Logger AUDIT_LOGGER =
      LoggerFactory.getLogger("SHUFFLE_SERVER_STORAGE_AUDIT_LOG");

  private final Configuration hadoopConf;
  private final String shuffleServerId;
  private Map<String, HadoopStorage> appIdToStorages = JavaUtils.newConcurrentMap();
  private Map<String, HadoopStorage> pathToStorages = JavaUtils.newConcurrentMap();
  private final boolean isStorageAuditLogEnabled;
  private final List<Path> allowedRemoteStoragePaths;
  // The failure of loading the file system of a scheme that was logged with its stack, so that
  // the stack is not logged again for every registration while the server stays broken
  private final ConcurrentMap<String, Throwable> loggedLoadFailures = JavaUtils.newConcurrentMap();

  HadoopStorageManager(ShuffleServerConf conf) {
    super(conf);
    hadoopConf = conf.getHadoopConf();
    shuffleServerId = conf.getString(ShuffleServerConf.SHUFFLE_SERVER_ID, "shuffleServerId");
    isStorageAuditLogEnabled = conf.getBoolean(ShuffleServerConf.SERVER_STORAGE_AUDIT_LOG_ENABLED);
    allowedRemoteStoragePaths = getAllowedRemoteStoragePaths(conf);
    if (allowedRemoteStoragePaths.isEmpty()) {
      LOG.warn(
          "No remote storage path is allowed, because {} is not configured or is empty. "
              + "Any register request with a remote storage will be rejected",
          ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS.key());
    }
  }

  private static List<Path> getAllowedRemoteStoragePaths(ShuffleServerConf conf) {
    List<String> paths = conf.get(ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS);
    if (paths == null) {
      return Collections.emptyList();
    }
    List<Path> allowedPaths = new ArrayList<>(paths.size());
    for (String path : paths) {
      // A comma that is followed by nothing or by a space is a common slip of the hand, and a
      // blank entry never denotes a location, so it is skipped instead of failing the server
      if (StringUtils.isBlank(path)) {
        LOG.warn(
            "Ignore the blank entry of {}",
            ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS.key());
        continue;
      }
      try {
        allowedPaths.add(checkRemoteStoragePath(path.trim()));
      } catch (InvalidRequestException e) {
        throw new IllegalArgumentException(
            "Invalid value of "
                + ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS.key()
                + ": "
                + e.getMessage(),
            e);
      }
    }
    LOG.info("The allowed remote storage paths are {}", allowedPaths);
    return Collections.unmodifiableList(allowedPaths);
  }

  /**
   * Converts a remote storage path into a qualified one, and refuses any path whose location is not
   * explicit in the path itself.
   *
   * <p>A path without scheme is resolved against fs.defaultFS of the hadoop configuration in use,
   * and a path of a non local file system without authority depends on it as well, so both of them
   * have to be rejected. A local path must not carry an authority either, because `file://tmp/rss`
   * reads like `/tmp/rss` but denotes `/rss` of the local file system.
   */
  private static Path checkRemoteStoragePath(String path) {
    String reason = null;
    Path qualifiedPath = null;
    if (StringUtils.isBlank(path)) {
      reason = "it is blank";
    } else if (RssUtils.containsIsoControl(path)) {
      reason = "it contains control characters";
    } else if (path.indexOf('\\') >= 0) {
      reason = "it must not contain \"\\\"";
    } else if (containsDotSegment(path)) {
      // Path resolves the dot segments silently, so they have to be refused beforehand
      reason = "it must not contain \".\" or \"..\" segments";
    } else {
      try {
        qualifiedPath = new Path(path);
        URI uri = qualifiedPath.toUri();
        if (uri.getScheme() == null) {
          reason = "it must be a fully qualified path with a scheme";
        } else if ("file".equals(uri.getScheme())) {
          if (StringUtils.isNotEmpty(uri.getAuthority())) {
            reason = "a local path must be written without authority, such as file:///rss";
          }
        } else if (StringUtils.isEmpty(uri.getAuthority())) {
          reason = "it must contain the authority, such as hdfs://nameservice1/rss";
        }
      } catch (IllegalArgumentException e) {
        reason = "it is not a valid uri";
      }
    }
    if (reason != null) {
      throw new InvalidRequestException(
          String.format("Remote storage path [%s] is invalid, %s", path, reason));
    }
    return qualifiedPath;
  }

  private static boolean containsDotSegment(String dir) {
    for (String segment : dir.split(Path.SEPARATOR, -1)) {
      if (segment.equals(".") || segment.equals("..")) {
        return true;
      }
    }
    return false;
  }

  /** Whether the path is the allowed one itself or is located under it. */
  private static boolean isUnderAllowedPath(Path path, Path allowedPath) {
    URI pathUri = path.toUri();
    URI allowedUri = allowedPath.toUri();
    if (!pathUri.getScheme().equals(allowedUri.getScheme())
        || !Objects.equals(pathUri.getAuthority(), allowedUri.getAuthority())) {
      return false;
    }
    String allowedDir = toDir(allowedUri.getPath());
    String pathDir = toDir(pathUri.getPath());
    return pathDir.equals(allowedDir)
        || pathDir.startsWith(allowedDir.equals("/") ? "/" : allowedDir + "/");
  }

  private static String toDir(String dir) {
    return dir == null || dir.isEmpty() ? Path.SEPARATOR : dir;
  }

  private boolean isAllowedRemoteStorage(Path path) {
    for (Path allowedPath : allowedRemoteStoragePaths) {
      if (isUnderAllowedPath(path, allowedPath)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void updateWriteMetrics(ShuffleDataFlushEvent event, long writeTime) {
    super.updateWriteMetrics(event, writeTime);
    Storage storage = event.getUnderStorage();
    if (storage == null) {
      LOG.warn("The storage owned by event: {} is null, this should not happen", event);
      return;
    }
    ShuffleServerMetrics.incHadoopStorageWriteDataSize(
        storage.getStorageHost(), event.getDataLength(), event.isOwnedByHugePartition());
  }

  @Override
  public Storage selectStorage(ShuffleDataFlushEvent event) {
    Storage storage = getStorageByAppId(event.getAppId());
    event.setUnderStorage(storage);
    return storage;
  }

  @Override
  public Storage selectStorage(ShuffleDataReadEvent event) {
    return getStorageByAppId(event.getAppId());
  }

  @Override
  public void removeResources(PurgeEvent event) {
    String appId = event.getAppId();
    HadoopStorage storage = getStorageByAppId(appId);
    if (storage != null) {
      boolean purgeForExpired = false;
      if (event instanceof AppPurgeEvent) {
        storage.removeHandlers(appId);
        appIdToStorages.remove(appId);
        purgeForExpired = ((AppPurgeEvent) event).isAppExpired();
      }
      ShuffleDeleteHandler deleteHandler =
          ShuffleHandlerFactory.getInstance()
              .createShuffleDeleteHandler(
                  new CreateShuffleDeleteHandlerRequest(
                      StorageType.HDFS.name(),
                      storage.getConf(),
                      purgeForExpired ? shuffleServerId : null,
                      event.isRenameAndDelete()));

      String basicPath =
          ShuffleStorageUtils.getFullShuffleDataFolder(storage.getStoragePath(), appId);
      List<String> deletePaths = new ArrayList<>();

      if (event instanceof AppPurgeEvent) {
        deletePaths.add(basicPath);
        if (isStorageAuditLogEnabled) {
          AUDIT_LOGGER.info(
              String.format(
                  "%s|%s|%s|%s",
                  AuditType.DELETE.getValue(),
                  appId,
                  storage.getStorageHost(),
                  storage.getStoragePath()));
        }
      } else {
        for (Integer shuffleId : event.getShuffleIds()) {
          deletePaths.add(
              ShuffleStorageUtils.getFullShuffleDataFolder(basicPath, String.valueOf(shuffleId)));
        }
        if (isStorageAuditLogEnabled) {
          AUDIT_LOGGER.info(
              String.format(
                  "%s|%s|%s|%s|%s",
                  AuditType.DELETE.getValue(),
                  appId,
                  event.getShuffleIds().stream()
                      .map(Object::toString)
                      .collect(Collectors.joining(",")),
                  storage.getStorageHost(),
                  storage.getStoragePath()));
        }
      }
      deleteHandler.delete(deletePaths.toArray(new String[0]), appId, event.getUser());
      removeAppStorageInfo(event);
    } else {
      LOG.warn("Storage gotten is null when removing resources for event: {}", event);
    }
  }

  @Override
  public Checker getStorageChecker() {
    throw new RssException("Not support storage checker");
  }

  /**
   * Registers the remote storage of an application. The remote storage comes with the register
   * request of a client, so it is only accepted when it is located under one of the allowed paths.
   *
   * @throws InvalidRequestException if the remote storage is not allowed on this server
   */
  @Override
  public void registerRemoteStorage(String appId, RemoteStorageInfo remoteStorageInfo) {
    String remoteStorage = remoteStorageInfo.getPath();
    Path remoteStoragePath = checkRemoteStoragePath(remoteStorage);
    if (!isAllowedRemoteStorage(remoteStoragePath)) {
      String reason =
          allowedRemoteStoragePaths.isEmpty()
              ? String.format(
                  "no remote storage is allowed by %s",
                  ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS.key())
              : String.format(
                  "it must be under one of the paths configured by %s",
                  ShuffleServerConf.SERVER_REMOTE_STORAGE_ALLOWED_PATHS.key());
      // Both have passed their checks, which refuse the control characters, so they can not fake
      // log lines: the path in checkRemoteStoragePath above, the appId in the rpc service
      LOG.warn("Reject the remote storage [{}] of appId[{}], {}", remoteStorage, appId, reason);
      throw new InvalidRequestException(
          String.format("Remote storage path [%s] is rejected, %s", remoteStorage, reason));
    }
    pathToStorages.computeIfAbsent(
        remoteStorage,
        key -> {
          Configuration remoteStorageHadoopConf = createHadoopConf(remoteStorageInfo);
          checkFileSystemScheme(remoteStoragePath.toUri().getScheme(), remoteStorageHadoopConf);
          return new HadoopStorage(remoteStorage, remoteStorageHadoopConf);
        });
    appIdToStorages.computeIfAbsent(appId, key -> pathToStorages.get(remoteStorage));
    // Only the path is logged, the conf items come from the client and may carry credentials or
    // characters that fake log lines
    Map<String, String> remoteStorageConf = remoteStorageInfo.getConfItems();
    LOG.info(
        "register remote storage [{}] with {} conf item(s) successfully for appId {}",
        remoteStorage,
        remoteStorageConf == null ? 0 : remoteStorageConf.size(),
        appId);
  }

  /**
   * Refuses a remote storage whose file system can not be found on this server, so that a client
   * which relied on the ignored fs.<scheme>.impl is told at registration instead of at the first
   * write.
   *
   * <p>A local file system is only accepted with the scheme file. An operator can map another
   * scheme to the local file system, and such a scheme takes an authority, so a path like
   * raw://host/rss would read like another location than the local /rss it denotes. Only the two
   * local file system classes of Hadoop and their subclasses are told apart here.
   */
  private void checkFileSystemScheme(String scheme, Configuration hadoopConf) {
    Class<? extends FileSystem> fileSystemClass;
    try {
      fileSystemClass = FileSystem.getFileSystemClass(scheme, hadoopConf);
    } catch (IOException | RuntimeException | LinkageError e) {
      // A class of a file system that can not be loaded is reported by a RuntimeException or a
      // LinkageError. That's a problem of this server, so it's logged for the operator, while the
      // client is only told that the scheme is not supported. Every registration of such a path
      // ends up here while the server stays broken, so the stack is logged for the first failure
      // of a scheme only, unless a later failure carries the reason the first one lacked: the
      // threads that wait for a failing class initialization get an error without a cause.
      if (recordLoadFailure(scheme, e)) {
        LOG.warn("This shuffle server can not load the file system of the scheme [{}]", scheme, e);
      } else {
        LOG.warn(
            "The file system of the scheme [{}] can not be loaded, the request is refused", scheme);
      }
      throw new InvalidRequestException(
          String.format(
              "This shuffle server has no file system for the scheme [%s], the implementation of a "
                  + "file system is decided by the server and fs.%s.impl of a request is ignored",
              scheme, scheme),
          e);
    }
    loggedLoadFailures.remove(scheme);
    boolean isLocal =
        LocalFileSystem.class.isAssignableFrom(fileSystemClass)
            || RawLocalFileSystem.class.isAssignableFrom(fileSystemClass);
    if (isLocal && !"file".equals(scheme)) {
      throw new InvalidRequestException(
          String.format(
              "The scheme [%s] is a local file system, a local path must be written as "
                  + "file:///path",
              scheme));
    }
  }

  /**
   * Remembers a failure of loading the file system of a scheme, and tells whether this failure is
   * worth logging with its stack: it's the first failure of the scheme, or the failure logged
   * before came without a reason while this one carries it.
   */
  @VisibleForTesting
  boolean recordLoadFailure(String scheme, Throwable failure) {
    Throwable logged = loggedLoadFailures.putIfAbsent(scheme, failure);
    return logged == null
        || (logged.getCause() == null
            && failure.getCause() != null
            && loggedLoadFailures.replace(scheme, logged, failure));
  }

  private Configuration createHadoopConf(RemoteStorageInfo remoteStorageInfo) {
    Configuration remoteStorageHadoopConf = new Configuration(hadoopConf);
    Map<String, String> remoteStorageConf = remoteStorageInfo.getConfItems();
    if (remoteStorageConf != null && remoteStorageConf.size() > 0) {
      int ignored = 0;
      for (Map.Entry<String, String> entry : remoteStorageConf.entrySet()) {
        // The classes of a file system are decided by this server, a client must not be able to
        // replace them, otherwise an allowed path could be redirected to another file system.
        // Configuration trims the name of a key when it's set, so the key is matched trimmed too.
        String key = entry.getKey().trim();
        if (key.startsWith("fs.") && key.endsWith(".impl")) {
          ignored++;
          continue;
        }
        remoteStorageHadoopConf.setStrings(key, entry.getValue());
      }
      // The keys are not logged, they come from the client and may carry characters that fake
      // log lines
      if (ignored > 0) {
        LOG.warn(
            "Ignore {} conf key(s) of the remote storage that select the classes of a file system, "
                + "such as fs.<scheme>.impl, they are decided by this shuffle server",
            ignored);
      }
    }
    return remoteStorageHadoopConf;
  }

  @Override
  public void checkAndClearLeakedShuffleData(Supplier<Collection<String>> appIdsSupplier) {}

  @Override
  public Map<String, StorageInfo> getStorageInfo() {
    // todo: report remote storage info
    return Maps.newHashMap();
  }

  public HadoopStorage getStorageByAppId(String appId) {
    if (!appIdToStorages.containsKey(appId)) {
      synchronized (this) {
        FileSystem fs;
        try {
          List<Path> appStoragePath =
              pathToStorages.keySet().stream()
                  .map(
                      basePath ->
                          new Path(ShuffleStorageUtils.getFullShuffleDataFolder(basePath, appId)))
                  .collect(Collectors.toList());
          for (Path path : appStoragePath) {
            fs = HadoopFilesystemProvider.getFilesystem(path, hadoopConf);
            if (fs.isDirectory(path)) {
              return new HadoopStorage(path.getParent().toString(), hadoopConf);
            }
          }
        } catch (Exception e) {
          LOG.error("Some error happened when fileSystem got the file status.", e);
        }
        // outside should deal with null situation
        // todo: it's better to have a fake storage for null situation
        return null;
      }
    }
    return appIdToStorages.get(appId);
  }

  @VisibleForTesting
  public Map<String, HadoopStorage> getAppIdToStorages() {
    return appIdToStorages;
  }

  @VisibleForTesting
  public Map<String, HadoopStorage> getPathToStorages() {
    return pathToStorages;
  }
}
