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

package org.apache.uniffle.server.merge;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.merger.Segment;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.serializer.SerOutputStream;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.proto.RssProtos.MergeContext;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.ShuffleServerConf;

import static org.apache.uniffle.common.merger.MergeState.INTERNAL_ERROR;
import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MERGE_CLASS_LOADER_JARS_PATH;

public class ShuffleMergeManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleMergeManager.class);
  public static final String MERGE_APP_SUFFIX = "@RemoteMerge";

  private ShuffleServerConf serverConf;
  private final ShuffleServer shuffleServer;
  // appId -> shuffleid -> Shuffle
  private final Map<String, Map<Integer, Shuffle>> shuffles = JavaUtils.newConcurrentMap();
  private final MergeEventHandler eventHandler;
  private final Map<String, ClassLoader> cachedClassLoader = new HashMap<>();

  // If comparator is not set, will use hashCode to compare. It is used for shuffle that does not
  // require
  // sort but require combine.
  private Comparator defaultComparator =
      new Comparator() {
        @Override
        public int compare(Object o1, Object o2) {
          int h1 = (o1 == null) ? 0 : o1.hashCode();
          int h2 = (o2 == null) ? 0 : o2.hashCode();
          return h1 < h2 ? -1 : h1 == h2 ? 0 : 1;
        }
      };

  public ShuffleMergeManager(ShuffleServerConf serverConf, ShuffleServer shuffleServer)
      throws Exception {
    this.serverConf = serverConf;
    this.shuffleServer = shuffleServer;
    this.eventHandler = new DefaultMergeEventHandler(this.serverConf, this::processEvent);
    initCacheClassLoader();
  }

  public void initCacheClassLoader() throws Exception {
    addCacheClassLoader("", serverConf.getString(SERVER_MERGE_CLASS_LOADER_JARS_PATH));
    Map<String, Object> props =
        serverConf.getPropsWithPrefix(SERVER_MERGE_CLASS_LOADER_JARS_PATH.key() + ".");
    for (Map.Entry<String, Object> prop : props.entrySet()) {
      addCacheClassLoader(prop.getKey(), (String) prop.getValue());
    }
  }

  public void addCacheClassLoader(String label, String jarsPath) throws Exception {
    if (StringUtils.isNotBlank(jarsPath)) {
      File jarsPathFile = new File(jarsPath);
      if (jarsPathFile.exists()) {
        if (jarsPathFile.isFile()) {
          URLClassLoader urlClassLoader =
              AccessController.doPrivileged(
                  new PrivilegedExceptionAction<URLClassLoader>() {
                    @Override
                    public URLClassLoader run() throws Exception {
                      return new URLClassLoader(
                          new URL[] {new URL("file://" + jarsPath)},
                          Thread.currentThread().getContextClassLoader());
                    }
                  });
          cachedClassLoader.put(label, urlClassLoader);
        } else if (jarsPathFile.isDirectory()) {
          File[] files = jarsPathFile.listFiles();
          List<URL> urlList = new ArrayList<>();
          if (files != null) {
            for (File file : files) {
              if (file.getName().endsWith(".jar")) {
                urlList.add(new URL("file://" + file.getAbsolutePath()));
              }
            }
          }
          URLClassLoader urlClassLoader =
              AccessController.doPrivileged(
                  new PrivilegedExceptionAction<URLClassLoader>() {
                    @Override
                    public URLClassLoader run() throws Exception {
                      return new URLClassLoader(
                          urlList.toArray(new URL[urlList.size()]),
                          Thread.currentThread().getContextClassLoader());
                    }
                  });
          cachedClassLoader.put(label, urlClassLoader);
        } else {
          // If not set, will use current thread classloader
          cachedClassLoader.put(label, Thread.currentThread().getContextClassLoader());
        }
      }
    } else {
      // If not set, will use current thread classloader
      cachedClassLoader.put(label, Thread.currentThread().getContextClassLoader());
    }
  }

  public ClassLoader getClassLoader(String label) {
    if (StringUtils.isBlank(label)) {
      return cachedClassLoader.get("");
    }
    return cachedClassLoader.getOrDefault(label, cachedClassLoader.get(""));
  }

  private Object decode(String encodeString, ClassLoader classLoader) {
    try {
      byte[] bytes = Base64.getDecoder().decode(encodeString.substring(1));
      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      ObjectInputStream ois =
          new ObjectInputStream(bais) {
            @Override
            protected Class<?> resolveClass(ObjectStreamClass desc)
                throws IOException, ClassNotFoundException {
              try {
                return Class.forName(desc.getName(), false, classLoader);
              } catch (ClassNotFoundException e) {
                return super.resolveClass(desc);
              }
            }
          };
      return ois.readObject();
    } catch (Exception e) {
      throw new RssException(e);
    }
  }

  public StatusCode registerShuffle(String appId, int shuffleId, MergeContext mergeContext) {
    try {
      ClassLoader classLoader = getClassLoader(mergeContext.getMergeClassLoader());
      Class kClass = ClassUtils.getClass(classLoader, mergeContext.getKeyClass());
      Class vClass = ClassUtils.getClass(classLoader, mergeContext.getValueClass());
      Comparator comparator;
      if (StringUtils.isNotBlank(mergeContext.getComparatorClass())) {
        if (mergeContext.getComparatorClass().startsWith("#")) {
          comparator = (Comparator) decode(mergeContext.getComparatorClass(), classLoader);
        } else {
          Constructor constructor =
              ClassUtils.getClass(classLoader, mergeContext.getComparatorClass())
                  .getDeclaredConstructor();
          constructor.setAccessible(true);
          comparator = (Comparator) constructor.newInstance();
        }
      } else {
        comparator = defaultComparator;
      }
      this.shuffles.putIfAbsent(appId, JavaUtils.newConcurrentMap());
      this.shuffles
          .get(appId)
          .putIfAbsent(
              shuffleId,
              new Shuffle(
                  serverConf,
                  eventHandler,
                  shuffleServer,
                  appId,
                  shuffleId,
                  kClass,
                  vClass,
                  comparator,
                  mergeContext.getMergedBlockSize(),
                  classLoader));
    } catch (Throwable e) {
      LOG.info("Cannot register shuffle, caused by ", e);
      removeBuffer(appId, shuffleId);
      return StatusCode.INTERNAL_ERROR;
    }
    return StatusCode.SUCCESS;
  }

  public void removeBuffer(String appId) {
    if (this.shuffles.containsKey(appId)) {
      for (Integer shuffleId : this.shuffles.get(appId).keySet()) {
        removeBuffer(appId, shuffleId);
      }
    }
  }

  public void removeBuffer(String appId, List<Integer> shuffleIds) {
    if (this.shuffles.containsKey(appId)) {
      for (Integer shuffleId : shuffleIds) {
        removeBuffer(appId, shuffleId);
      }
    }
  }

  public void removeBuffer(String appId, int shuffleId) {
    if (this.shuffles.containsKey(appId)) {
      if (this.shuffles.get(appId).containsKey(shuffleId)) {
        this.shuffles.get(appId).get(shuffleId).cleanup();
        this.shuffles.get(appId).remove(shuffleId);
      }
      if (this.shuffles.get(appId).size() == 0) {
        this.shuffles.remove(appId);
      }
    }
  }

  public void startSortMerge(
      String appId, int shuffleId, int partitionId, Roaring64NavigableMap expectedBlockIdMap)
      throws IOException {
    Map<Integer, Shuffle> shuffleMap = this.shuffles.get(appId);
    if (shuffleMap != null) {
      Shuffle shuffle = shuffleMap.get(shuffleId);
      if (shuffle != null) {
        shuffle.startSortMerge(partitionId, expectedBlockIdMap);
      }
    }
  }

  public void processEvent(MergeEvent event) {
    boolean success = false;
    Partition partition = null;
    Map<Long, ByteBuf> cachedBlocks = new HashMap<>();
    try {
      Thread.currentThread()
          .setContextClassLoader(
              this.getShuffle(event.getAppId(), event.getShuffleId()).getClassLoader());
      partition = this.getPartition(event.getAppId(), event.getShuffleId(), event.getPartitionId());
      if (partition == null) {
        LOG.info("Can not find partition for event: {}", event);
        return;
      }

      // 1 collect blocks, retain block from bufferPool, need to release.
      boolean allCached =
          partition.collectBlocks(event.getExpectedBlockIdMap().iterator(), cachedBlocks);

      // 2 If the size of cacheBlock is less than total block, we will read from file, so construct
      // reader
      BlockFlushFileReader reader = null;
      if (!allCached) {
        // create reader do not allocate resource.
        reader = partition.createReader(serverConf);
      }

      // 3 collect input segments, but not init. So do not allocate any resource.
      List<Segment> segments = new ArrayList<>();
      boolean allFound =
          partition.collectSegments(
              serverConf,
              event.getExpectedBlockIdMap().iterator(),
              event.getKeyClass(),
              event.getValueClass(),
              cachedBlocks,
              segments,
              reader);
      if (!allFound) {
        return;
      }

      // 4 create output, but not init. So do not allocate any resource.
      // Because of the presence of EOF, the totalBytes are generally slightly larger than the
      // required space.
      long totalBytes = segments.stream().mapToLong(segment -> segment.getSize()).sum();
      SerOutputStream output = partition.createSerOutputStream(totalBytes);

      // 5 merge segments to output
      partition.merge(segments, output, reader);
      success = true;
    } finally {
      if (!success && partition != null) {
        partition.setState(INTERNAL_ERROR);
      }
      cachedBlocks.values().forEach(byteBuf -> byteBuf.release());
    }
  }

  public ShuffleDataResult getShuffleData(
      String appId, int shuffleId, int partitionId, long blockId) throws IOException {
    return this.getPartition(appId, shuffleId, partitionId).getShuffleData(blockId);
  }

  public void setDirect(String appId, int shuffleId, boolean direct) throws IOException {
    if (this.shuffles.containsKey(appId) && this.shuffles.get(appId).containsKey(shuffleId)) {
      this.getShuffle(appId, shuffleId).setDirect(direct);
    }
  }

  public MergeStatus tryGetBlock(String appId, int shuffleId, int partitionId, long blockId) {
    return this.getPartition(appId, shuffleId, partitionId).tryGetBlock(blockId);
  }

  @VisibleForTesting
  MergeEventHandler getEventHandler() {
    return eventHandler;
  }

  Shuffle getShuffle(String appId, int shuffleId) {
    return this.shuffles.get(appId).get(shuffleId);
  }

  @VisibleForTesting
  Partition getPartition(String appId, int shuffleId, int partitionId) {
    if (this.shuffles.containsKey(appId)) {
      if (this.shuffles.get(appId).containsKey(shuffleId)) {
        return this.shuffles.get(appId).get(shuffleId).getPartition(partitionId);
      }
    }
    return null;
  }

  public void refreshAppId(String appId) {
    shuffleServer.getShuffleTaskManager().refreshAppId(appId + MERGE_APP_SUFFIX);
  }
}
