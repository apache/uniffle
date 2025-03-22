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

package org.apache.uniffle.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.config.RssConf;

public class ReconfigurableRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(ReconfigurableRegistry.class);

  private static final HashMap<Set<String>, List<ReconfigureListener>> LISTENER_MAP =
      new HashMap<>();

  // prevent instantiation
  private ReconfigurableRegistry() {}

  /**
   * Add a listener which listens to all properties.
   *
   * @param listener the given property listener
   */
  public static synchronized void register(ReconfigureListener listener) {
    register(Collections.emptySet(), listener);
  }

  /**
   * Add a listener which listens to the given keys.
   *
   * @param key the given key
   * @param listener the given property listener
   */
  public static synchronized void register(String key, ReconfigureListener listener) {
    register(Sets.newHashSet(key), listener);
  }

  /**
   * Add a listener which listens to the given keys.
   *
   * @param keys the given keys
   * @param listener the given property listener
   */
  public static synchronized void register(Set<String> keys, ReconfigureListener listener) {
    List<ReconfigureListener> listenerList =
        LISTENER_MAP.computeIfAbsent(keys, k -> new ArrayList<>());
    listenerList.add(listener);
  }

  /**
   * Remove all listeners related to the given key.
   *
   * @param key the given key
   * @return true if the listeners are removed, otherwise false
   */
  public static synchronized boolean unregister(String key) {
    return unregister(Sets.newHashSet(key));
  }

  /**
   * Remove all listeners related to the given keys.
   *
   * @param keys the given keys
   * @return true if the listeners are removed, otherwise false
   */
  public static synchronized boolean unregister(Set<String> keys) {
    return LISTENER_MAP.remove(keys) != null;
  }

  /**
   * Remove the listener related to the given keys.
   *
   * @param key the given key
   * @param listener the given listener
   * @return true if the listeners are removed, otherwise false
   */
  public static synchronized boolean unregister(String key, ReconfigureListener listener) {
    return unregister(Sets.newHashSet(key), listener);
  }

  /**
   * Remove the listener related to the given keys.
   *
   * @param keys the given keys
   * @param listener the given listener
   * @return true if the listeners are removed, otherwise false
   */
  public static synchronized boolean unregister(Set<String> keys, ReconfigureListener listener) {
    List<ReconfigureListener> listenerList = LISTENER_MAP.get(keys);
    if (listenerList == null) {
      return false;
    }
    boolean removed = listenerList.remove(listener);
    if (listenerList.isEmpty()) {
      LISTENER_MAP.remove(keys);
    }
    return removed;
  }

  /**
   * Remove the listener from all keys listeners first, if the listener is not in any keys, will
   * scan all the listener maps to remove the listener.
   *
   * @param listener the given listener
   * @return true if the listeners are removed, otherwise false
   */
  public static synchronized boolean unregister(ReconfigureListener listener) {
    boolean removed = unregister(Collections.emptySet(), listener);
    if (!removed) {
      for (Map.Entry<Set<String>, List<ReconfigureListener>> entry : LISTENER_MAP.entrySet()) {
        removed = unregister(entry.getKey(), listener);
        if (removed) {
          break;
        }
      }
    }
    return removed;
  }

  @VisibleForTesting
  public static int getSize() {
    return LISTENER_MAP.size();
  }

  @VisibleForTesting
  public static void clear() {
    LISTENER_MAP.clear();
  }

  /**
   * When the property was reconfigured, this function will be invoked. This property listeners will
   * be notified.
   *
   * @param conf the rss conf
   * @param changedProperties the changed properties
   */
  public static synchronized void update(RssConf conf, Set<String> changedProperties) {
    for (Map.Entry<Set<String>, List<ReconfigureListener>> entry : LISTENER_MAP.entrySet()) {
      // check if the keys is empty, if empty, it means all keys are listened.
      Set<String> intersection =
          entry.getKey().isEmpty()
              ? changedProperties
              : Sets.intersection(entry.getKey(), changedProperties);
      if (!intersection.isEmpty()) {
        Set<String> filteredSet = Sets.filter(changedProperties, intersection::contains);
        for (ReconfigureListener listener : entry.getValue()) {
          try {
            listener.update(conf, filteredSet);
          } catch (Throwable e) {
            LOG.warn("Exception while updating config for {}", changedProperties, e);
          }
        }
      }
    }
  }

  public interface ReconfigureListener {
    /**
     * When the property changed, this function will be invoked.
     *
     * @param conf the rss conf
     * @param changedProperties the changed properties
     */
    void update(RssConf conf, Set<String> changedProperties);
  }
}
