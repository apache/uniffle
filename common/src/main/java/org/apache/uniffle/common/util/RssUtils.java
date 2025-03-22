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

package org.apache.uniffle.common.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.net.BindException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.net.InetAddresses;
import io.netty.channel.unix.Errors;
import io.netty.util.internal.PlatformDependent;
import org.apache.commons.collections4.CollectionUtils;
import org.eclipse.jetty.util.MultiException;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.rpc.ServerInterface;

import static org.apache.uniffle.common.config.RssClientConf.RSS_CLIENT_EXTRA_JAVA_SYSTEM_PROPERTIES;

public class RssUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(RssUtils.class);
  public static final String RSS_LOCAL_DIR_KEY = "RSS_LOCAL_DIRS";

  private RssUtils() {}

  /** Load properties present in the given file. */
  public static Map<String, String> getPropertiesFromFile(String filename) {
    if (filename == null) {
      String rssHome = System.getenv("RSS_HOME");
      if (rssHome == null) {
        LOGGER.error("Both conf file and RSS_HOME env is null");
        return null;
      }

      LOGGER.info("Conf file is null use {}'s server.conf", rssHome);
      filename = rssHome + "/server.conf";
    }

    File file = new File(filename);

    if (!file.exists()) {
      LOGGER.error("Properties file " + filename + " does not exist");
      return null;
    }

    if (!file.isFile()) {
      LOGGER.error("Properties file " + filename + " is not a normal file");
      return null;
    }

    LOGGER.info("Load config from {}", filename);
    final Map<String, String> result = new HashMap<>();

    try (InputStreamReader inReader =
        new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)) {
      Properties properties = new Properties();
      properties.load(inReader);
      properties
          .stringPropertyNames()
          .forEach(k -> result.put(k, properties.getProperty(k).trim()));
    } catch (IOException ignored) {
      LOGGER.error("Failed when loading rss properties from " + filename);
    }

    return result;
  }

  // `InetAddress.getLocalHost().getHostAddress()` could return
  // 127.0.0.1 (127.0.1.1 on Debian). To avoid this situation, we can get current
  // ip through network interface (filtered ipv6, loop back, etc.).
  // If the network interface in the machine is more than one, we will choose the first IP.
  public static String getHostIp() throws Exception {
    // For K8S, there are too many IPs, it's hard to decide which we should use.
    // So we use the environment variable to tell RSS to use which one.
    String ip = System.getenv("RSS_IP");
    if (ip != null) {
      if (!InetAddresses.isInetAddress(ip)) {
        throw new RssException("Environment RSS_IP: " + ip + " is wrong format");
      }
      return ip;
    }
    // Unit tests are executed on a single machine and do not interact with other machines.
    // Therefore, unit tests should not require a valid broadcast address.
    // When running UTs, we will still return the IP address whose broadcast address is invalid.
    boolean isTestMode = Boolean.parseBoolean(System.getProperty("test.mode", "false"));
    Enumeration<NetworkInterface> nif = NetworkInterface.getNetworkInterfaces();
    String siteLocalAddress = null;
    while (nif.hasMoreElements()) {
      NetworkInterface ni = nif.nextElement();
      if (!ni.isUp() || ni.isLoopback() || ni.isPointToPoint() || ni.isVirtual()) {
        continue;
      }
      for (InterfaceAddress ifa : ni.getInterfaceAddresses()) {
        InetAddress ia = ifa.getAddress();
        InetAddress brd = ifa.getBroadcast();
        if ((brd == null || brd.isAnyLocalAddress()) && !isTestMode) {
          LOGGER.info(
              "ip {} was filtered, because it don't have effective broadcast address",
              ia.getHostAddress());
          continue;
        }
        if (!ia.isLinkLocalAddress()
            && !ia.isAnyLocalAddress()
            && !ia.isLoopbackAddress()
            && ia instanceof Inet4Address
            && ia.isReachable(5000)) {
          if (!ia.isSiteLocalAddress()) {
            return ia.getHostAddress();
          } else if (siteLocalAddress == null) {
            LOGGER.info(
                "ip {} was candidate, if there is no better choice, we will choose it",
                ia.getHostAddress());
            siteLocalAddress = ia.getHostAddress();
          } else {
            LOGGER.info(
                "ip {} was filtered, because it's not first effect site local address",
                ia.getHostAddress());
          }
        } else if (!(ia instanceof Inet4Address)) {
          LOGGER.info("ip {} was filtered, because it's just a ipv6 address", ia.getHostAddress());
        } else if (ia.isLinkLocalAddress()) {
          LOGGER.info(
              "ip {} was filtered, because it's just a link local address", ia.getHostAddress());
        } else if (ia.isAnyLocalAddress()) {
          LOGGER.info(
              "ip {} was filtered, because it's just a any local address", ia.getHostAddress());
        } else if (ia.isLoopbackAddress()) {
          LOGGER.info(
              "ip {} was filtered, because it's just a loop back address", ia.getHostAddress());
        } else {
          LOGGER.info(
              "ip {} was filtered, because it's just not reachable address", ia.getHostAddress());
        }
      }
    }
    return siteLocalAddress;
  }

  public static int startServiceOnPort(
      ServerInterface service, String serviceName, int servicePort, RssBaseConf conf) {
    if (servicePort < 0 || servicePort > 65535) {
      throw new IllegalArgumentException(
          String.format("Bad service %s on port (%s)", serviceName, servicePort));
    }
    int actualPort = servicePort;
    int maxRetries = conf.get(RssBaseConf.SERVER_PORT_MAX_RETRIES);
    for (int i = 0; i < maxRetries; i++) {
      try {
        if (servicePort == 0) {
          actualPort = findRandomTcpPort(conf);
        } else {
          actualPort += i;
        }
        service.startOnPort(actualPort);
        return actualPort;
      } catch (Exception e) {
        if (isServerPortBindCollision(e)) {
          LOGGER.warn(
              String.format(
                  "%s:Service %s failed after %s retries (on a random free port (%s))!",
                  e.getMessage(), serviceName, i + 1, actualPort));
        } else {
          throw new RssException(
              String.format("Failed to start service %s on port %s", serviceName, servicePort), e);
        }
      }
    }
    throw new RssException(
        String.format("Failed to start service %s on port %s", serviceName, servicePort));
  }

  /** check whether the exception is caused by an address-port collision when binding. */
  public static boolean isServerPortBindCollision(Throwable e) {
    if (e instanceof BindException) {
      if (e.getMessage() != null) {
        return true;
      }
      return isServerPortBindCollision(e.getCause());
    } else if (e instanceof MultiException) {
      return !((MultiException) e)
          .getThrowables().stream()
              .noneMatch((Throwable throwable) -> isServerPortBindCollision(throwable));
    } else if (e instanceof Errors.NativeIoException) {
      return (e.getMessage() != null && e.getMessage().startsWith("bind() failed: "))
          || isServerPortBindCollision(e.getCause());
    } else if (e instanceof IOException) {
      return (e.getMessage() != null && e.getMessage().startsWith("Failed to bind to address"))
          || isServerPortBindCollision(e.getCause());
    } else {
      return false;
    }
  }

  public static int findRandomTcpPort(RssBaseConf baseConf) {
    int portRangeMin = baseConf.getInteger(RssBaseConf.RSS_RANDOM_PORT_MIN);
    int portRangeMax = baseConf.getInteger(RssBaseConf.RSS_RANDOM_PORT_MAX);
    int portRange = portRangeMax - portRangeMin;
    return portRangeMin + ThreadLocalRandom.current().nextInt(portRange + 1);
  }

  public static byte[] serializeBitMap(Roaring64NavigableMap bitmap) throws IOException {
    long size = bitmap.serializedSizeInBytes();
    if (size > Integer.MAX_VALUE) {
      throw new RssException("Unsupported serialized size of bitmap: " + size);
    }
    ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream((int) size);
    DataOutputStream dataOutputStream = new DataOutputStream(arrayOutputStream);
    bitmap.serialize(dataOutputStream);
    return arrayOutputStream.toByteArray();
  }

  public static Roaring64NavigableMap deserializeBitMap(byte[] bytes) throws IOException {
    Roaring64NavigableMap bitmap = Roaring64NavigableMap.bitmapOf();
    if (bytes.length == 0) {
      return bitmap;
    }
    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
    bitmap.deserialize(dataInputStream);
    return bitmap;
  }

  public static Roaring64NavigableMap cloneBitMap(Roaring64NavigableMap bitmap) {
    Roaring64NavigableMap clone = Roaring64NavigableMap.bitmapOf();
    clone.or(bitmap);
    return clone;
  }

  public static String generateShuffleKey(String appId, int shuffleId) {
    return String.join(Constants.KEY_SPLIT_CHAR, appId, String.valueOf(shuffleId));
  }

  public static String generatePartitionKey(String appId, Integer shuffleId, Integer partition) {
    return String.join(
        Constants.KEY_SPLIT_CHAR, appId, String.valueOf(shuffleId), String.valueOf(partition));
  }

  public static <T> T loadExtension(Class<T> extCls, String clsPackage, Object obj) {
    List<T> exts = loadExtensions(extCls, Arrays.asList(clsPackage), obj);
    if (exts != null && exts.size() == 1) {
      return exts.get(0);
    }
    throw new IllegalArgumentException("No such extension for " + clsPackage);
  }

  @SuppressWarnings("unchecked")
  public static <T> List<T> loadExtensions(Class<T> extClass, List<String> classes, Object obj) {
    if (classes == null || classes.isEmpty()) {
      throw new RssException("Empty classes");
    }

    List<T> extensions = Lists.newArrayList();
    for (String name : classes) {
      name = name.trim();
      try {
        Class<?> klass = Class.forName(name);
        if (!extClass.isAssignableFrom(klass)) {
          throw new RssException(name + " is not subclass of " + extClass.getName());
        }

        Constructor<?> constructor;
        T instance;
        try {
          constructor = klass.getConstructor(obj.getClass());
          instance = (T) constructor.newInstance(obj);
        } catch (Exception e) {
          LOGGER.error("Fail to new instance.", e);
          instance = (T) klass.getConstructor().newInstance();
        }
        extensions.add(instance);
      } catch (Exception e) {
        LOGGER.error("Fail to new instance using default constructor.", e);
        throw new RssException(e);
      }
    }
    return extensions;
  }

  public static void checkQuorumSetting(int replica, int replicaWrite, int replicaRead) {
    if (replica < 1 || replicaWrite > replica || replicaRead > replica) {
      throw new RssException(
          "Replica config is invalid, it cannot be less than 1 or less than replica.write  or less than replica.read. Please check it.");
    }
    if (replicaWrite + replicaRead <= replica) {
      throw new RssException(
          "Replica config is unsafe, recommend replica.write + replica.read > replica");
    }
  }

  // the method is used to transfer hostname to metric name.
  // With standard of hostname, `-` and `.` will be included in hostname,
  // but they are not valid for metric name, replace them with '_'
  public static String getMetricNameForHostName(String hostName) {
    if (hostName == null) {
      return "";
    }
    return hostName.replaceAll("[\\.-]", "_");
  }

  public static Map<Integer, Roaring64NavigableMap> generatePartitionToBitmap(
      Roaring64NavigableMap shuffleBitmap,
      int startPartition,
      int endPartition,
      BlockIdLayout layout) {
    Map<Integer, Roaring64NavigableMap> result = Maps.newHashMap();
    for (int partitionId = startPartition; partitionId < endPartition; partitionId++) {
      result.computeIfAbsent(partitionId, key -> Roaring64NavigableMap.bitmapOf());
    }
    Iterator<Long> it = shuffleBitmap.iterator();
    while (it.hasNext()) {
      Long blockId = it.next();
      int partitionId = layout.getPartitionId(blockId);
      if (partitionId >= startPartition && partitionId < endPartition) {
        result.get(partitionId).add(blockId);
      }
    }
    return result;
  }

  public static Map<ShuffleServerInfo, Set<Integer>> generateServerToPartitions(
      Map<Integer, List<ShuffleServerInfo>> partitionToServers) {
    Map<ShuffleServerInfo, Set<Integer>> serverToPartitions = Maps.newHashMap();
    for (Map.Entry<Integer, List<ShuffleServerInfo>> entry : partitionToServers.entrySet()) {
      int partitionId = entry.getKey();
      for (ShuffleServerInfo serverInfo : entry.getValue()) {
        if (!serverToPartitions.containsKey(serverInfo)) {
          serverToPartitions.put(serverInfo, Sets.newHashSet(partitionId));
        } else {
          serverToPartitions.get(serverInfo).add(partitionId);
        }
      }
    }
    return serverToPartitions;
  }

  public static void checkProcessedBlockIds(
      Roaring64NavigableMap blockIdBitmap, Roaring64NavigableMap processedBlockIds) {
    // processedBlockIds can be a superset of blockIdBitmap,
    // here we check that processedBlockIds has all bits of blockIdBitmap set
    // first a quick check:
    //   we only need to do the bitwise AND when blockIdBitmap is not equal to processedBlockIds
    if (!blockIdBitmap.equals(processedBlockIds)) {
      Roaring64NavigableMap cloneBitmap;
      cloneBitmap = RssUtils.cloneBitMap(blockIdBitmap);
      cloneBitmap.and(processedBlockIds);
      if (!blockIdBitmap.equals(cloneBitmap)) {
        throw new RssException(
            "Blocks read inconsistent: expected "
                + blockIdBitmap.getLongCardinality()
                + " blocks, actual "
                + cloneBitmap.getLongCardinality()
                + " blocks");
      }
    }
  }

  public static Roaring64NavigableMap generateTaskIdBitMap(
      Roaring64NavigableMap blockIdBitmap, IdHelper idHelper) {
    Iterator<Long> iterator = blockIdBitmap.iterator();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf();
    while (iterator.hasNext()) {
      taskIdBitmap.addLong(idHelper.getTaskAttemptId(iterator.next()));
    }
    return taskIdBitmap;
  }

  public static List<String> getConfiguredLocalDirs(RssConf conf) {
    if (conf.getEnv(RSS_LOCAL_DIR_KEY) != null) {
      return Arrays.asList(conf.getEnv(RSS_LOCAL_DIR_KEY).split(","));
    } else {
      return conf.get(RssBaseConf.RSS_STORAGE_BASE_PATH);
    }
  }

  public static void releaseByteBuffer(ByteBuffer byteBuffer) {
    if (byteBuffer == null || !byteBuffer.isDirect()) {
      return;
    }
    PlatformDependent.freeDirectBuffer(byteBuffer);
  }

  public static Constructor<?> getConstructor(String className, Class<?>... parameterTypes)
      throws ClassNotFoundException, NoSuchMethodException {
    Class<?> klass = Class.forName(className);
    Constructor<?> constructor = klass.getConstructor(parameterTypes);
    return constructor;
  }

  public static void setExtraJavaProperties(RssConf conf) {
    List<String> properties = conf.get(RSS_CLIENT_EXTRA_JAVA_SYSTEM_PROPERTIES);
    if (CollectionUtils.isEmpty(properties)) {
      return;
    }

    for (String propertyKv : properties) {
      LOGGER.info("Setting system property: {}", propertyKv);
      String[] raw = propertyKv.split("=", 2);
      System.setProperty(raw[0], raw[1]);
    }
  }
}
