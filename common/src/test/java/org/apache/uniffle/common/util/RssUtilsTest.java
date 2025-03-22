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

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;
import javax.net.ServerSocketFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.rpc.ServerInterface;

import static org.apache.uniffle.common.config.RssClientConf.RSS_CLIENT_EXTRA_JAVA_SYSTEM_PROPERTIES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.org.webcompere.systemstubs.SystemStubs.withEnvironmentVariable;

public class RssUtilsTest {

  @Test
  public void testGetPropertiesFromFile() {
    final String filePath =
        Objects.requireNonNull(getClass().getClassLoader().getResource("rss-defaults.conf"))
            .getFile();
    Map<String, String> properties = RssUtils.getPropertiesFromFile(filePath);
    assertEquals("12121", properties.get("rss.coordinator.port"));
    assertEquals("155", properties.get("rss.server.heartbeat.interval"));
    assertEquals("true", properties.get("rss.x.y.z"));
    assertEquals(
        "-XX:+PrintGCDetails-Dkey=value-Dnumbers=\"one two three\"",
        properties.get("rss.a.b.c.extraJavaOptions"));
  }

  @Test
  public void testGetHostIp() {
    try {
      String realIp = RssUtils.getHostIp();
      InetAddress ia = InetAddress.getByName(realIp);
      assertTrue(ia instanceof Inet4Address);
      assertFalse(ia.isLinkLocalAddress() || ia.isAnyLocalAddress() || ia.isLoopbackAddress());
      assertNotNull(NetworkInterface.getByInetAddress(ia));
      assertTrue(ia.isReachable(5000));
      withEnvironmentVariable("RSS_IP", "8.8.8.8")
          .execute(() -> assertEquals("8.8.8.8", RssUtils.getHostIp()));
      withEnvironmentVariable("RSS_IP", "xxxx")
          .execute(
              () -> {
                boolean isException = false;
                try {
                  RssUtils.getHostIp();
                } catch (Exception e) {
                  isException = true;
                }
                assertTrue(isException);
              });
      withEnvironmentVariable("RSS_IP", realIp).execute(RssUtils::getHostIp);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testStartServiceOnPort() throws InterruptedException {
    RssBaseConf rssBaseConf = new RssBaseConf();
    rssBaseConf.set(RssBaseConf.SERVER_PORT_MAX_RETRIES, 100);
    rssBaseConf.set(RssBaseConf.RSS_RANDOM_PORT_MIN, 30000);
    rssBaseConf.set(RssBaseConf.RSS_RANDOM_PORT_MAX, 39999);
    // zero port to get random port
    MockServer mockServer = new MockServer();
    int port = 0;
    try {
      int actualPort = RssUtils.startServiceOnPort(mockServer, "MockServer", port, rssBaseConf);
      assertTrue(
          actualPort >= 30000
              && actualPort < 39999 + rssBaseConf.get(RssBaseConf.SERVER_PORT_MAX_RETRIES));
    } finally {
      if (mockServer != null) {
        mockServer.stop();
      }
    }
    // error port test
    try {
      port = -1;
      RssUtils.startServiceOnPort(mockServer, "MockServer", port, rssBaseConf);
    } catch (RuntimeException e) {
      assertTrue(e.toString().startsWith("java.lang.IllegalArgumentException: Bad service"));
    }
    // a specific port to start
    try {
      mockServer = new MockServer();
      port = 10000;
      rssBaseConf.set(RssBaseConf.SERVER_PORT_MAX_RETRIES, 100);
      int actualPort = RssUtils.startServiceOnPort(mockServer, "MockServer", port, rssBaseConf);
      assertTrue(
          actualPort >= port
              && actualPort < port + rssBaseConf.get(RssBaseConf.SERVER_PORT_MAX_RETRIES));
    } finally {
      if (mockServer != null) {
        mockServer.stop();
      }
    }

    // bind exception
    MockServer toStartSockServer = new MockServer();
    try {
      mockServer = new MockServer();
      port = 10000;
      int actualPort1 = RssUtils.startServiceOnPort(mockServer, "MockServer", port, rssBaseConf);
      rssBaseConf.set(RssBaseConf.SERVER_PORT_MAX_RETRIES, 10);
      int actualPort2 =
          RssUtils.startServiceOnPort(toStartSockServer, "MockServer", actualPort1, rssBaseConf);
      assertTrue(actualPort1 < actualPort2);
      toStartSockServer.stop();
      rssBaseConf.set(RssBaseConf.SERVER_PORT_MAX_RETRIES, 0);
      RssUtils.startServiceOnPort(toStartSockServer, "MockServer", actualPort1, rssBaseConf);
      assertFalse(false);
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().startsWith("Failed to start service"));
    } finally {
      if (mockServer != null) {
        mockServer.stop();
      }
      if (toStartSockServer != null) {
        toStartSockServer.stop();
      }
    }
  }

  @Test
  public void testSerializeBitmap() throws Exception {
    Roaring64NavigableMap bitmap1 = Roaring64NavigableMap.bitmapOf(1, 2, 100, 10000);
    byte[] bytes = RssUtils.serializeBitMap(bitmap1);
    Roaring64NavigableMap bitmap2 = RssUtils.deserializeBitMap(bytes);
    assertEquals(bitmap1, bitmap2);
    assertEquals(Roaring64NavigableMap.bitmapOf(), RssUtils.deserializeBitMap(new byte[] {}));
  }

  @Test
  public void testCloneBitmap() {
    Roaring64NavigableMap bitmap1 = Roaring64NavigableMap.bitmapOf(1, 2, 100, 10000);
    Roaring64NavigableMap bitmap2 = RssUtils.cloneBitMap(bitmap1);
    assertNotSame(bitmap1, bitmap2);
    assertEquals(bitmap1, bitmap2);
  }

  @Test
  public void getMetricNameForHostNameTest() {
    assertEquals("a_b_c", RssUtils.getMetricNameForHostName("a.b.c"));
    assertEquals("a_b_c", RssUtils.getMetricNameForHostName("a-b-c"));
    assertEquals("a_b_c", RssUtils.getMetricNameForHostName("a.b-c"));
  }

  @Test
  public void testLoadExtentions() {
    List<String> exts = Collections.singletonList("Dummy");
    try {
      RssUtils.loadExtensions(RssUtilTestDummy.class, exts, 1);
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().startsWith("java.lang.ClassNotFoundException: Dummy"));
    }
    exts =
        Collections.singletonList(
            "org.apache.uniffle.common.util.RssUtilsTest$RssUtilTestDummyFailNotSub");
    try {
      RssUtils.loadExtensions(RssUtilTestDummy.class, exts, 1);
    } catch (RuntimeException e) {
      assertTrue(
          e.getMessage()
              .contains(
                  "RssUtilTestDummyFailNotSub is not subclass of "
                      + "org.apache.uniffle.common.util.RssUtilsTest$RssUtilTestDummy"));
    }
    exts =
        Collections.singletonList(
            "org.apache.uniffle.common.util.RssUtilsTest$RssUtilTestDummyNoConstructor");
    try {
      RssUtils.loadExtensions(RssUtilTestDummy.class, exts, "Test");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("RssUtilTestDummyNoConstructor.<init>()"));
    }
    exts =
        Collections.singletonList(
            "org.apache.uniffle.common.util.RssUtilsTest$RssUtilTestDummySuccess");
    String testStr = String.valueOf(new Random().nextInt());
    List<RssUtilTestDummy> extsObjs =
        RssUtils.loadExtensions(RssUtilTestDummy.class, exts, testStr);
    assertEquals(1, extsObjs.size());
    assertEquals(testStr, extsObjs.get(0).get());
  }

  public static Stream<Arguments> testBlockIdLayouts() {
    return Stream.of(
        Arguments.of(BlockIdLayout.DEFAULT), Arguments.of(BlockIdLayout.from(20, 21, 22)));
  }

  @ParameterizedTest
  @MethodSource("testBlockIdLayouts")
  public void testShuffleBitmapToPartitionBitmap(BlockIdLayout layout) {
    Roaring64NavigableMap partition1Bitmap =
        Roaring64NavigableMap.bitmapOf(
            layout.getBlockId(0, 0, 0),
            layout.getBlockId(1, 0, 0),
            layout.getBlockId(0, 0, 1),
            layout.getBlockId(1, 0, 1));
    Roaring64NavigableMap partition2Bitmap =
        Roaring64NavigableMap.bitmapOf(
            layout.getBlockId(0, 1, 0),
            layout.getBlockId(1, 1, 0),
            layout.getBlockId(0, 1, 1),
            layout.getBlockId(1, 1, 1));
    Roaring64NavigableMap shuffleBitmap = Roaring64NavigableMap.bitmapOf();
    shuffleBitmap.or(partition1Bitmap);
    shuffleBitmap.or(partition2Bitmap);
    assertEquals(8, shuffleBitmap.getLongCardinality());
    Map<Integer, Roaring64NavigableMap> toPartitionBitmap =
        RssUtils.generatePartitionToBitmap(shuffleBitmap, 0, 2, layout);
    assertEquals(2, toPartitionBitmap.size());
    assertEquals(partition1Bitmap, toPartitionBitmap.get(0));
    assertEquals(partition2Bitmap, toPartitionBitmap.get(1));
  }

  @Test
  public void testGenerateServerToPartitions() {
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = Maps.newHashMap();
    ShuffleServerInfo server1 = new ShuffleServerInfo("server1", "0.0.0.1", 100);
    ShuffleServerInfo server2 = new ShuffleServerInfo("server2", "0.0.0.2", 200);
    ShuffleServerInfo server3 = new ShuffleServerInfo("server3", "0.0.0.3", 300);
    ShuffleServerInfo server4 = new ShuffleServerInfo("server4", "0.0.0.4", 400);
    partitionToServers.put(1, Lists.newArrayList(server1, server2));
    partitionToServers.put(2, Lists.newArrayList(server3, server4));
    partitionToServers.put(3, Lists.newArrayList(server1, server2));
    partitionToServers.put(4, Lists.newArrayList(server3, server4));
    Map<ShuffleServerInfo, Set<Integer>> serverToPartitions =
        RssUtils.generateServerToPartitions(partitionToServers);
    assertEquals(4, serverToPartitions.size());
    assertEquals(serverToPartitions.get(server1), Sets.newHashSet(1, 3));
    assertEquals(serverToPartitions.get(server2), Sets.newHashSet(1, 3));
    assertEquals(serverToPartitions.get(server3), Sets.newHashSet(2, 4));
    assertEquals(serverToPartitions.get(server4), Sets.newHashSet(2, 4));
  }

  @Test
  public void testSettingProperties() {
    RssConf conf = new RssConf();
    conf.set(RSS_CLIENT_EXTRA_JAVA_SYSTEM_PROPERTIES, Arrays.asList("k1=v1", "k2=v2"));

    assertNull(System.getProperty("k1"));
    assertNull(System.getProperty("k2"));
    RssUtils.setExtraJavaProperties(conf);
    assertEquals("v1", System.getProperty("k1"));
    assertEquals("v2", System.getProperty("k2"));
  }

  @Test
  public void testGetConfiguredLocalDirs() throws Exception {
    RssConf conf = new RssConf();
    withEnvironmentVariable(RssUtils.RSS_LOCAL_DIR_KEY, "/path/a")
        .execute(
            () -> {
              assertEquals(
                  Collections.singletonList("/path/a"), RssUtils.getConfiguredLocalDirs(conf));
            });

    withEnvironmentVariable(RssUtils.RSS_LOCAL_DIR_KEY, "/path/a,/path/b")
        .execute(
            () -> {
              assertEquals(
                  Arrays.asList("/path/a", "/path/b"), RssUtils.getConfiguredLocalDirs(conf));
            });

    withEnvironmentVariable(RssUtils.RSS_LOCAL_DIR_KEY, null)
        .execute(
            () -> {
              assertNull(RssUtils.getConfiguredLocalDirs(conf));
              conf.set(RssBaseConf.RSS_STORAGE_BASE_PATH, Arrays.asList("/path/a", "/path/b"));
              assertEquals(
                  Arrays.asList("/path/a", "/path/b"), RssUtils.getConfiguredLocalDirs(conf));
            });
  }

  interface RssUtilTestDummy {
    String get();
  }

  public static class RssUtilTestDummyFailNotSub {
    public RssUtilTestDummyFailNotSub() {}
  }

  public static class RssUtilTestDummyNoConstructor implements RssUtilTestDummy {
    public RssUtilTestDummyNoConstructor(int a) {}

    public String get() {
      return null;
    }
  }

  public static class RssUtilTestDummySuccess implements RssUtilTestDummy {
    private final String s;

    public RssUtilTestDummySuccess(String s) {
      this.s = s;
    }

    public String get() {
      return s;
    }
  }

  public static class MockServer implements ServerInterface {

    ServerSocket serverSocket;

    @Override
    public int start() throws IOException {
      // not implement
      return -1;
    }

    @Override
    public void startOnPort(int port) throws IOException {
      serverSocket =
          ServerSocketFactory.getDefault()
              .createServerSocket(port, 1, InetAddress.getByName("localhost"));
      new Thread(
              () -> {
                Socket accept;
                try {
                  accept = serverSocket.accept();
                  accept.close();
                } catch (IOException e) {
                  // e.printStackTrace();
                }
              })
          .start();
    }

    @Override
    public void stop() throws InterruptedException {
      if (serverSocket != null && !serverSocket.isClosed()) {
        try {
          serverSocket.close();
        } catch (IOException e) {
          // e.printStackTrace();
        }
      }
    }

    @Override
    public void blockUntilShutdown() throws InterruptedException {
      // not implement
    }
  }
}
