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

package org.apache.uniffle.common.netty.client;

import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.netty.IOMode;

public class TransportConf {

  private final RssConf rssConf;

  public TransportConf(RssConf rssConf) {
    this.rssConf = rssConf;
  }

  public IOMode ioMode() {
    return rssConf.get(RssClientConf.NETTY_IO_MODE);
  }

  public int connectTimeoutMs() {
    return rssConf.get(RssClientConf.NETTY_IO_CONNECT_TIMEOUT_MS);
  }

  public int connectionTimeoutMs() {
    return rssConf.get(RssClientConf.NETTY_IO_CONNECTION_TIMEOUT_MS);
  }

  public int clientThreads() {
    return rssConf.get(RssClientConf.NETTY_CLIENT_THREADS);
  }

  public double clientThreadsRatio() {
    return rssConf.get(RssClientConf.NETTY_CLIENT_THREADS_RATIO);
  }

  public int numConnectionsPerPeer() {
    return rssConf.get(RssClientConf.NETTY_CLIENT_NUM_CONNECTIONS_PER_PEER);
  }

  public boolean preferDirectBufs() {
    return rssConf.get(RssClientConf.NETTY_CLIENT_PREFER_DIRECT_BUFS);
  }

  public boolean isPooledAllocatorEnabled() {
    return rssConf.get(RssClientConf.NETTY_CLIENT_POOLED_ALLOCATOR_ENABLED);
  }

  public boolean isSharedAllocatorEnabled() {
    return rssConf.get(RssClientConf.NETTY_CLIENT_SHARED_ALLOCATOR_ENABLED);
  }

  public int receiveBuf() {
    return rssConf.get(RssClientConf.NETTY_CLIENT_RECEIVE_BUFFER);
  }

  public int sendBuf() {
    return rssConf.get(RssClientConf.NETTY_CLIENT_SEND_BUFFER);
  }
}
