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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.ShuffleServerMetrics;

import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MERGE_THREAD_ALIVE_TIME;
import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MERGE_THREAD_POOL_QUEUE_SIZE;
import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MERGE_THREAD_POOL_SIZE;

public class DefaultMergeEventHandler implements MergeEventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMergeEventHandler.class);

  private Executor threadPoolExecutor;
  protected final BlockingQueue<MergeEvent> queue = Queues.newLinkedBlockingQueue();
  private Consumer<MergeEvent> eventConsumer;
  private volatile boolean stopped = false;

  public DefaultMergeEventHandler(
      ShuffleServerConf serverConf, Consumer<MergeEvent> eventConsumer) {
    this.eventConsumer = eventConsumer;
    int poolSize = serverConf.get(SERVER_MERGE_THREAD_POOL_SIZE);
    int queueSize = serverConf.get(SERVER_MERGE_THREAD_POOL_QUEUE_SIZE);
    int keepAliveTime = serverConf.get(SERVER_MERGE_THREAD_ALIVE_TIME);
    BlockingQueue<Runnable> waitQueue = Queues.newLinkedBlockingQueue(queueSize);
    threadPoolExecutor =
        new ThreadPoolExecutor(
            poolSize,
            poolSize,
            keepAliveTime,
            TimeUnit.SECONDS,
            waitQueue,
            ThreadUtils.getThreadFactory("DefaultMergeEventHandler"));
    startEventProcessor();
  }

  private void startEventProcessor() {
    Thread processEventThread = new Thread(this::eventLoop);
    processEventThread.setName("ProcessEventThread");
    processEventThread.setDaemon(true);
    processEventThread.start();
  }

  protected void eventLoop() {
    while (!stopped && !Thread.currentThread().isInterrupted()) {
      processNextEvent();
    }
  }

  protected void processNextEvent() {
    try {
      MergeEvent event = queue.take();
      threadPoolExecutor.execute(() -> handleEventAndUpdateMetrics(event));
    } catch (Exception e) {
      LOG.error("Exception happened when process event.", e);
    }
  }

  private void handleEventAndUpdateMetrics(MergeEvent event) {
    try {
      eventConsumer.accept(event);
    } finally {
      ShuffleServerMetrics.gaugeMergeEventQueueSize.dec();
    }
  }

  @Override
  public boolean handle(MergeEvent event) {
    if (queue.offer(event)) {
      ShuffleServerMetrics.gaugeMergeEventQueueSize.inc();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int getEventNumInMerge() {
    return queue.size();
  }

  @Override
  public void stop() {
    stopped = true;
  }
}
