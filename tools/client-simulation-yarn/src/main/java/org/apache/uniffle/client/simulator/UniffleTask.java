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

package org.apache.uniffle.client.simulator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;

import org.apache.uniffle.client.factory.ShuffleServerClientFactory;
import org.apache.uniffle.client.impl.grpc.ShuffleServerGrpcClient;
import org.apache.uniffle.client.request.RssSendShuffleDataRequest;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.shaded.io.netty.buffer.Unpooled;

public class UniffleTask {
  private static byte[] data;
  private static AtomicLong sId = new AtomicLong(0);

  public static void main(String[] args) {
    System.out.println(
        "Start " + UniffleTask.class.getSimpleName() + " with " + Arrays.toString(args));
    if (args.length < 1) {
      System.err.println(
          "Usage: "
              + UniffleTask.class.getSimpleName()
              + " [--conf <CONFIG_FILE>] [-D<KEY>=<VALUE>]");
      System.exit(1);
    }

    Configuration conf = new Configuration();
    List<String> combinedArgs = new ArrayList<>(args.length);
    combinedArgs.addAll(Arrays.asList(args));
    combinedArgs.add("--conf");
    combinedArgs.add("./" + Constants.JOB_CONF_NAME);
    HadoopConfigApp hadoopConfigApp = new HadoopConfigApp(conf);
    hadoopConfigApp.run(combinedArgs.toArray(new String[0]));
    System.out.println(hadoopConfigApp.getLocalConf());
    String appId = conf.get(Constants.KEY_YARN_APP_ID);
    int taskIndex = conf.getInt(Constants.KEY_CONTAINER_INDEX, 0);
    String serverId = conf.get(Constants.KEY_SERVER_ID, "");
    int shuffleCount = conf.getInt(Constants.KEY_SHUFFLE_COUNT, 1);
    int partitionCount = conf.getInt(Constants.KEY_PARTITION_COUNT, 1);
    int blockCount = conf.getInt(Constants.KEY_BLOCK_COUNT, 1000);
    int blockSize = conf.getInt(Constants.KEY_BLOCK_SIZE, 1024);
    int threadCount = conf.getInt(Constants.KEY_THREAD_COUNT, 1);
    data = new byte[blockSize];
    System.out.println(taskIndex + ": start to send shuffle data to " + serverId);

    ExecutorService executorService =
        new ThreadPoolExecutor(
            threadCount,
            threadCount,
            60L,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            Executors.defaultThreadFactory(),
            new ThreadPoolExecutor.CallerRunsPolicy());

    try {
      while (true) {
        executorService.submit(
            () ->
                sendData(
                    taskIndex,
                    appId,
                    serverId,
                    shuffleCount,
                    partitionCount,
                    blockCount,
                    blockSize));
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      executorService.shutdown();
    }
    System.out.println("end to send shuffle data...");
  }

  private static void sendData(
      int taskIndex,
      String appId,
      String serverId,
      int shuffleCount,
      int partitionCount,
      int blockCount,
      int blockSize) {
    try {
      String[] parts = serverId.split("-");
      String host = parts[0];
      int port0 = Integer.parseInt(parts[1]);
      int port1 = Integer.parseInt(parts[2]);
      ShuffleServerInfo shuffleServerInfo = new ShuffleServerInfo(host, port0, port1);
      ShuffleServerGrpcClient client =
          ((ShuffleServerGrpcClient)
              ShuffleServerClientFactory.getInstance()
                  .getShuffleServerClient("GRPC_NETTY", shuffleServerInfo));
      Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = new HashMap<>();
      constructShuffleBlockInfo(
          shuffleToBlocks, taskIndex, shuffleCount, partitionCount, blockCount, blockSize);

      RssSendShuffleDataRequest sendShuffleDataRequest =
          new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
      RssSendShuffleDataResponse response = client.sendShuffleData(sendShuffleDataRequest);
      if (response != null && response.getStatusCode() != StatusCode.SUCCESS) {
        System.out.println(
            "send shuffle data error with "
                + response.getStatusCode()
                + " message: "
                + response.getMessage());
      } else {
        System.out.printf(".");
      }
    } catch (Exception e) {
      System.err.printf(
          "Error during task %d for sending shuffleCount=%d,partitionCount=%d,blockCount=%d,blockSize=%d: %s%n",
          taskIndex, shuffleCount, partitionCount, blockCount, blockSize, e.getMessage());
      e.printStackTrace();
    }
  }

  @VisibleForTesting
  protected static void constructShuffleBlockInfo(
      Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks,
      int taskIndex,
      int shuffleCount,
      int partitionCount,
      int blockCount,
      int blockSize) {
    for (int shuffleId = 0; shuffleId < shuffleCount; shuffleId++) {
      Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = new HashMap<>();
      for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
        List<ShuffleBlockInfo> blocks = partitionToBlocks.get(partitionId);
        if (blocks == null) {
          blocks = new ArrayList<>();
          partitionToBlocks.put(partitionId, blocks);
        }
        for (int blockIndex = 0; blockIndex < blockCount; blockIndex++) {
          long blockId = (((long) taskIndex + 1) << (Long.SIZE - 5)) | sId.getAndIncrement();
          blocks.add(
              new ShuffleBlockInfo(
                  shuffleId,
                  partitionId,
                  blockId,
                  blockSize,
                  0,
                  Unpooled.wrappedBuffer(data).retain(),
                  Collections.emptyList(),
                  0,
                  0,
                  0));
        }
      }
      shuffleToBlocks.put(shuffleId, partitionToBlocks);
    }
  }
}
