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

package org.apache.uniffle.test;

import org.apache.spark.SparkConf;

import org.apache.uniffle.server.MockedGrpcServer;
import org.apache.uniffle.server.ShuffleServer;
import org.apache.uniffle.server.buffer.ShuffleBufferManager;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_PARTITION_REASSIGN_BLOCK_RETRY_MAX_TIMES;
import static org.apache.uniffle.client.util.RssClientConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER;
import static org.apache.uniffle.client.util.RssClientConfig.RSS_CLIENT_RETRY_MAX;
import static org.apache.uniffle.client.util.RssClientConfig.RSS_RESUBMIT_STAGE;
import static org.apache.uniffle.common.config.RssClientConf.RSS_CLIENT_REASSIGN_ENABLED;

/**
 * This class is to test the compatibility of reassign and stage retry mechanism that were enabled
 * at the same time.
 */
public class ReassignAndStageRetryTest extends PartitionBlockDataReassignMultiTimesTest {

  @Override
  public void updateSparkConfCustomer(SparkConf sparkConf) {
    sparkConf.set("spark.task.maxFailures", String.valueOf(1));
    sparkConf.set("spark." + RSS_RESUBMIT_STAGE, "true");

    sparkConf.set("spark.sql.shuffle.partitions", "4");
    sparkConf.set("spark." + RSS_CLIENT_RETRY_MAX, "2");
    sparkConf.set("spark." + RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER, "1");
    sparkConf.set("spark." + RSS_CLIENT_REASSIGN_ENABLED.key(), "true");
    sparkConf.set("spark." + RSS_PARTITION_REASSIGN_BLOCK_RETRY_MAX_TIMES.key(), "1");

    // simulate the grpc servers has different free memory
    // and make the assign priority seq: g1 -> g2 -> g3
    ShuffleServer g1 = grpcShuffleServers.get(0);
    ShuffleBufferManager bufferManager = g1.getShuffleBufferManager();
    bufferManager.setUsedMemory(bufferManager.getCapacity() - 3000000);
    g1.sendHeartbeat();

    ShuffleServer g2 = grpcShuffleServers.get(1);
    bufferManager = g2.getShuffleBufferManager();
    bufferManager.setUsedMemory(bufferManager.getCapacity() - 2000000);
    g2.sendHeartbeat();

    ShuffleServer g3 = grpcShuffleServers.get(2);
    bufferManager = g3.getShuffleBufferManager();
    bufferManager.setUsedMemory(bufferManager.getCapacity() - 1000000);
    g3.sendHeartbeat();

    // This will make the partition of g1 reassign to g2 servers.
    ((MockedGrpcServer) g1.getServer()).getService().setMockSendDataFailedStageNumber(0);
    // And then reassign to g3. But reassign max times reaches due to max reassign times.
    ((MockedGrpcServer) g2.getServer()).getService().setMockSendDataFailedStageNumber(0);
  }
}
