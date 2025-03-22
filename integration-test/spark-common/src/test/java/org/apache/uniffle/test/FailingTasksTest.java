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

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.storage.util.StorageType;

// This test has all tasks fail twice, the third attempt succeeds.
// The failing attempts all provide zeros to the shuffle step, while the succeeding attempts
// provide the actual non-zero integers (actually only one zero). If blocks from the failing
// attempts leak into the read shuffle data, we would see those zeros and fail when comparing
// to without RSS.
public class FailingTasksTest extends SparkTaskFailureIntegrationTestBase {

  @BeforeAll
  public static void setupServers(@TempDir File tmpDir) throws Exception {
    shutdownServers();

    CoordinatorConf coordinatorConf = coordinatorConfWithoutPort();
    Map<String, String> dynamicConf = Maps.newHashMap();
    dynamicConf.put(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_PATH.key(), HDFS_URI + "rss/test");
    dynamicConf.put(
        RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE_HDFS.name());
    addDynamicConf(coordinatorConf, dynamicConf);
    storeCoordinatorConf(coordinatorConf);

    storeShuffleServerConf(shuffleServerConfWithoutPort(0, tmpDir, ServerType.GRPC));
    storeShuffleServerConf(shuffleServerConfWithoutPort(1, tmpDir, ServerType.GRPC_NETTY));

    startServersWithRandomPorts();
  }

  @Override
  Map runTest(SparkSession spark, String fileName) throws Exception {
    int n = 1000000;
    return spark.range(0, n, 1, 4)
        .mapPartitions(
            (MapPartitionsFunction<Long, Long>)
                it ->
                    new Iterator<Long>() {
                      final TaskContext context = TaskContext.get();

                      @Override
                      public boolean hasNext() {
                        // the first two attempts fail in the end
                        return context.attemptNumber() < 2 || it.hasNext();
                      }

                      @Override
                      public Long next() {
                        if (it.hasNext()) {
                          Long next = it.next();
                          // the failing attempt returns only zeros
                          if (context.attemptNumber() < 2) {
                            return 0L;
                          } else {
                            return next;
                          }
                        } else {
                          throw new RuntimeException("let this task fail");
                        }
                      }
                    },
            Encoders.LONG())
        .repartition(3, new Column("value"))
        .mapPartitions((MapPartitionsFunction<Long, Long>) it -> it, Encoders.LONG())
        .collectAsList().stream()
        .collect(Collectors.toMap(v -> v, v -> v));
  }

  @Test
  public void testFailedTasks() throws Exception {
    run();
  }
}
