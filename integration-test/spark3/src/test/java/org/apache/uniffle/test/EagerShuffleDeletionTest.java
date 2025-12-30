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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import scala.Tuple2;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.UniffleStageDependencyListener;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.shuffle.ShuffleManager;
import org.apache.spark.sql.SparkSession;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.shuffle.manager.RssShuffleManagerBase;
import org.apache.uniffle.shuffle.manager.StageDependencyTracker;

public class EagerShuffleDeletionTest extends SimpleTestBase {

  @Test
  public void deleteUnusedShuffle() throws Exception {
    run();
  }

  @Override
  public void updateSparkConfCustomer(SparkConf sparkConf) {
    super.updateSparkConfCustomer(sparkConf);
    sparkConf.set("spark." + RssSparkConfig.RSS_EAGER_SHUFFLE_DELETION_ENABLED.key(), "true");
    sparkConf.set("spark." + RssClientConf.RSS_CLIENT_REASSIGN_ENABLED.key(), "true");
  }

  @Override
  Map runTest(SparkSession spark, String fileName) throws Exception {
    spark.sparkContext().addSparkListener(new UniffleStageDependencyListener());

    // take a rest to make sure shuffle server is registered
    Thread.sleep(3000);

    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    JavaPairRDD<String, String> rdd1 =
        jsc.parallelizePairs(
                Lists.newArrayList(
                    new Tuple2<>("a", "1"), new Tuple2<>("b", "2"),
                    new Tuple2<>("c", "3"), new Tuple2<>("d", "4")),
                3)
            .repartition(2);
    JavaPairRDD<String, String> rdd2 = rdd1.repartition(4);
    JavaPairRDD<String, String> rdd3 = rdd1.repartition(4);

    final AtomicBoolean isRssEnabled = new AtomicBoolean(false);
    ShuffleManager shuffleManager = SparkEnv.get().shuffleManager();
    if (shuffleManager instanceof RssShuffleManagerBase) {
      isRssEnabled.set(true);
    }
    final Map<Long, Long> result = new HashMap<>();
    Thread t =
        new Thread(
            () -> {
              long count =
                  rdd3.union(rdd2)
                      .mapPartitions(
                          (FlatMapFunction<
                                  Iterator<Tuple2<String, String>>, Tuple2<String, String>>)
                              tuple2Iterator -> {
                                if (isRssEnabled.get()) {
                                  Thread.sleep(10 * 1000);
                                }
                                return tuple2Iterator;
                              })
                      .count();
              result.put(1L, count);
            });
    t.start();

    if (isRssEnabled.get()) {
      Optional<StageDependencyTracker> tracker =
          ((RssShuffleManagerBase) shuffleManager).getStageDependencyTracker();
      if (!tracker.isPresent()) {
        throw new RuntimeException("No tracker found");
      }
      try {
        Awaitility.await()
            .timeout(5, TimeUnit.SECONDS)
            .until(
                () ->
                    tracker.get().getActiveShuffleCount() == 2
                        && tracker.get().getCleanedShuffleCount() == 1);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    t.join();

    return result;
  }
}
