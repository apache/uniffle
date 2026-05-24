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

package org.apache.spark.shuffle.compat;

import scala.Option;
import scala.math.Ordering;

import org.apache.spark.Aggregator;
import org.apache.spark.Partitioner;
import org.apache.spark.TaskContext;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.util.collection.ExternalSorter;

/**
 * Compatibility shim for Spark 4.0.x. Selected by the build via the
 * {@code src/main/java-spark4_0} source root under the {@code spark4} profile.
 *
 * <p>Mirror of {@code java-spark4_1/.../Spark4Compat.java}; keep the public surface in lock-step.
 */
public final class Spark4Compat {

  private Spark4Compat() {}

  public static MapStatus mapStatus(
      BlockManagerId loc, long[] uncompressedSizes, long mapTaskId) {
    return MapStatus.apply(loc, uncompressedSizes, mapTaskId);
  }

  public static <K, V, C> ExternalSorter<K, V, C> newExternalSorter(
      TaskContext context,
      Option<Aggregator<K, V, C>> aggregator,
      Option<Partitioner> partitioner,
      Option<Ordering<K>> ordering,
      Serializer serializer) {
    return new ExternalSorter<>(context, aggregator, partitioner, ordering, serializer);
  }
}
