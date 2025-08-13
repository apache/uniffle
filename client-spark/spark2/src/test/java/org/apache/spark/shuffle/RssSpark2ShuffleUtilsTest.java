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

package org.apache.spark.shuffle;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.exception.RssFetchFailedException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RssSpark2ShuffleUtilsTest {

  @Test
  public void testGetActiveSparkContext() {
    assertThrows(RssException.class, RssSparkShuffleUtils::getActiveSparkContext);
    SparkConf conf = new SparkConf();
    conf.setMaster("local[1]");
    conf.setAppName("test");
    SparkContext sc = null;
    try {
      sc = SparkContext.getOrCreate(conf);
      assertEquals(sc, RssSparkShuffleUtils.getActiveSparkContext());
    } finally {
      if (sc != null) {
        sc.stop();
      }
    }
  }

  @Test
  public void testCreateFetchFailedException() {
    FetchFailedException ffe = RssSparkShuffleUtils.createFetchFailedException(0, -1, 10, null);
    ffe =
        RssSparkShuffleUtils.createFetchFailedException(
            0, -1, 100, new RssFetchFailedException("xx"));
  }

  @Test
  public void testIsStageResubmitSupported() {
    // by default spark 2.3/2.4 is used, which supports stage resubmit.
    assertTrue(RssSparkShuffleUtils.isStageResubmitSupported());
  }
}
