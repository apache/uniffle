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

package org.apache.uniffle.common;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.config.RssConf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests enum type {@link ReconfigurableRegistry}. */
public class ReconfigurableRegistryTest {

  @BeforeEach
  public void before() {
    ReconfigurableRegistry.clear();
  }

  @Test
  public void testUpdate() {
    ReconfigurableBad bad = new ReconfigurableBad();
    ReconfigurableGood good0 = new ReconfigurableGood();
    ReconfigurableGood good1 = new ReconfigurableGood();
    try {
      ReconfigurableRegistry.register(good0);
      ReconfigurableRegistry.register(bad);
      ReconfigurableRegistry.register(good1);
      ReconfigurableRegistry.update(null, Collections.singleton("key"));
      assertEquals(1, good0.mInvokeCount);
      assertEquals(1, good1.mInvokeCount);
      ReconfigurableRegistry.update(null, Collections.singleton("key"));
      assertEquals(2, good0.mInvokeCount);
      assertEquals(2, good1.mInvokeCount);
      // remove bad guy
      ReconfigurableRegistry.unregister(bad);
      ReconfigurableRegistry.update(null, Collections.singleton("key"));
      assertEquals(3, good0.mInvokeCount);
      assertEquals(3, good1.mInvokeCount);
      ReconfigurableRegistry.update(null, Collections.singleton("key"));
      assertEquals(4, good0.mInvokeCount);
      assertEquals(4, good1.mInvokeCount);
    } finally {
      assertTrue(!ReconfigurableRegistry.unregister(bad));
      assertTrue(ReconfigurableRegistry.unregister(good0));
      assertTrue(ReconfigurableRegistry.unregister(good1));
      assertEquals(0, ReconfigurableRegistry.getSize());
    }
  }

  @Test
  public void testUpdateSpecificKey() {
    ReconfigurableBad bad = new ReconfigurableBad();
    ReconfigurableGood good0 = new ReconfigurableGood();
    ReconfigurableGood good1 = new ReconfigurableGood();
    ReconfigurableGood good1Follow = new ReconfigurableGood();
    ReconfigurableGood goodAny = new ReconfigurableGood();
    ReconfigurableGood good2 = new ReconfigurableGood();
    ReconfigurableGood good01 = new ReconfigurableGood();
    try {
      ReconfigurableRegistry.register("key0", good0);
      ReconfigurableRegistry.register(bad);
      ReconfigurableRegistry.register("key1", good1);
      ReconfigurableRegistry.register("key1", good1Follow);
      ReconfigurableRegistry.register(goodAny);
      ReconfigurableRegistry.register("key2", good2);
      ReconfigurableRegistry.register(Sets.newHashSet("key0", "key1"), good01);
      ReconfigurableRegistry.update(null, Collections.singleton("key"));

      assertEquals(0, good0.mInvokeCount);
      assertEquals(0, good1.mInvokeCount);
      assertEquals(0, good1Follow.mInvokeCount);
      assertEquals(1, goodAny.mInvokeCount);
      assertEquals(0, good2.mInvokeCount);
      assertEquals(0, good01.mInvokeCount);

      ReconfigurableRegistry.update(null, Collections.singleton("key1"));

      assertEquals(0, good0.mInvokeCount);
      assertEquals(1, good1.mInvokeCount);
      assertEquals(1, good1Follow.mInvokeCount);
      assertEquals(2, goodAny.mInvokeCount);
      assertEquals(0, good2.mInvokeCount);
      assertEquals(1, good01.mInvokeCount);

      Set<String> changedProperties = new HashSet<>();
      changedProperties.add("key0");
      changedProperties.add("key1");
      changedProperties.add("key2");
      ReconfigurableRegistry.update(null, changedProperties);

      assertEquals(1, good0.mInvokeCount);
      assertEquals(2, good1.mInvokeCount);
      assertEquals(2, good1Follow.mInvokeCount);
      assertEquals(5, goodAny.mInvokeCount);
      assertEquals(1, good2.mInvokeCount);
      assertEquals(3, good01.mInvokeCount);
    } finally {
      assertTrue(ReconfigurableRegistry.unregister(bad));
      assertTrue(ReconfigurableRegistry.unregister(Sets.newHashSet("key0"), good0));
      assertTrue(ReconfigurableRegistry.unregister(Sets.newHashSet("key1")));
      assertTrue(ReconfigurableRegistry.unregister(goodAny));
      assertTrue(ReconfigurableRegistry.unregister("key2", good2));
      assertTrue(ReconfigurableRegistry.unregister(Sets.newHashSet("key0", "key1"), good01));
      assertEquals(0, ReconfigurableRegistry.getSize());
    }
  }

  class ReconfigurableBad implements ReconfigurableRegistry.ReconfigureListener {
    @Override
    public void update(RssConf conf, Set<String> changedProperties) {
      throw new RuntimeException("I am bad guy");
    }
  }

  class ReconfigurableGood implements ReconfigurableRegistry.ReconfigureListener {
    int mInvokeCount = 0;

    @Override
    public void update(RssConf rssConf, Set<String> changedProperties) {
      mInvokeCount += changedProperties.size();
    }
  }
}
