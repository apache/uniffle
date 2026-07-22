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

package org.apache.spark.ui

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

class ServletCompatTest {
  @Test
  def testCollapseMarkupMatchesSparkUi(): Unit = {
    val attributes =
      ServletCompat.collapseToggleAttributes("collapse-test-properties", "test-table")
        .asAttrMap
    val contentClasses = ServletCompat.collapsibleTableClass("test-table").split(" ").toSet
    val isSpark42 = org.apache.spark.SPARK_VERSION_SHORT.startsWith("4.2.")

    if (isSpark42) {
      assertEquals("collapse", attributes("data-bs-toggle"))
      assertEquals("#test-table", attributes("data-bs-target"))
      assertEquals("collapse-test-properties", attributes("data-collapse-name"))
      assertEquals("false", attributes("aria-expanded"))
      assertEquals("test-table", attributes("aria-controls"))
      assertFalse(attributes.keys.exists(_.equalsIgnoreCase("onclick")))
      assertTrue(contentClasses.contains("collapse"))
      assertFalse(contentClasses.contains("collapsed"))
    } else {
      assertTrue(attributes.keys.exists(_.equalsIgnoreCase("onclick")))
      assertFalse(attributes.contains("data-bs-toggle"))
      assertTrue(contentClasses.contains("collapsed"))
      assertFalse(contentClasses.contains("collapse"))
    }
  }
}
