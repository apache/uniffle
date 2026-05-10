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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

import org.apache.uniffle.common.KerberizedHadoopBase;

@DisabledIfSystemProperty(
    named = "uniffle.test.skip.kerberos",
    matches = "true",
    disabledReason = "MiniKdc is not compatible with JDK 17+")
public class DynamicClientConfServiceKerberlizedHadoopTest extends KerberizedHadoopBase {

  @BeforeAll
  public static void beforeAll() throws Exception {
    testRunner = DynamicClientConfServiceKerberlizedHadoopTest.class;
    KerberizedHadoopBase.init();
  }

  @Test
  public void testConfInHadoop() throws Exception {
    String cfgFile = kerberizedHadoop.getSchemeAndAuthorityPrefix() + "/test/client_conf";
    DynamicClientConfServiceHadoopTest.createAndRunCases(
        kerberizedHadoop.getSchemeAndAuthorityPrefix(),
        cfgFile,
        kerberizedHadoop.getFileSystem(),
        kerberizedHadoop.getConf());
  }
}
