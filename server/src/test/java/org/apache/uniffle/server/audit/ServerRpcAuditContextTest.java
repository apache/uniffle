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

package org.apache.uniffle.server.audit;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The class is to test the appId handling of {@link ServerRpcAuditContext}. */
public class ServerRpcAuditContextTest {

  @Test
  public void testControlCharactersOfAppIdAreReplaced() {
    // Besides the line break and the tab, the C1 controls are covered as well: NEL is a line end
    // for some readers of the log, and CSI starts an escape sequence in a terminal
    String appId = "a\r\nb\tc" + (char) 0x85 + "d" + (char) 0x9B + "e";

    String line = new ServerRpcAuditContext(null).withAppId(appId).withShuffleId(1).toString();

    assertTrue(line.endsWith("appId=a??b?c?d?e\tshuffleId=1"), line);
    assertFalse(line.contains("\r"), line);
    assertFalse(line.contains("\n"), line);
    assertFalse(line.contains("" + (char) 0x85), line);
    assertFalse(line.contains("" + (char) 0x9B), line);
  }

  @Test
  public void testAppIdWithoutControlCharacterIsRecordedAsItIs() {
    String line = new ServerRpcAuditContext(null).withAppId("application_1_2").toString();

    assertTrue(line.endsWith("appId=application_1_2\tshuffleId=-1"), line);
  }

  @Test
  public void testEveryIsoControlCharacterIsReplaced() {
    // the same characters are refused as an appId, so none of them can forge a record
    for (int c = Character.MIN_VALUE; c <= Character.MAX_VALUE; c++) {
      char character = (char) c;
      String line = new ServerRpcAuditContext(null).withAppId("a" + character + "b").toString();
      String expected =
          "appId=a" + (Character.isISOControl(character) ? "?" : character) + "b\tshuffleId=-1";
      int code = c;
      assertTrue(
          line.endsWith(expected), () -> "char U+" + Integer.toHexString(code) + ": " + line);
    }
  }
}
