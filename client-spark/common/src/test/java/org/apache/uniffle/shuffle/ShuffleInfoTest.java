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

package org.apache.uniffle.shuffle;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShuffleInfoTest {

  @Test
  public void testEmptyValidationInfo() {
    ShuffleInfo shuffleInfo = new ShuffleInfo(Optional.empty(), 1);
    byte[] encode = shuffleInfo.encode();
    ShuffleInfo extract = shuffleInfo.decode(encode);
    assertFalse(extract.getValidationInfo().isPresent());
    assertEquals(1, shuffleInfo.getTaskAttemptId());
  }

  @Test
  public void testValidValidationInfo() {
    ShuffleValidationInfo validationInfo = new ShuffleValidationInfo(2);
    validationInfo.incPartitionRecord(0);
    validationInfo.incPartitionRecord(1);

    ShuffleInfo shuffleInfo = new ShuffleInfo(Optional.of(validationInfo), 1);
    byte[] encode = shuffleInfo.encode();
    ShuffleInfo extract = shuffleInfo.decode(encode);
    assertTrue(extract.getValidationInfo().isPresent());
    assertEquals(1, shuffleInfo.getTaskAttemptId());

    ShuffleValidationInfo extractValidationInfo = extract.getValidationInfo().get();
    assertEquals(extractValidationInfo.getRecordsWritten(0), 1);
    assertEquals(extractValidationInfo.getRecordsWritten(1), 1);
  }
}
