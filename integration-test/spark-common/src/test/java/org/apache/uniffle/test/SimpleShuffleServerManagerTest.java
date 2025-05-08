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

import org.junit.jupiter.api.Test;

import org.apache.uniffle.client.request.RssReportShuffleFetchFailureRequest;
import org.apache.uniffle.client.response.RssReportShuffleFetchFailureResponse;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.shuffle.manager.DummyRssShuffleManager;
import org.apache.uniffle.shuffle.manager.RssShuffleManagerInterface;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimpleShuffleServerManagerTest extends ShuffleServerManagerTestBase {

  @Test
  public void testClientAndServerConnections() {
    RssShuffleManagerInterface dummy = new DummyRssShuffleManager();
    RssReportShuffleFetchFailureRequest req =
        new RssReportShuffleFetchFailureRequest(dummy.getAppId(), 0, 0, 0, 0, 0, null);
    RssReportShuffleFetchFailureResponse res = client.reportShuffleFetchFailure(req);
    assertEquals(StatusCode.SUCCESS, res.getStatusCode());

    // wrong appId
    req = new RssReportShuffleFetchFailureRequest("wrongAppId", 0, 0, 0, 0, 0, null);
    res = client.reportShuffleFetchFailure(req);
    assertEquals(StatusCode.INVALID_REQUEST, res.getStatusCode());
  }
}
