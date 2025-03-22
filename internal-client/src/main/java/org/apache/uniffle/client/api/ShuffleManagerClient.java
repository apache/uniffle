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

package org.apache.uniffle.client.api;

import org.apache.uniffle.client.request.RssGetShuffleResultForMultiPartRequest;
import org.apache.uniffle.client.request.RssGetShuffleResultRequest;
import org.apache.uniffle.client.request.RssPartitionToShuffleServerRequest;
import org.apache.uniffle.client.request.RssReassignOnBlockSendFailureRequest;
import org.apache.uniffle.client.request.RssReportShuffleFetchFailureRequest;
import org.apache.uniffle.client.request.RssReportShuffleResultRequest;
import org.apache.uniffle.client.request.RssReportShuffleWriteFailureRequest;
import org.apache.uniffle.client.response.RssGetShuffleResultResponse;
import org.apache.uniffle.client.response.RssReassignOnBlockSendFailureResponse;
import org.apache.uniffle.client.response.RssReassignOnStageRetryResponse;
import org.apache.uniffle.client.response.RssReportShuffleFetchFailureResponse;
import org.apache.uniffle.client.response.RssReportShuffleResultResponse;
import org.apache.uniffle.client.response.RssReportShuffleWriteFailureResponse;
import org.apache.uniffle.common.util.StatefulCloseable;

public interface ShuffleManagerClient extends StatefulCloseable {
  RssReportShuffleFetchFailureResponse reportShuffleFetchFailure(
      RssReportShuffleFetchFailureRequest request);

  /**
   * In Stage Retry mode,Gets the mapping between partitions and ShuffleServer from the
   * ShuffleManager server.
   *
   * @param req request
   * @return RssPartitionToShuffleServerResponse
   */
  RssReassignOnStageRetryResponse getPartitionToShufflerServerWithStageRetry(
      RssPartitionToShuffleServerRequest req);

  /**
   * In Block Retry mode,Gets the mapping between partitions and ShuffleServer from the
   * ShuffleManager server.
   *
   * @param req request
   * @return RssPartitionToShuffleServerResponse
   */
  RssReassignOnBlockSendFailureResponse getPartitionToShufflerServerWithBlockRetry(
      RssPartitionToShuffleServerRequest req);

  RssReportShuffleWriteFailureResponse reportShuffleWriteFailure(
      RssReportShuffleWriteFailureRequest req);

  RssReassignOnBlockSendFailureResponse reassignOnBlockSendFailure(
      RssReassignOnBlockSendFailureRequest request);

  RssGetShuffleResultResponse getShuffleResult(RssGetShuffleResultRequest request);

  RssGetShuffleResultResponse getShuffleResultForMultiPart(
      RssGetShuffleResultForMultiPartRequest request);

  RssReportShuffleResultResponse reportShuffleResult(RssReportShuffleResultRequest request);
}
