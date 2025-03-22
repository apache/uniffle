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

package org.apache.spark.shuffle.reader;

import java.util.Objects;
import java.util.function.Supplier;

import scala.Product2;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;

import org.apache.spark.shuffle.FetchFailedException;
import org.apache.spark.shuffle.RssSparkShuffleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.request.RssReportShuffleFetchFailureRequest;
import org.apache.uniffle.client.response.RssReportShuffleFetchFailureResponse;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.exception.RssFetchFailedException;

public class RssFetchFailedIterator<K, C> extends AbstractIterator<Product2<K, C>> {
  private static final Logger LOG = LoggerFactory.getLogger(RssFetchFailedIterator.class);
  final Iterator<Product2<K, C>> iter;
  final Builder builder;

  private RssFetchFailedIterator(Builder builder, Iterator<Product2<K, C>> iterator) {
    this.builder = builder;
    this.iter = iterator;
  }

  public static class Builder {
    private String appId;
    private int shuffleId;
    private int partitionId;
    private int stageAttemptId;
    private Supplier<ShuffleManagerClient> managerClientSupplier;

    private Builder() {}

    Builder appId(String appId) {
      this.appId = appId;
      return this;
    }

    Builder shuffleId(int shuffleId) {
      this.shuffleId = shuffleId;
      return this;
    }

    Builder partitionId(int partitionId) {
      this.partitionId = partitionId;
      return this;
    }

    Builder stageAttemptId(int stageAttemptId) {
      this.stageAttemptId = stageAttemptId;
      return this;
    }

    Builder managerClientSupplier(Supplier<ShuffleManagerClient> managerClientSupplier) {
      this.managerClientSupplier = managerClientSupplier;
      return this;
    }

    <K, C> RssFetchFailedIterator<K, C> build(Iterator<Product2<K, C>> iter) {
      Objects.requireNonNull(this.appId);
      return new RssFetchFailedIterator<>(this, iter);
    }
  }

  static Builder newBuilder() {
    return new Builder();
  }

  private RssException generateFetchFailedIfNecessary(RssFetchFailedException e) {
    ShuffleManagerClient client = builder.managerClientSupplier.get();
    RssReportShuffleFetchFailureRequest req =
        new RssReportShuffleFetchFailureRequest(
            builder.appId,
            builder.shuffleId,
            builder.stageAttemptId,
            builder.partitionId,
            e.getMessage());
    RssReportShuffleFetchFailureResponse response = client.reportShuffleFetchFailure(req);
    if (response.getReSubmitWholeStage()) {
      // since we are going to roll out the whole stage, mapIndex shouldn't matter, hence -1 is
      // provided.
      FetchFailedException ffe =
          RssSparkShuffleUtils.createFetchFailedException(
              builder.shuffleId, -1, builder.partitionId, e);
      return new RssException(ffe);
    }
    return e;
  }

  @Override
  public boolean hasNext() {
    try {
      return this.iter.hasNext();
    } catch (RssFetchFailedException e) {
      throw generateFetchFailedIfNecessary(e);
    }
  }

  @Override
  public Product2<K, C> next() {
    try {
      return this.iter.next();
    } catch (RssFetchFailedException e) {
      throw generateFetchFailedIfNecessary(e);
    }
  }
}
