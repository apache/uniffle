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

package org.apache.uniffle.storage.request;

import org.apache.hadoop.conf.Configuration;

public class CreateShuffleDeleteHandlerRequest {

  private String storageType;
  private Configuration conf;
  private String shuffleServerId;
  private boolean isAsync;

  public CreateShuffleDeleteHandlerRequest(String storageType, Configuration conf) {
    this(storageType, conf, null);
  }

  public CreateShuffleDeleteHandlerRequest(
      String storageType, Configuration conf, boolean isAsync) {
    this(storageType, conf, null, isAsync);
  }

  public CreateShuffleDeleteHandlerRequest(
      String storageType, Configuration conf, String shuffleServerId) {
    this(storageType, conf, shuffleServerId, false);
  }

  public CreateShuffleDeleteHandlerRequest(
      String storageType, Configuration conf, String shuffleServerId, boolean isAsync) {
    this.storageType = storageType;
    this.conf = conf;
    this.shuffleServerId = shuffleServerId;
    this.isAsync = isAsync;
  }

  public String getStorageType() {
    return storageType;
  }

  public Configuration getConf() {
    return conf;
  }

  public String getShuffleServerId() {
    return shuffleServerId;
  }

  public boolean isAsync() {
    return isAsync;
  }
}
