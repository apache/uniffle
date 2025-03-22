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

package org.apache.uniffle.common.storage;

import java.util.concurrent.atomic.AtomicLong;

public class ApplicationStorageInfo {
  private String appId;
  private AtomicLong fileNum;
  private AtomicLong usedBytes;

  public ApplicationStorageInfo(String appId) {
    this.appId = appId;
    this.fileNum = new AtomicLong();
    this.usedBytes = new AtomicLong();
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public long getFileNum() {
    return fileNum.get();
  }

  public void incFileNum(long fileNum) {
    this.fileNum.addAndGet(fileNum);
  }

  public long getUsedBytes() {
    return usedBytes.get();
  }

  public void incUsedBytes(long usedBytes) {
    this.usedBytes.addAndGet(usedBytes);
  }
}
