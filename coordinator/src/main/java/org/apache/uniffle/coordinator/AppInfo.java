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

package org.apache.uniffle.coordinator;

import java.util.Objects;

public class AppInfo implements Comparable<AppInfo> {
  private String appId;
  private long updateTime;
  private long registrationTime;
  private String version;
  private String gitCommitId;

  public AppInfo(
      String appId, long updateTime, long registrationTime, String version, String gitCommitId) {
    this.appId = appId;
    this.updateTime = updateTime;
    this.registrationTime = registrationTime;
    this.version = version;
    this.gitCommitId = gitCommitId;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public long getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(long updateTime) {
    this.updateTime = updateTime;
  }

  public long getRegistrationTime() {
    return registrationTime;
  }

  public void setRegistrationTime(long registrationTime) {
    this.registrationTime = registrationTime;
  }

  public String getVersion() {
    return version;
  }

  public String getGitCommitId() {
    return gitCommitId;
  }

  @Override
  public int compareTo(AppInfo appInfo) {
    return Long.compare(registrationTime, appInfo.getRegistrationTime());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AppInfo)) {
      return false;
    }
    AppInfo appInfo = (AppInfo) o;
    return updateTime == appInfo.updateTime
        && registrationTime == appInfo.registrationTime
        && appId.equals(appInfo.appId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(appId, updateTime, registrationTime);
  }

  public static AppInfo createAppInfo(String appId, long updateTime) {
    return createAppInfo(appId, updateTime, "", "");
  }

  public static AppInfo createAppInfo(
      String appId, long updateTime, String version, String gitCommitId) {
    return new AppInfo(appId, updateTime, updateTime, version, gitCommitId);
  }
}
