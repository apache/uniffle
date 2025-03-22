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

import java.io.Closeable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.Application;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.coordinator.access.checker.AccessQuotaChecker;
import org.apache.uniffle.coordinator.conf.ClientConf;
import org.apache.uniffle.coordinator.conf.LegacyClientConfParser;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;
import org.apache.uniffle.coordinator.strategy.storage.AppBalanceSelectStorageStrategy;
import org.apache.uniffle.coordinator.strategy.storage.LowestIOSampleCostSelectStorageStrategy;
import org.apache.uniffle.coordinator.strategy.storage.RankValue;
import org.apache.uniffle.coordinator.strategy.storage.SelectStorageStrategy;

public class ApplicationManager implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationManager.class);
  // TODO: Add anomaly detection for other storage
  private static final List<String> REMOTE_PATH_SCHEMA = Arrays.asList("hdfs");
  private final long expired;
  private final StrategyName storageStrategy;
  private final SelectStorageStrategy selectStorageStrategy;
  // store appId -> remote path to make sure all shuffle data of the same application
  // will be written to the same remote storage
  private final Map<String, RemoteStorageInfo> appIdToRemoteStorageInfo;
  // store remote path -> application count for assignment strategy
  private final Map<String, RankValue> remoteStoragePathRankValue;
  private final Map<String, String> remoteStorageToHost = JavaUtils.newConcurrentMap();
  private final Map<String, RemoteStorageInfo> availableRemoteStorageInfo;
  private final ScheduledExecutorService detectStorageScheduler;
  private final ScheduledExecutorService checkAppScheduler;
  private Map<String, Map<String, AppInfo>> currentUserAndApp = JavaUtils.newConcurrentMap();
  private Map<String, String> appIdToUser = JavaUtils.newConcurrentMap();
  private QuotaManager quotaManager;
  // it's only for test case to check if status check has problem
  private boolean hasErrorInStatusCheck = false;

  public ApplicationManager(CoordinatorConf conf) {
    storageStrategy = conf.get(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_SELECT_STRATEGY);
    appIdToRemoteStorageInfo = JavaUtils.newConcurrentMap();
    remoteStoragePathRankValue = JavaUtils.newConcurrentMap();
    availableRemoteStorageInfo = JavaUtils.newConcurrentMap();
    if (StrategyName.IO_SAMPLE == storageStrategy) {
      selectStorageStrategy =
          new LowestIOSampleCostSelectStorageStrategy(
              remoteStoragePathRankValue,
              appIdToRemoteStorageInfo,
              availableRemoteStorageInfo,
              conf);
    } else if (StrategyName.APP_BALANCE == storageStrategy) {
      selectStorageStrategy =
          new AppBalanceSelectStorageStrategy(
              remoteStoragePathRankValue,
              appIdToRemoteStorageInfo,
              availableRemoteStorageInfo,
              conf);
    } else {
      throw new UnsupportedOperationException("Unsupported selected storage strategy.");
    }
    expired = conf.getLong(CoordinatorConf.COORDINATOR_APP_EXPIRED);
    String quotaCheckerClass = AccessQuotaChecker.class.getCanonicalName();
    for (String checker : conf.get(CoordinatorConf.COORDINATOR_ACCESS_CHECKERS)) {
      if (quotaCheckerClass.equals(checker.trim())) {
        this.quotaManager = new QuotaManager(conf);
        this.currentUserAndApp = quotaManager.getCurrentUserAndApp();
        this.appIdToUser = quotaManager.getAppIdToUser();
        break;
      }
    }
    // the thread for checking application status
    checkAppScheduler = ThreadUtils.getDaemonSingleThreadScheduledExecutor("ApplicationManager");
    checkAppScheduler.scheduleAtFixedRate(
        this::statusCheck, expired / 2, expired / 2, TimeUnit.MILLISECONDS);
    // the thread for checking if the storage is normal
    detectStorageScheduler =
        ThreadUtils.getDaemonSingleThreadScheduledExecutor("detectStoragesScheduler");
    // should init later than the refreshRemoteStorage init
    detectStorageScheduler.scheduleAtFixedRate(
        selectStorageStrategy::detectStorage,
        1000,
        conf.getLong(CoordinatorConf.COORDINATOR_REMOTE_STORAGE_SCHEDULE_TIME),
        TimeUnit.MILLISECONDS);
  }

  public void registerApplicationInfo(String appId, String user) {
    registerApplicationInfo(appId, user, "", "");
  }

  public void registerApplicationInfo(
      String appId, String user, String version, String gitCommitId) {
    // using computeIfAbsent is just for MR and spark which is used RssShuffleManager as
    // implementation class
    // in such case by default, there is no currentUserAndApp, so a unified user implementation
    // named "user" is used.
    Map<String, AppInfo> appAndTime =
        currentUserAndApp.computeIfAbsent(user, x -> JavaUtils.newConcurrentMap());
    appIdToUser.put(appId, user);
    if (!appAndTime.containsKey(appId)) {
      CoordinatorMetrics.counterTotalAppNum.inc();
      LOG.info("New application is registered: {}", appId);
    }
    AppInfo appInfo =
        AppInfo.createAppInfo(appId, System.currentTimeMillis(), version, gitCommitId);
    if (quotaManager != null) {
      quotaManager.registerApplicationInfo(appId, appAndTime, version, gitCommitId);
    } else {
      appAndTime.put(appId, appInfo);
    }
  }

  public void refreshAppId(String appId) {
    String user = appIdToUser.get(appId);
    // compatible with lower version clients
    if (user == null) {
      registerApplicationInfo(appId, "", "", "");
    } else {
      Map<String, AppInfo> appAndTime = currentUserAndApp.get(user);
      AppInfo appInfo = appAndTime.get(appId);
      long currentTimeMs = System.currentTimeMillis();
      if (appInfo != null) {
        appInfo.setUpdateTime(currentTimeMs);
      } else {
        appInfo = new AppInfo(appId, currentTimeMs, currentTimeMs, "", "");
        appAndTime.put(appId, appInfo);
      }
    }
  }

  public void refreshRemoteStorages(ClientConf dynamicClientConf) {
    if (dynamicClientConf == null || MapUtils.isEmpty(dynamicClientConf.getRemoteStorageInfos())) {
      LOG.info("Refresh remote storage with empty config");
      for (String path : availableRemoteStorageInfo.keySet()) {
        removePathFromCounter(path);
      }
      availableRemoteStorageInfo.clear();
      return;
    }

    Map<String, RemoteStorageInfo> remoteStorageInfoMap = dynamicClientConf.getRemoteStorageInfos();
    LOG.info("Refresh remote storage with {}", remoteStorageInfoMap);

    for (Map.Entry<String, RemoteStorageInfo> entry : remoteStorageInfoMap.entrySet()) {
      String path = entry.getKey();
      RemoteStorageInfo rsInfo = entry.getValue();

      if (!availableRemoteStorageInfo.containsKey(path)) {
        remoteStoragePathRankValue.computeIfAbsent(
            path,
            key -> {
              // refreshRemoteStorage is designed without multiple thread problem
              // metrics shouldn't be added duplicated
              addRemoteStorageMetrics(path);
              return new RankValue(0);
            });
      }
      availableRemoteStorageInfo.put(path, rsInfo);
    }
    // remove unused remote path if exist
    List<String> unusedPath = Lists.newArrayList();
    for (String existPath : availableRemoteStorageInfo.keySet()) {
      if (!remoteStorageInfoMap.containsKey(existPath)) {
        unusedPath.add(existPath);
      }
    }
    // remote unused path
    for (String path : unusedPath) {
      availableRemoteStorageInfo.remove(path);
      // try to remove if counter = 0, or it will be removed in decRemoteStorageCounter() later
      removePathFromCounter(path);
    }
  }

  // only for test
  @VisibleForTesting
  public void refreshRemoteStorage(String remoteStoragePath, String remoteStorageConf) {
    try {
      LegacyClientConfParser parser = new LegacyClientConfParser();

      String remoteStorageConfRaw =
          String.format(
              "%s %s %n %s %s",
              CoordinatorConf.COORDINATOR_REMOTE_STORAGE_PATH.key(),
              remoteStoragePath,
              CoordinatorConf.COORDINATOR_REMOTE_STORAGE_CLUSTER_CONF.key(),
              remoteStorageConf);

      ClientConf conf =
          parser.tryParse(IOUtils.toInputStream(remoteStorageConfRaw, StandardCharsets.UTF_8));
      refreshRemoteStorages(conf);
    } catch (Exception e) {
      LOG.error("Errors on refreshing remote storage", e);
    }
  }

  // the strategy of pick remote storage is according to assignment count
  // todo: better strategy with workload balance
  public RemoteStorageInfo pickRemoteStorage(String appId) {
    if (appIdToRemoteStorageInfo.containsKey(appId)) {
      return appIdToRemoteStorageInfo.get(appId);
    }
    RemoteStorageInfo pickStorage = selectStorageStrategy.pickStorage(appId);
    incRemoteStorageCounter(pickStorage.getPath());
    return appIdToRemoteStorageInfo.get(appId);
  }

  @VisibleForTesting
  public synchronized void incRemoteStorageCounter(String remoteStoragePath) {
    RankValue counter = remoteStoragePathRankValue.get(remoteStoragePath);
    if (counter != null) {
      counter.getAppNum().incrementAndGet();
    } else {
      // it may be happened when assignment remote storage
      // and refresh remote storage at the same time
      LOG.warn(
          "Remote storage path lost during assignment: {} doesn't exist, reset it to 1",
          remoteStoragePath);
      remoteStoragePathRankValue.put(remoteStoragePath, new RankValue(1));
    }
  }

  @VisibleForTesting
  public synchronized void decRemoteStorageCounter(String storagePath) {
    if (!StringUtils.isEmpty(storagePath)) {
      RankValue atomic = remoteStoragePathRankValue.get(storagePath);
      if (atomic != null) {
        double count = atomic.getAppNum().decrementAndGet();
        if (count < 0) {
          LOG.warn(
              "Unexpected counter for remote storage: {}, which is {}, reset to 0",
              storagePath,
              count);
          atomic.getAppNum().set(0);
        }
      } else {
        LOG.warn("Can't find counter for remote storage: {}", storagePath);
        remoteStoragePathRankValue.putIfAbsent(storagePath, new RankValue(0));
      }
      if (remoteStoragePathRankValue.get(storagePath).getAppNum().get() == 0
          && !availableRemoteStorageInfo.containsKey(storagePath)) {
        remoteStoragePathRankValue.remove(storagePath);
      }
    }
  }

  public synchronized void removePathFromCounter(String storagePath) {
    RankValue atomic = remoteStoragePathRankValue.get(storagePath);
    // The time spent reading and writing cannot be used to determine whether the current path is
    // still used by apps.
    // Therefore, determine whether the Hadoop FS path is still used by the number of apps
    if (atomic != null && atomic.getAppNum().get() == 0) {
      remoteStoragePathRankValue.remove(storagePath);
    }
  }

  public Set<String> getAppIds() {
    return appIdToUser.keySet();
  }

  @VisibleForTesting
  public Map<String, RankValue> getRemoteStoragePathRankValue() {
    return remoteStoragePathRankValue;
  }

  @VisibleForTesting
  public SelectStorageStrategy getSelectStorageStrategy() {
    return selectStorageStrategy;
  }

  @VisibleForTesting
  public Map<String, RemoteStorageInfo> getAvailableRemoteStorageInfo() {
    return availableRemoteStorageInfo;
  }

  @VisibleForTesting
  public Map<String, RemoteStorageInfo> getAppIdToRemoteStorageInfo() {
    return appIdToRemoteStorageInfo;
  }

  @VisibleForTesting
  public boolean hasErrorInStatusCheck() {
    return hasErrorInStatusCheck;
  }

  @VisibleForTesting
  public void closeDetectStorageScheduler() {
    // this method can only be used during testing
    detectStorageScheduler.shutdownNow();
  }

  protected void statusCheck() {
    List<Map<String, AppInfo>> appAndNums = Lists.newArrayList(currentUserAndApp.values());
    Map<String, AppInfo> appIds = Maps.newHashMap();
    // The reason for setting an expired uuid here is that there is a scenario where accessCluster
    // succeeds,
    // but the registration of shuffle fails, resulting in no normal heartbeat, and no normal update
    // of uuid to appId.
    // Therefore, an expiration time is set to automatically remove expired uuids
    Set<String> expiredAppIds = Sets.newHashSet();
    try {
      for (Map<String, AppInfo> appAndTimes : appAndNums) {
        for (Map.Entry<String, AppInfo> appAndTime : appAndTimes.entrySet()) {
          String appId = appAndTime.getKey();
          AppInfo lastReport = appAndTime.getValue();
          appIds.put(appId, lastReport);
          if (System.currentTimeMillis() - lastReport.getUpdateTime() > expired) {
            expiredAppIds.add(appId);
            appAndTimes.remove(appId);
            appIdToUser.remove(appId);
          }
        }
      }
      LOG.info("Start to check status for {} applications.", appIds.size());
      for (String appId : expiredAppIds) {
        LOG.info("Remove expired application : {}.", appId);
        appIds.remove(appId);
        if (appIdToRemoteStorageInfo.containsKey(appId)) {
          decRemoteStorageCounter(appIdToRemoteStorageInfo.get(appId).getPath());
          appIdToRemoteStorageInfo.remove(appId);
        }
      }
      CoordinatorMetrics.gaugeRunningAppNum.set(appIds.size());
      updateRemoteStorageMetrics();
      if (quotaManager != null) {
        quotaManager.updateQuotaMetrics();
      }
    } catch (Exception e) {
      // the flag is only for test case
      hasErrorInStatusCheck = true;
      LOG.warn("Error happened in statusCheck", e);
    }
  }

  private void updateRemoteStorageMetrics() {
    for (String remoteStoragePath : availableRemoteStorageInfo.keySet()) {
      try {
        String storageHost = getStorageHost(remoteStoragePath);
        CoordinatorMetrics.updateDynamicGaugeForRemoteStorage(
            storageHost, remoteStoragePathRankValue.get(remoteStoragePath).getAppNum().get());
      } catch (Exception e) {
        LOG.warn("Update remote storage metrics for {} failed ", remoteStoragePath);
      }
    }
  }

  private void addRemoteStorageMetrics(String remoteStoragePath) {
    String storageHost = getStorageHost(remoteStoragePath);
    if (!StringUtils.isEmpty(storageHost)) {
      CoordinatorMetrics.addDynamicGaugeForRemoteStorage(getStorageHost(remoteStoragePath));
      LOG.info("Add remote storage metrics for {} successfully ", remoteStoragePath);
    }
  }

  private String getStorageHost(String remoteStoragePath) {
    if (remoteStorageToHost.containsKey(remoteStoragePath)) {
      return remoteStorageToHost.get(remoteStoragePath);
    }
    String storageHost = "";
    try {
      URI uri = new URI(remoteStoragePath);
      storageHost = uri.getHost();
      remoteStorageToHost.put(remoteStoragePath, storageHost);
    } catch (URISyntaxException e) {
      LOG.warn("Invalid format of remoteStoragePath to get host, {}", remoteStoragePath);
    }
    return storageHost;
  }

  /**
   * Get Applications, The list contains applicationId, user, lastHeartBeatTime, remoteStoragePath.
   *
   * <p>We have set 6 criteria for filtering applicationId, and these criteria are in an 'AND'
   * relationship. All the criteria must be met for the applicationId to be selected.
   *
   * @param appIds Application List.
   * @param pageSize Items per page.
   * @param currentPage The number of pages to be queried.
   * @param pHeartBeatStartTime heartbeat start time.
   * @param pHeartBeatEndTime heartbeat end time.
   * @param appIdRegex applicationId regular expression.
   * @return Applications.
   */
  public List<Application> getApplications(
      Set<String> appIds,
      int pageSize,
      int currentPage,
      String pHeartBeatStartTime,
      String pHeartBeatEndTime,
      String appIdRegex) {
    List<Application> applications = new ArrayList<>();
    for (Map.Entry<String, Map<String, AppInfo>> entry : currentUserAndApp.entrySet()) {
      String user = entry.getKey();
      Map<String, AppInfo> apps = entry.getValue();
      apps.forEach(
          (appId, appInfo) -> {
            // Filter condition 1: Check whether applicationId is included in the filter list.
            boolean match = appIds.size() == 0 || appIds.contains(appId);

            // Filter condition 2: whether it meets the applicationId rule.
            if (StringUtils.isNotBlank(appIdRegex) && match) {
              match = match && matchApplicationId(appId, appIdRegex);
            }

            // Filter condition 3: Determine whether the start and
            // end of the heartbeat time are in line with expectations.
            if (StringUtils.isNotBlank(pHeartBeatStartTime)
                || StringUtils.isNotBlank(pHeartBeatEndTime)) {
              match =
                  matchHeartBeatStartTimeAndEndTime(
                      pHeartBeatStartTime, pHeartBeatEndTime, appInfo.getUpdateTime());
            }

            // If it meets expectations, add to the list to be returned.
            if (match) {
              RemoteStorageInfo remoteStorageInfo =
                  appIdToRemoteStorageInfo.getOrDefault(appId, null);
              Application application =
                  new Application.Builder()
                      .applicationId(appId)
                      .user(user)
                      .lastHeartBeatTime(appInfo.getUpdateTime())
                      .registrationTime(appInfo.getRegistrationTime())
                      .remoteStoragePath(remoteStorageInfo)
                      .build();
              applications.add(application);
            }
          });
    }
    Collections.sort(applications);
    int startIndex = (currentPage - 1) * pageSize;
    int endIndex = Math.min(startIndex + pageSize, applications.size());

    LOG.info("getApplications >> appIds = {}.", applications);
    return applications.subList(startIndex, endIndex);
  }

  /**
   * Based on the regular expression, determine if the applicationId matches the regular expression.
   *
   * @param applicationId applicationId
   * @param regex regular expression pattern.
   * @return If it returns true, it means the regular expression successfully matches the
   *     applicationId; otherwise, the regular expression fails to match the applicationId.
   */
  private boolean matchApplicationId(String applicationId, String regex) {
    Pattern pattern = Pattern.compile(regex);
    return pattern.matcher(applicationId).matches();
  }

  /**
   * Filter heartbeat time based on query conditions.
   *
   * @param pStartTime heartbeat start time.
   * @param pEndTime heartbeat end time.
   * @param appHeartBeatTime application HeartBeatTime
   * @return Returns true if the heartbeat time is within the given query range, otherwise returns
   *     false.
   */
  private boolean matchHeartBeatStartTimeAndEndTime(
      String pStartTime, String pEndTime, long appHeartBeatTime) {
    long startTime = 0;
    long endTime = Long.MAX_VALUE;

    if (StringUtils.isNotBlank(pStartTime)) {
      startTime = parseLongValue(pStartTime, "heartBeatStartTime");
    }

    if (StringUtils.isNotBlank(pEndTime)) {
      endTime = parseLongValue(pEndTime, "heartBeatEndTime");
    }

    Range<Long> heartBeatTime = Range.between(startTime, endTime);
    return heartBeatTime.contains(appHeartBeatTime);
  }

  /**
   * Convert String to Long.
   *
   * @param strValue String Value
   * @param fieldName Field Name
   * @return Long value.
   */
  private long parseLongValue(String strValue, String fieldName) {
    try {
      return Long.parseLong(strValue);
    } catch (NumberFormatException e) {
      throw new NumberFormatException(fieldName + " value must be a number!");
    }
  }

  public Map<String, Integer> getDefaultUserApps() {
    return quotaManager.getDefaultUserApps();
  }

  public QuotaManager getQuotaManager() {
    return quotaManager;
  }

  public static List<String> getPathSchema() {
    return REMOTE_PATH_SCHEMA;
  }

  public Map<String, Map<String, AppInfo>> getCurrentUserAndApp() {
    return currentUserAndApp;
  }

  public void close() {
    if (detectStorageScheduler != null) {
      detectStorageScheduler.shutdownNow();
    }
    if (checkAppScheduler != null) {
      checkAppScheduler.shutdownNow();
    }
  }

  public enum StrategyName {
    APP_BALANCE,
    IO_SAMPLE
  }
}
