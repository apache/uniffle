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

package org.apache.uniffle.server.bench;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.time.DateFormatUtils;

import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.server.web.vo.BenchArgumentVO;

public class BenchHandle implements AutoCloseable {
  private final String id;
  private final BenchArgumentVO argument;

  public final List<String> log = new Vector<>();
  private long startTime;
  private long endTime;
  private LinkedHashMap<String, AutoCloseable> allCloseable = new LinkedHashMap<>();
  private BenchTrace benchTrace;
  private long generateEventNums;
  private long totalEventNum;
  private AtomicLong completedEventNumCounter;

  public BenchHandle(String id, BenchArgumentVO argument) {
    this.id = id;
    this.argument = argument;
  }

  public void startTrace() {
    this.startTime = System.currentTimeMillis();
    this.benchTrace = new BenchTrace();
    benchTrace.start();
  }

  public void endTrace() {
    endTime = System.currentTimeMillis();
    benchTrace.end();
  }

  public BenchTrace getBenchTrace() {
    return benchTrace;
  }

  public String getId() {
    return id;
  }

  public String getStartTime() {
    return DateFormatUtils.format(startTime, Constants.DATE_PATTERN);
  }

  public String getEndTime() {
    return DateFormatUtils.format(endTime, Constants.DATE_PATTERN);
  }

  public long getDuration() {
    return endTime - startTime;
  }

  public List<String> getLog() {
    return log;
  }

  public void addLog(String content) {
    log.add(
        DateFormatUtils.format(System.currentTimeMillis(), Constants.DATE_PATTERN)
            + ": ["
            + id
            + "] "
            + content);
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public BenchArgumentVO getArgument() {
    return argument;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void setBenchTrace(BenchTrace benchTrace) {
    this.benchTrace = benchTrace;
  }

  @Override
  public void close() throws Exception {
    ArrayList<Map.Entry<String, AutoCloseable>> entries = new ArrayList<>(allCloseable.entrySet());
    Collections.reverse(entries);
    for (Map.Entry<String, AutoCloseable> entry : entries) {
      addLog("Closing " + entry.getKey());
      entry.getValue().close();
      addLog("Closed " + entry.getKey());
    }
    addLog("Closed all registered closable. Count is: " + allCloseable.size());
    allCloseable.clear();
  }

  public <T extends AutoCloseable> T registerClosable(String description, T closeable) {
    allCloseable.put(description, closeable);
    addLog("register closable : " + description);
    return closeable;
  }

  public void setGenerateEventNums(long generateEventNums) {
    this.generateEventNums = generateEventNums;
  }

  public void setTotalEventNum(long num) {
    this.totalEventNum = num;
  }

  public String getGenerateEventInfo() {
    double percentage = generateEventNums * 100.0 / totalEventNum;
    return String.format("%.2f%% (%d/%d)", percentage, generateEventNums, totalEventNum);
  }

  public long getTotalEventNum() {
    return this.totalEventNum;
  }

  public void setCompletedEventNumsCounter(AtomicLong completedEventNumCounter) {
    this.completedEventNumCounter = completedEventNumCounter;
  }

  public long getCompletedEventNum() {
    return completedEventNumCounter.get();
  }

  public String getCompletedEventInfo() {
    double percentage = completedEventNumCounter.get() * 100.0 / totalEventNum;
    return String.format(
        "%.2f%% (%d/%d)", percentage, completedEventNumCounter.get(), totalEventNum);
  }
}
