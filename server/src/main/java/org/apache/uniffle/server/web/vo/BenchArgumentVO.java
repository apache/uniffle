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

package org.apache.uniffle.server.web.vo;

import picocli.CommandLine;

public class BenchArgumentVO {
  @CommandLine.Option(
      names = {"-c", "--conf"},
      description = "config file",
      defaultValue = "1k")
  private String configFile;

  @CommandLine.Option(
      names = {"-b", "--blockSize"},
      description = "block size",
      required = true)
  private String blockSize;

  @CommandLine.Option(
      names = {"-n", "--blockNum"},
      description = "block num",
      required = true)
  private int blockNumPerEvent;

  @CommandLine.Option(
      names = {"-e", "--eventNum"},
      description = "event num",
      required = true)
  private int eventNum;

  @CommandLine.Option(
      names = {"-p", "--partitionNum"},
      description = "partition num",
      required = true)
  private int partitionNum;

  @CommandLine.Option(
      names = {"--threadNumGenerateEvent"},
      description = "threadNumGenerateEvent",
      defaultValue = "10")
  private int threadNumGenerateEvent = 10;

  @CommandLine.Option(
      names = {"--generateSleep"},
      description = "generateSleep",
      defaultValue = "10")
  private int sleepMsExceedBlockNumThreshold = 10;

  @CommandLine.Option(
      names = {"--blockNumThresholdInMemory"},
      description = "sleep ${generateSleep} when block num >= ${blockNumThresholdInMemory}",
      defaultValue = "10000000")
  private int blockNumThresholdInMemory = 10000000;

  @CommandLine.Option(
      names = {"--inQueueEventLimit"},
      description = "sleep ${generateSleep} when block num >= ${blockNumThresholdInMemory}",
      defaultValue = "-1")
  private int inQueueEventLimit = -1;

  @CommandLine.Option(
      names = {"--dumpfile"},
      description = "flush event dump input file",
      required = false)
  private String dumpfile;

  @CommandLine.Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "display this help message")
  private boolean helpRequested = false;

  public boolean isHelp() {
    return helpRequested;
  }

  public String getConfigFile() {
    return configFile;
  }

  public String getBlockSize() {
    return blockSize;
  }

  public int getBlockNumPerEvent() {
    return blockNumPerEvent;
  }

  public int getEventNum() {
    return eventNum;
  }

  public int getPartitionNum() {
    return partitionNum;
  }

  public int getThreadNumGenerateEvent() {
    return threadNumGenerateEvent;
  }

  public int getSleepMsExceedBlockNumThreshold() {
    return sleepMsExceedBlockNumThreshold;
  }

  public int getBlockNumThresholdInMemory() {
    return blockNumThresholdInMemory;
  }

  public String getDumpfile() {
    return dumpfile;
  }

  public void setInQueueEventLimit(int inQueueEventLimit) {
    this.inQueueEventLimit = inQueueEventLimit;
  }

  public int getInQueueEventLimit() {
    return inQueueEventLimit;
  }

  @Override
  public String toString() {
    return "{"
        + "configFile='"
        + configFile
        + '\''
        + ", blockSize='"
        + blockSize
        + '\''
        + ", blockNumPerEvent="
        + blockNumPerEvent
        + ", eventNum="
        + eventNum
        + ", partitionNum="
        + partitionNum
        + ", threadNumGenerateEvent="
        + threadNumGenerateEvent
        + ", sleepMsExceedBlockNumThreshold="
        + sleepMsExceedBlockNumThreshold
        + ", blockNumThresholdInMemory="
        + blockNumThresholdInMemory
        + ", helpRequested="
        + helpRequested
        + '}';
  }
}
