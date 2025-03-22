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

package org.apache.uniffle.client.simulator;

public class Constants {
  public static final String KEY_PREFIX = "uniffle.client.sim.";
  public static final String KEY_AM_MEMORY = KEY_PREFIX + "am.memory";
  public static final String KEY_AM_VCORES = KEY_PREFIX + "am.vCores";
  public static final String KEY_CONTAINER_MEMORY = KEY_PREFIX + "container.memory";
  public static final String KEY_CONTAINER_VCORES = KEY_PREFIX + "container.vCores";
  public static final String KEY_QUEUE_NAME = KEY_PREFIX + "queueName";
  public static final String KEY_CONTAINER_NUM = KEY_PREFIX + "container.num";
  public static final int CONTAINER_NUM_DEFAULT = 3;
  public static final String KEY_SERVER_ID = KEY_PREFIX + "serverId";
  public static final String KEY_SHUFFLE_COUNT = KEY_PREFIX + "shuffleCount";
  public static final String KEY_PARTITION_COUNT = KEY_PREFIX + "partitionCount";
  public static final String KEY_BLOCK_COUNT = KEY_PREFIX + "blockCount";
  public static final String KEY_BLOCK_SIZE = KEY_PREFIX + "blockSize";
  public static final String KEY_EXTRA_JAR_PATH_LIST = KEY_PREFIX + "jarPath.list";
  public static final String KEY_YARN_APP_ID = KEY_PREFIX + "appId";
  public static final String KEY_CONTAINER_INDEX = KEY_PREFIX + "containerIndex";
  public static final String KEY_AM_EXTRA_JVM_OPTS = KEY_PREFIX + "am.jvm.opts";
  public static final String KEY_CONTAINER_EXTRA_JVM_OPTS = KEY_PREFIX + "container.jvm.opts";
  public static final String KEY_TMP_HDFS_PATH = KEY_PREFIX + "tmp.hdfs.path";
  public static final String TMP_HDFS_PATH_DEFAULT = "./tmp/uniffle-client-sim/";
  public static final String JOB_CONF_NAME = "job.conf";
  public static final String KEY_THREAD_COUNT = KEY_PREFIX + ".threadCount";
}
