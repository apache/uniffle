---
layout: page
displayTitle: Deploy MapReduce Client Plugin & Configurations
title: Deploy MapReduce Client Plugin & Configurations
description: Deploy MapReduce Client Plugin & Configurations
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---
# Deploy MapReduce Client Plugin & Configurations
## Deploy MapReduce Client Plugin

1. Add client jar to the classpath of each NodeManager, e.g., <HADOOP>/share/hadoop/mapreduce/

The jar for MapReduce is located in <RSS_HOME>/jars/client/mr/rss-client-mr-XXXXX-shaded.jar

2. Update MapReduce conf to enable Uniffle, e.g.

   ```
   -Dmapreduce.rss.coordinator.quorum=<coordinatorIp1>:19999,<coordinatorIp2>:19999
   -Dyarn.app.mapreduce.am.command-opts=org.apache.hadoop.mapreduce.v2.app.RssMRAppMaster
   -Dmapreduce.job.map.output.collector.class=org.apache.hadoop.mapred.RssMapOutputCollector
   -Dmapreduce.job.reduce.shuffle.consumer.plugin.class=org.apache.hadoop.mapreduce.task.reduce.RssShuffle
   ```
Note that the RssMRAppMaster will automatically disable slow start (i.e., `mapreduce.job.reduce.slowstart.completedmaps=1`)
and job recovery (i.e., `yarn.app.mapreduce.am.job.recovery.enable=false`)

## MapReduce Specific Configurations

|Property Name|Default|Description|
|---|---|---|
|mapreduce.rss.client.max.buffer.size|3k|The max buffer size in map side|
|mapreduce.rss.client.batch.trigger.num|50|The max batch of buffers to send data in map side|


### Remote Spill (Experimental)

In cloud environment, VM may have very limited disk space and performance.
This experimental feature allows to reduce tasks to spill data to remote storage (e.g., hdfs)

|Property Name|Default| Description                                                            |
|---|---|------------------------------------------------------------------------|
|mapreduce.rss.reduce.remote.spill.enable|false| Whether to use remote spill                                            |
|mapreduce.rss.reduce.remote.spill.attempt.inc|1| Increase reduce attempts as Hadoop FS may be easier to crash than disk |
|mapreduce.rss.reduce.remote.spill.replication|1| The replication number to spill data to Hadoop FS                      |
|mapreduce.rss.reduce.remote.spill.retries|5| The retry number to spill data to Hadoop FS                            |

Notice: this feature requires the MEMORY_LOCAL_HADOOP mode.