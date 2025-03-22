---
layout: page
displayTitle: Deploy Tez Client Plugin & Configurations
title: Deploy Tez Client Plugin & Configurations
description: Deploy Tez Client Plugin & Configurations
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
# Deploy Tez Client Plugin & Configurations
## Deploy Tez Client Plugin

1. Append client jar to package which is set by 'tez.lib.uris'.

In production mode, you can append client jar (rss-client-tez-XXXXX-shaded.jar) to package which is set by 'tez.lib.uris'.

In development mode, you can append client jar (rss-client-tez-XXXXX-shaded.jar) to HADOOP_CLASSPATH.

2. Update tez-site.xml to enable Uniffle.

| Property Name              |Default| Description                  |
|----------------------------|---|------------------------------|
| tez.am.launch.cmd-opts     |-XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps -XX:+UseNUMA -XX:+UseParallelGC org.apache.tez.dag.app.RssDAGAppMaster| enable remote shuffle service |
| tez.rss.coordinator.quorum |coordinatorIp1:19999,coordinatorIp2:19999|coordinator address|

Note that the RssDAGAppMaster will automatically disable slow start (i.e., `tez.shuffle-vertex-manager.min-src-fraction=1`, `tez.shuffle-vertex-manager.max-src-fraction=1`).

## Tez Specific Configurations

| Property Name                  | Default | Description                                                             |
|--------------------------------|---------|-------------------------------------------------------------------------|
| tez.rss.avoid.recompute.succeeded.task | false   | Whether to avoid recompute succeeded task when node is unhealthy or black-listed |
| tez.rss.client.max.buffer.size | 3k | The max buffer size in map side. Control the size of each segment(WrappedBuffer) in the buffer. |
| tez.rss.client.batch.trigger.num | 50 | The max batch of buffers to send data in map side. Affect the number of blocks sent to the server in each batch, and may affect rss_worker_used_buffer_size |
| tez.rss.client.send.thread.num | 5 | The thread pool size for the client to send data to the server. |
| tez.shuffle.mode               | remote  | Use Remote Shuffle if the value is set to 'remote' or use default config value, or set 'local' to use local shuffle when needs to fall back.                   |


### Remote Spill (Experimental)

In cloud environment, VM may have very limited disk space and performance.
This experimental feature allows to reduce tasks to spill data to remote storage (e.g., hdfs)

|Property Name|Default| Description                                                            |
|---|---|------------------------------------------------------------------------|
|tez.rss.reduce.remote.spill.enable|false| Whether to use remote spill                                            |
|tez.rss.remote.spill.storage.path |     | The remote spill path                                                |
|tez.rss.reduce.remote.spill.replication|1| The replication number to spill data to Hadoop FS                      |
|tez.rss.reduce.remote.spill.retries|5| The retry number to spill data to Hadoop FS                            |

Notice: this feature requires the MEMORY_LOCAL_HADOOP mode.