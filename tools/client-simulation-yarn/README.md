<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

# Uniffle Client Simulation On Yarn - Usage Guide

Currently, we have evaluated the performance of the flush operation using the Uniffle server's flush event recording and flush benchmark feature.
This allows us to assess the server's maximum capability to handle flush block requests for small blocks (e.g., 1 KiB) and the write throughput limit for large blocks (e.g., 1 MiB).

However, there may also be performance bottlenecks between the server receiving requests and the actual flush operation. Therefore, we need a simulated client that continuously sends data to the server.

## Parameter Description

| Parameter Name                               | Default Value | Description                                                                                                                                               |
|----------------------------------------------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `uniffle.client.sim.serverId`                | None          | Uniffle server ID                                                                                                                                         |
| `uniffle.client.sim.container.num`           | 3             | Number of containers to start in the Yarn application, which corresponds to the number of concurrent client processes                                     |
| `uniffle.client.sim.threadCount`             | 1             | Number of concurrent threads running in each container process. The actual number of working threads is `threadCount + 1` when each thread is concurrent. |
| `uniffle.client.sim.queueName`               | default       | Yarn resource queue name                                                                                                                                  |
| `uniffle.client.sim.jarPath.list`            | None          | HDFS addresses of additional JARs or other resources to download to AM or Task local, separated by commas (e.g., HDFS address of RSS shaded JAR)          |
| `uniffle.client.sim.tmp.hdfs.path`           | None          | A writable HDFS address for uploading temporary application resources                                                                                     |
| `uniffle.client.sim.shuffleCount`            | 1             | Number of shuffles included in a single `sendShuffleData` request                                                                                         |
| `uniffle.client.sim.partitionCount`          | 1             | Number of partitions included in each shuffle of a single `sendShuffleData` request                                                                       |
| `uniffle.client.sim.blockCount`              | 1             | Number of blocks included in each partition of a single `sendShuffleData` request                                                                         |
| `uniffle.client.sim.blockSize`               | 1024          | Size of each block in a single `sendShuffleData` request                                                                                                  |
| `uniffle.client.sim.am.vCores`               | 8             | Number of virtual cores specified when requesting the Application Master (AM)                                                                             |
| `uniffle.client.sim.am.memory`               | 4096          | Memory size (in MB) specified when requesting the AM                                                                                                      |
| `uniffle.client.sim.container.vCores`        | 2             | Number of virtual cores specified when requesting task containers                                                                                         |
| `uniffle.client.sim.container.memory`        | 2048          | Memory size (in MB) specified when requesting task containers                                                                                             |
| `uniffle.client.sim.am.jvm.opts`             | None          | Additional JVM options for debugging when the execution result is abnormal                                                                                |
| `uniffle.client.sim.container.jvm.opts`      | None          | Additional JVM options for debugging when the execution result is abnormal                                                                                |

## Running Example

1. Change to the Hadoop directory and execute the test on Yarn program:

```bash
cd $HADOOP_HOME
```

2. Execute the example command:

```bash
$ bin/yarn jar rss-client-simulation-yarn-0.11.0-SNAPSHOT.jar \
-Duniffle.client.sim.serverId=<UNIFFLE_SERVER_ID> \
-Duniffle.client.sim.container.num=1000 \
-Duniffle.client.sim.queueName=<YOUR_QUEUE_NAME>  \
-Duniffle.client.sim.jarPath.list=hdfs://ns1/tmp/rss-client-spark3-shaded.jar  \
-Duniffle.client.sim.tmp.hdfs.path=hdfs://ns1/user/xx/tmp/uniffle-client-sim/  \
-Duniffle.client.sim.shuffleCount=5 \
-Duniffle.client.sim.partitionCount=50 \
-Duniffle.client.sim.blockCount=100 \
-Duniffle.client.sim.blockSize=10240 \
-Duniffle.client.sim.threadCount=10
```

3. Example Output:

```plaintext
24/12/30 15:03:47 INFO simulator.UniffleClientSimOnYarnClient: appId: application_1729845342052_5295913
...
Application killed: application_1729845342052_5295913
Application status: KILLED
```

This guide provides a comprehensive overview of how to run the Uniffle Client simulator on Yarn, including parameter descriptions and a step-by-step example. Adjust the parameters as needed for your specific use case.
