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

# Apache Uniffle (Incubating)

Uniffle is a high performance, general purpose remote shuffle service for distributed computing engines.
It provides the ability to push shuffle data into centralized storage service,
changing the shuffle style from "local file pull-like style" to "remote block push-like style".
It brings in several advantages like supporting disaggregated storage deployment,
super large shuffle jobs, and high elasticity.
Currently it supports [Apache Spark][1], [Apache Hadoop MapReduce][2] and [Apache Tez][3].

[1]: https://spark.apache.org
[2]: https://hadoop.apache.org
[3]: https://tez.apache.org

[![Build](https://github.com/apache/incubator-uniffle/actions/workflows/build.yml/badge.svg?branch=master&event=push)](https://github.com/apache/incubator-uniffle/actions/workflows/build.yml)
[![Codecov](https://codecov.io/gh/apache/incubator-uniffle/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/incubator-uniffle)
[![License](https://img.shields.io/github/license/apache/incubator-uniffle)](https://github.com/apache/incubator-uniffle/blob/master/LICENSE)
[![Release](https://img.shields.io/github/v/release/apache/incubator-uniffle)](https://github.com/apache/incubator-uniffle/releases)
[![Slack](https://img.shields.io/badge/chat-on%20Slack-brightgreen.svg)](https://join.slack.com/t/the-asf/shared_invite/zt-1fm9561yr-uzTpjqg3jf5nxSJV5AE3KQ)

## Architecture
![Rss Architecture](docs/asset/rss_architecture.png)
Uniffle cluster consists of three components, a coordinator cluster, a shuffle server cluster and an optional remote storage (e.g., HDFS).

Coordinator will collect the status of shuffle servers and assign jobs based on some strategy.

Shuffle server will receive the shuffle data, merge them and write to storage.

Depending on different situations, Uniffle supports Memory & Local, Memory & Remote Storage(e.g., HDFS), Memory & Local & Remote Storage(recommendation for production environment).

## Shuffle Process with Uniffle

* Spark driver ask coordinator to get shuffle server for shuffle process
* Spark task write shuffle data to shuffle server with following step:
![Rss Shuffle_Write](docs/asset/rss_shuffle_write.png)
 1. Send KV data to buffer
 2. Flush buffer to queue when buffer is full or buffer manager is full
 3. Thread pool get data from queue
 4. Request memory from shuffle server first and send the shuffle data
 5. Shuffle server cache data in memory first and flush to queue when buffer manager is full
 6. Thread pool get data from queue
 7. Write data to storage with index file and data file
 8. After write data, task report all blockId to shuffle server, this step is used for data validation later
 9. Store taskAttemptId in MapStatus to support Spark speculation

* Depending on different storage types, the spark task will read shuffle data from shuffle server or remote storage or both of them.

## Shuffle file format
The shuffle data is stored with index file and data file. Data file has all blocks for a specific partition and the index file has metadata for every block.

![Rss Shuffle_Write](docs/asset/rss_data_format.png)

## Supported Spark Version
Currently supports Spark 2.3.x, Spark 2.4.x, Spark 3.0.x, Spark 3.1.x, Spark 3.2.x, Spark 3.3.x, Spark 3.4.x, Spark 3.5.x

Note: To support dynamic allocation, the patch(which is included in patch/spark folder) should be applied to Spark

## Supported MapReduce Version
Currently supports the MapReduce framework of Hadoop 2.8.5, Hadoop 3.2.1

## Building Uniffle
> note: currently Uniffle requires JDK 1.8 to build, adding later JDK support is on our roadmap.

Uniffle is built using [Apache Maven](https://maven.apache.org/).
To build it, run:

    ./mvnw -DskipTests clean package

To fix code style issues, run:

    ./mvnw spotless:apply -Pspark3 -Pspark2 -Ptez -Pmr -Phadoop2.8 -Pdashboard

Build against profile Spark 2 (2.4.6)

    ./mvnw -DskipTests clean package -Pspark2

Build against profile Spark 3 (3.1.2)

    ./mvnw -DskipTests clean package -Pspark3

Build against Spark 3.2.x, Except 3.2.0

    ./mvnw -DskipTests clean package -Pspark3.2

Build against Spark 3.2.0

    ./mvnw -DskipTests clean package -Pspark3.2.0

Build against Hadoop MapReduce 2.8.5

    ./mvnw -DskipTests clean package -Pmr,hadoop2.8

Build against Hadoop MapReduce 3.2.1

    ./mvnw -DskipTests clean package -Pmr,hadoop3.2

Build against Tez 0.9.1

    ./mvnw -DskipTests clean package -Ptez

Build against Tez 0.9.1 and Hadoop 3.2.1

    ./mvnw -DskipTests clean package -Ptez,hadoop3.2

Build with dashboard

    ./mvnw -DskipTests clean package -Pdashboard

> note: currently Uniffle build the project against Java 8. If you want to compile it against other Java versions, you can build the code with `-Dmaven.compiler.release=${release-version}`.

To package the Uniffle, run:

    ./build_distribution.sh

Package against Spark 3.2.x, Except 3.2.0, run:

    ./build_distribution.sh --spark3-profile 'spark3.2'

Package against Spark 3.2.0, run:

    ./build_distribution.sh --spark3-profile 'spark3.2.0'

Package will build against Hadoop 2.8.5 in default. If you want to build package against Hadoop 3.2.1, run:

    ./build_distribution.sh --hadoop-profile 'hadoop3.2'

Package with hadoop jars, If you want to build package against Hadoop 3.2.1, run:

    ./build_distribution.sh --hadoop-profile 'hadoop3.2' -Phadoop-dependencies-included

rss-xxx.tgz will be generated for deployment

## Deploy

If you have packaged tgz with hadoop jars, the env of `HADOOP_HOME` is needn't specified in `rss-env.sh`.

### Deploy Coordinator

1. unzip package to RSS_HOME
2. update RSS_HOME/conf/rss-env.sh, e.g.,
   ```
     JAVA_HOME=<java_home>
     HADOOP_HOME=<hadoop home>
     COORDINATOR_XMX_SIZE="16g"
     # You can set coordinator memory size by `XMX_SIZE` too, but it affects all components.
     # XMX_SIZE="16g"
   ```
3. update RSS_HOME/conf/coordinator.conf, e.g.,
   ```
     rss.rpc.server.port 19999
     rss.jetty.http.port 19998
     rss.coordinator.server.heartbeat.timeout 30000
     rss.coordinator.app.expired 60000
     rss.coordinator.shuffle.nodes.max 5
     # enable dynamicClientConf, and coordinator will be responsible for most of client conf
     rss.coordinator.dynamicClientConf.enabled true
     # config the path of client conf
     rss.coordinator.dynamicClientConf.path <RSS_HOME>/conf/dynamic_client.conf
     # config the path of excluded shuffle server
     rss.coordinator.exclude.nodes.file.path <RSS_HOME>/conf/exclude_nodes
   ```
4. update <RSS_HOME>/conf/dynamic_client.conf, rss client will get default conf from coordinator e.g.,
   ```
    # MEMORY_LOCALFILE_HDFS is recommended for production environment
    rss.storage.type MEMORY_LOCALFILE_HDFS
    # multiple remote storages are supported, and client will get assignment from coordinator
    rss.coordinator.remote.storage.path hdfs://cluster1/path,hdfs://cluster2/path
    rss.writer.require.memory.retryMax 1200
    rss.client.retry.max 50
    rss.client.send.check.timeout.ms 600000
    rss.client.read.buffer.size 14m
   ```
5. start Coordinator
   ```
    bash RSS_HOME/bin/start-coordnator.sh
   ```

### Deploy Shuffle Server
We recommend to use JDK 11+ if we want to have better performance when we deploy the shuffle server.
Some benchmark tests among different JDK is as below:
(using spark to write shuffle data with 20 executors. Single executor will total write 1G, and each time write 14M.
Shuffle Server use GRPC to transfer data)

| Java version | ShuffleServer GC  | Max pause time | ThroughOutput |
| ------------- | ------------- | ------------- | ------------- |
| 8  | G1  | 30s | 0.3 |
| 11  | G1  | 2.5s | 0.8 |
| 18  | G1  | 2.5s | 0.8 |
| 18  | ZGC  | 0.2ms | 0.99997 |

Deploy Steps:
1. unzip package to RSS_HOME
2. update RSS_HOME/conf/rss-env.sh, e.g.,
   ```
     JAVA_HOME=<java_home>
     HADOOP_HOME=<hadoop home>
     SHUFFLE_SERVER_XMX_SIZE="80g"
     # You can set shuffle server memory size by `XMX_SIZE` too, but it affects all components.
     # XMX_SIZE="80g"
   ```
3. update RSS_HOME/conf/server.conf, e.g.,
   ```
     rss.rpc.server.port 19999
     rss.jetty.http.port 19998
     rss.rpc.executor.size 2000
     # it should be configured the same as in coordinator
     rss.storage.type MEMORY_LOCALFILE_HDFS
     rss.coordinator.quorum <coordinatorIp1>:19999,<coordinatorIp2>:19999
     # local storage path for shuffle server
     rss.storage.basePath /data1/rssdata,/data2/rssdata....
     # it's better to config thread num according to local disk num
     rss.server.flush.thread.alive 5
     rss.server.flush.localfile.threadPool.size 10
     rss.server.flush.hadoop.threadPool.size 60
     rss.server.buffer.capacity 40g
     rss.server.read.buffer.capacity 20g
     rss.server.heartbeat.interval 10000
     rss.rpc.message.max.size 1073741824
     rss.server.preAllocation.expired 120000
     rss.server.commit.timeout 600000
     rss.server.app.expired.withoutHeartbeat 120000
     # note: the default value of rss.server.flush.cold.storage.threshold.size is 64m
     # there will be no data written to DFS if set it as 100g even rss.storage.type=MEMORY_LOCALFILE_HDFS
     # please set a proper value if DFS is used, e.g., 64m, 128m.
     rss.server.flush.cold.storage.threshold.size 100g
   ```
4. start Shuffle Server
   ```
    bash RSS_HOME/bin/start-shuffle-server.sh
   ```

### Deploy Spark Client
1. Add client jar to Spark classpath, e.g., SPARK_HOME/jars/

   The jar for Spark2 is located in <RSS_HOME>/jars/client/spark2/rss-client-spark2-shaded-${version}.jar

   The jar for Spark3 is located in <RSS_HOME>/jars/client/spark3/rss-client-spark3-shaded-${version}.jar

2. Update Spark conf to enable Uniffle, e.g.,

   ```
   # Uniffle transmits serialized shuffle data over network, therefore a serializer that supports relocation of
   # serialized object should be used. 
   spark.serializer org.apache.spark.serializer.KryoSerializer # this could also be in the spark-defaults.conf
   spark.shuffle.manager org.apache.spark.shuffle.RssShuffleManager
   spark.rss.coordinator.quorum <coordinatorIp1>:19999,<coordinatorIp2>:19999
   # Note: For Spark2, spark.sql.adaptive.enabled should be false because Spark2 doesn't support AQE.
   ```

### Support Spark dynamic allocation

To support spark dynamic allocation with Uniffle, spark code should be updated.
There are 7 patches for spark (2.3.4/2.4.6/3.0.1/3.1.2/3.2.1/3.3.1/3.4.1) in patch/spark folder for reference.

After apply the patch and rebuild spark, add following configuration in spark conf to enable dynamic allocation:
  ```
  spark.shuffle.service.enabled false
  spark.dynamicAllocation.enabled true
  ```
For spark3.5 or above just add one more configuration:
  ```
  spark.shuffle.sort.io.plugin.class org.apache.spark.shuffle.RssShuffleDataIo
  ```

### Deploy MapReduce Client

1. Add client jar to the classpath of each NodeManager, e.g., <HADOOP>/share/hadoop/mapreduce/

The jar for MapReduce is located in <RSS_HOME>/jars/client/mr/rss-client-mr-XXXXX-shaded.jar

2. Update MapReduce conf to enable Uniffle, e.g.,

   ```
   -Dmapreduce.rss.coordinator.quorum=<coordinatorIp1>:19999,<coordinatorIp2>:19999
   -Dyarn.app.mapreduce.am.command-opts=org.apache.hadoop.mapreduce.v2.app.RssMRAppMaster
   -Dmapreduce.job.map.output.collector.class=org.apache.hadoop.mapred.RssMapOutputCollector
   -Dmapreduce.job.reduce.shuffle.consumer.plugin.class=org.apache.hadoop.mapreduce.task.reduce.RssShuffle
   ```
Note that the RssMRAppMaster will automatically disable slow start (i.e., `mapreduce.job.reduce.slowstart.completedmaps=1`)
and job recovery (i.e., `yarn.app.mapreduce.am.job.recovery.enable=false`)

### Deploy Tez Client

1. Append client jar to pacakge which is set by 'tez.lib.uris'.

In production mode, you can append client jar (rss-client-tez-XXXXX-shaded.jar) to package which is set by 'tez.lib.uris'. 

In development mode, you can append client jar (rss-client-tez-XXXXX-shaded.jar) to HADOOP_CLASSPATH.

2. Update tez-site.xml to enable Uniffle.

| Property Name              |Default| Description                  |
|----------------------------|---|------------------------------|
| tez.am.launch.cmd-opts     |-XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps -XX:+UseNUMA -XX:+UseParallelGC org.apache.tez.dag.app.RssDAGAppMaster| enable remote shuffle service |
| tez.rss.coordinator.quorum |coordinatorIp1:19999,coordinatorIp2:19999|coordinator address|

Note that the RssDAGAppMaster will automatically disable slow start (i.e., `tez.shuffle-vertex-manager.min-src-fraction=1`, `tez.shuffle-vertex-manager.max-src-fraction=1`).

### Deploy In Kubernetes

We have provided an operator for deploying uniffle in kubernetes environments.

For details, see the following document:

[operator docs](docs/operator)

## Configuration

The important configuration is listed as follows.

|Role|Link|
|---|---|
|coordinator|[Uniffle Coordinator Guide](https://github.com/apache/incubator-uniffle/blob/master/docs/coordinator_guide.md)|
|shuffle server|[Uniffle Shuffle Server Guide](https://github.com/apache/incubator-uniffle/blob/master/docs/server_guide.md)|
|client|[Uniffle Shuffle Client Guide](https://github.com/apache/incubator-uniffle/blob/master/docs/client_guide/client_guide.md)|

## Security: Hadoop kerberos authentication
The primary goals of the Uniffle Kerberos security are:
1. to enable secure data access for coordinator/shuffle-servers, like dynamic conf/exclude-node files stored in secured dfs cluster
2. to write shuffle data to kerberos secured dfs cluster for shuffle-servers.

The following security configurations are introduced.

|Property Name|Default|Description|
|---|---|---|
|rss.security.hadoop.kerberos.enable|false|Whether enable access secured hadoop cluster|
|rss.security.hadoop.kerberos.krb5-conf.file|-|The file path of krb5.conf. And only when rss.security.hadoop.kerberos.enable is enabled, the option will be valid|
|rss.security.hadoop.kerberos.keytab.file|-|The kerberos keytab file path. And only when rss.security.hadoop.kerberos.enable is enabled, the option will be valid|
|rss.security.hadoop.kerberos.principal|-|The kerberos keytab principal. And only when rss.security.hadoop.kerberos.enable is enabled, the option will be valid|
|rss.security.hadoop.kerberos.relogin.interval.sec|60|The kerberos authentication relogin interval. unit: sec|
|rss.security.hadoop.kerberos.proxy.user.enable|true|Whether using proxy user for job user to access secured Hadoop cluster.|

* The proxy user mechanism is used to keep the data isolation in uniffle, which means the shuffle-data written by 
  shuffle-servers is owned by spark app's user. To achieve the this, the login user specified by above config should 
  be as the superuser for HDFS. For more details of related sections, 
  please see [Proxy user - Superusers Acting On Behalf Of Other Users](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/Superusers.html)

## Benchmark
We provide some benchmark tests for Uniffle. For details, you can see [Uniffle 0.2.0 Benchmark](docs/benchmark.md), [Uniffle 0.9.0 Benchmark](docs/benchmark_netty_case_report.md).

## LICENSE

Uniffle is under the Apache License Version 2.0. See the [LICENSE](https://github.com/apache/incubator-uniffle/blob/master/LICENSE) file for details.

## Contributing
For more information about contributing issues or pull requests, see [Uniffle Contributing Guide](https://github.com/apache/incubator-uniffle/blob/master/CONTRIBUTING.md).
