---
layout: page
displayTitle: Deploy Spark Client Plugin & Configurations
title: Deploy Spark Client Plugin & Configurations
description: Deploy Spark Client Plugin & Configurations
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
# Deploy Spark Client Plugin & Configurations
## Deploy Spark Client Plugin

1. Add client jar to Spark classpath, e.g. SPARK_HOME/jars/

   The jar for Spark2 is located in <RSS_HOME>/jars/client/spark2/rss-client-spark2-shaded-${version}.jar

   The jar for Spark3 is located in <RSS_HOME>/jars/client/spark3/rss-client-spark3-shaded-${version}.jar

2. Update Spark conf to enable Uniffle, e.g.

   ```
   # Uniffle transmits serialized shuffle data over network, therefore a serializer that supports relocation of
   # serialized object should be used. 
   spark.serializer org.apache.spark.serializer.KryoSerializer # this could also be in the spark-defaults.conf
   spark.shuffle.manager org.apache.spark.shuffle.RssShuffleManager
   spark.rss.coordinator.quorum <coordinatorIp1>:19999,<coordinatorIp2>:19999
   # Note: For Spark2, spark.sql.adaptive.enabled should be false because Spark2 doesn't support AQE.
   ```

### Support Spark Dynamic Allocation

To support spark dynamic allocation with Uniffle, spark code should be updated.
There are 2 patches for spark-2.4.6 and spark-3.1.2 in spark-patches folder for reference.

After apply the patch and rebuild spark, add following configuration in spark conf to enable dynamic allocation:
  ```
  spark.shuffle.service.enabled false
  spark.dynamicAllocation.enabled true
  ```

### Support Spark AQE

To improve performance of AQE skew optimization, uniffle introduces the LOCAL_ORDER shuffle-data distribution mechanism 
and Continuous partition assignment mechanism.

1. LOCAL_ORDER shuffle-data distribution mechanism filter the lots of data to reduce network bandwidth and shuffle-server local-disk pressure. 
   It will be enabled by default when AQE is enabled.

2. Continuous partition assignment mechanism assign consecutive partitions to the same ShuffleServer to reduce the frequency of getShuffleResult.

    It can be enabled by the following config
      ```bash
        # Another value is ROUND, it will poll to allocate partitions to ShuffleServer
        rss.coordinator.select.partition.strategy CONTINUOUS
        
        # Default value is 1.0, used to estimate task concurrency, how likely is this part of the resource between spark.dynamicAllocation.minExecutors and spark.dynamicAllocation.maxExecutors to be allocated
        --conf spark.rss.estimate.task.concurrency.dynamic.factor=1.0
      ```

Since v0.8.0, `RssShuffleManager` would disable local shuffle reader(`set spark.sql.adaptive.localShuffleReader.enabled=false`) optimization by default.

Local shuffle reader as its name indicates is suitable and optimized for spark's external shuffle service, and shall not be used for remote shuffle service. It would cause many random small IOs and network connections with Uniffle's shuffle server


## Spark Specific Configurations

The important configuration is listed as following.

| Property Name                                         | Default | Description                                                                                                                                                                                                                                                                                                                                                                                           |
|-------------------------------------------------------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| spark.rss.writer.buffer.spill.size                    | 128m    | Buffer size for total partition data. It is recommended to set spark.rss.writer.buffer.spill.size to 512m (default is 128m, 1g is preferable, theoretically the larger the better, but the executor's own memory should be considered, it may cause OOM when the executor's memory is not enough), this configuration can effectively improve task performance and alleviate server-side GC pressure. |
| spark.rss.client.send.size.limit                      | 16m     | The max data size sent to shuffle server                                                                                                                                                                                                                                                                                                                                                              |
| spark.rss.client.unregister.thread.pool.size          | 10      | The max size of thread pool of unregistering                                                                                                                                                                                                                                                                                                                                                          |
| spark.rss.client.unregister.request.timeout.sec       | 10      | The max timeout sec when doing unregister to remote shuffle-servers                                                                                                                                                                                                                                                                                                                                   |
| spark.rss.client.off.heap.memory.enable               | false   | The client use off heap memory to process data                                                                                                                                                                                                                                                                                                                                                        |
| spark.rss.client.remote.storage.useLocalConfAsDefault | false   | This option is only valid when the remote storage path is specified. If ture, the remote storage conf will use the client side hadoop configuration loaded from the classpath                                                                                                                                                                                                                         |
| spark.rss.hadoop.*                                    | -       | The prefix key for Hadoop conf. For Spark like that: `spark.rss.hadoop.fs.defaultFS=hdfs://rbf-x1`, this will be as `fs.defaultFS=hdfs://rbf-x1` for Hadoop storage                                                                                                                                                                                                                                   |
| spark.rss.access.id                                   | -       | The access id for request access rss cluster. This is used for DelegationRssShuffleManager                                                                                                                                                                                                                                                                                                            |
| spark.rss.access.id.providerKey                       | -       | Get access id from the value of the given provider key. This is used for DelegationRssShuffleManager                                                                                                                                                                                                                                                                                                  |

### Block id bits

If you observe an error like

    Don't support sequenceNo[…], the max value should be …
    Don't support partitionId[…], the max value should be …
    Don't support taskAttemptId[…], the max value should be …
    Observing attempt number … while maxFailures is set to ….
    Observing mapIndex[…] that would produce a taskAttemptId with … bits which is larger than the allowed … bits (maxFailures[…], speculation[…]). Please consider providing more bits for taskAttemptIds.
    Cannot register shuffle with … partitions because the configured block id layout supports at most … partitions.

you should consider increasing the bits reserved in the blockId for that number / id (while decreasing the other number of bits).

Using the Spark client, configuring the blockId bits is as easy as defining a maximum number of supported partitions only:

| Property Name                   | Default | Description                                                                                                                                                         |
|---------------------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| spark.rss.blockId.maxPartitions | 1048576 | Number of partitions supported by the Spark client (`[2..2,147,483,648]`). |

The Spark client derives the optimal values for the following properties.
Alternatively, these properties can be configured instead of `spark.rss.blockId.maxPartitions`:

| Property Name                       | Default | Description                                                                                                                                                         |
|-------------------------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| spark.rss.blockId.sequenceNoBits    | 18      | Number of bits reserved in the blockId for the sequence number (`[1..31]`). Note that `sequenceNoBits + partitionIdBits + taskAttemptIdBits` has to sum up to `63`. |
| spark.rss.blockId.partitionIdBits   | 24      | Number of bits reserved in the blockId for the partition id (`[1..31]`). Note that `sequenceNoBits + partitionIdBits + taskAttemptIdBits` has to sum up to `63`.    |
| spark.rss.blockId.taskAttemptIdBits | 21      | Number of bits reserved in the blockId for the task attempt id (`[1..31]`). Note that `sequenceNoBits + partitionIdBits + taskAttemptIdBits` has to sum up to `63`. |

The bits reserved for sequence number, partition id and task attempt id are best specified for Spark clients as follows (done automatically if `spark.rss.blockId.maxPartitions` is set):

1. Reserve the bits required to support the largest number of partitions that you anticipate. Pick `ceil( log(max number of partitions) / log(2) )` bits.
   For instance, `20` bits support `1,048,576` partitions.
2. The number of bits for the task attempt ids should be `partitionIdBits + ceil( log(max attempts) / log(2))`,
   where `max attempts` is set via Spark conf `spark.task.maxFailures` (default is `4`). In the presence of
   speculative execution enabled via Spark conf `spark.speculation` (default is false), that `max attempts` has to be incremented by one.
   For example: `22` bits is sufficient for `taskAttemptIdBits` with `partitionIdBits=20`, and Spark conf `spark.task.maxFailures=4` and `spark.speculation=false`.
3. Reserve the remaining bits to `sequenceNoBits`: `sequenceNoBits = 63 - partitionIdBits - taskAttemptIdBits`.

### Block id self management (experimental)

Now, the block id could be managed by the spark driver self when specifying the `spark.rss.blockId.selfManagementEnabled=true`.
And this will reduce shuffle server pressure but significantly increase memory consumption on the Spark driver side.

### Adaptive Remote Shuffle Enabling 
Currently, this feature only supports Spark. 

To select build-in shuffle or remote shuffle in a smart manner, Uniffle support adaptive enabling. 
The client should use `DelegationRssShuffleManager` and provide its unique <access_id> so that the coordinator could distinguish whether it should enable remote shuffle.

```
spark.shuffle.manager org.apache.spark.shuffle.DelegationRssShuffleManager
spark.rss.access.id=<access_id> 
```


Other configuration:

|Property Name|Default|Description|
|---|---|---|
|spark.rss.access.timeout.ms|10000|The timeout to access Uniffle coordinator|
|spark.rss.client.access.retry.interval.ms|20000|The interval between retries fallback to SortShuffleManager|
|spark.rss.client.access.retry.times|0|The number of retries fallback to SortShuffleManager|

### Partition reassign in one shuffle attempt

To achieve better task stability, the partition reassignment mechanism has been introduced, which requests new replacement shuffle servers to overcome server instability caused by unhealthy conditions or high memory pressure in a single shuffle attempt. 
Currently, this feature is not compatible with stage retry and multiple replica mechanisms (additional testing is required).

Using the following configs to enable this feature 

```bash
# whether to enable reassign mechanism
spark.rss.client.reassign.enabled                  true
# The max reassign server num for one partition when using partition reassign mechanism.
spark.rss.client.reassign.maxReassignServerNum     10
# The block retry max times when partition reassign is enabled. 
spark.rss.client.reassign.blockRetryMaxTimes       1
```

### Map side combine

Map side combine is a feature for rdd aggregation operators that combines the shuffle data on map side before sending it to the shuffle server, which can reduce the amount of data transmitted and the pressure on the shuffle server.

We can enable this feature by using the following configuration:

| Property Name                           | Default | Description                                           |
|-----------------------------------------|---------|-------------------------------------------------------|
| spark.rss.client.mapSideCombine.enabled | false   | Whether to enable map side combine of shuffle writer. |

**Note**: Map side combine will handle entire map side shuffle write data, which may cause data spills and delay shuffle writes.
