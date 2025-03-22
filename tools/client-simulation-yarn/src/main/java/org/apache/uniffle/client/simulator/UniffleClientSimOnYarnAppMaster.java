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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.util.ThreadUtils;

public class UniffleClientSimOnYarnAppMaster {
  private static final Logger LOG = LoggerFactory.getLogger(UniffleClientSimOnYarnAppMaster.class);
  private final Configuration conf;
  private final String appId;
  // object lock
  private Object lock = new Object();
  // child tasks num
  private int childTaskNum;
  // completed tasks num
  private int childTaskCompletedNum = 0;
  private AtomicInteger allocatedContainerNum = new AtomicInteger(0);
  NMClientAsyncImpl nmClientAsync;

  public static void main(String[] args) {
    UniffleClientSimOnYarnAppMaster master = new UniffleClientSimOnYarnAppMaster();
    master.run();
  }

  public UniffleClientSimOnYarnAppMaster() {
    conf = new Configuration();
    HadoopConfigApp hadoopConfigApp = new HadoopConfigApp(conf);
    hadoopConfigApp.run(new String[] {"--conf", "./" + Constants.JOB_CONF_NAME});
    appId = conf.get(Constants.KEY_YARN_APP_ID, "unknown");
    childTaskNum = conf.getInt(Constants.KEY_CONTAINER_NUM, Constants.CONTAINER_NUM_DEFAULT);
  }

  private void run() {
    try {
      String serverId = conf.get(Constants.KEY_SERVER_ID);
      System.out.println("serverId:" + serverId);
      // start am-rm client，build rm-am connection to register AM, allocListener responsible for
      // handle AM's status
      AMRMClientAsync<AMRMClient.ContainerRequest> amRmClient =
          AMRMClientAsync.createAMRMClientAsync(1000, new RMCallBackHandler());
      amRmClient.init(new Configuration());
      amRmClient.start();
      String hostName = NetUtils.getHostname();
      // register to RM
      amRmClient.registerApplicationMaster(hostName, -1, null);
      // init nmClient
      nmClientAsync = new NMClientAsyncImpl(new NMCallBackHandler());
      nmClientAsync.init(new Configuration());
      nmClientAsync.start();
      // run
      doRun(amRmClient);
      // unregister AM
      amRmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "SUCCESS", null);
      // stop am-rm
      amRmClient.stop();
      // stop nm client
      nmClientAsync.stop();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /** run the main logic */
  private void doRun(AMRMClientAsync amRmClient) throws InterruptedException {
    int shuffleCount = conf.getInt(Constants.KEY_SHUFFLE_COUNT, 1);
    int partitionCount = conf.getInt(Constants.KEY_PARTITION_COUNT, 1);
    String serverId = conf.get(Constants.KEY_SERVER_ID, "");
    registerShuffleAndSetupHeartbeat(shuffleCount, partitionCount, serverId);
    // apply child tasks
    for (int i = 0; i < childTaskNum; i++) {
      int memory = conf.getInt(Constants.KEY_CONTAINER_MEMORY, 2048);
      int vCores = conf.getInt(Constants.KEY_CONTAINER_VCORES, 2);
      // apply container
      AMRMClient.ContainerRequest containerRequest =
          new AMRMClient.ContainerRequest(
              Resource.newInstance(memory, vCores), null, null, Priority.UNDEFINED);
      amRmClient.addContainerRequest(containerRequest);
    }
    synchronized (lock) {
      while (childTaskCompletedNum < childTaskNum) {
        lock.wait(1000);
      }
    }
    System.out.println("AM: Finish main logic");
  }

  private void registerShuffleAndSetupHeartbeat(
      int shuffleCount, int partitionCount, String serverId) {
    String[] parts = serverId.split("-");
    String host = parts[0];
    int port0 = Integer.parseInt(parts[1]);
    int port1 = Integer.parseInt(parts[2]);

    ShuffleWriteClientImpl shuffleWriteClientImpl =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(3)
            .retryIntervalMax(1000)
            .heartBeatThreadNum(4)
            .replica(1)
            .replicaWrite(1)
            .replicaRead(1)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(10)
            .dataCommitPoolSize(10)
            .unregisterThreadPoolSize(10)
            .unregisterRequestTimeSec(10)
            .build();

    ShuffleServerInfo shuffleServerInfo = new ShuffleServerInfo(host, port0, port1);
    registerShuffle(shuffleCount, partitionCount, shuffleWriteClientImpl, shuffleServerInfo);
    long heartbeatInterval = 10_000;
    ScheduledExecutorService heartBeatScheduledExecutorService =
        ThreadUtils.getDaemonSingleThreadScheduledExecutor("rss-heartbeat");
    heartBeatScheduledExecutorService.scheduleAtFixedRate(
        () -> {
          try {
            String appId = this.appId.toString();
            shuffleWriteClientImpl.sendAppHeartbeat(appId, 10_000);
            LOG.info("Finish send heartbeat to coordinator and servers");
          } catch (Exception e) {
            LOG.warn("Fail to send heartbeat to coordinator and servers", e);
          }
        },
        heartbeatInterval / 2,
        heartbeatInterval,
        TimeUnit.MILLISECONDS);
  }

  private void registerShuffle(
      int shuffleCount,
      int partitionCount,
      ShuffleWriteClientImpl shuffleWriteClientImpl,
      ShuffleServerInfo shuffleServerInfo) {
    List<PartitionRange> partitionRanges = new ArrayList<>(partitionCount);
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      partitionRanges.add(new PartitionRange(partitionId, partitionId));
    }
    for (int shuffleId = 0; shuffleId < shuffleCount; shuffleId++) {
      shuffleWriteClientImpl.registerShuffle(
          shuffleServerInfo,
          appId.toString(),
          shuffleId,
          partitionRanges,
          new RemoteStorageInfo(""),
          ShuffleDataDistributionType.NORMAL,
          1);
    }
  }

  class RMCallBackHandler extends AMRMClientAsync.AbstractCallbackHandler {

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
      for (ContainerStatus status : statuses) {
        synchronized (lock) {
          System.out.println(++childTaskCompletedNum + " container completed");
          try {
            Thread.sleep(10_000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
          // notify main thread when all child tasks are completed
          if (childTaskCompletedNum == childTaskNum) {
            lock.notify();
          }
        }
      }
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
      try {
        for (Container container : containers) {
          System.out.println("container allocated, Node=" + container.getNodeHttpAddress());
          // build AM<->NM client and start container
          Map<String, String> env = new HashMap<>();
          StringBuilder classPathEnv =
              new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                  .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                  .append("./*");
          for (String c :
              conf.getStrings(
                  YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                  YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
          }
          env.put("CLASSPATH", classPathEnv.toString());
          List<String> commands = new ArrayList<>();
          int index = allocatedContainerNum.getAndIncrement();
          String extraJvmOpts = conf.get(Constants.KEY_CONTAINER_EXTRA_JVM_OPTS, "");
          commands.add(
              ApplicationConstants.Environment.JAVA_HOME.$$()
                  + "/bin/java  "
                  + " -Djava.specification.version="
                  + System.getProperty("java.specification.version")
                  + " "
                  + extraJvmOpts
                  + " "
                  + UniffleTask.class.getName()
                  + " -D"
                  + Constants.KEY_CONTAINER_INDEX
                  + "="
                  + index);

          Map<String, LocalResource> localResources =
              new HashMap<String, LocalResource>() {
                {
                  String[] extraJarPathList = conf.getStrings(Constants.KEY_EXTRA_JAR_PATH_LIST);
                  if (extraJarPathList != null) {
                    for (String extraJarPath : extraJarPathList) {
                      Path path = new Path(extraJarPath);
                      String name = path.getName();
                      put(name, Utils.addHdfsToResource(conf, path));
                    }
                  }
                }
              };
          ContainerLaunchContext containerLaunchContext =
              ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null);
          // request nm to start container
          nmClientAsync.startContainerAsync(container, containerLaunchContext);
          System.out.println(index + ": container started, Node=" + container.getNodeHttpAddress());
          System.out.println("containerLaunchContext: " + containerLaunchContext);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    @Override
    public void onContainersUpdated(List<UpdatedContainer> containers) {}

    @Override
    public void onShutdownRequest() {
      System.out.println("RM: shutdown request");
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {}

    @Override
    public float getProgress() {
      return 0;
    }

    @Override
    public void onError(Throwable e) {
      e.printStackTrace();
    }
  }
}
