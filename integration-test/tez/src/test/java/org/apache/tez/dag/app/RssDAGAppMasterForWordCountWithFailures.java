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

package org.apache.tez.dag.app;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.AsyncDispatcher;
import org.apache.tez.common.TezClassLoader;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.VersionInfo;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventAttemptFailed;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.rm.node.AMNodeEventStateChanged;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.runtime.api.TaskFailureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.RssException;

import static org.apache.tez.common.RssTezConfig.RSS_AVOID_RECOMPUTE_SUCCEEDED_TASK;
import static org.apache.tez.common.RssTezConfig.RSS_AVOID_RECOMPUTE_SUCCEEDED_TASK_DEFAULT;
import static org.apache.tez.dag.api.TezConfiguration.TEZ_AM_NODE_UNHEALTHY_RESCHEDULE_TASKS;
import static org.apache.tez.dag.api.TezConfiguration.TEZ_AM_NODE_UNHEALTHY_RESCHEDULE_TASKS_DEFAULT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/*
 * RssDAGAppMasterForWordCountWithFailures is only used for TezWordCountWithFailuresTest.
 * We want to simulate that some task have succeeded, but the node which these task have run is label as black list.
 * Then we will verify whether these task is recompute or not.
 *
 * Two test mode are supported:
 * (a) testMode 0
 * The test example is WordCount. The parallelism of Tokenizer is 5, it means at lease one node run more than one
 * container. Here if a task succeeded in node1, will kill the next container runs on node1. Because
 * maxtaskfailures.per.node is set to 1, so the node1 will labeled as black list, then verify whether the succeeded
 * task is recomputed or not.
 *
 * (b) testMode 1
 * The test example is WordCount. The parallelism of Tokenizer is 5, it means at lease one node run more than one
 * container. Here if a task succeeded in node1, then will label node1 as DECOMMISSIONED. Then verify whether
 * the succeeded task is recomputed or not.
 * */
public class RssDAGAppMasterForWordCountWithFailures extends RssDAGAppMaster {

  private static final Logger LOG =
      LoggerFactory.getLogger(RssDAGAppMasterForWordCountWithFailures.class);

  private final int testMode;

  public RssDAGAppMasterForWordCountWithFailures(
      ApplicationAttemptId applicationAttemptId,
      ContainerId containerId,
      String nmHost,
      int nmPort,
      int nmHttpPort,
      Clock clock,
      long appSubmitTime,
      boolean isSession,
      String workingDirectory,
      String[] localDirs,
      String[] logDirs,
      String clientVersion,
      Credentials credentials,
      String jobUserName,
      DAGProtos.AMPluginDescriptorProto pluginDescriptorProto,
      int testMode) {
    super(
        applicationAttemptId,
        containerId,
        nmHost,
        nmPort,
        nmHttpPort,
        clock,
        appSubmitTime,
        isSession,
        workingDirectory,
        localDirs,
        logDirs,
        clientVersion,
        credentials,
        jobUserName,
        pluginDescriptorProto);
    this.testMode = testMode;
  }

  @Override
  public synchronized void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    overrideTaskAttemptEventDispatcher();
  }

  public static void main(String[] args) {
    int testMode = 0;
    try {
      // We use trick way to introduce RssDAGAppMaster by the config tez.am.launch.cmd-opts.
      // It means some property which is set by command line will be ingored, so we must reload it.
      boolean sessionModeCliOption = false;
      for (int i = 0; i < args.length; i++) {
        if (args[i].startsWith("-D")) {
          String[] property = args[i].split("=");
          if (property.length < 2) {
            System.setProperty(property[0].substring(2), "");
          } else {
            System.setProperty(property[0].substring(2), property[1]);
          }
        } else if (args[i].contains("--session") || args[i].contains("-s")) {
          sessionModeCliOption = true;
        } else if (args[i].startsWith("--testMode")) {
          testMode = Integer.parseInt(args[i].substring(10));
        }
      }
      // Load the log4j config is only init in static code block of LogManager, so we must
      // reconfigure.
      reconfigureLog4j();

      // Install the tez class loader, which can be used add new resources
      TezClassLoader.setupTezClassLoader();
      Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
      final String pid = System.getenv().get("JVM_PID");
      String containerIdStr = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
      String appSubmitTimeStr = System.getenv(ApplicationConstants.APP_SUBMIT_TIME_ENV);
      String clientVersion = System.getenv(TezConstants.TEZ_CLIENT_VERSION_ENV);
      if (clientVersion == null) {
        clientVersion = VersionInfo.UNKNOWN;
      }

      Objects.requireNonNull(
          appSubmitTimeStr, ApplicationConstants.APP_SUBMIT_TIME_ENV + " is null");

      ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
      ApplicationAttemptId applicationAttemptId = containerId.getApplicationAttemptId();

      String jobUserName = System.getenv(ApplicationConstants.Environment.USER.name());

      LOG.info(
          "Creating RssDAGAppMaster for "
              + "applicationId="
              + applicationAttemptId.getApplicationId()
              + ", attemptNum="
              + applicationAttemptId.getAttemptId()
              + ", AMContainerId="
              + containerId
              + ", jvmPid="
              + pid
              + ", userFromEnv="
              + jobUserName
              + ", cliSessionOption="
              + sessionModeCliOption
              + ", pwd="
              + System.getenv(ApplicationConstants.Environment.PWD.name())
              + ", localDirs="
              + System.getenv(ApplicationConstants.Environment.LOCAL_DIRS.name())
              + ", logDirs="
              + System.getenv(ApplicationConstants.Environment.LOG_DIRS.name()));

      Configuration conf = new Configuration(new YarnConfiguration());

      DAGProtos.ConfigurationProto confProto =
          TezUtilsInternal.readUserSpecifiedTezConfiguration(
              System.getenv(ApplicationConstants.Environment.PWD.name()));
      TezUtilsInternal.addUserSpecifiedTezConfiguration(conf, confProto.getConfKeyValuesList());

      DAGProtos.AMPluginDescriptorProto amPluginDescriptorProto = null;
      if (confProto.hasAmPluginDescriptor()) {
        amPluginDescriptorProto = confProto.getAmPluginDescriptor();
      }

      UserGroupInformation.setConfiguration(conf);
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();

      TezUtilsInternal.setSecurityUtilConfigration(LOG, conf);

      String nodeHostString = System.getenv(ApplicationConstants.Environment.NM_HOST.name());
      String nodePortString = System.getenv(ApplicationConstants.Environment.NM_PORT.name());
      String nodeHttpPortString =
          System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.name());
      long appSubmitTime = Long.parseLong(appSubmitTimeStr);
      RssDAGAppMasterForWordCountWithFailures appMaster =
          new RssDAGAppMasterForWordCountWithFailures(
              applicationAttemptId,
              containerId,
              nodeHostString,
              Integer.parseInt(nodePortString),
              Integer.parseInt(nodeHttpPortString),
              new SystemClock(),
              appSubmitTime,
              sessionModeCliOption,
              System.getenv(ApplicationConstants.Environment.PWD.name()),
              TezCommonUtils.getTrimmedStrings(
                  System.getenv(ApplicationConstants.Environment.LOCAL_DIRS.name())),
              TezCommonUtils.getTrimmedStrings(
                  System.getenv(ApplicationConstants.Environment.LOG_DIRS.name())),
              clientVersion,
              credentials,
              jobUserName,
              amPluginDescriptorProto,
              testMode);
      ShutdownHookManager.get()
          .addShutdownHook(
              new RssDAGAppMaster.RssDAGAppMasterShutdownHook(appMaster),
              RSS_SHUTDOWN_HOOK_PRIORITY);

      // log the system properties
      if (LOG.isInfoEnabled()) {
        String systemPropsToLog = TezCommonUtils.getSystemPropertiesToLog(conf);
        if (systemPropsToLog != null) {
          LOG.info(systemPropsToLog);
        }
      }

      LOG.info(
          "recompute is {}, reschedule is {}",
          conf.getBoolean(
              RSS_AVOID_RECOMPUTE_SUCCEEDED_TASK, RSS_AVOID_RECOMPUTE_SUCCEEDED_TASK_DEFAULT),
          conf.getBoolean(
              TEZ_AM_NODE_UNHEALTHY_RESCHEDULE_TASKS,
              TEZ_AM_NODE_UNHEALTHY_RESCHEDULE_TASKS_DEFAULT));
      if (conf.getBoolean(
              RSS_AVOID_RECOMPUTE_SUCCEEDED_TASK, RSS_AVOID_RECOMPUTE_SUCCEEDED_TASK_DEFAULT)
          && conf.getBoolean(
              TEZ_AM_NODE_UNHEALTHY_RESCHEDULE_TASKS,
              TEZ_AM_NODE_UNHEALTHY_RESCHEDULE_TASKS_DEFAULT)) {
        LOG.info(
            "When rss.avoid.recompute.succeeded.task is enabled, "
                + "we can not rescheduler succeeded task on unhealthy node");
        conf.setBoolean(TEZ_AM_NODE_UNHEALTHY_RESCHEDULE_TASKS, false);
      }
      initAndStartAppMaster(appMaster, conf);
    } catch (Throwable t) {
      LOG.error("Error starting RssDAGAppMaster", t);
      System.exit(1);
    }
  }

  public void overrideTaskAttemptEventDispatcher()
      throws NoSuchFieldException, IllegalAccessException {
    AsyncDispatcher dispatcher = (AsyncDispatcher) this.getDispatcher();
    Field field = dispatcher.getClass().getDeclaredField("eventHandlers");
    field.setAccessible(true);
    Map<Class<? extends Enum>, EventHandler> eventHandlers =
        (Map<Class<? extends Enum>, EventHandler>) field.get(dispatcher);
    eventHandlers.put(
        TaskAttemptEventType.class, new RssTaskAttemptEventDispatcher(this.getConfig()));
  }

  private class RssTaskAttemptEventDispatcher implements EventHandler<TaskAttemptEvent> {

    Map<NodeId, Integer> succeed = new HashMap<>();
    boolean killed = false;
    boolean avoidRecompute;

    RssTaskAttemptEventDispatcher(Configuration conf) {
      avoidRecompute =
          conf.getBoolean(
              RSS_AVOID_RECOMPUTE_SUCCEEDED_TASK, RSS_AVOID_RECOMPUTE_SUCCEEDED_TASK_DEFAULT);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskAttemptEvent event) {
      DAG dag = getContext().getCurrentDAG();
      int eventDagIndex = event.getTaskAttemptID().getTaskID().getVertexID().getDAGId().getId();
      if (dag == null || eventDagIndex != dag.getID().getId()) {
        return; // event not relevant any more
      }
      Task task =
          dag.getVertex(event.getTaskAttemptID().getTaskID().getVertexID())
              .getTask(event.getTaskAttemptID().getTaskID());
      TaskAttempt attempt = task.getAttempt(event.getTaskAttemptID());

      LOG.info("handle task attempt event: {}", event);
      if (avoidRecompute) {
        if (attempt.getState() == TaskAttemptState.SUCCEEDED
            && event.getType() == TaskAttemptEventType.TA_NODE_FAILED) {
          LOG.info(
              "We should not recompute the succeeded task attempt, though taskattempt {} recieved event {}",
              attempt,
              event);
          return;
        }
      }
      ((EventHandler<TaskAttemptEvent>) attempt).handle(event);
      // For Tokenizer, record the first succeeded task and its node. When next task runs on this
      // node, will kill this task or label this node as unhealthy.
      int vertexId = attempt.getVertexID().getId();
      if (vertexId == 0) {
        if (attempt.getState() == TaskAttemptState.SUCCEEDED) {
          NodeId nodeId = attempt.getAssignedContainer().getNodeId();
          if (!succeed.containsKey(nodeId)) {
            succeed.put(nodeId, 1);
          } else {
            succeed.put(nodeId, succeed.get(nodeId) + 1);
          }
        } else if (attempt.getState() == TaskAttemptState.RUNNING) {
          NodeId nodeId = attempt.getAssignedContainer().getNodeId();
          if (succeed.getOrDefault(nodeId, 0) == 1 && !killed) {
            if (testMode == 0) {
              TaskAttemptEventAttemptFailed eventAttemptFailed =
                  new TaskAttemptEventAttemptFailed(
                      attempt.getID(),
                      TaskAttemptEventType.TA_FAILED,
                      TaskFailureType.NON_FATAL,
                      "Triggerd by " + this.getClass().getName(),
                      TaskAttemptTerminationCause.CONTAINER_LAUNCH_FAILED);
              LOG.info(
                  "Killing running task attempt: {} at node: {}",
                  attempt,
                  attempt.getAssignedContainer().getNodeId());
              ((EventHandler<TaskAttemptEvent>) attempt).handle(eventAttemptFailed);
              dag.getEventHandler().handle(eventAttemptFailed);
            } else if (testMode == 1) {
              NodeReport nodeReport = mock(NodeReport.class);
              when(nodeReport.getNodeState()).thenReturn(NodeState.DECOMMISSIONED);
              when(nodeReport.getNodeId()).thenReturn(nodeId);
              LOG.info(
                  "Label the node {} as DECOMMISSIONED."
                      + attempt.getAssignedContainer().getNodeId());
              dag.getEventHandler().handle(new AMNodeEventStateChanged(nodeReport, 0));
            } else {
              throw new RssException("testMode " + testMode + " is not supported!");
            }
            killed = true;
          }
        }
      }
    }
  }
}
