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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UniffleClientSimOnYarnClient {

  private static final Logger LOG = LoggerFactory.getLogger(UniffleClientSimOnYarnClient.class);
  private final Configuration conf;

  public UniffleClientSimOnYarnClient(Configuration conf) {
    this.conf = conf;
  }

  public static void main(String[] args) {
    Configuration conf = new Configuration();
    // Load configuration from parameters
    HadoopConfigApp hadoopConfigApp = new HadoopConfigApp(conf);
    int exitCode = hadoopConfigApp.run(args);
    if (exitCode != 0) {
      System.exit(exitCode);
    }
    UniffleClientSimOnYarnClient client = new UniffleClientSimOnYarnClient(conf);
    try {
      client.run(hadoopConfigApp.getLocalConf());
    } catch (Exception e) {
      LOG.error("client run exception , please check log file.", e);
    }
  }

  public void run(Map<String, String> localConf) throws Exception {
    // YarnClient to talk to ResourceManager
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    // request RM to create an application
    YarnClientApplication application = yarnClient.createApplication();
    // prepare app submission context
    ApplicationSubmissionContext applicationSubmissionContext =
        application.getApplicationSubmissionContext();
    // get application id and store to conf for am and sub-tasks to use
    ApplicationId appId = applicationSubmissionContext.getApplicationId();
    LOG.info("appId: {}", appId);
    localConf.put(Constants.KEY_YARN_APP_ID, appId.toString());
    applicationSubmissionContext.setApplicationName("UniffleClientSimOnYarn");

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  System.out.println("Received Ctrl+C, terminating application...");

                  try {
                    yarnClient.killApplication(appId);
                    System.out.println("Application killed: " + appId);
                  } catch (Exception e) {
                    e.printStackTrace();
                  }

                  try {
                    ApplicationReport report = yarnClient.getApplicationReport(appId);
                    System.out.println("Application status: " + report.getYarnApplicationState());
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
                }));

    // copy jar to hdfs
    String jarLocalPathStr = Utils.getCurrentJarPath(UniffleClientSimOnYarnClient.class);
    System.out.println("jarLocalPath: " + jarLocalPathStr);
    if (jarLocalPathStr == null || !jarLocalPathStr.endsWith(".jar")) {
      String cmdline = System.getProperty("sun.java.command");
      if (cmdline != null) {
        String[] parts = cmdline.split("\\s+");
        if (parts.length > 1 && parts[1].endsWith(".jar")) {
          jarLocalPathStr = parts[1];
          System.out.println(
              "jarLocalPath: " + jarLocalPathStr + " updated it from command line: " + cmdline);
        }
      }
    }
    Path jarLocalPath = new Path(jarLocalPathStr);
    Path jarHdfsPath =
        Utils.copyToHdfs(conf, jarLocalPath, Utils.getHdfsDestPath(conf, jarLocalPath.getName()));
    System.out.println("jarHdfsPath: " + jarHdfsPath);
    localConf.put(
        Constants.KEY_EXTRA_JAR_PATH_LIST,
        Utils.isBlank(localConf.get(Constants.KEY_EXTRA_JAR_PATH_LIST))
            ? jarHdfsPath.toString()
            : localConf.get(Constants.KEY_EXTRA_JAR_PATH_LIST) + "," + jarHdfsPath);
    conf.set(Constants.KEY_EXTRA_JAR_PATH_LIST, localConf.get(Constants.KEY_EXTRA_JAR_PATH_LIST));

    // write job configuration to hdfs
    Path jobConfHdfsPath = Utils.getHdfsDestPath(conf, Constants.JOB_CONF_NAME);
    localConf.put(
        Constants.KEY_EXTRA_JAR_PATH_LIST,
        Utils.isBlank(localConf.get(Constants.KEY_EXTRA_JAR_PATH_LIST))
            ? jobConfHdfsPath.toString()
            : localConf.get(Constants.KEY_EXTRA_JAR_PATH_LIST) + "," + jobConfHdfsPath);
    conf.set(Constants.KEY_EXTRA_JAR_PATH_LIST, localConf.get(Constants.KEY_EXTRA_JAR_PATH_LIST));
    Utils.writeStringToHdfs(
        conf, HadoopConfigApp.writeConfigurationToString(localConf), jobConfHdfsPath);

    // prepare local resources
    Map<String, LocalResource> localResources = new HashMap<>();
    String[] extraJarPathList = conf.getStrings(Constants.KEY_EXTRA_JAR_PATH_LIST);
    if (extraJarPathList != null) {
      for (String extraJarPath : extraJarPathList) {
        Path path = new Path(extraJarPath);
        String name = path.getName();
        localResources.put(name, Utils.addHdfsToResource(conf, path));
      }
    }
    // prepare environment classpath
    Map<String, String> env = new HashMap<>();
    // the dependencies of am
    StringBuilder classPathEnv =
        new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
            .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
            .append("./*");
    // yarn dependencies
    for (String c :
        conf.getStrings(
            YarnConfiguration.YARN_APPLICATION_CLASSPATH,
            YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPathEnv.append(c.trim());
    }
    env.put("CLASSPATH", classPathEnv.toString());

    String extraJvmOpts = conf.get(Constants.KEY_AM_EXTRA_JVM_OPTS, "");
    // prepare launch command to run am
    List<String> commands =
        new ArrayList<String>() {
          {
            add(
                ApplicationConstants.Environment.JAVA_HOME.$$()
                    + "/bin/java "
                    + " -Djava.specification.version="
                    + System.getProperty("java.specification.version")
                    + " "
                    + extraJvmOpts
                    + " "
                    + UniffleClientSimOnYarnAppMaster.class.getName());
          }
        };

    // create am container and submit the application
    ContainerLaunchContext amContainer =
        ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null);
    // prepare the am container context
    applicationSubmissionContext.setAMContainerSpec(amContainer);
    int memory = conf.getInt(Constants.KEY_AM_MEMORY, 4096);
    int vCores = conf.getInt(Constants.KEY_AM_VCORES, 8);
    applicationSubmissionContext.setResource(Resource.newInstance(memory, vCores));
    String queueName = conf.get(Constants.KEY_QUEUE_NAME, YarnConfiguration.DEFAULT_QUEUE_NAME);
    if (!YarnConfiguration.DEFAULT_QUEUE_NAME.equals(queueName)) {
      applicationSubmissionContext.setQueue(queueName);
    }
    System.out.println(applicationSubmissionContext);
    yarnClient.submitApplication(applicationSubmissionContext);

    // monitor application progress
    for (; ; ) {
      ApplicationReport applicationReport = yarnClient.getApplicationReport(appId);
      YarnApplicationState state = applicationReport.getYarnApplicationState();
      FinalApplicationStatus status = applicationReport.getFinalApplicationStatus();
      if (state.equals(YarnApplicationState.FINISHED)) {
        if (status.equals(FinalApplicationStatus.SUCCEEDED)) {
          LOG.info("SUCCESSFUL FINISH.");
          break;
        } else {
          LOG.error("FINISHED WITH ERROR.");
          break;
        }
      } else if (state.equals(YarnApplicationState.FAILED)
          || state.equals(YarnApplicationState.KILLED)) {
        LOG.error("Application failed with state: {}", state);
        break;
      }
      LOG.info("running...");
      Thread.sleep(5000);
    }
  }
}
