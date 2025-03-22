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

import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.client.TezApiVersionInfo;
import org.apache.tez.common.AsyncDispatcher;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.security.ACLManager;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.history.HistoryEventHandler;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.hadoop.shim.DefaultHadoopShim;
import org.apache.tez.hadoop.shim.HadoopShim;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.input.RssOrderedGroupedKVInput;
import org.apache.tez.runtime.library.input.RssUnorderedKVInput;
import org.apache.tez.runtime.library.output.RssOrderedPartitionedKVOutput;
import org.apache.tez.runtime.library.output.RssUnorderedKVOutput;
import org.apache.tez.runtime.library.output.RssUnorderedPartitionedKVOutput;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.tez.common.RssTezConfig.RSS_AM_SHUFFLE_MANAGER_ADDRESS;
import static org.apache.tez.common.RssTezConfig.RSS_AM_SHUFFLE_MANAGER_PORT;
import static org.apache.tez.common.RssTezConfig.RSS_SHUFFLE_DESTINATION_VERTEX_ID;
import static org.apache.tez.common.RssTezConfig.RSS_SHUFFLE_SOURCE_VERTEX_ID;
import static org.apache.tez.common.RssTezConfig.RSS_STORAGE_TYPE;
import static org.apache.tez.runtime.library.api.TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RssDAGAppMasterTest {

  private static final File TEST_DIR =
      new File(
              System.getProperty("test.build.data", System.getProperty("java.io.tmpdir")),
              RssDAGAppMasterTest.class.getSimpleName())
          .getAbsoluteFile();

  static Stream<Arguments> clientTypeProvider() {
    return Stream.of(
        Arguments.of(ClientType.GRPC.name()), Arguments.of(ClientType.GRPC_NETTY.name()));
  }

  @Test
  public void testDagStateChangeCallback() throws Exception {
    // 1 Init and mock some basic module
    AppContext appContext = mock(AppContext.class);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1);
    HadoopShim defaultShim = new DefaultHadoopShim();
    when(appContext.getHadoopShim()).thenReturn(defaultShim);
    when(appContext.getApplicationID()).thenReturn(appAttemptId.getApplicationId());
    ClusterInfo clusterInfo = new ClusterInfo();
    clusterInfo.setMaxContainerCapability(
        Resource.newInstance(Integer.MAX_VALUE, Integer.MAX_VALUE));
    when(appContext.getClusterInfo()).thenReturn(clusterInfo);
    HistoryEventHandler historyEventHandler = mock(HistoryEventHandler.class);
    doReturn(historyEventHandler).when(appContext).getHistoryHandler();
    ACLManager aclManager = new ACLManager("amUser");
    doReturn(aclManager).when(appContext).getAMACLManager();
    RssDAGAppMaster appMaster = mock(RssDAGAppMaster.class);
    TezRemoteShuffleManager shuffleManager = mock(TezRemoteShuffleManager.class);
    InetSocketAddress address = NetUtils.createSocketAddrForHost("host", 0);
    when(shuffleManager.getAddress()).thenReturn(address);
    when(shuffleManager.unregisterShuffleByDagId(any())).thenReturn(true);
    when(appMaster.getTezRemoteShuffleManager()).thenReturn(shuffleManager);
    Configuration clientConf = new Configuration(false);
    clientConf.set(RSS_STORAGE_TYPE, StorageType.MEMORY_LOCALFILE_HDFS.name());
    clientConf.set("tez.config1", "value1");
    clientConf.set("config2", "value2");
    Map<String, String> dynamicConf = new HashMap<>();
    dynamicConf.put(RSS_STORAGE_TYPE, StorageType.LOCALFILE.name());
    dynamicConf.put("tez.config3", "value3");
    when(appMaster.getClusterClientConf()).thenReturn(dynamicConf);
    when(appMaster.getConfig()).thenReturn(clientConf);

    // 2 init dispatcher
    AsyncDispatcher dispatcher = new AsyncDispatcher("core");

    // 3 init dag
    Configuration conf = new Configuration();
    DAG dag = createDAG("test", conf);
    TezDAGID dagId = TezDAGID.getInstance(appAttemptId.getApplicationId(), 1);
    DAGProtos.DAGPlan dagPlan = dag.createDag(conf, null, null, null, false, null, null);
    DAGImpl dagImpl =
        new DAGImpl(
            dagId,
            conf,
            dagPlan,
            dispatcher.getEventHandler(),
            null,
            new Credentials(),
            new SystemClock(),
            "user",
            null,
            appContext);
    when(appContext.getCurrentDAG()).thenReturn(dagImpl);

    // 4 register call back function
    RssDAGAppMaster.registerStateEnteredCallback(dagImpl, appMaster);

    // 5 register DAGEvent, init and start dispatcher
    dispatcher.register(DAGEventType.class, dagImpl);
    dispatcher.init(conf);
    dispatcher.start();

    // 6 send DAG_INIT to dispatcher
    dispatcher.getEventHandler().handle(new DAGEvent(dagImpl.getID(), DAGEventType.DAG_INIT));

    // 7 wait DAGImpl transient to INITED state
    await().atMost(2, TimeUnit.SECONDS).until(() -> dagImpl.getState().equals(DAGState.INITED));

    // 8 verify I/O for vertexImpl
    verifyOutput(dagImpl, "vertex1", RssOrderedPartitionedKVOutput.class.getName(), 0, 1);
    verifyInput(dagImpl, "vertex2", RssOrderedGroupedKVInput.class.getName(), 0, 1);
    verifyOutput(dagImpl, "vertex2", RssUnorderedKVOutput.class.getName(), 1, 2);
    verifyInput(dagImpl, "vertex3", RssUnorderedKVInput.class.getName(), 1, 2);
    verifyOutput(dagImpl, "vertex3", RssUnorderedPartitionedKVOutput.class.getName(), 2, 3);
    verifyInput(dagImpl, "vertex4", RssUnorderedKVInput.class.getName(), 2, 3);

    // 9 send INTERNAL_ERROR to dispatcher
    dispatcher.getEventHandler().handle(new DAGEvent(dagImpl.getID(), DAGEventType.INTERNAL_ERROR));

    // 10 wait DAGImpl transient to ERROR state
    await().atMost(2, TimeUnit.SECONDS).until(() -> dagImpl.getState().equals(DAGState.ERROR));

    // 11 verify
    verify(shuffleManager, times(1)).unregisterShuffleByDagId(dagId);
  }

  private static void verifyCommonAssertions(
      Configuration conf, int expectedSourceVertexId, int expectedDestinationVertexId) {
    Assertions.assertEquals("host", conf.get(RSS_AM_SHUFFLE_MANAGER_ADDRESS));
    Assertions.assertEquals(0, conf.getInt(RSS_AM_SHUFFLE_MANAGER_PORT, -1));
    Assertions.assertEquals(StorageType.LOCALFILE.name(), conf.get(RSS_STORAGE_TYPE));
    Assertions.assertEquals("value1", conf.get("tez.config1"));
    Assertions.assertEquals("value3", conf.get("tez.config3"));
    Assertions.assertNull(conf.get("tez.config2"));
    Assertions.assertEquals(expectedSourceVertexId, conf.getInt(RSS_SHUFFLE_SOURCE_VERTEX_ID, -1));
    Assertions.assertEquals(
        expectedDestinationVertexId, conf.getInt(RSS_SHUFFLE_DESTINATION_VERTEX_ID, -1));
  }

  public static void verifyInput(
      DAGImpl dag,
      String name,
      String expectedInputClassName,
      int expectedSourceVertexId,
      int expectedDestinationVertexId)
      throws Exception {
    List<InputSpec> inputSpecs = dag.getVertex(name).getInputSpecList(0);
    Assertions.assertEquals(1, inputSpecs.size());
    Assertions.assertEquals(
        expectedInputClassName, inputSpecs.get(0).getInputDescriptor().getClassName());
    UserPayload payload = inputSpecs.get(0).getInputDescriptor().getUserPayload();
    Configuration conf = TezUtils.createConfFromUserPayload(payload);
    verifyCommonAssertions(conf, expectedSourceVertexId, expectedDestinationVertexId);
    // TEZ_RUNTIME_IFILE_READAHEAD_BYTES is in getConfigurationKeySet, so the config from client
    // should deliver
    // to Input/Output. But tez.config.from.client is not in getConfigurationKeySet, so the config
    // from client
    // should not deliver to Input/Output.
    Assertions.assertEquals(12345, conf.getInt(TEZ_RUNTIME_IFILE_READAHEAD_BYTES, -1));
    Assertions.assertNull(conf.get("tez.config.from.client"));
  }

  public static void verifyOutput(
      DAGImpl dag,
      String name,
      String expectedOutputClassName,
      int expectedSourceVertexId,
      int expectedDestinationVertexId)
      throws Exception {
    List<OutputSpec> outputSpecs = dag.getVertex(name).getOutputSpecList(0);
    Assertions.assertEquals(1, outputSpecs.size());
    Assertions.assertEquals(
        expectedOutputClassName, outputSpecs.get(0).getOutputDescriptor().getClassName());
    UserPayload payload = outputSpecs.get(0).getOutputDescriptor().getUserPayload();
    Configuration conf = TezUtils.createConfFromUserPayload(payload);
    verifyCommonAssertions(conf, expectedSourceVertexId, expectedDestinationVertexId);
  }

  private static DAG createDAG(String dagName, Configuration conf) {
    conf.setInt(TEZ_RUNTIME_IFILE_READAHEAD_BYTES, 12345);
    conf.set("tez.config.from.client", "value.from.client");

    DataSourceDescriptor dummyInput =
        DataSourceDescriptor.create(
            InputDescriptor.create("dummyclass"), InputInitializerDescriptor.create(""), null);

    EdgeManagerPluginDescriptor cpEdgeManager =
        EdgeManagerPluginDescriptor.create(DummyProductEdgeManager.class.getName());

    Vertex vertex1 = Vertex.create("vertex1", ProcessorDescriptor.create(DummyOp.class.getName()));
    Vertex vertex2 = Vertex.create("vertex2", ProcessorDescriptor.create(DummyOp.class.getName()));
    Vertex vertex3 = Vertex.create("vertex3", ProcessorDescriptor.create(DummyOp.class.getName()));
    Vertex vertex4 = Vertex.create("vertex4", ProcessorDescriptor.create(DummyOp.class.getName()));

    vertex1.addDataSource("dummyInput", dummyInput);
    OrderedPartitionedKVEdgeConfig edgeConf12 =
        OrderedPartitionedKVEdgeConfig.newBuilder(
                NullWritable.class.getName(),
                NullWritable.class.getName(),
                HashPartitioner.class.getName())
            .setFromConfiguration(conf)
            .build();
    UnorderedKVEdgeConfig edgeConf23 =
        UnorderedKVEdgeConfig.newBuilder(NullWritable.class.getName(), NullWritable.class.getName())
            .setFromConfiguration(conf)
            .build();
    UnorderedPartitionedKVEdgeConfig edgeConf34 =
        UnorderedPartitionedKVEdgeConfig.newBuilder(
                NullWritable.class.getName(),
                NullWritable.class.getName(),
                HashPartitioner.class.getName())
            .setFromConfiguration(conf)
            .build();

    DAG dag = DAG.create(dagName);
    dag.addVertex(vertex1)
        .addVertex(vertex2)
        .addVertex(vertex3)
        .addVertex(vertex4)
        .addEdge(
            Edge.create(
                vertex1, vertex2, edgeConf12.createDefaultCustomEdgeProperty(cpEdgeManager)))
        .addEdge(
            Edge.create(
                vertex2, vertex3, edgeConf23.createDefaultCustomEdgeProperty(cpEdgeManager)))
        .addEdge(
            Edge.create(
                vertex3, vertex4, edgeConf34.createDefaultCustomEdgeProperty(cpEdgeManager)));
    return dag;
  }

  public static class DummyOp extends SimpleProcessor {

    public DummyOp(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() {}
  }

  public static class DummyProductEdgeManager extends EdgeManagerPluginOnDemand {

    public DummyProductEdgeManager(EdgeManagerPluginContext context) {
      super(context);
    }

    @Override
    public void initialize() throws Exception {}

    @Override
    public void prepareForRouting() throws Exception {}

    @Override
    public int getNumDestinationTaskPhysicalInputs(int destinationTaskIndex) throws Exception {
      return 1;
    }

    @Override
    public int getNumSourceTaskPhysicalOutputs(int sourceTaskIndex) throws Exception {
      return 1;
    }

    @Override
    public int getNumDestinationConsumerTasks(int sourceTaskIndex) throws Exception {
      return 1;
    }

    @Override
    public int routeInputErrorEventToSource(
        int destinationTaskIndex, int destinationFailedInputIndex) throws Exception {
      return 1;
    }

    @Nullable @Override
    public EventRouteMetadata routeDataMovementEventToDestination(
        int sourceTaskIndex, int sourceOutputIndex, int destinationTaskIndex) throws Exception {
      return null;
    }

    @Nullable @Override
    public CompositeEventRouteMetadata routeCompositeDataMovementEventToDestination(
        int sourceTaskIndex, int destinationTaskIndex) throws Exception {
      return null;
    }

    @Nullable @Override
    public EventRouteMetadata routeInputSourceTaskFailedEventToDestination(
        int sourceTaskIndex, int destinationTaskIndex) throws Exception {
      return null;
    }
  }

  @ParameterizedTest
  @MethodSource("clientTypeProvider")
  public void testFetchRemoteStorageFromDynamicConf(String clientType) throws Exception {
    final ApplicationId appId = ApplicationId.newInstance(1, 1);
    final ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    TezConfiguration conf = new TezConfiguration();

    Credentials amCreds = new Credentials();
    JobTokenSecretManager jtsm = new JobTokenSecretManager();
    JobTokenIdentifier identifier = new JobTokenIdentifier(new Text(appId.toString()));
    Token<JobTokenIdentifier> sessionToken = new Token<>(identifier, jtsm);
    sessionToken.setService(identifier.getJobId());
    TokenCache.setSessionToken(sessionToken, amCreds);

    FileSystem fs = FileSystem.getLocal(conf);
    FSDataOutputStream sessionJarsPBOutStream =
        TezCommonUtils.createFileForAM(
            fs, new Path(TEST_DIR.toString(), TezConstants.TEZ_AM_LOCAL_RESOURCES_PB_FILE_NAME));
    DAGProtos.PlanLocalResourcesProto.getDefaultInstance().writeDelimitedTo(sessionJarsPBOutStream);
    sessionJarsPBOutStream.close();

    RssDAGAppMaster appMaster =
        new RssDAGAppMaster(
            appAttemptId,
            ContainerId.newInstance(appAttemptId, 1),
            "127.0.0.1",
            0,
            0,
            new SystemClock(),
            1,
            true,
            TEST_DIR.toString(),
            new String[] {TEST_DIR.toString()},
            new String[] {TEST_DIR.toString()},
            new TezApiVersionInfo().getVersion(),
            amCreds,
            "someuser",
            null);
    appMaster.setShuffleWriteClient(new FakedShuffleWriteClient(1, clientType));
    appMaster.init(conf);

    Configuration mergedConf = new Configuration(false);
    RssTezUtils.applyDynamicClientConf(mergedConf, appMaster.getClusterClientConf());
    Assertions.assertEquals(4, mergedConf.size());
    Assertions.assertEquals("hdfs://ns1/rss/", mergedConf.get("tez.rss.remote.storage.path"));
    Assertions.assertEquals(
        "key1=value1,key2=value2", mergedConf.get("tez.rss.remote.storage.conf"));
    Assertions.assertEquals("MEMORY_LOCALFILE_HDFS", mergedConf.get("tez.rss.storage.type"));
    Assertions.assertEquals("testvalue", mergedConf.get("tez.rss.test.config"));
  }

  @ParameterizedTest
  @MethodSource("clientTypeProvider")
  public void testFetchRemoteStorageFromCoordinator(String clientType) throws Exception {
    final ApplicationId appId = ApplicationId.newInstance(1, 1);
    final ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    TezConfiguration conf = new TezConfiguration();

    Credentials amCreds = new Credentials();
    JobTokenSecretManager jtsm = new JobTokenSecretManager();
    JobTokenIdentifier identifier = new JobTokenIdentifier(new Text(appId.toString()));
    Token<JobTokenIdentifier> sessionToken = new Token<>(identifier, jtsm);
    sessionToken.setService(identifier.getJobId());
    TokenCache.setSessionToken(sessionToken, amCreds);

    FileSystem fs = FileSystem.getLocal(conf);
    FSDataOutputStream sessionJarsPBOutStream =
        TezCommonUtils.createFileForAM(
            fs, new Path(TEST_DIR.toString(), TezConstants.TEZ_AM_LOCAL_RESOURCES_PB_FILE_NAME));
    DAGProtos.PlanLocalResourcesProto.getDefaultInstance().writeDelimitedTo(sessionJarsPBOutStream);
    sessionJarsPBOutStream.close();

    RssDAGAppMaster appMaster =
        new RssDAGAppMaster(
            appAttemptId,
            ContainerId.newInstance(appAttemptId, 1),
            "127.0.0.1",
            0,
            0,
            new SystemClock(),
            1,
            true,
            TEST_DIR.toString(),
            new String[] {TEST_DIR.toString()},
            new String[] {TEST_DIR.toString()},
            new TezApiVersionInfo().getVersion(),
            amCreds,
            "someuser",
            null);
    appMaster.setShuffleWriteClient(new FakedShuffleWriteClient(2, clientType));
    appMaster.init(conf);

    Configuration mergedConf = new Configuration(false);
    RssTezUtils.applyDynamicClientConf(mergedConf, appMaster.getClusterClientConf());
    Assertions.assertEquals(4, mergedConf.size());
    Assertions.assertEquals("hdfs://ns2/rss/", mergedConf.get("tez.rss.remote.storage.path"));
    Assertions.assertEquals(
        "key11=value11,key22=value22", mergedConf.get("tez.rss.remote.storage.conf"));
    Assertions.assertEquals("MEMORY_LOCALFILE_HDFS", mergedConf.get("tez.rss.storage.type"));
    Assertions.assertEquals("testvalue", mergedConf.get("tez.rss.test.config"));
  }

  static class FakedShuffleWriteClient extends ShuffleWriteClientImpl {

    /*
     * Mode 1: rss.remote.storage.path and rss.remote.storage.conf is set by dynamic config,
     *         appMaster will use this as default remote storage path.
     * Mode 2: rss.remote.storage.path and rss.remote.storage.conf is not set by dynamic config,
     *         appMaster will fetch remote storage conf from coordinator.
     * */
    private final int mode;

    FakedShuffleWriteClient(int mode, String clientType) {
      super(
          ShuffleClientFactory.newWriteBuilder()
              .clientType(clientType)
              .retryMax(1)
              .retryIntervalMax(1)
              .heartBeatThreadNum(10)
              .replica(1)
              .replicaWrite(1)
              .replicaRead(1)
              .replicaSkipEnabled(true)
              .dataTransferPoolSize(1)
              .dataCommitPoolSize(1)
              .unregisterThreadPoolSize(1)
              .unregisterTimeSec(1)
              .unregisterRequestTimeSec(1));
      this.mode = mode;
    }

    @Override
    public void registerCoordinators(String coordinators) {}

    @Override
    public Map<String, String> fetchClientConf(int timeoutMs) {
      Map<String, String> clientConf = new HashMap<>();
      if (mode == 1) {
        clientConf.put("rss.remote.storage.path", "hdfs://ns1/rss/");
        clientConf.put("rss.remote.storage.conf", "key1=value1,key2=value2");
        clientConf.put("rss.storage.type", "MEMORY_LOCALFILE_HDFS");
        clientConf.put("rss.test.config", "testvalue");
      } else if (mode == 2) {
        clientConf.put("rss.storage.type", "MEMORY_LOCALFILE_HDFS");
        clientConf.put("rss.test.config", "testvalue");
      } else {
        throw new RssException("Wrong test mode.");
      }
      return clientConf;
    }

    @Override
    public RemoteStorageInfo fetchRemoteStorage(String appId) {
      if (mode == 2) {
        return new RemoteStorageInfo("hdfs://ns2/rss/", "key11=value11,key22=value22");
      } else {
        throw new RssException("Wrong test mode.");
      }
    }
  }
}
