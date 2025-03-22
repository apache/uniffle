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

package org.apache.uniffle.common.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.StorageType;
import org.apache.uniffle.common.rpc.ServerType;
import org.apache.uniffle.common.serializer.kryo.KryoSerializer;
import org.apache.uniffle.common.serializer.writable.WritableSerializer;
import org.apache.uniffle.common.util.RssUtils;

public class RssBaseConf extends RssConf {

  public static final ConfigOption<String> RSS_COORDINATOR_QUORUM =
      ConfigOptions.key("rss.coordinator.quorum")
          .stringType()
          .noDefaultValue()
          .withDescription("Coordinator quorum");

  public static final ConfigOption<ServerType> RPC_SERVER_TYPE =
      ConfigOptions.key("rss.rpc.server.type")
          .enumType(ServerType.class)
          .defaultValue(ServerType.GRPC_NETTY)
          .withDescription(
              "Shuffle server type, supports GRPC_NETTY, GRPC. The default value is GRPC_NETTY. We recommend using GRPC_NETTY to enable Netty on the server side for better stability and performance.");

  public static final ConfigOption<Integer> RPC_SERVER_PORT =
      ConfigOptions.key("rss.rpc.server.port")
          .intType()
          .defaultValue(19999)
          .withDescription("Shuffle server service port");

  public static final ConfigOption<Boolean> RPC_METRICS_ENABLED =
      ConfigOptions.key("rss.rpc.metrics.enabled")
          .booleanType()
          .defaultValue(true)
          .withDescription("If enable metrics for rpc connection");

  public static final ConfigOption<Integer> RPC_NETTY_PAGE_SIZE =
      ConfigOptions.key("rss.rpc.netty.pageSize")
          .intType()
          .defaultValue(4096)
          .withDescription(
              "The value of pageSize for PooledByteBufAllocator when using gRPC internal Netty on the server-side. "
                  + "This configuration will only take effect when rss.rpc.server.type is set to GRPC_NETTY.");

  public static final ConfigOption<Integer> RPC_NETTY_MAX_ORDER =
      ConfigOptions.key("rss.rpc.netty.maxOrder")
          .intType()
          .defaultValue(3)
          .withDescription(
              "The value of maxOrder for PooledByteBufAllocator when using gRPC internal Netty on the server-side. "
                  + "This configuration will only take effect when rss.rpc.server.type is set to GRPC_NETTY.");

  public static final ConfigOption<Integer> RPC_NETTY_SMALL_CACHE_SIZE =
      ConfigOptions.key("rss.rpc.netty.smallCacheSize")
          .intType()
          .defaultValue(1024)
          .withDescription(
              "The value of smallCacheSize for PooledByteBufAllocator when using gRPC internal Netty on the server-side. "
                  + "This configuration will only take effect when rss.rpc.server.type is set to GRPC_NETTY.");

  public static final ConfigOption<Integer> JETTY_HTTP_PORT =
      ConfigOptions.key("rss.jetty.http.port")
          .intType()
          .defaultValue(19998)
          .withDescription("jetty http port");

  public static final ConfigOption<Integer> JETTY_CORE_POOL_SIZE =
      ConfigOptions.key("rss.jetty.corePool.size")
          .intType()
          .defaultValue(256)
          .withDescription("jetty corePool size");

  public static final ConfigOption<Integer> JETTY_MAX_POOL_SIZE =
      ConfigOptions.key("rss.jetty.maxPool.size")
          .intType()
          .defaultValue(256)
          .withDescription("jetty max pool size");

  public static final ConfigOption<Boolean> JETTY_SSL_ENABLE =
      ConfigOptions.key("rss.jetty.ssl.enable")
          .booleanType()
          .defaultValue(false)
          .withDescription("jetty ssl enable");

  public static final ConfigOption<Integer> JETTY_HTTPS_PORT =
      ConfigOptions.key("rss.jetty.https.port")
          .intType()
          .noDefaultValue()
          .withDescription("jetty https port");

  public static final ConfigOption<String> JETTY_SSL_KEYSTORE_PATH =
      ConfigOptions.key("rss.jetty.ssl.keystore.path")
          .stringType()
          .noDefaultValue()
          .withDescription("jetty ssl keystore path");

  public static final ConfigOption<String> JETTY_SSL_KEYMANAGER_PASSWORD =
      ConfigOptions.key("rss.jetty.ssl.keymanager.password")
          .stringType()
          .noDefaultValue()
          .withDescription("jetty ssl keymanager password");

  public static final ConfigOption<String> JETTY_SSL_KEYSTORE_PASSWORD =
      ConfigOptions.key("rss.jetty.ssl.keystore.password")
          .stringType()
          .noDefaultValue()
          .withDescription("jetty ssl keystore password");

  public static final ConfigOption<String> JETTY_SSL_TRUSTSTORE_PASSWORD =
      ConfigOptions.key("rss.jetty.ssl.truststore.password")
          .stringType()
          .noDefaultValue()
          .withDescription("jetty ssl truststore password");

  public static final ConfigOption<Long> JETTY_STOP_TIMEOUT =
      ConfigOptions.key("rss.jetty.stop.timeout")
          .longType()
          .defaultValue(30 * 1000L)
          .withDescription("jetty stop timeout (ms) ");

  public static final ConfigOption<Long> JETTY_HTTP_IDLE_TIMEOUT =
      ConfigOptions.key("rss.jetty.http.idle.timeout")
          .longType()
          .defaultValue(30 * 1000L)
          .withDescription("jetty http idle timeout (ms) ");

  public static final ConfigOption<Long> RPC_MESSAGE_MAX_SIZE =
      ConfigOptions.key("rss.rpc.message.max.size")
          .longType()
          .checkValue(ConfigUtils.POSITIVE_INTEGER_VALIDATOR, "The value must be positive integer")
          .defaultValue(1024L * 1024L * 1024L)
          .withDescription("Max size of rpc message (byte)");

  public static final ConfigOption<ClientType> RSS_COORDINATOR_CLIENT_TYPE =
      ConfigOptions.key("rss.coordinator.rpc.client.type")
          .enumType(ClientType.class)
          .defaultValue(ClientType.GRPC)
          .withDescription("client type for coordinator rpc client.");

  public static final ConfigOption<StorageType> RSS_STORAGE_TYPE =
      ConfigOptions.key("rss.storage.type")
          .enumType(StorageType.class)
          .noDefaultValue()
          .withDescription("Data storage for remote shuffle service");

  public static final ConfigOption<Integer> RSS_STORAGE_DATA_REPLICA =
      ConfigOptions.key("rss.storage.data.replica")
          .intType()
          .defaultValue(1)
          .withDescription("Data replica in storage");

  public static final ConfigOption<List<String>> RSS_STORAGE_BASE_PATH =
      ConfigOptions.key("rss.storage.basePath")
          .stringType()
          .asList()
          .noDefaultValue()
          .withDescription("Common storage path for remote shuffle data");

  public static final ConfigOption<Integer> RPC_EXECUTOR_SIZE =
      ConfigOptions.key("rss.rpc.executor.size")
          .intType()
          .defaultValue(1000)
          .withDescription("Thread number for grpc to process request");
  public static final ConfigOption<Integer> RPC_EXECUTOR_QUEUE_SIZE =
      ConfigOptions.key("rss.rpc.executor.queue.size")
          .intType()
          .defaultValue(Integer.MAX_VALUE)
          .withDescription("Thread pool waiting queue size");

  public static final ConfigOption<Boolean> RSS_JVM_METRICS_VERBOSE_ENABLE =
      ConfigOptions.key("rss.jvm.metrics.verbose.enable")
          .booleanType()
          .defaultValue(true)
          .withDescription("The switch for jvm metrics verbose");

  public static final ConfigOption<Boolean> RSS_SECURITY_HADOOP_KERBEROS_ENABLE =
      ConfigOptions.key("rss.security.hadoop.kerberos.enable")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether enable visiting secured hadoop cluster.");

  public static final ConfigOption<Boolean> RSS_SECURITY_HADOOP_KERBEROS_PROXY_USER_ENABLE =
      ConfigOptions.key("rss.security.hadoop.kerberos.proxy.user.enable")
          .booleanType()
          .defaultValue(true)
          .withDescription(
              "Whether using proxy user for job user to access secured Hadoop cluster.");

  public static final ConfigOption<String> RSS_SECURITY_HADOOP_KRB5_CONF_FILE =
      ConfigOptions.key("rss.security.hadoop.kerberos.krb5-conf.file")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "The file path of krb5.conf. And only when "
                  + RSS_SECURITY_HADOOP_KERBEROS_ENABLE.key()
                  + " enabled, the option will be valid.");

  public static final ConfigOption<String> RSS_SECURITY_HADOOP_KERBEROS_KEYTAB_FILE =
      ConfigOptions.key("rss.security.hadoop.kerberos.keytab.file")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "The kerberos keytab file path. And only when "
                  + RSS_SECURITY_HADOOP_KERBEROS_ENABLE.key()
                  + " enabled, the option will be valid.");

  public static final ConfigOption<String> RSS_SECURITY_HADOOP_KERBEROS_PRINCIPAL =
      ConfigOptions.key("rss.security.hadoop.kerberos.principal")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "The kerberos keytab principal. And only when "
                  + RSS_SECURITY_HADOOP_KERBEROS_ENABLE.key()
                  + " enabled, the option will be valid.");

  public static final ConfigOption<Long> RSS_SECURITY_HADOOP_KERBEROS_RELOGIN_INTERVAL_SEC =
      ConfigOptions.key("rss.security.hadoop.kerberos.relogin.interval.sec")
          .longType()
          .checkValue(ConfigUtils.POSITIVE_INTEGER_VALIDATOR, "The value must be positive integer")
          .defaultValue(60L)
          .withDescription("The kerberos authentication relogin interval. unit: sec");

  public static final ConfigOption<Boolean> RSS_TEST_MODE_ENABLE =
      ConfigOptions.key("rss.test.mode.enable")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether enable test mode for the shuffle server.");

  public static final ConfigOption<String> RSS_METRICS_REPORTER_CLASS =
      ConfigOptions.key("rss.metrics.reporter.class")
          .stringType()
          .noDefaultValue()
          .withDescription("The class of metrics reporter.");

  public static final ConfigOption<Long> RSS_RECONFIGURE_INTERVAL_SEC =
      ConfigOptions.key("rss.reconfigure.interval.sec")
          .longType()
          .checkValue(ConfigUtils.POSITIVE_LONG_VALIDATOR, "The value must be posite long")
          .defaultValue(5L)
          .withDescription("Reconfigure check interval.");

  public static final ConfigOption<Integer> RSS_RANDOM_PORT_MIN =
      ConfigOptions.key("rss.random.port.min")
          .intType()
          .defaultValue(40000)
          .withDescription("Min value for random for range");

  public static final ConfigOption<Integer> RSS_RANDOM_PORT_MAX =
      ConfigOptions.key("rss.random.port.max")
          .intType()
          .defaultValue(65535)
          .withDescription("Max value for random for range");

  public static final ConfigOption<Integer> SERVER_PORT_MAX_RETRIES =
      ConfigOptions.key("rss.port.max.retry")
          .intType()
          .defaultValue(16)
          .withDescription("start server service max retry");

  /* Serialization */
  public static final ConfigOption<Boolean> RSS_KRYO_REGISTRATION_REQUIRED =
      ConfigOptions.key("rss.kryo.registrationRequired")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether registration is required.");
  public static final ConfigOption<Boolean> RSS_KRYO_REFERENCE_TRACKING =
      ConfigOptions.key("rss.kryo.referenceTracking")
          .booleanType()
          .defaultValue(true)
          .withDescription(
              "Whether to track references to the same object when serializing data with Kryo.");
  public static final ConfigOption<Boolean> RSS_KRYO_SCALA_REGISTRATION_REQUIRED =
      ConfigOptions.key("rss.kryo.scalaRegistrationRequired")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "Whether to require registration of some common scala classes, "
                  + "usually used for spark applications");
  public static final ConfigOption<String> RSS_KRYO_REGISTRATION_CLASSES =
      ConfigOptions.key("rss.kryo.registrationClasses")
          .stringType()
          .defaultValue("")
          .withDescription(
              "The classes to be registered. This configuration must ensure that the"
                  + "client and server are exactly the same. Dynamic configuration is recommended");
  public static final ConfigOption<String> RSS_IO_SERIALIZATIONS =
      ConfigOptions.key("rss.io.serializations")
          .stringType()
          .defaultValue(WritableSerializer.class.getName() + "," + KryoSerializer.class.getName())
          .withDescription("Serializations are used for creative Serializers and Deserializers");

  public static final ConfigOption<String> REST_AUTHORIZATION_CREDENTIALS =
      ConfigOptions.key("rss.http.basic.authorizationCredentials")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Authorization credentials for the rest interface. "
                  + "For Basic authentication the credentials are constructed by"
                  + " first combining the username and the password with a colon (uniffle:uniffle123)"
                  + ", and then by encoding the resulting string in base64 (dW5pZmZsZTp1bmlmZmxlMTIz).");

  public static final ConfigOption<String> RSS_STORAGE_LOCALFILE_WRITE_DATA_BUFFER_SIZE =
      ConfigOptions.key("rss.storage.localfile.write.dataBufferSize")
          .stringType()
          .defaultValue("8k")
          .withDescription("The buffer size to cache the write data content for LOCALFILE.");

  public static final ConfigOption<String> RSS_STORAGE_LOCALFILE_WRITE_INDEX_BUFFER_SIZE =
      ConfigOptions.key("rss.storage.localfile.write.indexBufferSize")
          .stringType()
          .defaultValue("8k")
          .withDescription("The buffer size to cache the write index content for LOCALFILE.");
  public static final ConfigOption<String> RSS_STORAGE_LOCALFILE_WRITER_CLASS =
      ConfigOptions.key("rss.storage.localFileWriterClass")
          .stringType()
          .defaultValue("org.apache.uniffle.storage.handler.impl.LocalFileWriter")
          .withDescription("The writer class to write shuffle data for LOCALFILE.");

  public static final ConfigOption<String> RSS_STORAGE_HDFS_WRITE_DATA_BUFFER_SIZE =
      ConfigOptions.key("rss.storage.hdfs.write.dataBufferSize")
          .stringType()
          .defaultValue("8k")
          .withDescription("The buffer size to cache the write data content for HDFS.");

  public static final ConfigOption<String> RSS_STORAGE_HDFS_WRITE_INDEX_BUFFER_SIZE =
      ConfigOptions.key("rss.storage.hdfs.write.indexBufferSize")
          .stringType()
          .defaultValue("8k")
          .withDescription("The buffer size to cache the write index content for HDFS.");

  public boolean loadConfFromFile(String fileName, List<ConfigOption<Object>> configOptions) {
    Map<String, String> properties = RssUtils.getPropertiesFromFile(fileName);
    if (properties == null) {
      return false;
    }
    Map<String, String> propertiesFromSystem = new HashMap<>();
    System.getProperties().stringPropertyNames().stream()
        .forEach(
            propName -> {
              propertiesFromSystem.put(propName, System.getProperty(propName));
            });
    return loadConf(properties, configOptions, true)
        && loadConf(propertiesFromSystem, configOptions, false);
  }
}
