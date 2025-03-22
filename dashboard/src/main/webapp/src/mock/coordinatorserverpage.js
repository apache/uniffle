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

import Mock from 'mockjs'

Mock.mock(/\/coordinator\/conf/, 'get', {
  code: 0,
  data: [
    {
      argumentKey: 'awt.toolkit',
      argumentValue: 'sun.awt.X11.XToolkit'
    },
    {
      argumentKey: 'java.specification.version',
      argumentValue: '11'
    },
    {
      argumentKey: 'sun.cpu.isalist',
      argumentValue: ''
    },
    {
      argumentKey: 'sun.jnu.encoding',
      argumentValue: 'UTF-8'
    },
    {
      argumentKey: 'rss.jetty.http.port',
      argumentValue: '29998'
    },
    {
      argumentKey: 'rss.coordinator.default.app.priority',
      argumentValue: '5'
    },
    {
      argumentKey: 'java.vm.vendor',
      argumentValue: 'Oracle Corporation'
    },
    {
      argumentKey: 'sun.arch.data.model',
      argumentValue: '64'
    },
    {
      argumentKey: 'log4j2.configurationFile',
      argumentValue: 'file:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/conf/log4j2.xml'
    },
    {
      argumentKey: 'java.vendor.url',
      argumentValue: 'http://java.oracle.com/'
    },
    {
      argumentKey: 'rss.coordinator.app.expired',
      argumentValue: '60000'
    },
    {
      argumentKey: 'user.timezone',
      argumentValue: 'Asia/Shanghai'
    },
    {
      argumentKey: 'java.vm.specification.version',
      argumentValue: '11'
    },
    {
      argumentKey: 'os.name',
      argumentValue: 'Linux'
    },
    {
      argumentKey: 'sun.java.launcher',
      argumentValue: 'SUN_STANDARD'
    },
    {
      argumentKey: 'user.country',
      argumentValue: 'US'
    },
    {
      argumentKey: 'sun.boot.library.path',
      argumentValue: '/home/hadoop/jdk-11.0.2/lib'
    },
    {
      argumentKey: 'rss.coordinator.select.partition.strategy',
      argumentValue: 'CONTINUOUS'
    },
    {
      argumentKey: 'sun.java.command',
      argumentValue:
        'org.apache.uniffle.coordinator.CoordinatorServer --conf /root/rss-0.10.0-SNAPSHOT-hadoop2.8/conf/coordinator.conf'
    },
    {
      argumentKey: 'jdk.debug',
      argumentValue: 'release'
    },
    {
      argumentKey: 'sun.cpu.endian',
      argumentValue: 'little'
    },
    {
      argumentKey: 'user.home',
      argumentValue: '/home/hadoop'
    },
    {
      argumentKey: 'rss.coordinator.server.heartbeat.timeout',
      argumentValue: '30000'
    },
    {
      argumentKey: 'user.language',
      argumentValue: 'en'
    },
    {
      argumentKey: 'java.specification.vendor',
      argumentValue: 'Oracle Corporation'
    },
    {
      argumentKey: 'java.version.date',
      argumentValue: '2019-01-15'
    },
    {
      argumentKey: 'java.home',
      argumentValue: '/home/hadoop/jdk-11.0.2'
    },
    {
      argumentKey: 'file.separator',
      argumentValue: '/'
    },
    {
      argumentKey: 'line.separator',
      argumentValue: '\n'
    },
    {
      argumentKey: 'rss.coordinator.quota.default.app.num',
      argumentValue: '25'
    },
    {
      argumentKey: 'java.specification.name',
      argumentValue: 'Java Platform API Specification'
    },
    {
      argumentKey: 'java.vm.specification.vendor',
      argumentValue: 'Oracle Corporation'
    },
    {
      argumentKey: 'java.awt.graphicsenv',
      argumentValue: 'sun.awt.X11GraphicsEnvironment'
    },
    {
      argumentKey: 'sun.management.compiler',
      argumentValue: 'HotSpot 64-Bit Tiered Compilers'
    },
    {
      argumentKey: 'rss.coordinator.dynamicClientConf.enabled',
      argumentValue: 'true'
    },
    {
      argumentKey: 'rss.coordinator.quota.default.path',
      argumentValue: 'file:///root/rss-0.10.0-SNAPSHOT-hadoop2.8/conf/userQuota.properties'
    },
    {
      argumentKey: 'java.runtime.version',
      argumentValue: '11.0.2+9'
    },
    {
      argumentKey: 'user.name',
      argumentValue: 'hadoop'
    },
    {
      argumentKey: 'path.separator',
      argumentValue: ':'
    },
    {
      argumentKey: 'os.version',
      argumentValue: '4.18.0-193.6.3.el8_2.v1.4.x86_64'
    },
    {
      argumentKey: 'java.runtime.name',
      argumentValue: 'OpenJDK Runtime Environment'
    },
    {
      argumentKey: 'rss.coordinator.access.loadChecker.memory.percentage',
      argumentValue: '10.0'
    },
    {
      argumentKey: 'rss.coordinator.dynamicClientConf.path',
      argumentValue: 'file:///root/rss-0.10.0-SNAPSHOT-hadoop2.8/conf/dynamic_client.conf'
    },
    {
      argumentKey: 'file.encoding',
      argumentValue: 'UTF-8'
    },
    {
      argumentKey: 'java.vm.name',
      argumentValue: 'OpenJDK 64-Bit Server VM'
    },
    {
      argumentKey: 'java.vendor.version',
      argumentValue: '18.9'
    },
    {
      argumentKey: 'log.path',
      argumentValue: '/root/cluster-data/logs/coordinator.log'
    },
    {
      argumentKey: 'rss.coordinator.exclude.nodes.file.path',
      argumentValue: 'file:///root/rss-0.10.0-SNAPSHOT-hadoop2.8/conf/exclude_nodes'
    },
    {
      argumentKey: 'java.vendor.url.bug',
      argumentValue: 'http://bugreport.java.com/bugreport/'
    },
    {
      argumentKey: 'java.io.tmpdir',
      argumentValue: '/tmp'
    },
    {
      argumentKey: 'java.version',
      argumentValue: '11.0.2'
    },
    {
      argumentKey: 'user.dir',
      argumentValue: '/root/rss-0.10.0-SNAPSHOT-hadoop2.8'
    },
    {
      argumentKey: 'os.arch',
      argumentValue: 'amd64'
    },
    {
      argumentKey: 'java.vm.specification.name',
      argumentValue: 'Java Virtual Machine Specification'
    },
    {
      argumentKey: 'java.awt.printerjob',
      argumentValue: 'sun.print.PSPrinterJob'
    },
    {
      argumentKey: 'coordinator.id',
      argumentValue: '10.55.123.88-29997'
    },
    {
      argumentKey: 'sun.os.patch.level',
      argumentValue: 'unknown'
    },
    {
      argumentKey: 'rss.rpc.server.port',
      argumentValue: '29997'
    },
    {
      argumentKey: 'java.library.path',
      argumentValue: '/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib'
    },
    {
      argumentKey: 'java.vendor',
      argumentValue: 'Oracle Corporation'
    },
    {
      argumentKey: 'java.vm.info',
      argumentValue: 'mixed mode'
    },
    {
      argumentKey: 'java.vm.version',
      argumentValue: '11.0.2+9'
    },
    {
      argumentKey: 'sun.io.unicode.encoding',
      argumentValue: 'UnicodeLittle'
    },
    {
      argumentKey: 'rss.coordinator.shuffle.nodes.max',
      argumentValue: '2'
    },
    {
      argumentKey: 'java.class.version',
      argumentValue: '55.0'
    },
    {'argumentKey':'java.class.path','argumentValue':':/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/activation-1.1.1.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/animal-sniffer-annotations-1.23.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/annotations-4.1.1.4.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/checker-qual-3.37.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/commons-collections4-4.4.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/commons-lang3-3.10.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/coordinator-0.10.0-SNAPSHOT.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/error_prone_annotations-2.23.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/failureaccess-1.0.1.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/grpc-api-1.64.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/grpc-context-1.64.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/grpc-core-1.64.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/grpc-netty-shaded-1.64.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/grpc-protobuf-1.64.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/grpc-protobuf-lite-1.64.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/grpc-stub-1.64.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/grpc-util-1.64.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/gson-2.10.1.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/guava-32.1.3-jre.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/hbase-shaded-jersey-4.1.4.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/j2objc-annotations-2.8.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/jackson-annotations-2.10.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/jackson-core-2.10.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/jackson-databind-2.10.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/jakarta.annotation-api-1.3.5.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/jakarta.inject-2.6.1.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/jakarta.validation-api-2.0.2.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/javassist-3.25.0-GA.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/javax.activation-api-1.2.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/javax.annotation-api-1.3.2.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/javax.servlet-api-3.1.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/jaxb-api-2.3.1.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/jetty-client-9.3.24.v20180605.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/jetty-http-9.3.24.v20180605.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/jetty-io-9.3.24.v20180605.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/jetty-proxy-9.3.24.v20180605.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/jetty-security-9.3.24.v20180605.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/jetty-server-9.3.24.v20180605.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/jetty-servlet-9.3.24.v20180605.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/jetty-util-9.3.24.v20180605.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/jsr305-3.0.2.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/log4j-api-2.23.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/log4j-core-2.23.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/log4j-slf4j-impl-2.23.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/lz4-1.3.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-all-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-buffer-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-codec-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-codec-dns-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-codec-haproxy-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-codec-http2-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-codec-http-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-codec-memcache-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-codec-mqtt-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-codec-redis-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-codec-smtp-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-codec-socks-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-codec-stomp-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-codec-xml-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-common-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-handler-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-handler-proxy-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-handler-ssl-ocsp-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-resolver-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-resolver-dns-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-resolver-dns-classes-macos-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-resolver-dns-native-macos-4.1.109.Final-osx-aarch_64.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-resolver-dns-native-macos-4.1.109.Final-osx-x86_64.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-transport-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-transport-classes-epoll-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-transport-classes-kqueue-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-transport-native-epoll-4.1.109.Final-linux-aarch_64.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-transport-native-epoll-4.1.109.Final-linux-riscv64.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-transport-native-epoll-4.1.109.Final-linux-x86_64.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-transport-native-kqueue-4.1.109.Final-osx-aarch_64.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-transport-native-kqueue-4.1.109.Final-osx-x86_64.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-transport-native-unix-common-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-transport-rxtx-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-transport-sctp-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/netty-transport-udt-4.1.109.Final.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/perfmark-api-0.26.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/picocli-4.5.2.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/protobuf-java-3.25.1.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/protobuf-java-util-3.25.1.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/proto-google-common-protos-2.29.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/RoaringBitmap-0.9.15.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/rss-common-0.10.0-SNAPSHOT.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/rss-internal-client-0.10.0-SNAPSHOT.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/rss-proto-0.10.0-SNAPSHOT.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/shims-0.9.15.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/simpleclient-0.9.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/simpleclient_common-0.9.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/simpleclient_hotspot-0.9.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/simpleclient_httpserver-0.9.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/simpleclient_jetty-0.9.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/simpleclient_pushgateway-0.9.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/simpleclient_servlet-0.9.0.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/slf4j-api-1.7.36.jar:/root/rss-0.10.0-SNAPSHOT-hadoop2.8/jars/coordinator/snakeyaml-2.2.jar:/root/hadoop/etc/hadoop:/root/hadoop/share/hadoop/common/lib/jersey-server-1.19.jar:/root/hadoop/share/hadoop/common/lib/kerb-server-1.0.1.jar:/root/hadoop/share/hadoop/common/lib/commons-beanutils-1.9.3.jar:/root/hadoop/share/hadoop/common/lib/xz-1.0.jar:/root/hadoop/share/hadoop/common/lib/slf4j-api-1.7.25.jar:/root/hadoop/share/hadoop/common/lib/metrics-core-3.2.4.jar:/root/hadoop/share/hadoop/common/lib/accessors-smart-1.2.jar:/root/hadoop/share/hadoop/common/lib/dnsjava-2.1.7.jar:/root/hadoop/share/hadoop/common/lib/kerby-config-1.0.1.jar:/root/hadoop/share/hadoop/common/lib/spark-network-yarn_2.11-2.1.4-270.jar:/root/hadoop/share/hadoop/common/lib/commons-collections-3.2.2.jar:/root/hadoop/share/hadoop/common/lib/netty-3.10.5.Final.jar:/root/hadoop/share/hadoop/common/lib/snappy-java-1.0.5.jar:/root/hadoop/share/hadoop/common/lib/jetty-xml-9.3.24.v20180605.jar:/root/hadoop/share/hadoop/common/lib/woodstox-core-5.0.3.jar:/root/hadoop/share/hadoop/common/lib/stax2-api-3.1.4.jar:/root/hadoop/share/hadoop/common/lib/jersey-json-1.19.jar:/root/hadoop/share/hadoop/common/lib/curator-recipes-2.12.0.jar:/root/hadoop/share/hadoop/common/lib/jackson-core-asl-1.9.13.jar:/root/hadoop/share/hadoop/common/lib/curator-framework-2.12.0.jar:/root/hadoop/share/hadoop/common/lib/commons-net-3.6.jar:/root/hadoop/share/hadoop/common/lib/commons-codec-1.11.jar:/root/hadoop/share/hadoop/common/lib/httpclient-4.5.2.jar:/root/hadoop/share/hadoop/common/lib/jsr311-api-1.1.1.jar:/root/hadoop/share/hadoop/common/lib/kerby-util-1.0.1.jar:/root/hadoop/share/hadoop/common/lib/kerby-xdr-1.0.1.jar:/root/hadoop/share/hadoop/common/lib/kerby-asn1-1.0.1.jar:/root/hadoop/share/hadoop/common/lib/kerb-crypto-1.0.1.jar:/root/hadoop/share/hadoop/common/lib/jackson-mapper-asl-1.9.13.jar:/root/hadoop/share/hadoop/common/lib/kerb-admin-1.0.1.jar:/root/hadoop/share/hadoop/common/lib/javax.servlet-api-3.1.0.jar:/root/hadoop/share/hadoop/common/lib/log4j-1.2.17.jar:/root/hadoop/share/hadoop/common/lib/jackson-databind-2.9.5.jar:/root/hadoop/share/hadoop/common/lib/jetty-io-9.3.24.v20180605.jar:/root/hadoop/share/hadoop/common/lib/commons-io-2.5.jar:/root/hadoop/share/hadoop/common/lib/jackson-annotations-2.9.5.jar:/root/hadoop/share/hadoop/common/lib/jcip-annotations-1.0-1.jar:/root/hadoop/share/hadoop/common/lib/commons-logging-1.1.3.jar:/root/hadoop/share/hadoop/common/lib/jaxb-impl-2.2.3-1.jar:/root/hadoop/share/hadoop/common/lib/kerby-pkix-1.0.1.jar:/root/hadoop/share/hadoop/common/lib/jetty-server-9.3.24.v20180605.jar:/root/hadoop/share/hadoop/common/lib/commons-math3-3.1.1.jar:/root/hadoop/share/hadoop/common/lib/commons-configuration2-2.1.1.jar:/root/hadoop/share/hadoop/common/lib/json-smart-2.3.jar:/root/hadoop/share/hadoop/common/lib/httpcore-4.4.4.jar:/root/hadoop/share/hadoop/common/lib/kerb-simplekdc-1.0.1.jar:/root/hadoop/share/hadoop/common/lib/jaxb-api-2.2.11.jar:/root/hadoop/share/hadoop/common/lib/asm-5.0.4.jar:/root/hadoop/share/hadoop/common/lib/commons-text-1.4.jar:/root/hadoop/share/hadoop/common/lib/audience-annotations-0.5.0.jar:/root/hadoop/share/hadoop/common/lib/avro-1.7.7.jar:/root/hadoop/share/hadoop/common/lib/guava-11.0.2.jar:/root/hadoop/share/hadoop/common/lib/kerb-common-1.0.1.jar:/root/hadoop/share/hadoop/common/lib/gson-2.2.4.jar:/root/hadoop/share/hadoop/common/lib/paranamer-2.3.jar:/root/hadoop/share/hadoop/common/lib/jackson-xc-1.9.13.jar:/root/hadoop/share/hadoop/common/lib/jackson-jaxrs-1.9.13.jar:/root/hadoop/share/hadoop/common/lib/jul-to-slf4j-1.7.25.jar:/root/hadoop/share/hadoop/common/lib/token-provider-1.0.1.jar:/root/hadoop/share/hadoop/common/lib/jetty-http-9.3.24.v20180605.jar:/root/hadoop/share/hadoop/common/lib/htrace-core4-4.1.0-incubating.jar:/root/hadoop/share/hadoop/common/lib/jsch-0.1.54.jar:/root/hadoop/share/hadoop/common/lib/nimbus-jose-jwt-4.41.1.jar:/root/hadoop/share/hadoop/common/lib/kerb-core-1.0.1.jar:/root/hadoop/share/hadoop/common/lib/jersey-servlet-1.19.jar:/root/hadoop/share/hadoop/common/lib/kerb-client-1.0.1.jar:/root/hadoop/share/hadoop/common/lib/hadoop-auth-3.2.0-142.jar:/root/hadoop/share/hadoop/common/lib/jettison-1.1.jar:/root/hadoop/share/hadoop/common/lib/kerb-identity-1.0.1.jar:/root/hadoop/share/hadoop/common/lib/commons-lang3-3.7.jar:/root/hadoop/share/hadoop/common/lib/curator-client-2.12.0.jar:/root/hadoop/share/hadoop/common/lib/protobuf-java-2.5.0.jar:/root/hadoop/share/hadoop/common/lib/jetty-webapp-9.3.24.v20180605.jar:/root/hadoop/share/hadoop/common/lib/re2j-1.1.jar:/root/hadoop/share/hadoop/common/lib/jetty-servlet-9.3.24.v20180605.jar:/root/hadoop/share/hadoop/common/lib/kerb-util-1.0.1.jar:/root/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar:/root/hadoop/share/hadoop/common/lib/commons-compress-1.4.1.jar:/root/hadoop/share/hadoop/common/lib/jackson-core-2.9.5.jar:/root/hadoop/share/hadoop/common/lib/hadoop-annotations-3.2.0-142.jar:/root/hadoop/share/hadoop/common/lib/hadoop-lzo-0.4.19.jar:/root/hadoop/share/hadoop/common/lib/jsr305-3.0.0.jar:/root/hadoop/share/hadoop/common/lib/jetty-util-9.3.24.v20180605.jar:/root/hadoop/share/hadoop/common/lib/jersey-core-1.19.jar:/root/hadoop/share/hadoop/common/lib/jsp-api-2.1.jar:/root/hadoop/share/hadoop/common/lib/zookeeper-3.4.13.jar:/root/hadoop/share/hadoop/common/lib/jetty-security-9.3.24.v20180605.jar:/root/hadoop/share/hadoop/common/hadoop-common-3.2.0-142-tests.jar:/root/hadoop/share/hadoop/common/hadoop-nfs-3.2.0-142.jar:/root/hadoop/share/hadoop/common/hadoop-kms-3.2.0-142.jar:/root/hadoop/share/hadoop/common/hadoop-common-3.2.0-142.jar:/root/hadoop/share/hadoop/hdfs:/root/hadoop/share/hadoop/hdfs/lib/jersey-server-1.19.jar:/root/hadoop/share/hadoop/hdfs/lib/kerb-server-1.0.1.jar:/root/hadoop/share/hadoop/hdfs/lib/jetty-util-ajax-9.3.24.v20180605.jar:/root/hadoop/share/hadoop/hdfs/lib/commons-beanutils-1.9.3.jar:/root/hadoop/share/hadoop/hdfs/lib/xz-1.0.jar:/root/hadoop/share/hadoop/hdfs/lib/accessors-smart-1.2.jar:/root/hadoop/share/hadoop/hdfs/lib/dnsjava-2.1.7.jar:/root/hadoop/share/hadoop/hdfs/lib/kerby-config-1.0.1.jar:/root/hadoop/share/hadoop/hdfs/lib/spark-network-yarn_2.11-2.1.4-270.jar:/root/hadoop/share/hadoop/hdfs/lib/commons-collections-3.2.2.jar:/root/hadoop/share/hadoop/hdfs/lib/netty-3.10.5.Final.jar:/root/hadoop/share/hadoop/hdfs/lib/snappy-java-1.0.5.jar:/root/hadoop/share/hadoop/hdfs/lib/jetty-xml-9.3.24.v20180605.jar:/root/hadoop/share/hadoop/hdfs/lib/woodstox-core-5.0.3.jar:/root/hadoop/share/hadoop/hdfs/lib/stax2-api-3.1.4.jar:/root/hadoop/share/hadoop/hdfs/lib/jersey-json-1.19.jar:/root/hadoop/share/hadoop/hdfs/lib/curator-recipes-2.12.0.jar:/root/hadoop/share/hadoop/hdfs/lib/jackson-core-asl-1.9.13.jar:/root/hadoop/share/hadoop/hdfs/lib/curator-framework-2.12.0.jar:/root/hadoop/share/hadoop/hdfs/lib/commons-net-3.6.jar:/root/hadoop/share/hadoop/hdfs/lib/netty-all-4.0.52.Final.jar:/root/hadoop/share/hadoop/hdfs/lib/commons-codec-1.11.jar:/root/hadoop/share/hadoop/hdfs/lib/httpclient-4.5.2.jar:/root/hadoop/share/hadoop/hdfs/lib/jsr311-api-1.1.1.jar:/root/hadoop/share/hadoop/hdfs/lib/kerby-util-1.0.1.jar:/root/hadoop/share/hadoop/hdfs/lib/kerby-xdr-1.0.1.jar:/root/hadoop/share/hadoop/hdfs/lib/kerby-asn1-1.0.1.jar:/root/hadoop/share/hadoop/hdfs/lib/kerb-crypto-1.0.1.jar:/root/hadoop/share/hadoop/hdfs/lib/jackson-mapper-asl-1.9.13.jar:/root/hadoop/share/hadoop/hdfs/lib/kerb-admin-1.0.1.jar:/root/hadoop/share/hadoop/hdfs/lib/javax.servlet-api-3.1.0.jar:/root/hadoop/share/hadoop/hdfs/lib/log4j-1.2.17.jar:/root/hadoop/share/hadoop/hdfs/lib/jackson-databind-2.9.5.jar:/root/hadoop/share/hadoop/hdfs/lib/jetty-io-9.3.24.v20180605.jar:/root/hadoop/share/hadoop/hdfs/lib/commons-io-2.5.jar:/root/hadoop/share/hadoop/hdfs/lib/jackson-annotations-2.9.5.jar:/root/hadoop/share/hadoop/hdfs/lib/jcip-annotations-1.0-1.jar:/root/hadoop/share/hadoop/hdfs/lib/json-simple-1.1.1.jar:/root/hadoop/share/hadoop/hdfs/lib/commons-logging-1.1.3.jar:/root/hadoop/share/hadoop/hdfs/lib/jaxb-impl-2.2.3-1.jar:/root/hadoop/share/hadoop/hdfs/lib/kerby-pkix-1.0.1.jar:/root/hadoop/share/hadoop/hdfs/lib/jetty-server-9.3.24.v20180605.jar:/root/hadoop/share/hadoop/hdfs/lib/commons-math3-3.1.1.jar:/root/hadoop/share/hadoop/hdfs/lib/commons-configuration2-2.1.1.jar:/root/hadoop/share/hadoop/hdfs/lib/json-smart-2.3.jar:/root/hadoop/share/hadoop/hdfs/lib/httpcore-4.4.4.jar:/root/hadoop/share/hadoop/hdfs/lib/kerb-simplekdc-1.0.1.jar:/root/hadoop/share/hadoop/hdfs/lib/commons-daemon-1.0.13.jar:/root/hadoop/share/hadoop/hdfs/lib/jaxb-api-2.2.11.jar:/root/hadoop/share/hadoop/hdfs/lib/asm-5.0.4.jar:/root/hadoop/share/hadoop/hdfs/lib/commons-text-1.4.jar:/root/hadoop/share/hadoop/hdfs/lib/audience-annotations-0.5.0.jar:/root/hadoop/share/hadoop/hdfs/lib/avro-1.7.7.jar:/root/hadoop/share/hadoop/hdfs/lib/guava-11.0.2.jar:/root/hadoop/share/hadoop/hdfs/lib/okhttp-2.7.5.jar:/root/hadoop/share/hadoop/hdfs/lib/kerb-common-1.0.1.jar:/root/hadoop/share/hadoop/hdfs/lib/gson-2.2.4.jar:/root/hadoop/share/hadoop/hdfs/lib/paranamer-2.3.jar:/root/hadoop/share/hadoop/hdfs/lib/jackson-xc-1.9.13.jar:/root/hadoop/share/hadoop/hdfs/lib/jackson-jaxrs-1.9.13.jar:/root/hadoop/share/hadoop/hdfs/lib/token-provider-1.0.1.jar:/root/hadoop/share/hadoop/hdfs/lib/jetty-http-9.3.24.v20180605.jar:/root/hadoop/share/hadoop/hdfs/lib/htrace-core4-4.1.0-incubating.jar:/root/hadoop/share/hadoop/hdfs/lib/jsch-0.1.54.jar:/root/hadoop/share/hadoop/hdfs/lib/nimbus-jose-jwt-4.41.1.jar:/root/hadoop/share/hadoop/hdfs/lib/kerb-core-1.0.1.jar:/root/hadoop/share/hadoop/hdfs/lib/jersey-servlet-1.19.jar:/root/hadoop/share/hadoop/hdfs/lib/kerb-client-1.0.1.jar:/root/hadoop/share/hadoop/hdfs/lib/hadoop-auth-3.2.0-142.jar:/root/hadoop/share/hadoop/hdfs/lib/jettison-1.1.jar:/root/hadoop/share/hadoop/hdfs/lib/kerb-identity-1.0.1.jar:/root/hadoop/share/hadoop/hdfs/lib/leveldbjni-all-1.8.jar:/root/hadoop/share/hadoop/hdfs/lib/commons-lang3-3.7.jar:/root/hadoop/share/hadoop/hdfs/lib/okio-1.6.0.jar:/root/hadoop/share/hadoop/hdfs/lib/curator-client-2.12.0.jar:/root/hadoop/share/hadoop/hdfs/lib/protobuf-java-2.5.0.jar:/root/hadoop/share/hadoop/hdfs/lib/jetty-webapp-9.3.24.v20180605.jar:/root/hadoop/share/hadoop/hdfs/lib/re2j-1.1.jar:/root/hadoop/share/hadoop/hdfs/lib/jetty-servlet-9.3.24.v20180605.jar:/root/hadoop/share/hadoop/hdfs/lib/kerb-util-1.0.1.jar:/root/hadoop/share/hadoop/hdfs/lib/commons-cli-1.2.jar:/root/hadoop/share/hadoop/hdfs/lib/commons-compress-1.4.1.jar:/root/hadoop/share/hadoop/hdfs/lib/jackson-core-2.9.5.jar:/root/hadoop/share/hadoop/hdfs/lib/hadoop-annotations-3.2.0-142.jar:/root/hadoop/share/hadoop/hdfs/lib/hadoop-lzo-0.4.19.jar:/root/hadoop/share/hadoop/hdfs/lib/jsr305-3.0.0.jar:/root/hadoop/share/hadoop/hdfs/lib/jetty-util-9.3.24.v20180605.jar:/root/hadoop/share/hadoop/hdfs/lib/jersey-core-1.19.jar:/root/hadoop/share/hadoop/hdfs/lib/zookeeper-3.4.13.jar:/root/hadoop/share/hadoop/hdfs/lib/jetty-security-9.3.24.v20180605.jar:/root/hadoop/share/hadoop/hdfs/hadoop-hdfs-rbf-3.2.0-142-tests.jar:/root/hadoop/share/hadoop/hdfs/hadoop-hdfs-3.2.0-142.jar:/root/hadoop/share/hadoop/hdfs/hadoop-hdfs-3.2.0-142-tests.jar:/root/hadoop/share/hadoop/hdfs/hadoop-hdfs-nfs-3.2.0-142.jar:/root/hadoop/share/hadoop/hdfs/hadoop-hdfs-client-3.2.0-142-tests.jar:/root/hadoop/share/hadoop/hdfs/hadoop-hdfs-httpfs-3.2.0-142.jar:/root/hadoop/share/hadoop/hdfs/hadoop-hdfs-native-client-3.2.0-142.jar:/root/hadoop/share/hadoop/hdfs/hadoop-hdfs-client-3.2.0-142.jar:/root/hadoop/share/hadoop/hdfs/hadoop-hdfs-rbf-3.2.0-142.jar:/root/hadoop/share/hadoop/hdfs/hadoop-hdfs-native-client-3.2.0-142-tests.jar:/root/hadoop/share/hadoop/mapreduce/lib/hamcrest-core-1.3.jar:/root/hadoop/share/hadoop/mapreduce/lib/junit-4.11.jar:/root/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-uploader-3.2.0-142.jar:/root/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-common-3.2.0-142.jar:/root/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.0-142.jar:/root/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-shuffle-3.2.0-142.jar:/root/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.2.0-142.jar:/root/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-nativetask-3.2.0-142.jar:/root/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-3.2.0-142.jar:/root/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-142-tests.jar:/root/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-plugins-3.2.0-142.jar:/root/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-app-3.2.0-142.jar:/root/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-142.jar:/root/hadoop/share/hadoop/yarn/lib/metrics-core-3.2.4.jar:/root/hadoop/share/hadoop/yarn/lib/guice-4.0.jar:/root/hadoop/share/hadoop/yarn/lib/aopalliance-1.0.jar:/root/hadoop/share/hadoop/yarn/lib/geronimo-jcache_1.0_spec-1.0-alpha-1.jar:/root/hadoop/share/hadoop/yarn/lib/java-util-1.9.0.jar:/root/hadoop/share/hadoop/yarn/lib/objenesis-1.0.jar:/root/hadoop/share/hadoop/yarn/lib/jackson-module-jaxb-annotations-2.9.5.jar:/root/hadoop/share/hadoop/yarn/lib/guice-servlet-4.0.jar:/root/hadoop/share/hadoop/yarn/lib/jackson-jaxrs-json-provider-2.9.5.jar:/root/hadoop/share/hadoop/yarn/lib/fst-2.50.jar:/root/hadoop/share/hadoop/yarn/lib/ehcache-3.3.1.jar:/root/hadoop/share/hadoop/yarn/lib/mssql-jdbc-6.2.1.jre7.jar:/root/hadoop/share/hadoop/yarn/lib/jersey-client-1.19.jar:/root/hadoop/share/hadoop/yarn/lib/jersey-guice-1.19.jar:/root/hadoop/share/hadoop/yarn/lib/snakeyaml-1.16.jar:/root/hadoop/share/hadoop/yarn/lib/javax.inject-1.jar:/root/hadoop/share/hadoop/yarn/lib/jackson-jaxrs-base-2.9.5.jar:/root/hadoop/share/hadoop/yarn/lib/HikariCP-java7-2.4.12.jar:/root/hadoop/share/hadoop/yarn/lib/swagger-annotations-1.5.4.jar:/root/hadoop/share/hadoop/yarn/lib/json-io-2.5.1.jar:/root/hadoop/share/hadoop/yarn/hadoop-yarn-server-sharedcachemanager-3.2.0-142.jar:/root/hadoop/share/hadoop/yarn/hadoop-yarn-common-3.2.0-142.jar:/root/hadoop/share/hadoop/yarn/hadoop-yarn-api-3.2.0-142.jar:/root/hadoop/share/hadoop/yarn/hadoop-yarn-applications-unmanaged-am-launcher-3.2.0-142.jar:/root/hadoop/share/hadoop/yarn/hadoop-yarn-server-common-3.2.0-142.jar:/root/hadoop/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-3.2.0-142.jar:/root/hadoop/share/hadoop/yarn/hadoop-yarn-registry-3.2.0-142.jar:/root/hadoop/share/hadoop/yarn/hadoop-yarn-server-resourcemanager-3.2.0-142.jar:/root/hadoop/share/hadoop/yarn/hadoop-yarn-submarine-3.2.0-142.jar:/root/hadoop/share/hadoop/yarn/hadoop-yarn-server-applicationhistoryservice-3.2.0-142.jar:/root/hadoop/share/hadoop/yarn/hadoop-yarn-server-router-3.2.0-142.jar:/root/hadoop/share/hadoop/yarn/hadoop-yarn-server-tests-3.2.0-142.jar:/root/hadoop/share/hadoop/yarn/hadoop-yarn-server-timeline-pluginstorage-3.2.0-142.jar:/root/hadoop/share/hadoop/yarn/hadoop-yarn-server-nodemanager-3.2.0-142.jar:/root/hadoop/share/hadoop/yarn/hadoop-yarn-services-core-3.2.0-142.jar:/root/hadoop/share/hadoop/yarn/hadoop-yarn-services-api-3.2.0-142.jar:/root/hadoop/share/hadoop/yarn/hadoop-yarn-server-web-proxy-3.2.0-142.jar:/root/hadoop/share/hadoop/yarn/hadoop-yarn-client-3.2.0-142.jar:/root/hadoop/contrib/capacity-scheduler/*.jar:/root/hadoop/etc/hadoop'}
  ],
  errMsg: 'success'
})

Mock.mock(/\/coordinator\/info/, 'get', {
  code: 0,
  data: {
    serverIp: '192.168.1.1',
    coordinatorId: '192.168.1.1-29997',
    serverPort: '29997',
    serverWebPort: '29998'
  },
  errMsg: 'success'
})

export default Mock
