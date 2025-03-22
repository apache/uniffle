#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -o pipefail
set -o nounset   # exit the script if you try to use an uninitialised variable
set -o errexit   # exit the script if any statement returns a non-true return value

source "$(dirname "$0")/utils.sh"
load_rss_env

cd "$RSS_HOME"

SHUFFLE_SERVER_CONF_FILE="${RSS_CONF_DIR}/server.conf"
JAR_DIR="${RSS_HOME}/jars"
LOG_CONF_FILE="${RSS_CONF_DIR}/log4j2.xml"
LOG_PATH="${RSS_LOG_DIR}/shuffle_server.log"
LOG_OUT_PATH="${RSS_LOG_DIR}/shuffle_server.out"
SHUFFLE_SERVER_STORAGE_AUDIT_LOG_PATH=${SHUFFLE_SERVER_STORAGE_AUDIT_LOG_PATH:-"${RSS_LOG_DIR}/shuffle_server_storage_audit.log"}
SHUFFLE_SERVER_RPC_AUDIT_LOG_PATH=${SHUFFLE_SERVER_RPC_AUDIT_LOG_PATH:-"${RSS_LOG_DIR}/shuffle_server_rpc_audit.log"}

SHUFFLE_SERVER_XMX_SIZE=${SHUFFLE_SERVER_XMX_SIZE:-${XMX_SIZE}}
if [ -z "${SHUFFLE_SERVER_XMX_SIZE:-}" ]; then
  echo "No env XMX_SIZE or SHUFFLE_SERVER_XMX_SIZE."
  exit 1
fi
echo "Shuffle Server JVM XMX size: ${SHUFFLE_SERVER_XMX_SIZE}"
if [ -n "${RSS_IP:-}" ]; then
  echo "Shuffle Server RSS_IP: ${RSS_IP}"
fi

# glibc use an arena memory allocator that causes virtual
# memory usage to explode. This interacts badly
# with the many threads that we use in rss. Tune the variable
# down to prevent vmem explosion.
# glibc's default value of MALLOC_ARENA_MAX is 8 * CORES.
# After referring to hadoop/presto, the default MALLOC_ARENA_MAX of shuffle server is set to 4.
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}

MAIN_CLASS="org.apache.uniffle.server.ShuffleServer"

echo "Check process existence"
RPC_PORT=`grep '^rss.rpc.server.port' $SHUFFLE_SERVER_CONF_FILE |awk '{print $2}'`
is_port_in_use $RPC_PORT


CLASSPATH=""
JAVA_LIB_PATH=""

for file in $(ls ${JAR_DIR}/server/*.jar 2>/dev/null); do
  CLASSPATH=$CLASSPATH:$file
done

mkdir -p "${RSS_LOG_DIR}"
mkdir -p "${RSS_PID_DIR}"

set +u
if [ $HADOOP_HOME ]; then
  HADOOP_DEPENDENCY="$("$HADOOP_HOME/bin/hadoop" classpath --glob)"
  CLASSPATH=$CLASSPATH:$HADOOP_DEPENDENCY
  JAVA_LIB_PATH="-Djava.library.path=$HADOOP_HOME/lib/native"
fi

if [ "$HADOOP_CONF_DIR" ]; then
  CLASSPATH=$CLASSPATH:$HADOOP_CONF_DIR
fi
set -u

echo "class path is $CLASSPATH"

MAX_DIRECT_MEMORY_OPTS=""
if [ -n "${MAX_DIRECT_MEMORY_SIZE:-}" ]; then
  MAX_DIRECT_MEMORY_OPTS="-XX:MaxDirectMemorySize=$MAX_DIRECT_MEMORY_SIZE"
fi

# Attention, the OOM dump file may be very big !
JAVA_OPTS=""
if [ -n "${OOM_DUMP_PATH:-}" ]; then
  JAVA_OPTS="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$OOM_DUMP_PATH"
fi

SHUFFLE_SERVER_BASE_JVM_ARGS=${SHUFFLE_SERVER_BASE_JVM_ARGS:-" -server \
          -Xmx${SHUFFLE_SERVER_XMX_SIZE} \
          -Xms${SHUFFLE_SERVER_XMX_SIZE} \
          ${MAX_DIRECT_MEMORY_OPTS} \
          ${JAVA_OPTS} \
          -XX:+CrashOnOutOfMemoryError \
          -XX:+ExitOnOutOfMemoryError \
          -XX:+UnlockExperimentalVMOptions \
          -XX:+PrintCommandLineFlags"}

DEFAULT_GC_ARGS=${SHUFFLE_SERVER_DEFAULT_GC_ARGS:-" -XX:+UseG1GC \
          -XX:MaxGCPauseMillis=200 \
          -XX:ParallelGCThreads=20 \
          -XX:ConcGCThreads=5 \
          -XX:InitiatingHeapOccupancyPercent=60 \
          -XX:G1HeapRegionSize=32m \
          -XX:G1NewSizePercent=10"}

GC_LOG_ARGS_LEGACY=" -XX:+PrintGC \
          -XX:+PrintAdaptiveSizePolicy \
          -XX:+PrintGCDateStamps \
          -XX:+PrintGCTimeStamps \
          -XX:+PrintTenuringDistribution \
          -XX:+PrintPromotionFailure \
          -XX:+PrintGCApplicationStoppedTime \
          -XX:+PrintGCCause \
          -XX:+PrintGCDetails \
          -Xloggc:${RSS_LOG_DIR}/gc-%t.log"

GC_LOG_ARGS_NEW=" -XX:+IgnoreUnrecognizedVMOptions \
          -Xlog:gc* \
          -Xlog:gc+age=trace \
          -Xlog:gc+heap=debug \
          -Xlog:gc+promotion=trace \
          -Xlog:gc+phases=debug \
          -Xlog:gc+ref=debug \
          -Xlog:gc+start=debug \
          -Xlog:gc*:file=${RSS_LOG_DIR}/gc-%t.log:tags,uptime,time,level"

JVM_LOG_ARGS=""

if [ -f ${LOG_CONF_FILE} ]; then
  JVM_LOG_ARGS=" -Dlog4j2.configurationFile=file:${LOG_CONF_FILE} -Dlog.path=${LOG_PATH} -Dstorage.audit.log.path=${SHUFFLE_SERVER_STORAGE_AUDIT_LOG_PATH} -Drpc.audit.log.path=${SHUFFLE_SERVER_RPC_AUDIT_LOG_PATH}"
else
  echo "Exit with error: ${LOG_CONF_FILE} file doesn't exist."
  exit 1
fi

version=$($RUNNER -version 2>&1 | awk -F[\".] '/version/ {print $2}')
if [[ "$version" -lt "9" ]]; then
  SHUFFLE_SERVER_JVM_GC_ARGS="${SHUFFLE_SERVER_JVM_GC_ARGS:-${DEFAULT_GC_ARGS} ${GC_LOG_ARGS_LEGACY}}"
else
  SHUFFLE_SERVER_JVM_GC_ARGS="${SHUFFLE_SERVER_JVM_GC_ARGS:-${DEFAULT_GC_ARGS} ${GC_LOG_ARGS_NEW}}"
fi

SHUFFLE_SERVER_JAVA_OPTS=${SHUFFLE_SERVER_JAVA_OPTS:-""}
(nohup $RUNNER ${SHUFFLE_SERVER_BASE_JVM_ARGS} ${SHUFFLE_SERVER_JVM_GC_ARGS} ${JVM_LOG_ARGS} ${JAVA_LIB_PATH} ${SHUFFLE_SERVER_JAVA_OPTS} -cp ${CLASSPATH} ${MAIN_CLASS} --conf "${SHUFFLE_SERVER_CONF_FILE}" $@ > ${LOG_OUT_PATH} 2>&1) &

get_pid_file_name shuffle-server
echo $! >${RSS_PID_DIR}/${pid_file}
