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

COORDINATOR_CONF_FILE="${RSS_CONF_DIR}/coordinator.conf"
JAR_DIR="${RSS_HOME}/jars"
LOG_CONF_FILE="${RSS_CONF_DIR}/log4j2.xml"
LOG_PATH="${RSS_LOG_DIR}/coordinator.log"
LOG_OUT_PATH="${RSS_LOG_DIR}/coordinator.out"
COORDINATOR_RPC_AUDIT_LOG_PATH="${RSS_LOG_DIR}/coordinator_rpc_audit.log"

MAIN_CLASS="org.apache.uniffle.coordinator.CoordinatorServer"

echo "Check process existence"
is_jvm_process_running "$JPS" $MAIN_CLASS

CLASSPATH=""
JAVA_LIB_PATH=""

for file in $(ls ${JAR_DIR}/coordinator/*.jar 2>/dev/null); do
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

if [ $HADOOP_CONF_DIR ]; then
  CLASSPATH=$CLASSPATH:$HADOOP_CONF_DIR
fi
set -u

echo "class path is $CLASSPATH"

COORDINATOR_XMX_SIZE=${COORDINATOR_XMX_SIZE:-${XMX_SIZE:-"8g"}}
COORDINATOR_BASE_JVM_ARGS=${COORDINATOR_BASE_JVM_ARGS:-" -server \
          -Xmx${COORDINATOR_XMX_SIZE} \
          -Xms${COORDINATOR_XMX_SIZE} \
          -XX:+PrintCommandLineFlags"}

DEFAULT_GC_ARGS=${COORDINATOR_DEFAULT_GC_ARGS:-" -XX:+UseG1GC \
          -XX:MaxGCPauseMillis=200 \
          -XX:ParallelGCThreads=20 \
          -XX:ConcGCThreads=5 \
          -XX:InitiatingHeapOccupancyPercent=45"}

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
  JVM_LOG_ARGS=" -Dlog4j2.configurationFile=file:${LOG_CONF_FILE} -Dlog.path=${LOG_PATH} -Drpc.audit.log.path=${COORDINATOR_RPC_AUDIT_LOG_PATH}"
else
  echo "Exit with error: ${LOG_CONF_FILE} file doesn't exist."
  exit 1
fi

version=$($RUNNER -version 2>&1 | awk -F[\".] '/version/ {print $2}')
if [[ "$version" -lt "9" ]]; then
  COORDINATOR_JVM_GC_ARGS="${COORDINATOR_JVM_GC_ARGS:-${DEFAULT_GC_ARGS} ${GC_LOG_ARGS_LEGACY}}"
else
  COORDINATOR_JVM_GC_ARGS="${COORDINATOR_JVM_GC_ARGS:-${DEFAULT_GC_ARGS} ${GC_LOG_ARGS_NEW}}"
fi

COORDINATOR_JAVA_OPTS=${COORDINATOR_JAVA_OPTS:-""}
(nohup $RUNNER ${COORDINATOR_BASE_JVM_ARGS} ${COORDINATOR_JVM_GC_ARGS} ${JVM_LOG_ARGS} ${COORDINATOR_JAVA_OPTS} -cp ${CLASSPATH} ${MAIN_CLASS} --conf "${COORDINATOR_CONF_FILE}" $@ > ${LOG_OUT_PATH} 2>&1) &

get_pid_file_name coordinator
echo $! >${RSS_PID_DIR}/${pid_file}
