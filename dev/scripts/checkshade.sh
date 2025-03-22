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

SHADED_JAR_PATH=${1}
echo "checkShaded:"${SHADED_JAR_PATH}
if [ ! -f "${SHADED_JAR_PATH}" ]; then
    echo "check failed, file does not exist."
    exit 1
fi

UNSHADED=$(unzip -l "${SHADED_JAR_PATH}" | awk 'NR>3 {print $4}' | grep -vE 'uniffle|org/apache/spark/shuffle|META-INF|git.properties|/$|\.html$|\.css$|\.xml$|^javax|^$')
UNSHADED_COUNT=0
if [ -n "$UNSHADED" ]; then
    UNSHADED_COUNT=$(wc -l <<< "$UNSHADED")
fi
echo "unshaded count: $UNSHADED_COUNT"
echo "unshaded content:"
echo "$UNSHADED"
if [ $UNSHADED_COUNT -eq 0 ]; then
    echo "check success."
else
    echo "check failed."
    exit 2
fi
