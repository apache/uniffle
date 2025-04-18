#!/bin/bash

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

function exit_with_usage() {
  set +x
  echo "./build.sh - Tool for building docker images of Remote Shuffle Service"
  echo ""
  echo "Usage:"
  echo "+------------------------------------------------------------------------------------------------------+"
  echo "| ./build.sh [--hadoop-version <hadoop version>] [--registry <registry url>] [--author <author name>]  |"
  echo "|            [--base-os-distribution <os distribution>] [--base-image <base image url>]                |"
  echo "|            [--push-image <true|false>] [--apache-mirror <apache mirror url>]                         |"
  echo "+------------------------------------------------------------------------------------------------------+"
  exit 1
}

REGISTRY="docker.io/library"
HADOOP_VERSION=2.8.5
HADOOP_SHORT_VERSION=$(echo $HADOOP_VERSION | awk -F "." '{print $1"."$2}')
AUTHOR=$(whoami)
# If you are based in China, you could pass --apache-mirror <a_mirror_url> when building this.
APACHE_MIRROR="https://dlcdn.apache.org"
OS_DISTRIBUTION=debian
BASE_IMAGE=""
PUSH_IMAGE="true"

while (( "$#" )); do
  case $1 in
    --registry)
      REGISTRY="$2"
      shift
      ;;
    --hadoop-version)
      HADOOP_VERSION="$2"
      HADOOP_SHORT_VERSION=$(echo "$HADOOP_VERSION" | awk -F "." '{print $1"."$2}')
      shift
      ;;
    --author)
      AUTHOR="$2"
      shift
      ;;
    --base-os-distribution)
      OS_DISTRIBUTION="$2"
      shift
      ;;
    --base-image)
      BASE_IMAGE="$2"
      shift
      ;;
    --push-image)
      PUSH_IMAGE="$2"
      shift
      ;;
    --help)
      exit_with_usage
      ;;
    --apache-mirror)
      APACHE_MIRROR="$2"
      shift
      ;;
    --*)
      echo "Error: $1 is not supported"
      exit_with_usage
      ;;
    -*)
      break
      ;;
    *)
      echo "Error: $1 is not supported"
      exit_with_usage
      ;;
  esac
  shift
done

if [ -z "$BASE_IMAGE" ]; then
  echo "start building base image: uniffle-base"
  docker build -t "uniffle-base:latest" \
    -f base/"${OS_DISTRIBUTION}"/Dockerfile .
  BASE_IMAGE="uniffle-base:latest"
else
  echo "using base image(${BASE_IMAGE}) to build rss server"
fi


HADOOP_FILE=hadoop-${HADOOP_VERSION}.tar.gz
ARCHIVE_HADOOP_URL=https://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP_VERSION}/${HADOOP_FILE}
HADOOP_URL=${APACHE_MIRROR}/hadoop/core/hadoop-${HADOOP_VERSION}/${HADOOP_FILE}
echo "HADOOP_URL is either ${HADOOP_URL} or ${ARCHIVE_HADOOP_URL}"
if [ ! -e "$HADOOP_FILE" ]; \
  then wget "${HADOOP_URL}" || wget "$ARCHIVE_HADOOP_URL"; \
  else echo "${HADOOP_FILE} has been downloaded"; \
fi

RSS_DIR=../../..
cd $RSS_DIR || exit
RSS_VERSION=$(./mvnw help:evaluate -Dexpression=project.version 2>/dev/null | grep -v "INFO" | grep -v "WARNING" | tail -n 1)
RSS_FILE=rss-${RSS_VERSION}-hadoop${HADOOP_SHORT_VERSION}.tgz
echo "RSS_VERSION: $RSS_VERSION"
echo "RSS_FILE: $RSS_FILE"
if [ ! -e "$RSS_FILE" ]; \
  then bash ./build_distribution.sh; \
  else echo "$RSS_FILE has been built"; \
fi
cd "$OLDPWD" || exit
cp "$RSS_DIR/$RSS_FILE" .

GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
GIT_COMMIT=$(git describe --dirty --always --tags | sed 's/-/./g')
echo "image version: ${IMAGE_VERSION:=$RSS_VERSION-$GIT_COMMIT}"
IMAGE=$REGISTRY/rss-server:$IMAGE_VERSION
echo "building image: $IMAGE"
docker build --network=host -t "$IMAGE" \
             --build-arg RSS_VERSION="$RSS_VERSION" \
             --build-arg HADOOP_VERSION="$HADOOP_VERSION" \
             --build-arg HADOOP_SHORT_VERSION="$HADOOP_SHORT_VERSION" \
             --build-arg AUTHOR="$AUTHOR" \
             --build-arg GIT_COMMIT="$GIT_COMMIT" \
             --build-arg GIT_BRANCH="$GIT_BRANCH" \
             --build-arg BASE_IMAGE="$BASE_IMAGE" \
             -f Dockerfile --no-cache .

if [ x"${PUSH_IMAGE}" == x"true" ]; then
  echo "pushing image: $IMAGE"
  docker push "$IMAGE"
fi
