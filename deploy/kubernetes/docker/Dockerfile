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

ARG BASE_IMAGE=uniffle-base:latest
FROM ${BASE_IMAGE}

ARG HADOOP_VERSION
ARG HADOOP_SHORT_VERSION
ARG RSS_VERSION
ARG AUTHOR=unknown
ARG GIT_BRANCH=unknown
ARG GIT_COMMIT=unknown

LABEL author=${AUTHOR}
LABEL git-branch=${GIT_BRANCH}
LABEL git-commit=${GIT_COMMIT}

RUN useradd -ms /bin/bash rssadmin
RUN mkdir -p /data/rssadmin/
RUN chown -R rssadmin:rssadmin /data
USER rssadmin

COPY rss-${RSS_VERSION}-hadoop${HADOOP_SHORT_VERSION}.tgz /data/rssadmin
RUN tar -xvf /data/rssadmin/rss-${RSS_VERSION}-hadoop${HADOOP_SHORT_VERSION}.tgz -C /data/rssadmin
RUN mv /data/rssadmin/rss-${RSS_VERSION}-hadoop${HADOOP_SHORT_VERSION} /data/rssadmin/rss
RUN rm /data/rssadmin/rss/conf/rss-env.sh
RUN rm -rf /data/rssadmin/rss-${RSS_VERSION}-hadoop${HADOOP_SHORT_VERSION}.tgz

COPY rss-env.sh /data/rssadmin/rss/conf

COPY start.sh /data/rssadmin/rss/bin

COPY hadoop-${HADOOP_VERSION}.tar.gz /data/rssadmin
RUN tar -zxvf /data/rssadmin/hadoop-${HADOOP_VERSION}.tar.gz -C /data/rssadmin
RUN mv /data/rssadmin/hadoop-${HADOOP_VERSION} /data/rssadmin/hadoop
RUN rm -rf /data/rssadmin/hadoop-${HADOOP_VERSION}.tar.gz
COPY hadoopconfig/ /data/rssadmin/hadoop/etc/hadoop

ENV RSS_VERSION ${RSS_VERSION}
ENV HADOOP_VERSION ${HADOOP_VERSION}

WORKDIR /data/rssadmin
CMD ["bash", "/data/rssadmin/rss/bin/start.sh"]
