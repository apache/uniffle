<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

# End-to-end Test

Currently, we can quickly pull up an e2e test environment locally by executing local-up-cluster.sh.

The kubernetes environment for local testing is built with [kind](https://github.com/kubernetes-sigs/kind).

The script reads parameters from the following environment variables, which are:

+ "TEST_REGISTRY": represents the url of the registry where the test image resides
+ "BUILD_NEW_CLUSTER": indicates whether a new kubernetes cluster needs to be built
+ "BUILD_RSS_IMAGE": indicates whether a new rss image needs to be built
+ "BUILD_RSS_OPERATOR": indicates whether a new operator image needs to be built

We can quickly build the environment by executing the following command:

```
$ TEST_REGISTRY=${users-registry-url} sh start-e2e.sh
```

## Dependence

We need to install [golang 1.25+](https://go.dev/doc/install), [docker](https://www.docker.com/get-started/) and 
[kind](https://github.com/kubernetes-sigs/kind) first.