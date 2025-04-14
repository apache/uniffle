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

# Installation

This section shows us how to install operator in our cluster.

## Requirements

1. Kubernetes 1.24+
2. Kubectl 1.24+

Please make sure the kubectl is properly configured to interact with the Kubernetes environment.

## Preparing Images of Coordinators and Shuffle Servers

Run the following command:

```
cd deploy/kubernetes/docker && sh build.sh --registry ${our-registry}
```

This compiles RSS with Hadoop 2.8 support und add the Hadoop binaries to the Docker image.
Use `--hadoop-version x.y.z` to choose a different Hadoop version. Use `--hadoop-provided false` to **not**
include the Hadoop installation in the image.

## Creating or Updating CRD

We can refer
to [crd yaml file](../../deploy/kubernetes/operator/config/crd/bases/uniffle.apache.org_remoteshuffleservices.yaml).

Run the following command:

```
# create, cannot use apply here, see https://github.com/apache/incubator-uniffle/issues/774
kubectl create -f ${crd-yaml-file}

# update, make sure the crd-yaml-file is a complete CRD file.
kubectl replace -f ${crd-yaml-file}
```

## Setup or Update Uniffle Webhook

We can refer to [webhook yaml file](../../deploy/kubernetes/operator/config/manager/rss-webhook.yaml).

Run the following command:

```
kubectl apply -f ${webhook-yaml-file}
```

## Setup or Update Uniffle Controller

We can refer to [controller yaml file](../../deploy/kubernetes/operator/config/manager/rss-controller.yaml).

Run the following command:

```
kubectl apply -f ${controller-yaml-file}
```

## How To Use

We can learn more details about usage of CRD
from [uniffle operator design](design.md).

## Examples

Example uses of CRD have been [provided](examples.md).
