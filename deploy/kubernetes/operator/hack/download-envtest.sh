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

# Downloads the envtest binaries (etcd, kube-apiserver, kubectl) that
# controller-runtime's envtest package needs, into <dest-dir>/controller-tools/envtest.
#
# We fetch them straight from the controller-tools GitHub releases instead of
# going through setup-envtest, because the gs://kubebuilder-tools bucket that
# older setup-envtest versions download from is no longer publicly readable
# (it answers 401/403), which silently left KUBEBUILDER_ASSETS empty and made
# the envtest suites fail with `exec: "etcd": executable file not found`.
#
# Usage: download-envtest.sh <k8s-version> <dest-dir>
#
# Environment overrides:
#   ENVTEST_INDEX_URL  release index to resolve the asset URL and hash from,
#                      for mirrors / offline setups.
#
# Already-present binaries under <dest-dir>/controller-tools/envtest are reused,
# so an offline build can pre-populate that directory and skip the download.

set -e
set -u
set -o pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $(basename "$0") <k8s-version> <dest-dir>" >&2
  exit 1
fi

VERSION="$1"
DEST_DIR="$2"
INDEX_URL="${ENVTEST_INDEX_URL:-https://raw.githubusercontent.com/kubernetes-sigs/controller-tools/main/envtest-releases.yaml}"

BIN_DIR="${DEST_DIR}/controller-tools/envtest"

if [ -x "${BIN_DIR}/etcd" ] && [ -x "${BIN_DIR}/kube-apiserver" ]; then
  echo "envtest binaries already present in ${BIN_DIR}" >&2
  exit 0
fi

for tool in curl tar go; do
  command -v "$tool" >/dev/null 2>&1 || { echo "$tool is required but not installed" >&2; exit 1; }
done

ASSET="envtest-v${VERSION}-$(go env GOOS)-$(go env GOARCH).tar.gz"

# The index nests the assets two levels deep under the release version, but the
# asset name is unique across the file, so match on it and read the two fields
# that follow.
entry="$(curl -fsSL "${INDEX_URL}" | grep -A2 -F "    ${ASSET}:")" || {
  echo "${ASSET} not found in ${INDEX_URL}" >&2
  exit 1
}
hash="$(echo "${entry}" | awk '$1 == "hash:" { print $2; exit }')"
url="$(echo "${entry}" | awk '$1 == "selfLink:" { print $2; exit }')"

if [ -z "${hash}" ] || [ -z "${url}" ]; then
  echo "could not resolve the download URL or hash of ${ASSET} from ${INDEX_URL}" >&2
  exit 1
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

echo "Downloading ${ASSET} from ${url}" >&2
curl -fsSL -o "${TMP_DIR}/${ASSET}" "${url}"

if command -v sha512sum >/dev/null 2>&1; then
  actual="$(sha512sum "${TMP_DIR}/${ASSET}" | awk '{print $1}')"
else
  actual="$(shasum -a 512 "${TMP_DIR}/${ASSET}" | awk '{print $1}')"
fi
if [ "${actual}" != "${hash}" ]; then
  echo "checksum mismatch for ${ASSET}: expected ${hash}, got ${actual}" >&2
  exit 1
fi

tar -xzf "${TMP_DIR}/${ASSET}" -C "${TMP_DIR}"
if [ ! -x "${TMP_DIR}/controller-tools/envtest/etcd" ]; then
  echo "${ASSET} does not contain controller-tools/envtest/etcd" >&2
  exit 1
fi

mkdir -p "$(dirname "${BIN_DIR}")"
rm -rf "${BIN_DIR}"
mv "${TMP_DIR}/controller-tools/envtest" "${BIN_DIR}"
echo "Installed envtest binaries into ${BIN_DIR}" >&2
