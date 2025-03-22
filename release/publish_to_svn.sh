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
set -e
set -x

PROJECT_DIR="$(cd "$(dirname "$0")"/..; pwd)"

ASF_USERNAME=${ASF_USERNAME:?"ASF_USERNAME is required"}
ASF_PASSWORD=${ASF_PASSWORD:?"ASF_PASSWORD is required"}
RELEASE_RC_NO=${RELEASE_RC_NO:?"RELEASE_RC_NO is required, e.g. 0"}

RELEASE_VERSION=$(grep 'uniffle-parent' "${PROJECT_DIR}/pom.xml" -C 3 |grep 'version' \
                | head -n 1 \
                | sed 's/<\/*version>//g' \
                | sed 's/ //g')

if [[ ${RELEASE_VERSION} =~ .*-SNAPSHOT ]]; then
  echo "Can not release a SNAPSHOT version: ${RELEASE_VERSION}"
  exit 1
fi

RELEASE_TAG="${RELEASE_VERSION}-rc${RELEASE_RC_NO}"

SVN_STAGING_REPO="https://dist.apache.org/repos/dist/dev/incubator/uniffle"

RELEASE_DIR="${PROJECT_DIR}/tmp"
SVN_STAGING_DIR="${PROJECT_DIR}/tmp/dev/uniffle"

package() {
  SKIP_GPG="false" $PROJECT_DIR/release/create-package.sh source
  SKIP_GPG="false" $PROJECT_DIR/release/create-package.sh binary
}

upload_svn_staging() {
  svn checkout --depth=empty "${SVN_STAGING_REPO}" "${SVN_STAGING_DIR}"
  mkdir -p "${SVN_STAGING_DIR}/${RELEASE_TAG}"
  rm -f "${SVN_STAGING_DIR}/${RELEASE_TAG}/*"

  SRC_TGZ_FILE="apache-uniffle-${RELEASE_VERSION}-src.tar.gz"
  BIN_TGZ_FILE="apache-uniffle-${RELEASE_VERSION}-bin.tar.gz"

  echo "Copying release tarballs"
  cp "${RELEASE_DIR}/${SRC_TGZ_FILE}"        "${SVN_STAGING_DIR}/${RELEASE_TAG}/${SRC_TGZ_FILE}"
  cp "${RELEASE_DIR}/${SRC_TGZ_FILE}.asc"    "${SVN_STAGING_DIR}/${RELEASE_TAG}/${SRC_TGZ_FILE}.asc"
  cp "${RELEASE_DIR}/${SRC_TGZ_FILE}.sha512" "${SVN_STAGING_DIR}/${RELEASE_TAG}/${SRC_TGZ_FILE}.sha512"
  cp "${RELEASE_DIR}/${BIN_TGZ_FILE}"        "${SVN_STAGING_DIR}/${RELEASE_TAG}/${BIN_TGZ_FILE}"
  cp "${RELEASE_DIR}/${BIN_TGZ_FILE}.asc"    "${SVN_STAGING_DIR}/${RELEASE_TAG}/${BIN_TGZ_FILE}.asc"
  cp "${RELEASE_DIR}/${BIN_TGZ_FILE}.sha512" "${SVN_STAGING_DIR}/${RELEASE_TAG}/${BIN_TGZ_FILE}.sha512"

  svn add "${SVN_STAGING_DIR}/${RELEASE_TAG}"

  echo "Uploading release tarballs to ${SVN_STAGING_DIR}/${RELEASE_TAG}"
  (
    cd "${SVN_STAGING_DIR}" && \
    svn commit --username "${ASF_USERNAME}" --password "${ASF_PASSWORD}" --message "prepare for ${RELEASE_TAG}"
  )
  echo "Uniffle tarballs uploaded"
}

package
upload_svn_staging
