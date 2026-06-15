#!/usr/bin/env bash
set -euo pipefail
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

VERSION="$(mvn -q -DforceStdout -f "${ROOT_DIR}/pom.xml" help:evaluate -Dexpression=project.version)"
if [[ -z "${VERSION}" || "${VERSION}" == *"ERROR"* || "${VERSION}" == *"Failed"* ]]; then
  echo "Failed to determine Nemo project.version from ${ROOT_DIR}/pom.xml" >&2
  exit 1
fi

CLIENT_JAR="${ROOT_DIR}/client/target/nemo-client-${VERSION}-shaded.jar"
NEXMARK_JAR="${ROOT_DIR}/examples/nexmark/target/nexmark-${VERSION}-shaded.jar"
LAMBDA_JAR="${ROOT_DIR}/offloading/workers/lambda/target/offloading-lambda-${VERSION}.jar"
HADOOP_COMMON_JAR="${HADOOP_COMMON_JAR:-${HOME}/.m2/repository/org/apache/hadoop/hadoop-common/2.7.2/hadoop-common-2.7.2.jar}"

for jar in "${CLIENT_JAR}" "${NEXMARK_JAR}" "${LAMBDA_JAR}" "${HADOOP_COMMON_JAR}"; do
  if [[ ! -f "${jar}" ]]; then
    echo "Missing required jar: ${jar}" >&2
    echo "Build Nemo first with: mvn clean install -DskipTests -T 2C" >&2
    exit 1
  fi
done

YARN_CLASSPATH=""
if command -v yarn >/dev/null 2>&1; then
  YARN_CLASSPATH="$(yarn classpath 2>/dev/null || true)"
fi

JAVA_OPTS="${NEMO_JAVA_OPTS:---add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=jdk.unsupported/sun.misc=ALL-UNNAMED}"

exec java ${JAVA_OPTS} \
  -Dlog4j.configuration="file://${ROOT_DIR}/log4j.properties" \
  -cp "${LAMBDA_JAR}:${HADOOP_COMMON_JAR}:${CLIENT_JAR}:${YARN_CLASSPATH}:${NEXMARK_JAR}" \
  org.apache.nemo.client.JobLauncher "$@"
