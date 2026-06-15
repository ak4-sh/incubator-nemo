#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
MAVEN_REPO="${MAVEN_REPO:-${M2_HOME:-${HOME}/.m2/repository}}"
VERSION="$(mvn -q -DforceStdout -f "${ROOT_DIR}/pom.xml" help:evaluate -Dexpression=project.version)"

mkdir -p "${SCRIPT_DIR}/jars"
rm -f "${SCRIPT_DIR}"/jars/*

copy_required() {
  local source="$1"
  if [[ ! -f "${source}" ]]; then
    echo "Missing required jar: ${source}" >&2
    exit 1
  fi
  cp "${source}" "${SCRIPT_DIR}/jars/"
}

copy_required "${MAVEN_REPO}/org/apache/beam/beam-sdks-java-nexmark/2.6.0-SNAPSHOT/beam-sdks-java-nexmark-2.6.0-SNAPSHOT.jar"
copy_required "${MAVEN_REPO}/org/apache/beam/beam-sdks-java-io-kafka/2.6.0-SNAPSHOT/beam-sdks-java-io-kafka-2.6.0-SNAPSHOT.jar"

#cp lib/* jars/
#cp kafka_lib/* jars/

copy_required "${ROOT_DIR}/compiler/frontend/beam/target/nemo-compiler-frontend-beam-${VERSION}.jar"
copy_required "${ROOT_DIR}/common/target/nemo-common-${VERSION}.jar"
copy_required "${ROOT_DIR}/offloading/common/target/offloading-common-${VERSION}.jar"
copy_required "${ROOT_DIR}/runtime/lambda-executor/target/nemo-lambda-executor-${VERSION}.jar"
copy_required "${ROOT_DIR}/runtime/executor-common/target/executor-common-${VERSION}.jar"
copy_required "${ROOT_DIR}/runtime/message/target/nemo-runtime-message-${VERSION}.jar"
copy_required "${ROOT_DIR}/conf/target/nemo-conf-${VERSION}.jar"
