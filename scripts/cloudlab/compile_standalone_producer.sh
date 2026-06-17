#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}
export PATH="$JAVA_HOME/bin:$PATH"

NEXMARK_JAR=${NEXMARK_JAR:-$REPO_ROOT/examples/nexmark/target/nexmark-0.2-SNAPSHOT-shaded.jar}
OUT_DIR=${OUT_DIR:-$REPO_ROOT/build/cloudlab-producer}

mkdir -p "$OUT_DIR"

if [[ ! -f "$NEXMARK_JAR" ]]; then
  echo "Missing Nexmark jar: $NEXMARK_JAR" >&2
  echo "Run scripts/cloudlab/build_cloudlab_java8.sh first." >&2
  exit 1
fi

javac -cp "$NEXMARK_JAR" -d "$OUT_DIR" "$SCRIPT_DIR/StandaloneNexmarkKafkaProducer.java"

echo "Compiled standalone producer to $OUT_DIR"
