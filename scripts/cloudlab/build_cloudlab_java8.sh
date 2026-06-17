#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}
export PATH="$JAVA_HOME/bin:$PATH"

cd "$REPO_ROOT"

echo "Building Nemo for CloudLab with Java 8 bytecode"
echo "  REPO_ROOT=$REPO_ROOT"
echo "  JAVA_HOME=$JAVA_HOME"

mvn -DskipTests -Djava.version=1.8 package

"$SCRIPT_DIR/compile_standalone_producer.sh"

echo "Build complete"
echo "  Nemo client: $REPO_ROOT/client/target/nemo-client-0.2-SNAPSHOT-shaded.jar"
echo "  Nexmark jar: $REPO_ROOT/examples/nexmark/target/nexmark-0.2-SNAPSHOT-shaded.jar"
echo "  VM worker:   $REPO_ROOT/offloading/workers/vm/target/offloading-vm-0.2-SNAPSHOT-shaded.jar"
echo "  Producer:    $REPO_ROOT/build/cloudlab-producer/StandaloneNexmarkKafkaProducer.class"
