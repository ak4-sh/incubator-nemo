#!/usr/bin/env bash
set -euo pipefail

FIRST_PORT=${1:-25321}
NUM_WORKERS=${2:-31}
VM_WORKER_JAR=${3:-/users/akash01/incubator-nemo/offloading/workers/vm/target/offloading-vm-0.2-SNAPSHOT-shaded.jar}
TIMEOUT=${4:-10000000}
JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}

echo "Starting $NUM_WORKERS VMWorkers on $(hostname)"
echo "  Port range: $FIRST_PORT-$((FIRST_PORT + NUM_WORKERS - 1))"
echo "  Jar: $VM_WORKER_JAR"

for i in $(seq 0 $((NUM_WORKERS - 1))); do
  PORT=$((FIRST_PORT + i))
  CORE=$((i % 40))
  LOGFILE="/tmp/vmworker-${PORT}.log"
  taskset -c "$CORE" "$JAVA_HOME/bin/java" -cp "$VM_WORKER_JAR" \
    org.apache.nemo.offloading.workers.vm.VMWorker "$PORT" "$TIMEOUT" \
    > "$LOGFILE" 2>&1 &
  echo "  Started VMWorker on core $CORE port $PORT pid $!"
done

echo "All workers started"
