#!/usr/bin/env bash
set -euo pipefail

FIRST_PORT=${1:-25321}
NUM_WORKERS=${2:-31}
VM_WORKER_JAR=${3:-/users/akash01/incubator-nemo/offloading/workers/vm/target/offloading-vm-0.2-SNAPSHOT-shaded.jar}
TIMEOUT=${4:-10000000}
EXTRA_CP=${5:-}
JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}

# Detect available CPUs and cap workers to fit
CORES=$(nproc)
if [[ "$NUM_WORKERS" -gt "$CORES" ]]; then
  echo "WARNING: Requested $NUM_WORKERS workers but only $CORES cores available; capping to $CORES"
  NUM_WORKERS=$CORES
fi

if [[ -n "$EXTRA_CP" ]]; then
  CP="$EXTRA_CP:$VM_WORKER_JAR"
else
  CP="$VM_WORKER_JAR"
fi

echo "Starting $NUM_WORKERS VMWorkers on $(hostname) ($CORES cores)"
echo "  Port range: $FIRST_PORT-$((FIRST_PORT + NUM_WORKERS - 1))"
echo "  Jar: $VM_WORKER_JAR"
echo "  Extra CP: $EXTRA_CP"

for i in $(seq 0 $((NUM_WORKERS - 1))); do
  PORT=$((FIRST_PORT + i))
  CORE=$((i % CORES))
  LOGFILE="/tmp/vmworker-${PORT}.log"
  taskset -c "$CORE" "$JAVA_HOME/bin/java" -cp "$CP" \
    org.apache.nemo.offloading.workers.vm.VMWorker "$PORT" "$TIMEOUT" \
    > "$LOGFILE" 2>&1 &
  echo "  Started VMWorker on core $CORE port $PORT pid $!"
done

echo "All workers started"
