#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/cloudlab_env.sh"

TOPIC=${TOPIC:-nexmark-cloudlab-vm-smoke-$(date +%H%M%S)}
WORK_DIR=${WORK_DIR:-/tmp/nx-cloudlab-vm-work-$TOPIC}
WARM_NODE=${WARM_NODE:-node10}
WORKERS=${WORKERS:-4}
FIRST_PORT=${FIRST_PORT:-25321}
LOG_FILE=${LOG_FILE:-/tmp/nx-cloudlab-vm-forced-$TOPIC.log}
EXECUTOR_JSON=${EXECUTOR_JSON:-$NEMO_REPO_ROOT/configs/cloudlab/nemo-yarn-kafka-1source-8slot-12compute.json}
REMOTE_WARM_DIR=${REMOTE_WARM_DIR:-/tmp/nemo-cloudlab-offload}

mkdir -p "$WORK_DIR"
python3 "$SCRIPT_DIR/generate_vm_addresses.py" --nodes "$WARM_NODE" --first-port "$FIRST_PORT" --workers-per-node "$WORKERS" --output "$NEMO_REPO_ROOT/vm_addresses.txt"

ssh "$KAFKA_NODE" "$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server '$KAFKA_BOOTSTRAP' --create --topic '$TOPIC' --partitions 1 --replication-factor 1 || true"
ssh "$KAFKA_NODE" "$KAFKA_HOME/bin/kafka-configs.sh --bootstrap-server '$KAFKA_BOOTSTRAP' --entity-type topics --entity-name '$TOPIC' --alter --add-config min.insync.replicas=1"

java -cp "$STANDALONE_PRODUCER_CP" StandaloneNexmarkKafkaProducer \
  "$KAFKA_BOOTSTRAP" "$TOPIC" 100 1000 50000 5 false >/tmp/nx-cloudlab-vm-prefill-$TOPIC.log 2>&1

ssh "$WARM_NODE" "mkdir -p '$REMOTE_WARM_DIR'"
scp "$SCRIPT_DIR/start_warm_pool.sh" "$WARM_NODE:$REMOTE_WARM_DIR/start_warm_pool.sh" >/dev/null
scp "$VM_WORKER_JAR" "$WARM_NODE:$REMOTE_WARM_DIR/offloading-vm-0.2-SNAPSHOT-shaded.jar" >/dev/null
ssh "$WARM_NODE" "pkill -f '[o]rg.apache.nemo.offloading.workers.vm.VMWorker' || true; rm -f /tmp/vmworker-2532*.log; bash '$REMOTE_WARM_DIR/start_warm_pool.sh' '$FIRST_PORT' '$WORKERS' '$REMOTE_WARM_DIR/offloading-vm-0.2-SNAPSHOT-shaded.jar' 10000000 >/tmp/start-warm-pool-smoke.log 2>&1"

echo "Launching subscriber and forcing add-lambda-executor through scaling.txt"
NEMO_WORK_DIR="$WORK_DIR" java -cp "$NEMO_CLIENT_CP" org.apache.nemo.client.JobLauncher \
  -job_id "nx-cloudlab-vm-forced-$TOPIC" \
  -user_main org.apache.beam.sdk.nexmark.Main \
  -deploy_mode yarn \
  -executor_json "$EXECUTOR_JSON" \
  -optimization_policy org.apache.nemo.compiler.optimizer.policy.DefaultPolicy \
  -enable_offloading true \
  -offloading_type cloudlab-vm \
  -num_max_lambda "$WORKERS" \
  -user_args "--runner=org.apache.nemo.client.beam.NemoRunner --streaming=true --query=8 --manageResources=false --monitorJobs=true --streamTimeout=120 --isRateLimited=false --windowSizeSec=10 --windowPeriodSec=1 --fanout=1 --cpuDelayMs=0 --samplingRate=1.0 --numEvents=1000 --sourceType=KAFKA --pubSubMode=SUBSCRIBE_ONLY --bootstrapServers=$KAFKA_BOOTSTRAP --kafkaTopic=$TOPIC --sinkType=COUNT_ONLY --jobName=nx-cloudlab-vm-forced-$TOPIC" \
  > "$LOG_FILE" 2>&1 &
SUB_PID=$!

for _ in $(seq 1 60); do
  grep -q 'Scaling service invoked' "$LOG_FILE" && break
  sleep 1
done

printf 'add-lambda-executor 1 1 1 1024\n' >> "$WORK_DIR/scaling.txt"
echo "Wrote scaling decision to $WORK_DIR/scaling.txt"
echo "Wait ~30s, then inspect:"
echo "  $LOG_FILE"
echo "  ssh $WARM_NODE 'tail -80 /tmp/vmworker-$FIRST_PORT.log'"
echo "Subscriber PID: $SUB_PID"
