#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/cloudlab_env.sh"

# Configuration
TOPIC=${TOPIC:-nexmark-auto-$(date +%H%M%S)}

# Burst mode (default: custom burst with ramp-up + multiple bursts)
BURST_MODE=${BURST_MODE:-custom}
FIRST_RATE=${FIRST_RATE:-50000}
NEXT_RATE=${NEXT_RATE:-200000}
STEP_START_RATE=${STEP_START_RATE:-5000}
STEP_RATE=${STEP_RATE:-5000}
STEP_DURATION_SEC=${STEP_DURATION_SEC:-20}
STEP_NUM_STEPS=${STEP_NUM_STEPS:-20}
PHASE_RATES=${PHASE_RATES:-20000,100000,200000}
PHASE_DURATIONS_SEC=${PHASE_DURATIONS_SEC:-100,150,150}
STEADY_DURATION_SEC=${STEADY_DURATION_SEC:-60}
BURST_DURATION_SEC=${BURST_DURATION_SEC:-45}
NUM_BURSTS=${NUM_BURSTS:-3}
RAMP_UP_SEC=${RAMP_UP_SEC:-60}

# Legacy mode support (when BURST_MODE=legacy)
TOTAL_EVENTS=${TOTAL_EVENTS:-500}
PREFILL_EVENTS=${PREFILL_EVENTS:-100}
LIVE_EVENTS=$((TOTAL_EVENTS - PREFILL_EVENTS))
RATE_PERIOD_SEC=${RATE_PERIOD_SEC:-50}
SUBSCRIBER_WAIT=${SUBSCRIBER_WAIT:-10}
PRODUCER_RATE_LIMITED=${PRODUCER_RATE_LIMITED:-true}

QUERY=${QUERY:-0}
EXECUTOR_JSON=${EXECUTOR_JSON:-$NEMO_REPO_ROOT/configs/cloudlab/nemo-yarn-kafka-1source-8slot-12compute.json}
STREAM_TIMEOUT=${STREAM_TIMEOUT:-900}
NUM_EVENTS=${NUM_EVENTS:-$TOTAL_EVENTS}
CPU_DELAY_MS=${CPU_DELAY_MS:-0}
AUTOSCALING=${AUTOSCALING:-false}
SCALER_START_MODE=${SCALER_START_MODE:-immediate}
SCALER_START_PHASE=${SCALER_START_PHASE:-2}
SCALER_START_PHASE_DELAY_SEC=${SCALER_START_PHASE_DELAY_SEC:-60}
SCALER_START_WAIT_TIMEOUT_SEC=${SCALER_START_WAIT_TIMEOUT_SEC:-900}
KAFKA_RESULTS_TOPIC=${KAFKA_RESULTS_TOPIC:-}
JOB_ID=${JOB_ID:-nx-q${QUERY}-${TOPIC}}

# Offloading nodes
OFFLOAD_NODES=${OFFLOAD_NODES:-node4,node6,node7,node8,node13}
WORKERS_PER_NODE=${WORKERS_PER_NODE:-32}
FIRST_PORT=${FIRST_PORT:-25321}
# Auto-compute max lambda executors from available VM workers (override via NUM_MAX_LAMBDA env var)
_OFFLOAD_COUNT=$(echo "$OFFLOAD_NODES" | tr "," "\n" | wc -l | tr -d " ")
NUM_MAX_LAMBDA=${NUM_MAX_LAMBDA:-$(( WORKERS_PER_NODE * _OFFLOAD_COUNT ))}

# Baseline nodes
BASELINE_NODES=${BASELINE_NODES:-node5 node9 node10 node11 node12}
EXPECTED_NM_COUNT=${EXPECTED_NM_COUNT:-5}

# Scaling parameters
LAMBDA_CAPACITY=${LAMBDA_CAPACITY:-1}
LAMBDA_SLOT=${LAMBDA_SLOT:-1}
LAMBDA_MEMORY=${LAMBDA_MEMORY:-1024}

# Kafka topic and producer parallelism (Sponge: PARALLELISM=8)
KAFKA_PARTITIONS=${KAFKA_PARTITIONS:-8}
PRODUCER_PARALLELISM=${PRODUCER_PARALLELISM:-8}

KAFKA_ZOOKEEPER=${KAFKA_ZOOKEEPER:-node1:2181,node2:2181,node3:2181}
WORK_DIR=${WORK_DIR:-/tmp/nx-auto-$TOPIC}
LOG_FILE=${LOG_FILE:-/tmp/nx-auto-$TOPIC.log}
SUB_LOG=${SUB_LOG:-/tmp/nx-auto-sub-$TOPIC.log}
SOURCE_LOG=$WORK_DIR/source.log

mkdir -p "$WORK_DIR"
: > "$SOURCE_LOG"
SCALER_ENABLE_LOG=$WORK_DIR/scaler_enable.csv
: > "$WORK_DIR/producer_phases.csv"
: > "$SCALER_ENABLE_LOG"
PRODUCER_PID=""
SCALER_STARTED=false

if [[ "$SCALER_START_MODE" == "after_phase_delay" && "$BURST_MODE" != "phases" ]]; then
  echo "ERROR: SCALER_START_MODE=after_phase_delay requires BURST_MODE=phases" >&2
  exit 1
fi

# Preflight: ensure YARN NodeManagers are running
ensure_yarn_nodes() {
  local nm_count
  nm_count=$(yarn node -list 2>/dev/null | grep -c RUNNING || true)
  if [[ "$nm_count" -lt "$EXPECTED_NM_COUNT" ]]; then
    echo "WARNING: Only $nm_count/$EXPECTED_NM_COUNT YARN NodeManagers are running. Restarting..."
    for node in $BASELINE_NODES; do
      ssh "$node" "$HADOOP_HOME/sbin/yarn-daemon.sh start nodemanager" >/dev/null 2>&1 || true
    done
    echo "Waiting for NMs to register..."
    for i in $(seq 1 30); do
      nm_count=$(yarn node -list 2>/dev/null | grep -c RUNNING || true)
      echo "  attempt $i: $nm_count/$EXPECTED_NM_COUNT nodes"
      if [[ "$nm_count" -ge "$EXPECTED_NM_COUNT" ]]; then
        break
      fi
      sleep 2
    done
    nm_count=$(yarn node -list 2>/dev/null | grep -c RUNNING || true)
    if [[ "$nm_count" -lt "$EXPECTED_NM_COUNT" ]]; then
      echo "ERROR: Only $nm_count/$EXPECTED_NM_COUNT NMs registered. Aborting." >&2
      exit 1
    fi
  fi
  echo "Preflight passed: $nm_count/$EXPECTED_NM_COUNT YARN NodeManagers running."
}

# Wait for YARN app to reach RUNNING
wait_for_yarn_app_running() {
  local log_file=$1
  local app_id=""
  local i
  for i in $(seq 1 60); do
    app_id=$(grep -o 'application_[0-9]\+_[0-9]\+' "$log_file" | head -1 || true)
    if [[ -n "$app_id" ]]; then
      break
    fi
    sleep 2
  done
  if [[ -z "$app_id" ]]; then
    echo "ERROR: YARN app ID not found in subscriber log." >&2
    return 1
  fi
  echo "YARN app submitted: $app_id"
  for i in $(seq 1 180); do
    if yarn application -status "$app_id" 2>/dev/null | grep -q 'State : RUNNING'; then
      echo "YARN app $app_id is RUNNING."
      return 0
    fi
    sleep 2
  done
  echo "ERROR: YARN app $app_id never reached RUNNING. Killing it." >&2
  yarn application -kill "$app_id" 2>/dev/null || true
  return 1
}

start_producer_with_source_log() {
  local events=$1
  local live=$2
  local parallelism=${3:-$PRODUCER_PARALLELISM}

  if [[ "$BURST_MODE" == "step" && "$live" == "true" ]]; then
    local maxEvents=0
    if [[ "$events" -gt 0 ]]; then
      maxEvents="$events"
    fi
    java -cp "$STANDALONE_PRODUCER_CP" StandaloneNexmarkKafkaProducer \
      "$KAFKA_BOOTSTRAP" "$TOPIC" STEP "$STEP_START_RATE" "$STEP_RATE" "$STEP_DURATION_SEC" \
      "$STEP_NUM_STEPS" "$PRODUCER_RATE_LIMITED" "$parallelism" "$maxEvents" &
  elif [[ "$BURST_MODE" == "phases" && "$live" == "true" ]]; then
    local maxEvents=0
    if [[ "$events" -gt 0 ]]; then
      maxEvents="$events"
    fi
    java -cp "$STANDALONE_PRODUCER_CP" StandaloneNexmarkKafkaProducer \
      "$KAFKA_BOOTSTRAP" "$TOPIC" PHASES "$PHASE_RATES" "$PHASE_DURATIONS_SEC" \
      "$PRODUCER_RATE_LIMITED" "$parallelism" "$maxEvents" &
  elif [[ "$BURST_MODE" == "phases" ]]; then
    # Keep prefill minimal and fixed-rate. The live phase performs the explicit schedule.
    local prefill_rate
    prefill_rate=${PHASE_PREFILL_RATE:-20000}
    java -cp "$STANDALONE_PRODUCER_CP" StandaloneNexmarkKafkaProducer \
      "$KAFKA_BOOTSTRAP" "$TOPIC" "$events" "$prefill_rate" "$prefill_rate" "1" "$PRODUCER_RATE_LIMITED" &
  elif [[ "$BURST_MODE" == "step" ]]; then
    # Keep prefill minimal and fixed-rate. The live phase performs the step sweep.
    java -cp "$STANDALONE_PRODUCER_CP" StandaloneNexmarkKafkaProducer \
      "$KAFKA_BOOTSTRAP" "$TOPIC" "$events" "$STEP_START_RATE" "$STEP_START_RATE" "$STEP_DURATION_SEC" "$PRODUCER_RATE_LIMITED" &
  elif [[ "$BURST_MODE" == "custom" ]]; then
    # Custom burst mode: steadyRate burstRate steadySec burstSec numBursts isRateLimited numGenerators rampUpSec maxEvents
    # maxEvents=0 means use the full burst pattern
    local maxEvents=0
    if [[ "$events" -gt 0 ]]; then
      maxEvents="$events"
    fi
    java -cp "$STANDALONE_PRODUCER_CP" StandaloneNexmarkKafkaProducer \
      "$KAFKA_BOOTSTRAP" "$TOPIC" "$FIRST_RATE" "$NEXT_RATE" "$STEADY_DURATION_SEC" "$BURST_DURATION_SEC" "$NUM_BURSTS" "$PRODUCER_RATE_LIMITED" "$parallelism" "$RAMP_UP_SEC" "$maxEvents" &
  else
    # Legacy BURSTY mode
    java -cp "$STANDALONE_PRODUCER_CP" StandaloneNexmarkKafkaProducer \
      "$KAFKA_BOOTSTRAP" "$TOPIC" "$events" "$FIRST_RATE" "$NEXT_RATE" "$RATE_PERIOD_SEC" "$PRODUCER_RATE_LIMITED" "$parallelism" &
  fi
  PRODUCER_PID=$!
}

run_producer_with_source_log() {
  local events=$1
  local live=$2
  local parallelism=${3:-$PRODUCER_PARALLELISM}
  local status

  start_producer_with_source_log "$events" "$live" "$parallelism"
  status=0
  wait "$PRODUCER_PID" || status=$?
  return "$status"
}

write_lambda_executor_command() {
  printf 'add-lambda-executor %d %d %d %d\n' "$NUM_MAX_LAMBDA" "$LAMBDA_CAPACITY" "$LAMBDA_SLOT" "$LAMBDA_MEMORY" >> "$WORK_DIR/scaling.txt"
}

start_autoscaler_commands() {
  local reason=${1:-manual}
  local phase=${2:-}
  local target_rate=${3:-}
  local delay_sec=${4:-0}
  local now

  if [[ "${AUTOSCALING:-false}" != "true" ]]; then
    return 0
  fi
  if [[ "$SCALER_STARTED" == "true" ]]; then
    return 0
  fi

  now=$(date +%s%3N)
  printf 'start-scaler\n' >> "$WORK_DIR/scaling.txt"
  printf 'start-backpressure\n' >> "$WORK_DIR/scaling.txt"
  if [[ ! -s "$SCALER_ENABLE_LOG" ]]; then
    printf 'timestamp,reason,phase,targetRate,delaySec\n' > "$SCALER_ENABLE_LOG"
  fi
  printf '%s,%s,%s,%s,%s\n' "$now" "$reason" "$phase" "$target_rate" "$delay_sec" >> "$SCALER_ENABLE_LOG"
  SCALER_STARTED=true
}

wait_for_phase_start() {
  local phase=$1
  local timeout_sec=$2
  local phase_file=$WORK_DIR/producer_phases.csv
  local started_at
  local line
  local waited

  echo "Waiting for producer phase $phase to start in $phase_file" >&2
  for waited in $(seq 0 "$timeout_sec"); do
    if [[ -s "$phase_file" ]]; then
      line=$(awk -F, -v phase="$phase" '$2 == "start" && $3 == phase {print; exit}' "$phase_file")
      if [[ -n "$line" ]]; then
        started_at=$(printf '%s\n' "$line" | awk -F, '{print $1}')
        echo "  Producer phase $phase started at $started_at: $line" >&2
        printf '%s\n' "$line"
        return 0
      fi
    fi
    if [[ -n "$PRODUCER_PID" ]] && ! kill -0 "$PRODUCER_PID" 2>/dev/null; then
      echo "ERROR: producer exited before phase $phase started" >&2
      return 1
    fi
    sleep 1
  done
  echo "ERROR: phase $phase did not start within ${timeout_sec}s" >&2
  return 1
}

# Wait for Nemo source tasks to finish Kafka partition assignment.
# Poll the subscriber log for stable "Curr input:" heartbeats (these appear once
# all executor tasks including the Kafka source are running), then sleep a fixed
# buffer so the consumer completes rebalancing before any events arrive.
PARTITION_ASSIGN_BUFFER=${PARTITION_ASSIGN_BUFFER:-90}
wait_for_nemo_source_ready() {
  echo "Waiting for Nemo source tasks to report 'Curr input:' in $SUB_LOG ..."
  local seen=0 i
  for i in $(seq 1 300); do
    if grep -q "Curr input:" "$SUB_LOG" 2>/dev/null; then
      seen=$((seen + 1))
      if [ "$seen" -ge 3 ]; then
        echo "  Nemo source metrics stable after ~${i}s; sleeping ${PARTITION_ASSIGN_BUFFER}s for partition assignment"
        sleep "$PARTITION_ASSIGN_BUFFER"
        return 0
      fi
    else
      seen=0
    fi
    sleep 1
  done
  echo "WARNING: Nemo source metrics did not stabilise within 300s; proceeding anyway" >&2
}

# 1. Generate vm_addresses.txt for all offload nodes
echo "Generating vm_addresses.txt for offloading nodes: $OFFLOAD_NODES"
IFS=',' read -ra NODE_LIST <<< "$OFFLOAD_NODES"
python3 "$SCRIPT_DIR/generate_vm_addresses.py" \
  --nodes "${NODE_LIST[@]}" \
  --first-port "$FIRST_PORT" \
  --workers-per-node "$WORKERS_PER_NODE" \
  --output "$NEMO_REPO_ROOT/vm_addresses.txt"

# 2. Create Kafka topic and prefill
echo "Creating Kafka topic $TOPIC with $KAFKA_PARTITIONS partitions"
ssh "$KAFKA_NODE" "$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server node1:9092 --create --topic '$TOPIC' --partitions $KAFKA_PARTITIONS --replication-factor 1 || true"
ssh "$KAFKA_NODE" "$KAFKA_HOME/bin/kafka-configs.sh --bootstrap-server node1:9092 --entity-type topics --entity-name '$TOPIC' --alter --add-config min.insync.replicas=1 || true"

if [[ "$PREFILL_EVENTS" -gt 0 ]]; then
  echo "Prefilling $PREFILL_EVENTS records"
  run_producer_with_source_log "$PREFILL_EVENTS" false
fi

# 3. Start warm VMWorker pools on all offload nodes
echo "Starting warm VMWorker pools"
IFS=',' read -ra OFFLOAD_NODE_ARRAY <<< "$OFFLOAD_NODES"
for node in "${OFFLOAD_NODE_ARRAY[@]}"; do
  echo "  Starting pool on $node"
  ssh "$node" "pkill -f '[o]rg.apache.nemo.offloading.workers.vm.VMWorker' || true; rm -f /tmp/vmworker-*.log /tmp/task_metrics.csv /tmp/source_task_metrics.csv" || true
  ssh "$node" "mkdir -p /tmp/nemo-cloudlab-offload" || true
  scp "$SCRIPT_DIR/start_warm_pool.sh" "$node:/tmp/nemo-cloudlab-offload/start_warm_pool.sh" >/dev/null
  scp "$VM_WORKER_JAR" "$node:/tmp/nemo-cloudlab-offload/offloading-vm.jar" >/dev/null
  scp "$REBUILT_NEMO" "$node:/tmp/nemo-cloudlab-offload/nemo-client.jar" >/dev/null
  scp "$REBUILT_NEXMARK" "$node:/tmp/nemo-cloudlab-offload/nexmark.jar" >/dev/null
  scp "$BEAM_GRPC_JAR" "$node:/tmp/nemo-cloudlab-offload/beam-grpc.jar" >/dev/null
  EXTRA_CP="/tmp/nemo-cloudlab-offload/nemo-client.jar:/tmp/nemo-cloudlab-offload/nexmark.jar:/tmp/nemo-cloudlab-offload/beam-grpc.jar"
  ssh "$node" "bash /tmp/nemo-cloudlab-offload/start_warm_pool.sh '$FIRST_PORT' '$WORKERS_PER_NODE' '/tmp/nemo-cloudlab-offload/offloading-vm.jar' 10000000 '$EXTRA_CP' >/tmp/start-warm-pool-$node.log 2>&1" || true
  sleep 2
done

# 4. Preflight check: YARN NMs must be up before we submit
check_vm_workers() {
  local vm_file=$1
  local max_fail=${2:-5}
  local fails=0
  while IFS=: read -r host port; do
    if ! timeout 2 bash -c "echo > /dev/tcp/$host/$port" 2>/dev/null; then
      echo "  WARNING: VM worker $host:$port unreachable"
      fails=$((fails + 1))
    fi
    if [[ "$fails" -ge "$max_fail" ]]; then
      echo "  ERROR: $fails VM workers unreachable; aborting" >&2
      return 1
    fi
  done < "$vm_file"
  echo "  All VM workers reachable"
}


# Verify all VM workers are listening before launching the YARN app
echo "Checking VM worker connectivity..."
check_vm_workers "$NEMO_REPO_ROOT/vm_addresses.txt" "${VM_CONNECTIVITY_FAIL_LIMIT:-5}" || exit 1

ensure_yarn_nodes

echo "Cleaning stale baseline worker metrics"
for node in $BASELINE_NODES; do
  ssh "$node" "rm -f /tmp/source_task_metrics.csv /tmp/task_metrics.csv" || true
done

# 5. Launch subscriber with offloading enabled
echo "Launching subscriber"
export TOPIC QUERY EXECUTOR_JSON NUM_EVENTS=$TOTAL_EVENTS STREAM_TIMEOUT
export LOG_FILE="$SUB_LOG"
export OFFLOADING=1
export NUM_MAX_LAMBDA
export AUTOSCALING
export JOB_ID
export NEMO_WORK_DIR="$WORK_DIR"
export NEMO_JOB_ID="$JOB_ID"

# Run subscriber from repo root so JobLauncher finds vm_addresses.txt
(cd "$NEMO_REPO_ROOT" && "$SCRIPT_DIR/run_q8_subscriber_yarn.sh")

# 6. Verify YARN app actually reached RUNNING before doing anything else
APP_ID=""
wait_for_yarn_app_running "$SUB_LOG" || { echo "YARN app failed to start. Aborting." >&2; exit 1; }

# 6b. Capture APP_ID and AM host for metrics collector
APP_ID=$(grep -o 'application_[0-9]\+_[0-9]\+' "$SUB_LOG" | head -1)
AM_HOST=$($HADOOP_HOME/bin/yarn application -status "$APP_ID" 2>/dev/null | grep "AM Host" | awk '{print $NF}')
echo "  App ID: $APP_ID"
echo "  AM Host: $AM_HOST"

if [[ -n "$AM_HOST" && "$AM_HOST" != "N/A" ]]; then
  echo "Cleaning stale AM-side fallback metrics on $AM_HOST"
  ssh "$AM_HOST" "rm -f /tmp/scaling_decisions.csv /tmp/scaler_metrics.csv /tmp/source_metrics.csv /tmp/source_aggregate_metrics.csv /tmp/source_task_metrics.csv /tmp/task_metrics.csv" || true
else
  echo "WARNING: AM host unavailable; skipping AM-side metrics cleanup" >&2
fi

# 7. Wait for subscriber scaling service to start reading scaling.txt
SUB_PID_FILE="/tmp/nemo-subscriber-${TOPIC}.pid"
SUB_PID=$(cat "$SUB_PID_FILE" 2>/dev/null || echo "unknown")

for _ in $(seq 1 300); do
  grep -Eq 'Scaling service invoked|Avg cpu:' "$SUB_LOG" && break
  sleep 1
done
if ! grep -Eq 'Scaling service invoked|Avg cpu:' "$SUB_LOG"; then
  echo "ERROR: subscriber/scaler metrics did not appear in time; aborting" >&2
  exit 1
fi

echo "Waiting ${SUBSCRIBER_WAIT}s before enabling autoscaler commands"
sleep "$SUBSCRIBER_WAIT"

# Wait for Nemo's Kafka source to finish partition assignment before producing
wait_for_nemo_source_ready

# 8. Write scaling commands after service is ready
echo "Writing scaling commands to $WORK_DIR/scaling.txt"
write_lambda_executor_command
if [[ "$SCALER_START_MODE" == "immediate" ]]; then
  start_autoscaler_commands "immediate" "" "" 0
elif [[ "$SCALER_START_MODE" == "after_phase_delay" ]]; then
  echo "Delaying scaler start until phase $SCALER_START_PHASE has run for ${SCALER_START_PHASE_DELAY_SEC}s"
else
  echo "ERROR: unsupported SCALER_START_MODE=$SCALER_START_MODE" >&2
  exit 1
fi

# 9. Start metrics collector in background (pass AM host for driver-side CSV polling)
echo "Starting metrics collector (AM host: $AM_HOST)"
python3 "$SCRIPT_DIR/metrics_collector.py" "$TOPIC" "$WORK_DIR" "$SUB_LOG" "$AM_HOST" "$KAFKA_RESULTS_TOPIC" "$BASELINE_NODES" > "$WORK_DIR/metrics_collector.log" 2>&1 &
METRICS_PID=$!
echo "Metrics collector PID: $METRICS_PID"

# 10. Produce live bursty records if configured
if [[ "$LIVE_EVENTS" -gt 0 ]]; then
  echo "Producing $LIVE_EVENTS live bursty records"
  if [[ "$SCALER_START_MODE" == "after_phase_delay" ]]; then
    start_producer_with_source_log "$LIVE_EVENTS" true
    if ! phase_line=$(wait_for_phase_start "$SCALER_START_PHASE" "$SCALER_START_WAIT_TIMEOUT_SEC"); then
      if [[ -n "$PRODUCER_PID" ]]; then
        kill "$PRODUCER_PID" >/dev/null 2>&1 || true
      fi
      exit 1
    fi
    phase_target_rate=$(printf '%s\n' "$phase_line" | awk -F, 'END {print $4}')
    echo "Sleeping ${SCALER_START_PHASE_DELAY_SEC}s before starting scaler/backpressure"
    sleep "$SCALER_START_PHASE_DELAY_SEC"
    echo "Starting scaler/backpressure after delayed phase gate"
    start_autoscaler_commands "after_phase_delay" "$SCALER_START_PHASE" "$phase_target_rate" "$SCALER_START_PHASE_DELAY_SEC"
    producer_status=0
    wait "$PRODUCER_PID" || producer_status=$?
    if [[ "$producer_status" -ne 0 ]]; then
      exit "$producer_status"
    fi
  else
    run_producer_with_source_log "$LIVE_EVENTS" true
  fi
fi

# 11. Report status
echo ""
echo "Autoscaler harness running."
echo "  Subscriber PID: $SUB_PID"
echo "  Subscriber log: $SUB_LOG"
echo "  Work dir: $WORK_DIR"
echo "  Scaling commands: $WORK_DIR/scaling.txt"
echo "  Scaler enable log: $SCALER_ENABLE_LOG"
echo "  Metrics collector PID: $METRICS_PID"
echo "  Metrics collector log: $WORK_DIR/metrics_collector.log"
echo ""
echo "To monitor:"
echo "  tail -f $SUB_LOG"
echo "  tail -f $WORK_DIR/metrics_collector.log"
echo "  yarn application -list"
echo ""
echo "To stop:"
echo "  yarn application -kill <APP_ID>"
echo "  kill $SUB_PID"
echo "  kill $METRICS_PID"
echo ""
echo "To check Kafka offsets:"
echo "  ssh $KAFKA_NODE \"$KAFKA_HOME/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list '$KAFKA_BOOTSTRAP' --topic '$TOPIC' --time -1\""
echo ""
echo "To plot metrics after the test:"
echo "  python3 $SCRIPT_DIR/plot_metrics.py $WORK_DIR"
