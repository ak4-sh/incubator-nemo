#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/cloudlab_env.sh"

# Full Sponge-style Q0 defaults. Override any variable from the environment.
RUN_ID=${RUN_ID:-sponge-q0-$(date +%Y%m%d-%H%M%S)}
QUERY=${QUERY:-0}
BENCHMARK_NAME=${BENCHMARK_NAME:-nexmark-q${QUERY}}
SINK_TYPE=${SINK_TYPE:-KAFKA}
COMPLETION_MODE=${COMPLETION_MODE:-exact_output}

# Burst mode configuration (default: custom sustained bursts)
BURST_MODE=${BURST_MODE:-custom}
FIRST_RATE=${FIRST_RATE:-50000}
NEXT_RATE=${NEXT_RATE:-200000}
STEADY_DURATION_SEC=${STEADY_DURATION_SEC:-60}
BURST_DURATION_SEC=${BURST_DURATION_SEC:-45}
NUM_BURSTS=${NUM_BURSTS:-3}
RAMP_UP_SEC=${RAMP_UP_SEC:-60}

# Legacy mode support (when BURST_MODE=legacy)
TOTAL_EVENTS=${TOTAL_EVENTS:-23850000}
PREFILL_EVENTS=${PREFILL_EVENTS:-100}
RATE_PERIOD_SEC=${RATE_PERIOD_SEC:-450}

CPU_DELAY_MS=${CPU_DELAY_MS:-2}
KAFKA_PARTITIONS=${KAFKA_PARTITIONS:-8}
PRODUCER_PARALLELISM=${PRODUCER_PARALLELISM:-8}
EXECUTOR_JSON=${EXECUTOR_JSON:-configs/cloudlab/nemo-yarn-kafka-1source-8slot-8compute.json}
OFFLOAD_NODES=${OFFLOAD_NODES:-node4,node6,node7,node8,node13}
WORKERS_PER_NODE=${WORKERS_PER_NODE:-32}
FIRST_PORT=${FIRST_PORT:-25321}
BASELINE_NODES=${BASELINE_NODES:-"node5 node9 node10 node11 node12"}
EXPECTED_NM_COUNT=${EXPECTED_NM_COUNT:-5}
NUM_MAX_LAMBDA=${NUM_MAX_LAMBDA:-160}
STREAM_TIMEOUT=${STREAM_TIMEOUT:-1800}
BENCHMARK_TIMEOUT_SEC=${BENCHMARK_TIMEOUT_SEC:-1800}
KEEP_WARM_POOL=${KEEP_WARM_POOL:-1}
ALLOW_ACTIVE_APPS=${ALLOW_ACTIVE_APPS:-0}
PLOT=${PLOT:-0}
PROGRESS_STALL_LIMIT=${PROGRESS_STALL_LIMIT:-12}

TOPIC=${TOPIC:-nexmark-${RUN_ID}}
KAFKA_RESULTS_TOPIC=${KAFKA_RESULTS_TOPIC:-${TOPIC}-results}
KAFKA_CONSUMER_GROUP=${KAFKA_CONSUMER_GROUP:-${RUN_ID}-consumer}
WORK_DIR=${WORK_DIR:-/tmp/nx-${RUN_ID}}
SUB_LOG=${SUB_LOG:-/tmp/nx-sub-${RUN_ID}.log}
ARTIFACT_DIR=${ARTIFACT_DIR:-$NEMO_REPO_ROOT/results/cloudlab/$RUN_ID}
HARNESS_LOG=${HARNESS_LOG:-$WORK_DIR/harness.log}

APP_ID=""
AM_HOST=""
SUB_PID=""
METRICS_PID=""
RUN_START_MS=$(date +%s%3N)
RUN_END_MS=""

log() {
  printf '[%s] %s\n' "$BENCHMARK_NAME" "$*"
}

require_file() {
  if [[ ! -f "$1" ]]; then
    echo "ERROR: required file missing: $1" >&2
    exit 1
  fi
}

sum_topic_offsets() {
  local topic=$1
  ssh "$KAFKA_NODE" \
    "$KAFKA_HOME/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list '$KAFKA_BOOTSTRAP' --topic '$topic' --time -1" \
    | awk -F: '{sum += $3} END {print sum + 0}'
}

get_source_count() {
  local max_count=${1:-0}
  if [[ -z "$AM_HOST" || "$AM_HOST" == "N/A" ]]; then
    echo 0
    return
  fi
  ssh "$AM_HOST" "if [ -f /tmp/source_aggregate_metrics.csv ]; then tail -n 50 /tmp/source_aggregate_metrics.csv | awk -F, -v max='$max_count' 'BEGIN {v=0} /^[0-9]/ {candidate=\$3 + 0; if (candidate >= 0 && (max <= 0 || candidate <= max)) v=candidate} END {print v + 0}'; else echo 0; fi" 2>/dev/null || echo 0
}

get_active_app_count() {
  "$HADOOP_HOME/bin/yarn" application -list 2>/dev/null \
    | awk 'NR > 2 && $1 ~ /^application_/ {count++} END {print count + 0}'
}

get_running_nm_count() {
  "$HADOOP_HOME/bin/yarn" node -list 2>/dev/null | grep -c RUNNING || true
}

get_app_state() {
  if [[ -z "$APP_ID" ]]; then
    echo "UNKNOWN"
    return
  fi
  { "$HADOOP_HOME/bin/yarn" application -status "$APP_ID" 2>/dev/null || true; } \
    | awk -F: '/^[[:space:]]*State[[:space:]]*:/ {gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2; exit}'
}

get_app_am_host() {
  if [[ -z "$APP_ID" ]]; then
    echo "N/A"
    return
  fi
  { "$HADOOP_HOME/bin/yarn" application -status "$APP_ID" 2>/dev/null || true; } \
    | awk -F: '/^[[:space:]]*AM Host[[:space:]]*:/ {gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2; exit}'
}

preflight() {
  log "Preflight checks"
  require_file "$REBUILT_NEMO"
  require_file "$REBUILT_NEXMARK"
  require_file "$VM_WORKER_JAR"
  require_file "$BEAM_GRPC_JAR"
  require_file "$SCRIPT_DIR/run_autoscaler_smoke.sh"

  local running_nm
  running_nm=$("$HADOOP_HOME/bin/yarn" node -list 2>/dev/null | grep -c RUNNING || true)
  if [[ "$running_nm" -lt "$EXPECTED_NM_COUNT" ]]; then
    echo "ERROR: only $running_nm/$EXPECTED_NM_COUNT YARN NodeManagers are RUNNING" >&2
    exit 1
  fi

  local active_apps
  active_apps=$(get_active_app_count)
  if [[ "$ALLOW_ACTIVE_APPS" != "1" && "$active_apps" -gt 0 ]]; then
    echo "ERROR: $active_apps active YARN application(s); set ALLOW_ACTIVE_APPS=1 to override" >&2
    exit 1
  fi

  timeout 10 ssh "$KAFKA_NODE" "$KAFKA_HOME/bin/kafka-broker-api-versions.sh --bootstrap-server '$KAFKA_BOOTSTRAP' >/dev/null" \
    || { echo "WARNING: Kafka bootstrap SSH check timed out; continuing"; }
}

create_result_topic() {
  log "Creating Kafka result topic $KAFKA_RESULTS_TOPIC"
  ssh "$KAFKA_NODE" \
    "$KAFKA_HOME/bin/kafka-topics.sh --zookeeper '${KAFKA_ZOOKEEPER:-node1:2181,node2:2181,node3:2181}' --create --topic '$KAFKA_RESULTS_TOPIC' --partitions $KAFKA_PARTITIONS --replication-factor 1 || true"
  ssh "$KAFKA_NODE" \
    "$KAFKA_HOME/bin/kafka-configs.sh --zookeeper '${KAFKA_ZOOKEEPER:-node1:2181,node2:2181,node3:2181}' --entity-type topics --entity-name '$KAFKA_RESULTS_TOPIC' --alter --add-config min.insync.replicas=1 || true"
}

run_harness() {
  mkdir -p "$WORK_DIR"
  log "Launching benchmark harness"
  log "Run ID: $RUN_ID"
  log "Input topic: $TOPIC"
  log "Result topic: $KAFKA_RESULTS_TOPIC"
  log "Work dir: $WORK_DIR"

  BURST_MODE=$BURST_MODE \
  TOTAL_EVENTS=$TOTAL_EVENTS \
  PREFILL_EVENTS=$PREFILL_EVENTS \
  FIRST_RATE=$FIRST_RATE \
  NEXT_RATE=$NEXT_RATE \
  STEADY_DURATION_SEC=$STEADY_DURATION_SEC \
  BURST_DURATION_SEC=$BURST_DURATION_SEC \
  NUM_BURSTS=$NUM_BURSTS \
  RAMP_UP_SEC=$RAMP_UP_SEC \
  RATE_PERIOD_SEC=$RATE_PERIOD_SEC \
  CPU_DELAY_MS=$CPU_DELAY_MS \
  QUERY=$QUERY \
  BENCHMARK_NAME=$BENCHMARK_NAME \
  COMPLETION_MODE=$COMPLETION_MODE \
  TOPIC=$TOPIC \
  SINK_TYPE=$SINK_TYPE \
  KAFKA_RESULTS_TOPIC=$KAFKA_RESULTS_TOPIC \
  KAFKA_CONSUMER_GROUP=$KAFKA_CONSUMER_GROUP \
  EXECUTOR_JSON=$EXECUTOR_JSON \
  OFFLOAD_NODES=$OFFLOAD_NODES \
  WORKERS_PER_NODE=$WORKERS_PER_NODE \
  FIRST_PORT=$FIRST_PORT \
  BASELINE_NODES=$BASELINE_NODES \
  EXPECTED_NM_COUNT=$EXPECTED_NM_COUNT \
  KAFKA_PARTITIONS=$KAFKA_PARTITIONS \
  PRODUCER_PARALLELISM=$PRODUCER_PARALLELISM \
  NUM_MAX_LAMBDA=$NUM_MAX_LAMBDA \
  STREAM_TIMEOUT=$STREAM_TIMEOUT \
  WORK_DIR=$WORK_DIR \
  SUB_LOG=$SUB_LOG \
  "$SCRIPT_DIR/run_autoscaler_smoke.sh" 2>&1 | tee "$HARNESS_LOG"

  APP_ID=$(grep -o 'application_[0-9]\+_[0-9]\+' "$SUB_LOG" | head -1 || true)
  SUB_PID=$(cat "/tmp/nemo-subscriber-${TOPIC}.pid" 2>/dev/null || true)
  METRICS_PID=$(grep -o 'Metrics collector PID: [0-9]\+' "$HARNESS_LOG" | awk '{print $4}' | tail -1 || true)
  if [[ -n "$APP_ID" ]]; then
    AM_HOST=$("$HADOOP_HOME/bin/yarn" application -status "$APP_ID" 2>/dev/null | awk '/AM Host/ {print $NF}' || true)
  fi
}

monitor_completion() {
  log "Monitoring completion for $TOTAL_EVENTS records with mode=$COMPLETION_MODE"
  local deadline=$((SECONDS + BENCHMARK_TIMEOUT_SEC))
  local input_total=0
  local result_total=0
  local source_count=0
  local last_valid_source_count=0
  local last_source_count=-1
  local last_result_total=-1
  local stalled_checks=0
  local app_state
  local app_am_host
  local running_nm

  while [[ "$SECONDS" -lt "$deadline" ]]; do
    input_total=$(sum_topic_offsets "$TOPIC")
    result_total=$(sum_topic_offsets "$KAFKA_RESULTS_TOPIC")
    source_count=$(get_source_count "$input_total")

    if [[ "$source_count" -gt "$TOTAL_EVENTS" ]]; then
      log "warning ignoring impossible source_count=$source_count greater than TOTAL_EVENTS=$TOTAL_EVENTS"
      source_count=$last_valid_source_count
    else
      last_valid_source_count=$source_count
    fi

    log "progress input=$input_total source=$source_count result=$result_total"

    case "$COMPLETION_MODE" in
      exact_output)
        if [[ "$input_total" -ge "$TOTAL_EVENTS" && "$source_count" -ge "$TOTAL_EVENTS" && "$result_total" -ge "$TOTAL_EVENTS" ]]; then
          RUN_END_MS=$(date +%s%3N)
          log "Success: input/source/result reached $TOTAL_EVENTS"
          return 0
        fi
        ;;
      source_only)
        if [[ "$input_total" -ge "$TOTAL_EVENTS" && "$source_count" -ge "$TOTAL_EVENTS" ]]; then
          RUN_END_MS=$(date +%s%3N)
          log "Success: input/source reached $TOTAL_EVENTS"
          return 0
        fi
        ;;
      source_plus_some_output)
        if [[ "$input_total" -ge "$TOTAL_EVENTS" && "$source_count" -ge "$TOTAL_EVENTS" && "$result_total" -gt 0 ]]; then
          RUN_END_MS=$(date +%s%3N)
          log "Success: input/source reached $TOTAL_EVENTS and result output was observed"
          return 0
        fi
        ;;
      *)
        echo "ERROR: unknown COMPLETION_MODE=$COMPLETION_MODE" >&2
        return 1
        ;;
    esac

    if [[ -n "$APP_ID" ]]; then
      app_state=$(get_app_state)
      app_am_host=$(get_app_am_host)
      if [[ "$app_state" != "RUNNING" ]]; then
        RUN_END_MS=$(date +%s%3N)
        echo "ERROR: YARN app $APP_ID is $app_state before completion" >&2
        return 1
      fi
      if [[ -z "$app_am_host" || "$app_am_host" == "N/A" ]]; then
        RUN_END_MS=$(date +%s%3N)
        echo "ERROR: YARN app $APP_ID has no active AM host before completion" >&2
        return 1
      fi
    fi

    running_nm=$(get_running_nm_count)
    if [[ "$running_nm" -lt "$EXPECTED_NM_COUNT" ]]; then
      RUN_END_MS=$(date +%s%3N)
      echo "ERROR: only $running_nm/$EXPECTED_NM_COUNT YARN NodeManagers remain RUNNING" >&2
      return 1
    fi

    if [[ "$input_total" -ge "$TOTAL_EVENTS" && "$source_count" -eq "$last_source_count" && "$result_total" -eq "$last_result_total" ]]; then
      stalled_checks=$((stalled_checks + 1))
    else
      stalled_checks=0
      last_source_count=$source_count
      last_result_total=$result_total
    fi

    if [[ "$input_total" -ge "$TOTAL_EVENTS" && "$stalled_checks" -ge "$PROGRESS_STALL_LIMIT" ]]; then
      RUN_END_MS=$(date +%s%3N)
      echo "ERROR: no source/result progress for $stalled_checks checks after producer completion" >&2
      return 1
    fi

    sleep 10
  done

  RUN_END_MS=$(date +%s%3N)
  echo "ERROR: benchmark did not complete before timeout" >&2
  echo "Final observed: input=$input_total source=$source_count result=$result_total" >&2
  return 1
}

copy_if_exists() {
  local src=$1
  local dst=$2
  if [[ -f "$src" ]]; then
    cp "$src" "$dst"
  fi
}

copy_remote_if_exists() {
  local node=$1
  local src=$2
  local dst=$3
  ssh "$node" "test -f '$src'" 2>/dev/null && scp "$node:$src" "$dst" >/dev/null || true
}

save_artifacts() {
  log "Saving artifacts to $ARTIFACT_DIR"
  mkdir -p "$ARTIFACT_DIR/am-${AM_HOST:-unknown}" "$ARTIFACT_DIR/offload-task-metrics"

  copy_if_exists "$WORK_DIR/combined_metrics.csv" "$ARTIFACT_DIR/combined_metrics.csv"
  copy_if_exists "$WORK_DIR/producer_metrics.csv" "$ARTIFACT_DIR/producer_metrics.csv"
  copy_if_exists "$WORK_DIR/source.log" "$ARTIFACT_DIR/source.log"
  copy_if_exists "$WORK_DIR/scaling.txt" "$ARTIFACT_DIR/scaling.txt"
  copy_if_exists "$WORK_DIR/metrics_collector.log" "$ARTIFACT_DIR/metrics_collector.log"
  copy_if_exists "$HARNESS_LOG" "$ARTIFACT_DIR/harness.log"
  copy_if_exists "$SUB_LOG" "$ARTIFACT_DIR/subscriber.log"

  if [[ -n "$AM_HOST" && "$AM_HOST" != "N/A" ]]; then
    copy_remote_if_exists "$AM_HOST" /tmp/scaling_decisions.csv "$ARTIFACT_DIR/am-$AM_HOST/scaling_decisions.csv"
    copy_remote_if_exists "$AM_HOST" /tmp/scaler_metrics.csv "$ARTIFACT_DIR/am-$AM_HOST/scaler_metrics.csv"
    copy_remote_if_exists "$AM_HOST" /tmp/source_aggregate_metrics.csv "$ARTIFACT_DIR/am-$AM_HOST/source_aggregate_metrics.csv"
    copy_remote_if_exists "$AM_HOST" /tmp/source_task_metrics.csv "$ARTIFACT_DIR/am-$AM_HOST/source_task_metrics.csv"
    copy_remote_if_exists "$AM_HOST" /tmp/task_metrics.csv "$ARTIFACT_DIR/am-$AM_HOST/task_metrics.csv"
    copy_if_exists "$ARTIFACT_DIR/am-$AM_HOST/source_aggregate_metrics.csv" "$ARTIFACT_DIR/source_aggregate_metrics.csv"
  fi

  if [[ -s "$ARTIFACT_DIR/am-$AM_HOST/scaling_decisions.csv" ]]; then
    awk -F, -v start="$RUN_START_MS" -v end="${RUN_END_MS:-$(date +%s%3N)}" '/^[0-9]/ && $1 >= start && $1 <= end {print}' \
      "$ARTIFACT_DIR/am-$AM_HOST/scaling_decisions.csv" > "$ARTIFACT_DIR/scaling_decisions.csv"
  fi

  printf 'timestamp,jobId,avgCpu,avgInput,avgProcess,queue,numExecutors,numLambdaExecutors,lastActionWasScaleOut,prevFutureCompleted\n' > "$ARTIFACT_DIR/scaler_metrics.csv"
  if [[ -s "$ARTIFACT_DIR/am-$AM_HOST/scaler_metrics.csv" ]]; then
    local job_id
    job_id=$(grep -o 'nx-q[0-9]-nexmark-[A-Za-z0-9._-]*' "$SUB_LOG" | head -1 || true)
    awk -F, -v start="$RUN_START_MS" -v end="${RUN_END_MS:-$(date +%s%3N)}" -v job_id="$job_id" \
      '/^[0-9]/ && NF == 10 && $1 >= start && $1 <= end && (job_id == "" || $2 == job_id) && $0 !~ /(NaN|Infinity)/ {print}' \
      "$ARTIFACT_DIR/am-$AM_HOST/scaler_metrics.csv" >> "$ARTIFACT_DIR/scaler_metrics.csv"
  fi

  printf 'timestamp,jobId,taskId,idleTimeNs,kafkaQueueTimeNs,kafkaQueueTimeAvgNs,kafkaQueueTimeMaxNs,kafkaQueueSamples,inputRate,recordsRead\n' > "$ARTIFACT_DIR/source_task_metrics.csv"
  for node in $BASELINE_NODES; do
    copy_remote_if_exists "$node" /tmp/source_task_metrics.csv "$ARTIFACT_DIR/${node}-source_task_metrics.csv"
    if [[ -s "$ARTIFACT_DIR/${node}-source_task_metrics.csv" ]]; then
      awk -F, -v start="$RUN_START_MS" -v end="${RUN_END_MS:-$(date +%s%3N)}" '/^[0-9]/ && NF == 10 && $1 >= start && $1 <= end {print}' "$ARTIFACT_DIR/${node}-source_task_metrics.csv" >> "$ARTIFACT_DIR/source_task_metrics.csv"
    fi
  done

  IFS=',' read -ra offload_array <<< "$OFFLOAD_NODES"
  for node in "${offload_array[@]}"; do
    copy_remote_if_exists "$node" /tmp/task_metrics.csv "$ARTIFACT_DIR/offload-task-metrics/${node}-task_metrics.csv"
  done

  printf 'timestamp,jobId,executorId,taskId,inputReceiveRate,inputRate,outputRate,processingTimeNs,deserTimeNs,inBytes,serializedTimeNs,outBytes\n' > "$ARTIFACT_DIR/task_metrics.csv"
  for file in "$ARTIFACT_DIR"/am-*/task_metrics.csv "$ARTIFACT_DIR"/offload-task-metrics/*-task_metrics.csv; do
    if [[ -s "$file" ]]; then
      awk -F, -v start="$RUN_START_MS" -v end="${RUN_END_MS:-$(date +%s%3N)}" '/^[0-9]/ && NF == 12 && $1 >= start && $1 <= end {print}' "$file" >> "$ARTIFACT_DIR/task_metrics.csv"
    fi
  done
}

write_manifest() {
  local input_total result_total source_count scale_count scale_out_count scale_in_count vm_rows
  local max_input_lag max_result_lag max_queue max_kafka_queue_p95
  input_total=$(sum_topic_offsets "$TOPIC")
  result_total=$(sum_topic_offsets "$KAFKA_RESULTS_TOPIC")
  source_count=$(get_source_count "$input_total")
  max_input_lag=0
  max_result_lag=0
  max_queue=0
  max_kafka_queue_p95=-1
  if [[ -f "$ARTIFACT_DIR/combined_metrics.csv" ]]; then
    max_input_lag=$(awk -F, 'NR > 1 && $5 >= 0 && $5 > m {m=$5} END {print m + 0}' "$ARTIFACT_DIR/combined_metrics.csv")
    max_result_lag=$(awk -F, 'NR > 1 && $6 >= 0 && $6 > m {m=$6} END {print m + 0}' "$ARTIFACT_DIR/combined_metrics.csv")
    max_kafka_queue_p95=$(awk -F, 'NR > 1 && $8 >= 0 && $8 > m {m=$8} END {if (m == "") print -1; else printf "%.3f", m}' "$ARTIFACT_DIR/combined_metrics.csv")
    max_queue=$(awk -F, 'NR > 1 && $17 != "" && $17 > m {m=$17} END {print m + 0}' "$ARTIFACT_DIR/combined_metrics.csv")
  fi
  if [[ -f "$ARTIFACT_DIR/source_aggregate_metrics.csv" ]]; then
    source_count=$(awk -F, -v max="$input_total" 'NR > 1 {candidate=$3 + 0; if (candidate >= 0 && (max <= 0 || candidate <= max)) v=candidate} END {print v + 0}' "$ARTIFACT_DIR/source_aggregate_metrics.csv")
  fi
  scale_count=0
  scale_out_count=0
  scale_in_count=0
  if [[ -f "$ARTIFACT_DIR/scaling_decisions.csv" ]]; then
    scale_count=$(wc -l < "$ARTIFACT_DIR/scaling_decisions.csv")
    scale_out_count=$(grep -c SCALE_OUT "$ARTIFACT_DIR/scaling_decisions.csv" || true)
    scale_in_count=$(grep -c SCALE_IN "$ARTIFACT_DIR/scaling_decisions.csv" || true)
  fi
  vm_rows=$(grep -c 'VM-' "$ARTIFACT_DIR/task_metrics.csv" 2>/dev/null || true)

  cat > "$ARTIFACT_DIR/README.md" <<EOF
# $BENCHMARK_NAME Benchmark Run

Kafka source benchmark with VM offloading.

## Run IDs

- Run ID: \`$RUN_ID\`
- Application: \`${APP_ID:-unknown}\`
- Query: \`$QUERY\`
- Completion mode: \`$COMPLETION_MODE\`
- Input topic: \`$TOPIC\`
- Result topic: \`$KAFKA_RESULTS_TOPIC\`
- Kafka consumer group: \`$KAFKA_CONSUMER_GROUP\`
- Executor config: \`$EXECUTOR_JSON\`
- Work dir: \`$WORK_DIR\`
- AM host: \`${AM_HOST:-unknown}\`
- Date: $(date -Iseconds)

## Settings

- Total events: \`$TOTAL_EVENTS\`
- Prefill events: \`$PREFILL_EVENTS\`
- First rate target: \`$FIRST_RATE\` events/s
- Next rate target: \`$NEXT_RATE\` events/s
- Rate period: \`$RATE_PERIOD_SEC\` seconds
- CPU delay: \`$CPU_DELAY_MS\` ms
- Completion mode: \`$COMPLETION_MODE\`
- Kafka partitions: \`$KAFKA_PARTITIONS\`
- Producer parallelism: \`$PRODUCER_PARALLELISM\`
- Offload nodes: \`$OFFLOAD_NODES\`
- Workers per offload node: \`$WORKERS_PER_NODE\`
- Max VM executors: \`$NUM_MAX_LAMBDA\`

## Results

- Input Kafka offset total: \`$input_total\`
- Result Kafka offset total: \`$result_total\`
- Final source count: \`$source_count\`
- Max input/source lag: \`$max_input_lag\`
- Max input/result lag: \`$max_result_lag\`
- Max scaler queue: \`$max_queue\`
- Max Kafka queue-time p95: \`$max_kafka_queue_p95\` ms
- Scaling decisions: \`$scale_count\`
- Scale-out decisions: \`$scale_out_count\`
- Scale-in decisions: \`$scale_in_count\`
- VM task metric rows: \`$vm_rows\`

## Files

- \`combined_metrics.csv\`: collector timeline.
- \`source_aggregate_metrics.csv\`: AM-side source progress timeline.
- \`source_task_metrics.csv\`: source-task queue time, idle time, and input rate.
- \`scaler_metrics.csv\`: periodic scaler state timeline.
- \`producer_metrics.csv\`: producer throughput timeline.
- \`scaling_decisions.csv\`: plot-compatible scaling decisions.
- \`source_metrics.csv\`: plot-compatible source metrics.
- \`task_metrics.csv\`: plot-compatible task metrics filtered to this run window.
- \`am-${AM_HOST:-unknown}/\`: raw AM-side metrics.
- \`offload-task-metrics/\`: raw per-offload-node task metrics.
- \`subscriber.log\`: submit-side JobLauncher log.
- \`harness.log\`: benchmark wrapper/harness output.

## Plotting

From the repository root:

\`\`\`bash
python3 scripts/cloudlab/plot_metrics.py $ARTIFACT_DIR
\`\`\`
EOF
}

cleanup() {
  log "Cleanup"
  if [[ -n "$APP_ID" ]]; then
    "$HADOOP_HOME/bin/yarn" application -kill "$APP_ID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$SUB_PID" ]]; then
    kill "$SUB_PID" >/dev/null 2>&1 || true
    sleep 1
    kill -9 "$SUB_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$METRICS_PID" ]]; then
    kill "$METRICS_PID" >/dev/null 2>&1 || true
  fi
  if [[ "$KEEP_WARM_POOL" == "0" ]]; then
    IFS=',' read -ra offload_array <<< "$OFFLOAD_NODES"
    for node in "${offload_array[@]}"; do
      ssh "$node" "pkill -f '[o]rg.apache.nemo.offloading.workers.vm.VMWorker' || true" || true
    done
  fi
}

maybe_plot() {
  if [[ "$PLOT" != "1" ]]; then
    return
  fi
  if ! python3 -c 'import pandas' >/dev/null 2>&1; then
    log "PLOT=1 requested, but pandas is unavailable; skipping plots"
    return
  fi
  python3 "$SCRIPT_DIR/plot_metrics.py" "$ARTIFACT_DIR"
}

main() {
  local monitor_status=0
  preflight
  create_result_topic
  run_harness
  monitor_completion || monitor_status=$?
  save_artifacts || true
  write_manifest || true
  maybe_plot || true
  cleanup
  if [[ "$monitor_status" -ne 0 ]]; then
    log "Failed. Artifacts saved to $ARTIFACT_DIR"
    exit "$monitor_status"
  fi
  log "Done. Artifacts saved to $ARTIFACT_DIR"
}

main "$@"
