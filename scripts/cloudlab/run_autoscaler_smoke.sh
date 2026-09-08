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
PRODUCER_IMPL=${PRODUCER_IMPL:-beam}
TOTAL_EVENTS=${TOTAL_EVENTS:-500}
if [[ "$PRODUCER_IMPL" == "holostream" ]]; then
  PREFILL_EVENTS=${PREFILL_EVENTS:-0}
else
  PREFILL_EVENTS=${PREFILL_EVENTS:-100}
fi
LIVE_EVENTS=$((TOTAL_EVENTS - PREFILL_EVENTS))
RATE_PERIOD_SEC=${RATE_PERIOD_SEC:-50}
SUBSCRIBER_WAIT=${SUBSCRIBER_WAIT:-10}
PRODUCER_RATE_LIMITED=${PRODUCER_RATE_LIMITED:-true}
HARNESS_PREFLIGHT_ONLY=${HARNESS_PREFLIGHT_ONLY:-false}
HOLOSTREAM_LAUNCHER=${HOLOSTREAM_LAUNCHER:-$SCRIPT_DIR/holostream/run_producers.py}
HOLOSTREAM_TOPIC_RESETTER=${HOLOSTREAM_TOPIC_RESETTER:-$SCRIPT_DIR/holostream/reset_topics.py}
HOLOSTREAM_CONFIG=${HOLOSTREAM_CONFIG:-$SCRIPT_DIR/holostream/config/sponge_q6_formal.json}
HOLOSTREAM_PRODUCER_BINARY=${HOLOSTREAM_PRODUCER_BINARY:-bin/nexmarkKafkaProducer}
HOLOSTREAM_EXPECTED_PRODUCER_SHA256=${HOLOSTREAM_EXPECTED_PRODUCER_SHA256:-}
HOLOSTREAM_PRODUCER_TIMEOUT=${HOLOSTREAM_PRODUCER_TIMEOUT:-600}
HOLOSTREAM_TELEMETRY_INTERVAL_MS=${HOLOSTREAM_TELEMETRY_INTERVAL_MS:-1000}
HOLOSTREAM_TELEMETRY_START_TIMEOUT=${HOLOSTREAM_TELEMETRY_START_TIMEOUT:-60}
METRICS_TERMINAL_TIMEOUT_SEC=${METRICS_TERMINAL_TIMEOUT_SEC:-300}
METRICS_TERMINAL_STABLE_SAMPLES=${METRICS_TERMINAL_STABLE_SAMPLES:-3}
METRICS_KAFKA_COMMAND_MODE=${METRICS_KAFKA_COMMAND_MODE:-local}
METRICS_MAX_CONSECUTIVE_FAILURES=${METRICS_MAX_CONSECUTIVE_FAILURES:-5}
HOLOSTREAM_RESET_TOPICS=${HOLOSTREAM_RESET_TOPICS:-false}
HOLOSTREAM_TOPIC_RESET_TIMEOUT=${HOLOSTREAM_TOPIC_RESET_TIMEOUT:-120}
HOLOSTREAM_AUCTION_TOPIC=${HOLOSTREAM_AUCTION_TOPIC:-nexmark-auction}
HOLOSTREAM_BID_TOPIC=${HOLOSTREAM_BID_TOPIC:-nexmark-bid}
HOLOSTREAM_AUCTION_EVENTS=${HOLOSTREAM_AUCTION_EVENTS:-}
HOLOSTREAM_BID_EVENTS=${HOLOSTREAM_BID_EVENTS:-}
if [[ "$PRODUCER_IMPL" == "holostream" ]]; then
  KAFKA_INPUT_FORMAT=${KAFKA_INPUT_FORMAT:-HOLOSTREAM_MUS}
else
  KAFKA_INPUT_FORMAT=${KAFKA_INPUT_FORMAT:-BEAM_EVENT}
fi

QUERY=${QUERY:-0}
EXECUTOR_JSON=${EXECUTOR_JSON:-$NEMO_REPO_ROOT/configs/cloudlab/nemo-yarn-kafka-1source-8slot-12compute.json}
STREAM_TIMEOUT=${STREAM_TIMEOUT:-900}
NUM_EVENTS=${NUM_EVENTS:-$TOTAL_EVENTS}
CPU_DELAY_MS=${CPU_DELAY_MS:-0}
AUTOSCALING=${AUTOSCALING:-false}
SAFE_WORKER_REACTIVATION=${SAFE_WORKER_REACTIVATION:-false}
REQUIRE_CLOUDLAB_LIFECYCLE_VALIDATION=${REQUIRE_CLOUDLAB_LIFECYCLE_VALIDATION:-false}
MIN_CLOUDLAB_REACTIVATIONS_PER_WORKER=${MIN_CLOUDLAB_REACTIVATIONS_PER_WORKER:-0}
OFFLOADING=${OFFLOADING:-1}
SCALER_START_MODE=${SCALER_START_MODE:-immediate}
SCALER_START_PHASE=${SCALER_START_PHASE:-2}
SCALER_START_PHASE_DELAY_SEC=${SCALER_START_PHASE_DELAY_SEC:-60}
SCALER_START_WAIT_TIMEOUT_SEC=${SCALER_START_WAIT_TIMEOUT_SEC:-900}
KAFKA_RESULTS_TOPIC=${KAFKA_RESULTS_TOPIC:-}
JOB_ID=${JOB_ID:-nx-q${QUERY}-${TOPIC}}
if [[ "$PRODUCER_IMPL" == "holostream" ]]; then
  KAFKA_CONSUMER_GROUP=${KAFKA_CONSUMER_GROUP:-sponge-$JOB_ID}
else
  KAFKA_CONSUMER_GROUP=${KAFKA_CONSUMER_GROUP:-disaggregated-streaming}
fi
SOURCE_HOSTS=${SOURCE_HOSTS:-}
COMPUTE_HOSTS=${COMPUTE_HOSTS:-}
STRICT_EXECUTOR_PLACEMENT=${STRICT_EXECUTOR_PLACEMENT:-false}
EXPECTED_EXECUTOR_PLACEMENT_ROWS=${EXPECTED_EXECUTOR_PLACEMENT_ROWS:-5}
PLACEMENT_WAIT_TIMEOUT_SEC=${PLACEMENT_WAIT_TIMEOUT_SEC:-240}
PIN_AM_TO_SOURCE=${PIN_AM_TO_SOURCE:-false}
REQUIRED_OPTIMIZATION_PASSES=${REQUIRED_OPTIMIZATION_PASSES:-}
OPTIMIZATION_PASS_WAIT_TIMEOUT_SEC=${OPTIMIZATION_PASS_WAIT_TIMEOUT_SEC:-120}
REQUIRED_SOURCE_ROOTS=${REQUIRED_SOURCE_ROOTS:-0}
REQUIRED_SOURCE_TASKS=${REQUIRED_SOURCE_TASKS:-0}
SOURCE_ROOT_WAIT_TIMEOUT_SEC=${SOURCE_ROOT_WAIT_TIMEOUT_SEC:-120}

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
KAFKA_REPLICATION_FACTOR=${KAFKA_REPLICATION_FACTOR:-1}

KAFKA_ZOOKEEPER=${KAFKA_ZOOKEEPER:-node1:2181,node2:2181,node3:2181}
WORK_DIR=${WORK_DIR:-/tmp/nx-auto-$TOPIC}
EXECUTOR_PLACEMENT_REPORT=${EXECUTOR_PLACEMENT_REPORT:-$WORK_DIR/executor_placement.csv}
LOG_FILE=${LOG_FILE:-/tmp/nx-auto-$TOPIC.log}
SUB_LOG=${SUB_LOG:-/tmp/nx-auto-sub-$TOPIC.log}
SOURCE_LOG=$WORK_DIR/source.log
HOLOSTREAM_LAUNCHER_LOG=$WORK_DIR/holostream_launcher.log
HOLOSTREAM_TELEMETRY_LOG=$WORK_DIR/kafka_input_telemetry.log
HOLOSTREAM_TELEMETRY_CSV=$WORK_DIR/kafka_input_telemetry.csv
TOPIC_CONFIG_DESCRIBE=$WORK_DIR/kafka_topic_config_input.txt
AUCTION_TOPIC_CONFIG_DESCRIBE=$WORK_DIR/kafka_topic_config_auction.txt
BID_TOPIC_CONFIG_DESCRIBE=$WORK_DIR/kafka_topic_config_bid.txt

mkdir -p "$WORK_DIR"
: > "$SOURCE_LOG"
SCALER_ENABLE_LOG=$WORK_DIR/scaler_enable.csv
: > "$WORK_DIR/producer_phases.csv"
: > "$SCALER_ENABLE_LOG"
PRODUCER_PID=""
HOLOSTREAM_TELEMETRY_PID=""
METRICS_PID=""
SCALER_STARTED=false
HOLOSTREAM_PLAN_JSON=$WORK_DIR/holostream_producer_plan.json
HOLOSTREAM_LOCK_DIR=/tmp/sponge-holostream-kafka.lock
HOLOSTREAM_LOCK_HELD=false
COMPUTE_NMS_WITHHELD=false

cleanup_holostream_launcher() {
  local exit_status=$?
  if [[ "${COMPUTE_NMS_WITHHELD:-false}" == "true" ]] &&
     declare -F restore_compute_nodemanagers >/dev/null; then
    echo "Restoring compute NodeManagers withheld for AM placement" >&2
    restore_compute_nodemanagers || true
  fi
  if [[ -n "${METRICS_PID:-}" ]] &&
     kill -0 "$METRICS_PID" 2>/dev/null; then
    echo "Stopping metrics collector PID $METRICS_PID" >&2
    kill -TERM "$METRICS_PID" 2>/dev/null || true
    wait "$METRICS_PID" 2>/dev/null || true
  fi
  METRICS_PID=""
  if [[ "$PRODUCER_IMPL" == "holostream" &&
        -n "${HOLOSTREAM_TELEMETRY_PID:-}" ]] &&
     kill -0 "$HOLOSTREAM_TELEMETRY_PID" 2>/dev/null; then
    echo "Stopping Kafka input telemetry PID $HOLOSTREAM_TELEMETRY_PID" >&2
    kill -TERM "$HOLOSTREAM_TELEMETRY_PID" 2>/dev/null || true
    wait "$HOLOSTREAM_TELEMETRY_PID" 2>/dev/null || true
  fi
  if [[ "$PRODUCER_IMPL" == "holostream" &&
        -n "${PRODUCER_PID:-}" ]] &&
     kill -0 "$PRODUCER_PID" 2>/dev/null; then
    echo "Stopping HoloStream producer launcher PID $PRODUCER_PID" >&2
    kill -TERM "$PRODUCER_PID" 2>/dev/null || true
    wait "$PRODUCER_PID" 2>/dev/null || true
  fi
  if [[ "$HOLOSTREAM_LOCK_HELD" == "true" ]]; then
    rm -f "$HOLOSTREAM_LOCK_DIR/owner"
    rmdir "$HOLOSTREAM_LOCK_DIR" 2>/dev/null || true
    HOLOSTREAM_LOCK_HELD=false
  fi
  if [[ "$exit_status" -ne 0 ]] &&
     declare -F cleanup_after_placement_failure >/dev/null; then
    echo "Cleaning failed YARN application and VM worker pools" >&2
    cleanup_after_placement_failure || true
  fi
  return "$exit_status"
}
trap cleanup_holostream_launcher EXIT

acquire_holostream_lock() {
  if ! mkdir "$HOLOSTREAM_LOCK_DIR" 2>/dev/null; then
    local owner_pid=""
    if [[ -f "$HOLOSTREAM_LOCK_DIR/owner" ]]; then
      owner_pid=$(sed -n 's/^pid=\([0-9][0-9]*\).*/\1/p' "$HOLOSTREAM_LOCK_DIR/owner")
    fi
    if [[ -n "$owner_pid" ]] && ! kill -0 "$owner_pid" 2>/dev/null; then
      echo "Removing stale HoloStream lock from dead PID $owner_pid"
      rm -f "$HOLOSTREAM_LOCK_DIR/owner"
      rmdir "$HOLOSTREAM_LOCK_DIR" 2>/dev/null || true
      mkdir "$HOLOSTREAM_LOCK_DIR"
    else
      echo "ERROR: another HoloStream fixed-topic run holds $HOLOSTREAM_LOCK_DIR" >&2
      if [[ -f "$HOLOSTREAM_LOCK_DIR/owner" ]]; then
        echo "Lock owner: $(cat "$HOLOSTREAM_LOCK_DIR/owner")" >&2
      fi
      return 1
    fi
  fi
  HOLOSTREAM_LOCK_HELD=true
  printf 'pid=%s jobId=%s workDir=%s started=%s\n' \
    "$$" "$JOB_ID" "$WORK_DIR" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    > "$HOLOSTREAM_LOCK_DIR/owner"
}

preflight_holostream_plan() {
  local plan_tmp="$HOLOSTREAM_PLAN_JSON.tmp"
  local configured_auction_events=$HOLOSTREAM_AUCTION_EVENTS
  local configured_bid_events=$HOLOSTREAM_BID_EVENTS
  local plan_value
  local -a plan_values=()

  if [[ ! -x "$HOLOSTREAM_LAUNCHER" ]]; then
    echo "ERROR: HoloStream launcher is not executable: $HOLOSTREAM_LAUNCHER" >&2
    return 1
  fi
  if [[ ! -f "$STANDALONE_PRODUCER_OUT/KafkaInputOffsetTelemetry.class" ]]; then
    echo "ERROR: Kafka input telemetry is not compiled: $STANDALONE_PRODUCER_OUT/KafkaInputOffsetTelemetry.class" >&2
    echo "Run scripts/cloudlab/compile_standalone_producer.sh before the benchmark." >&2
    return 1
  fi
  if [[ ! -f "$HOLOSTREAM_CONFIG" ]]; then
    echo "ERROR: HoloStream config does not exist: $HOLOSTREAM_CONFIG" >&2
    return 1
  fi

  python3 "$HOLOSTREAM_LAUNCHER" \
    --config "$HOLOSTREAM_CONFIG" \
    --run-id "$JOB_ID" \
    --output-dir "$WORK_DIR" \
    --expected-partitions "$KAFKA_PARTITIONS" \
    --describe-json > "$plan_tmp"
  mv "$plan_tmp" "$HOLOSTREAM_PLAN_JSON"

  while IFS= read -r plan_value; do
    plan_values+=("$plan_value")
  done < <(
    python3 -c '
import json
import sys
plan = json.load(open(sys.argv[1]))
print(plan["replicas"])
print(plan["auctionTopic"])
print(plan["bidTopic"])
print(plan["auctionTotal"])
print(plan["bidTotal"])
print(plan["totalEvents"])
print(",".join(str(round(phase["targetRate"])) for phase in plan["phases"]))
print(",".join(str(phase["durationSec"]).rstrip("0").rstrip(".") for phase in plan["phases"]))
' "$HOLOSTREAM_PLAN_JSON"
  )
  if [[ "${#plan_values[@]}" -ne 8 ]]; then
    echo "ERROR: incomplete HoloStream launcher plan in $HOLOSTREAM_PLAN_JSON" >&2
    return 1
  fi

  if [[ "${plan_values[0]}" -ne "$PRODUCER_PARALLELISM" ]]; then
    echo "ERROR: producer config has ${plan_values[0]} replicas but PRODUCER_PARALLELISM=$PRODUCER_PARALLELISM" >&2
    return 1
  fi
  if [[ "$HOLOSTREAM_AUCTION_TOPIC" != "${plan_values[1]}" ||
        "$HOLOSTREAM_BID_TOPIC" != "${plan_values[2]}" ]]; then
    echo "ERROR: unchanged HoloStream requires topics ${plan_values[1]} and ${plan_values[2]}" >&2
    return 1
  fi
  if [[ -n "$configured_auction_events" &&
        "$configured_auction_events" != "${plan_values[3]}" ]]; then
    echo "ERROR: HOLOSTREAM_AUCTION_EVENTS=$configured_auction_events disagrees with config total ${plan_values[3]}" >&2
    return 1
  fi
  if [[ -n "$configured_bid_events" &&
        "$configured_bid_events" != "${plan_values[4]}" ]]; then
    echo "ERROR: HOLOSTREAM_BID_EVENTS=$configured_bid_events disagrees with config total ${plan_values[4]}" >&2
    return 1
  fi

  HOLOSTREAM_AUCTION_EVENTS=${plan_values[3]}
  HOLOSTREAM_BID_EVENTS=${plan_values[4]}
  TOTAL_EVENTS=${plan_values[5]}
  LIVE_EVENTS=$TOTAL_EVENTS
  NUM_EVENTS=$TOTAL_EVENTS
  BURST_MODE=phases
  PHASE_RATES=${plan_values[6]}
  PHASE_DURATIONS_SEC=${plan_values[7]}

  echo "Validated HoloStream plan:"
  echo "  config=$HOLOSTREAM_CONFIG"
  echo "  plan=$HOLOSTREAM_PLAN_JSON"
  echo "  replicas=${plan_values[0]}"
  echo "  auctions=$HOLOSTREAM_AUCTION_EVENTS bids=$HOLOSTREAM_BID_EVENTS total=$TOTAL_EVENTS"
  echo "  phases=$PHASE_RATES durations=$PHASE_DURATIONS_SEC"
}

if [[ "$PRODUCER_IMPL" == "holostream" ]]; then
  acquire_holostream_lock
  preflight_holostream_plan
fi

verify_kafka_topic_log_append_time() {
  local topic=$1
  local output_file=$2
  local config_output
  local topic_output
  config_output=$(ssh "$KAFKA_NODE" "$KAFKA_HOME/bin/kafka-configs.sh --bootstrap-server '$KAFKA_BOOTSTRAP' --entity-type topics --entity-name '$topic' --describe")
  topic_output=$(ssh "$KAFKA_NODE" "$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server '$KAFKA_BOOTSTRAP' --describe --topic '$topic'")
  {
    echo "topic=$topic"
    echo "bootstrap=$KAFKA_BOOTSTRAP"
    echo "$topic_output"
    echo "$config_output"
  } > "$output_file"
  cat "$output_file"
  if ! grep -q "message.timestamp.type=LogAppendTime" "$output_file"; then
    echo "ERROR: Kafka input topic '$topic' is not configured with message.timestamp.type=LogAppendTime" >&2
    echo "See $output_file for kafka-configs.sh --describe output." >&2
    exit 1
  fi
  if ! grep -Eq "PartitionCount:[[:space:]]*$KAFKA_PARTITIONS([[:space:]]|$)" "$output_file"; then
    echo "ERROR: Kafka input topic '$topic' does not have $KAFKA_PARTITIONS partitions" >&2
    echo "See $output_file for kafka-topics.sh --describe output." >&2
    exit 1
  fi
}

configure_input_topic() {
  local topic=$1
  local output_file=$2
  echo "Creating Kafka topic $topic with $KAFKA_PARTITIONS partitions"
  ssh "$KAFKA_NODE" "$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server '$KAFKA_BOOTSTRAP' --create --topic '$topic' --partitions $KAFKA_PARTITIONS --replication-factor 1 || true"
  ssh "$KAFKA_NODE" "$KAFKA_HOME/bin/kafka-configs.sh --bootstrap-server '$KAFKA_BOOTSTRAP' --entity-type topics --entity-name '$topic' --alter --add-config min.insync.replicas=1,message.timestamp.type=LogAppendTime || true"
  verify_kafka_topic_log_append_time "$topic" "$output_file"
}

case "$PRODUCER_IMPL" in
  beam)
    if [[ "$KAFKA_INPUT_FORMAT" != "BEAM_EVENT" ]]; then
      echo "ERROR: PRODUCER_IMPL=beam requires KAFKA_INPUT_FORMAT=BEAM_EVENT" >&2
      exit 1
    fi
    ;;
  holostream)
    if [[ "$QUERY" -ne 6 ]]; then
      echo "ERROR: HoloStream MUS input currently supports QUERY=6 only" >&2
      exit 1
    fi
    if [[ "$KAFKA_INPUT_FORMAT" != "HOLOSTREAM_MUS" ]]; then
      echo "ERROR: PRODUCER_IMPL=holostream requires KAFKA_INPUT_FORMAT=HOLOSTREAM_MUS" >&2
      exit 1
    fi
    if [[ "$HOLOSTREAM_RESET_TOPICS" != "true" ]]; then
      echo "ERROR: HoloStream mode deletes and recreates nexmark-auction and nexmark-bid; set HOLOSTREAM_RESET_TOPICS=true to authorize it" >&2
      exit 1
    fi
    if [[ ! -x "$HOLOSTREAM_TOPIC_RESETTER" ]]; then
      echo "ERROR: HoloStream topic resetter is not executable: $HOLOSTREAM_TOPIC_RESETTER" >&2
      exit 1
    fi
    if ! [[ "$HOLOSTREAM_EXPECTED_PRODUCER_SHA256" =~ ^[0-9a-fA-F]{64}$ ]]; then
      echo "ERROR: HoloStream mode requires the 64-character checksum from the comparison producer binary in HOLOSTREAM_EXPECTED_PRODUCER_SHA256" >&2
      exit 1
    fi
    if ! [[ "$HOLOSTREAM_PRODUCER_TIMEOUT" =~ ^[1-9][0-9]*$ ]]; then
      echo "ERROR: HOLOSTREAM_PRODUCER_TIMEOUT must be a positive integer" >&2
      exit 1
    fi
    if ! [[ "$HOLOSTREAM_TELEMETRY_INTERVAL_MS" =~ ^[1-9][0-9]*$ ]] ||
       ! [[ "$HOLOSTREAM_TELEMETRY_START_TIMEOUT" =~ ^[1-9][0-9]*$ ]]; then
      echo "ERROR: HoloStream telemetry interval and start timeout must be positive integers" >&2
      exit 1
    fi
    if ! [[ "$METRICS_TERMINAL_TIMEOUT_SEC" =~ ^[1-9][0-9]*$ ]] ||
       ! [[ "$METRICS_TERMINAL_STABLE_SAMPLES" =~ ^[1-9][0-9]*$ ]] ||
       ! [[ "$METRICS_MAX_CONSECUTIVE_FAILURES" =~ ^[1-9][0-9]*$ ]]; then
      echo "ERROR: metrics terminal timeout, stable samples, and failure limit must be positive integers" >&2
      exit 1
    fi
    if [[ "$METRICS_KAFKA_COMMAND_MODE" != "local" &&
          "$METRICS_KAFKA_COMMAND_MODE" != "ssh" ]]; then
      echo "ERROR: METRICS_KAFKA_COMMAND_MODE must be local or ssh" >&2
      exit 1
    fi
    if [[ "$METRICS_KAFKA_COMMAND_MODE" == "local" &&
          ! -x "$KAFKA_HOME/bin/kafka-run-class.sh" ]]; then
      echo "ERROR: local Kafka metrics command is unavailable: $KAFKA_HOME/bin/kafka-run-class.sh" >&2
      exit 1
    fi
    if ! [[ "$HOLOSTREAM_TOPIC_RESET_TIMEOUT" =~ ^[1-9][0-9]*$ ]]; then
      echo "ERROR: HOLOSTREAM_TOPIC_RESET_TIMEOUT must be a positive integer" >&2
      exit 1
    fi
    if ! [[ "$KAFKA_REPLICATION_FACTOR" =~ ^[1-9][0-9]*$ ]]; then
      echo "ERROR: KAFKA_REPLICATION_FACTOR must be a positive integer" >&2
      exit 1
    fi
    if [[ -z "$HOLOSTREAM_AUCTION_TOPIC" || -z "$HOLOSTREAM_BID_TOPIC" ]]; then
      echo "ERROR: HoloStream Auction and Bid topic names must be non-empty" >&2
      exit 1
    fi
    if [[ "$HOLOSTREAM_AUCTION_TOPIC" == "$HOLOSTREAM_BID_TOPIC" ]]; then
      echo "ERROR: HoloStream Auction and Bid topics must be different" >&2
      exit 1
    fi
    if ! [[ "$HOLOSTREAM_AUCTION_EVENTS" =~ ^[1-9][0-9]*$ ]] ||
       ! [[ "$HOLOSTREAM_BID_EVENTS" =~ ^[1-9][0-9]*$ ]]; then
      echo "ERROR: HoloStream expected Auction and Bid event counts must be positive integers" >&2
      exit 1
    fi
    if [[ "$PRODUCER_PARALLELISM" -ne "$KAFKA_PARTITIONS" ]]; then
      echo "ERROR: HoloStream producer parallelism ($PRODUCER_PARALLELISM) must match Kafka partitions ($KAFKA_PARTITIONS)" >&2
      exit 1
    fi
    if [[ "$PREFILL_EVENTS" -ne 0 ]]; then
      echo "ERROR: HoloStream mode requires PREFILL_EVENTS=0 so the bounded producer command runs exactly once" >&2
      exit 1
    fi
    if [[ "$LIVE_EVENTS" -le 0 ]]; then
      echo "ERROR: HoloStream mode requires TOTAL_EVENTS > 0 to launch the producer command" >&2
      exit 1
    fi
    ;;
  *)
    echo "ERROR: unsupported PRODUCER_IMPL=$PRODUCER_IMPL; expected beam or holostream" >&2
    exit 1
    ;;
esac

if [[ "$OFFLOADING" != "0" && "$OFFLOADING" != "1" ]]; then
  echo "ERROR: OFFLOADING must be 0 or 1" >&2
  exit 1
fi
if [[ "$AUTOSCALING" == "true" && "$OFFLOADING" != "1" ]]; then
  echo "ERROR: AUTOSCALING=true requires OFFLOADING=1" >&2
  exit 1
fi
if [[ "$SAFE_WORKER_REACTIVATION" != "true" && "$SAFE_WORKER_REACTIVATION" != "false" ]]; then
  echo "ERROR: SAFE_WORKER_REACTIVATION must be true or false" >&2
  exit 1
fi
if [[ "$REQUIRE_CLOUDLAB_LIFECYCLE_VALIDATION" != "true" &&
      "$REQUIRE_CLOUDLAB_LIFECYCLE_VALIDATION" != "false" ]]; then
  echo "ERROR: REQUIRE_CLOUDLAB_LIFECYCLE_VALIDATION must be true or false" >&2
  exit 1
fi
if ! [[ "$MIN_CLOUDLAB_REACTIVATIONS_PER_WORKER" =~ ^[0-9]+$ ]]; then
  echo "ERROR: MIN_CLOUDLAB_REACTIVATIONS_PER_WORKER must be a non-negative integer" >&2
  exit 1
fi
if [[ "$REQUIRE_CLOUDLAB_LIFECYCLE_VALIDATION" == "true" && "$OFFLOADING" != "1" ]]; then
  echo "ERROR: CloudLab lifecycle validation requires OFFLOADING=1" >&2
  exit 1
fi

if [[ "$SCALER_START_MODE" == "after_phase_delay" && "$BURST_MODE" != "phases" ]]; then
  echo "ERROR: SCALER_START_MODE=after_phase_delay requires BURST_MODE=phases" >&2
  exit 1
fi

"$SCRIPT_DIR/verify_cloudlab_artifacts.sh" "$REBUILT_NEMO" "$REBUILT_NEXMARK"

if [[ "$HARNESS_PREFLIGHT_ONLY" == "true" ]]; then
  echo "Harness preflight completed; no cluster resources were changed."
  exit 0
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

withhold_compute_nodemanagers() {
  local node ssh_node nm_count
  local -a compute_nodes
  IFS=',' read -ra compute_nodes <<< "$COMPUTE_HOSTS"
  if [[ "${#compute_nodes[@]}" -eq 0 || -z "${compute_nodes[0]}" ]]; then
    echo "ERROR: PIN_AM_TO_SOURCE=true requires COMPUTE_HOSTS" >&2
    return 1
  fi

  echo "Temporarily withholding compute NodeManagers for deterministic AM placement"
  for node in "${compute_nodes[@]}"; do
    ssh_node=${node%-link-1}
    ssh "$ssh_node" "$HADOOP_HOME/sbin/yarn-daemon.sh stop nodemanager" >/dev/null 2>&1 || true
  done
  COMPUTE_NMS_WITHHELD=true

  # Hadoop 2.7 can continue reporting cleanly stopped NodeManagers as RUNNING
  # until their expiry timeout. Reset only the ResourceManager so its live-node
  # view immediately contains the still-running source NodeManager.
  "$HADOOP_HOME/sbin/yarn-daemon.sh" stop resourcemanager >/dev/null 2>&1 || true
  sleep 2
  "$HADOOP_HOME/sbin/yarn-daemon.sh" start resourcemanager >/dev/null 2>&1

  for _ in $(seq 1 30); do
    nm_count=$(yarn node -list 2>/dev/null | grep -c RUNNING || true)
    if [[ "$nm_count" -eq 1 ]]; then
      echo "  Only the source NodeManager remains available for AM allocation"
      return 0
    fi
    sleep 1
  done
  echo "ERROR: compute NodeManagers did not leave the RUNNING set" >&2
  return 1
}

restore_compute_nodemanagers() {
  local node ssh_node nm_count
  local -a compute_nodes
  IFS=',' read -ra compute_nodes <<< "$COMPUTE_HOSTS"
  echo "Restoring compute NodeManagers"
  for node in "${compute_nodes[@]}"; do
    ssh_node=${node%-link-1}
    ssh "$ssh_node" "$HADOOP_HOME/sbin/yarn-daemon.sh start nodemanager" >/dev/null 2>&1 || true
  done

  for _ in $(seq 1 30); do
    nm_count=$(yarn node -list 2>/dev/null | grep -c RUNNING || true)
    if [[ "$nm_count" -ge "$EXPECTED_NM_COUNT" ]]; then
      COMPUTE_NMS_WITHHELD=false
      echo "  $nm_count/$EXPECTED_NM_COUNT NodeManagers available"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: only $nm_count/$EXPECTED_NM_COUNT NodeManagers returned" >&2
  return 1
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

verify_executor_placement() {
  if [[ "$STRICT_EXECUTOR_PLACEMENT" != "true" ]]; then
    return 0
  fi
  if [[ -z "$AM_HOST" || "$AM_HOST" == "N/A" ]]; then
    echo "ERROR: strict executor placement requested but AM host is unavailable" >&2
    return 1
  fi

  echo "Waiting for executor placement report on $AM_HOST:$EXECUTOR_PLACEMENT_REPORT"
  local tmp_report="$WORK_DIR/executor_placement.csv.tmp"
  local local_report="$WORK_DIR/executor_placement.csv"
  local rows=0
  local waited
  for waited in $(seq 1 "$PLACEMENT_WAIT_TIMEOUT_SEC"); do
    if scp "$AM_HOST:$EXECUTOR_PLACEMENT_REPORT" "$tmp_report" >/dev/null 2>&1; then
      rows=$(awk -F, 'NR > 1 && ($4 == "Source" || $4 == "Compute") { n++ } END { print n + 0 }' "$tmp_report")
      if [[ "$rows" -ge "$EXPECTED_EXECUTOR_PLACEMENT_ROWS" ]]; then
        mv "$tmp_report" "$local_report"
        break
      fi
    fi
    sleep 1
  done

  if [[ "$rows" -lt "$EXPECTED_EXECUTOR_PLACEMENT_ROWS" ]]; then
    echo "ERROR: placement report did not reach $EXPECTED_EXECUTOR_PLACEMENT_ROWS Source/Compute rows within ${PLACEMENT_WAIT_TIMEOUT_SEC}s" >&2
    return 1
  fi

  python3 "$SCRIPT_DIR/verify_executor_placement.py" \
    --report "$local_report" \
    --source-hosts "$SOURCE_HOSTS" \
    --compute-hosts "$COMPUTE_HOSTS" \
    --strict "$STRICT_EXECUTOR_PLACEMENT" \
    --output-json "$WORK_DIR/executor_placement_verification.json"
}

cleanup_after_placement_failure() {
  if [[ -n "${APP_ID:-}" ]]; then
    "$HADOOP_HOME/bin/yarn" application -kill "$APP_ID" 2>/dev/null || true
  fi
  if [[ -f "/tmp/nemo-subscriber-${TOPIC}.pid" ]]; then
    local sub_pid
    sub_pid=$(cat "/tmp/nemo-subscriber-${TOPIC}.pid" 2>/dev/null || true)
    if [[ -n "$sub_pid" ]]; then
      kill "$sub_pid" >/dev/null 2>&1 || true
    fi
  fi
  IFS=',' read -ra cleanup_nodes <<< "$OFFLOAD_NODES"
  for node in "${cleanup_nodes[@]}"; do
    ssh "$node" "pkill -f '[o]rg.apache.nemo.offloading.workers.vm.VMWorker' || true" >/dev/null 2>&1 || true
  done
}

verify_optimization_passes() {
  if [[ -z "$REQUIRED_OPTIMIZATION_PASSES" ]]; then
    return 0
  fi
  if [[ -z "$AM_HOST" || "$AM_HOST" == "N/A" ]]; then
    echo "ERROR: optimization-pass verification requested but AM host is unavailable" >&2
    return 1
  fi

  local verification_file="$WORK_DIR/optimization_pass_verification.txt"
  local pass_lines=""
  local all_found
  local pass
  local -a required_passes
  IFS=',' read -ra required_passes <<< "$REQUIRED_OPTIMIZATION_PASSES"

  echo "Waiting for required optimization passes: $REQUIRED_OPTIMIZATION_PASSES"
  for _ in $(seq 1 "$OPTIMIZATION_PASS_WAIT_TIMEOUT_SEC"); do
    pass_lines=$(ssh "$AM_HOST" \
      "grep -h 'policy.PolicyImpl: Apply .* to the DAG' /data/hadoop/yarn/logs/$APP_ID/container_*/driver.stderr 2>/dev/null" \
      || true)
    all_found=true
    for pass in "${required_passes[@]}"; do
      if ! grep -Fq "Apply $pass to the DAG" <<< "$pass_lines"; then
        all_found=false
        break
      fi
    done
    if [[ "$all_found" == "true" ]]; then
      printf '%s\n' "$pass_lines" > "$verification_file"
      echo "  Required optimization passes verified in driver log"
      return 0
    fi
    sleep 1
  done

  printf '%s\n' "$pass_lines" > "$verification_file"
  echo "ERROR: required optimization passes were not all observed within ${OPTIMIZATION_PASS_WAIT_TIMEOUT_SEC}s" >&2
  for pass in "${required_passes[@]}"; do
    if grep -Fq "Apply $pass to the DAG" <<< "$pass_lines"; then
      echo "  found: $pass" >&2
    else
      echo "  missing: $pass" >&2
    fi
  done
  return 1
}

verify_source_roots_running() {
  if [[ "$REQUIRED_SOURCE_ROOTS" -eq 0 && "$REQUIRED_SOURCE_TASKS" -eq 0 ]]; then
    return 0
  fi
  if [[ -z "$AM_HOST" || "$AM_HOST" == "N/A" ]]; then
    echo "ERROR: source-root verification requested but AM host is unavailable" >&2
    return 1
  fi

  local plan_file="$WORK_DIR/plan-logical.json"
  local plan_tmp="$plan_file.tmp"
  local stage_file="$WORK_DIR/source_root_stages.csv"
  local execution_file="$WORK_DIR/source_root_execution_verification.txt"
  local remote_plan="/data/hadoop/yarn/local/usercache/$USER/appcache/$APP_ID/dag/plan-logical.json"
  local remote_driver="/data/hadoop/yarn/logs/$APP_ID/container_*/driver.stderr"
  local waited

  echo "Waiting for both unbounded Kafka source roots to enter EXECUTING"
  for waited in $(seq 1 "$SOURCE_ROOT_WAIT_TIMEOUT_SEC"); do
    if scp "$AM_HOST:$remote_plan" "$plan_tmp" >/dev/null 2>&1; then
      mv "$plan_tmp" "$plan_file"
      break
    fi
    sleep 1
  done
  if [[ ! -s "$plan_file" ]]; then
    echo "ERROR: physical plan was unavailable for source-root verification" >&2
    return 1
  fi

  python3 - "$plan_file" > "$stage_file" <<'PY'
import json
import sys

plan = json.load(open(sys.argv[1], encoding="utf-8"))
for stage in plan.get("vertices", []):
    properties = stage.get("properties", {})
    vertices = properties.get("irDag", {}).get("vertices", [])
    if any("SourceVertex" in vertex.get("properties", {}).get("class", "") for vertex in vertices):
        print(f"{stage['id']},{properties.get('parallelism', 0)}")
PY

  local root_count
  local task_count
  root_count=$(awk -F, 'NF == 2 {count++} END {print count + 0}' "$stage_file")
  task_count=$(awk -F, 'NF == 2 {count += $2} END {print count + 0}' "$stage_file")
  if [[ "$root_count" -ne "$REQUIRED_SOURCE_ROOTS" || "$task_count" -ne "$REQUIRED_SOURCE_TASKS" ]]; then
    echo "ERROR: optimized plan has source roots/tasks=$root_count/$task_count; expected $REQUIRED_SOURCE_ROOTS/$REQUIRED_SOURCE_TASKS" >&2
    cat "$stage_file" >&2
    return 1
  fi

  local execution_lines=""
  local all_running
  local stage
  local parallelism
  local index
  for waited in $(seq 1 "$SOURCE_ROOT_WAIT_TIMEOUT_SEC"); do
    execution_lines=$(ssh "$AM_HOST" \
      "grep -h 'Receive task executing message' $remote_driver 2>/dev/null" || true)
    all_running=true
    while IFS=, read -r stage parallelism; do
      for index in $(seq 0 $((parallelism - 1))); do
        if ! grep -Fq "Receive task executing message $stage-$index-0" <<< "$execution_lines"; then
          all_running=false
          break 2
        fi
      done
    done < "$stage_file"
    if [[ "$all_running" == "true" ]]; then
      {
        echo "sourceRoots=$root_count sourceTasks=$task_count"
        cat "$stage_file"
        grep -F 'Receive task executing message' <<< "$execution_lines" \
          | while IFS= read -r line; do
              while IFS=, read -r stage parallelism; do
                if grep -Fq "message $stage-" <<< "$line"; then
                  echo "$line"
                  break
                fi
              done < "$stage_file"
            done
      } > "$execution_file"
      echo "  Verified $root_count source roots and $task_count source tasks in EXECUTING"
      return 0
    fi
    sleep 1
  done

  echo "ERROR: not all source-root tasks entered EXECUTING within ${SOURCE_ROOT_WAIT_TIMEOUT_SEC}s" >&2
  cat "$stage_file" >&2
  return 1
}

start_producer_with_source_log() {
  local events=$1
  local live=$2
  local parallelism=${3:-$PRODUCER_PARALLELISM}

  if [[ "$PRODUCER_IMPL" == "holostream" ]]; then
    local -a launcher_args=(
      python3 "$HOLOSTREAM_LAUNCHER"
      --config "$HOLOSTREAM_CONFIG"
      --run-id "$JOB_ID"
      --output-dir "$WORK_DIR"
      --kafka-node "$KAFKA_NODE"
      --kafka-home "$KAFKA_HOME"
      --bootstrap-servers "$KAFKA_BOOTSTRAP"
      --auction-topic "$HOLOSTREAM_AUCTION_TOPIC"
      --bid-topic "$HOLOSTREAM_BID_TOPIC"
      --producer-binary "$HOLOSTREAM_PRODUCER_BINARY"
      --expected-partitions "$KAFKA_PARTITIONS"
      --timeout-seconds "$HOLOSTREAM_PRODUCER_TIMEOUT"
    )
    if [[ -n "$HOLOSTREAM_EXPECTED_PRODUCER_SHA256" ]]; then
      launcher_args+=(
        --expected-producer-sha256 "$HOLOSTREAM_EXPECTED_PRODUCER_SHA256"
      )
    fi
    echo "Starting Kafka end-offset telemetry for HoloStream input"
    java -cp "$STANDALONE_PRODUCER_CP" KafkaInputOffsetTelemetry \
      "$KAFKA_BOOTSTRAP" \
      "$HOLOSTREAM_AUCTION_TOPIC,$HOLOSTREAM_BID_TOPIC" \
      "$KAFKA_PARTITIONS" \
      "$events" \
      "$SOURCE_LOG" \
      "$HOLOSTREAM_TELEMETRY_CSV" \
      "$HOLOSTREAM_TELEMETRY_INTERVAL_MS" \
      "$((HOLOSTREAM_PRODUCER_TIMEOUT + 120))" \
      true >"$HOLOSTREAM_TELEMETRY_LOG" 2>&1 &
    HOLOSTREAM_TELEMETRY_PID=$!

    local waited
    for waited in $(seq 1 "$HOLOSTREAM_TELEMETRY_START_TIMEOUT"); do
      if grep -qx '0 events' "$SOURCE_LOG" 2>/dev/null; then
        break
      fi
      if ! kill -0 "$HOLOSTREAM_TELEMETRY_PID" 2>/dev/null; then
        echo "ERROR: Kafka input telemetry exited before establishing its baseline" >&2
        wait "$HOLOSTREAM_TELEMETRY_PID" 2>/dev/null || true
        HOLOSTREAM_TELEMETRY_PID=""
        tail -n 50 "$HOLOSTREAM_TELEMETRY_LOG" >&2 || true
        return 1
      fi
      sleep 1
    done
    if ! grep -qx '0 events' "$SOURCE_LOG" 2>/dev/null; then
      echo "ERROR: Kafka input telemetry did not establish a zero baseline within ${HOLOSTREAM_TELEMETRY_START_TIMEOUT}s" >&2
      tail -n 50 "$HOLOSTREAM_TELEMETRY_LOG" >&2 || true
      return 1
    fi

    echo "Launching supervised HoloStream producers with $HOLOSTREAM_LAUNCHER"
    "${launcher_args[@]}" >"$HOLOSTREAM_LAUNCHER_LOG" 2>&1 &
    PRODUCER_PID=$!

    for waited in $(seq 1 "$HOLOSTREAM_TELEMETRY_START_TIMEOUT"); do
      if grep -Eq '^[1-9][0-9]* events$' "$SOURCE_LOG" 2>/dev/null &&
         grep -Eq 'Avg input: [1-9][0-9]*(\.[0-9]+)?' "$SUB_LOG" 2>/dev/null; then
        echo "Kafka input telemetry is live and visible to the scaler"
        return 0
      fi
      if ! kill -0 "$PRODUCER_PID" 2>/dev/null; then
        echo "ERROR: HoloStream producer exited before telemetry became visible to the scaler" >&2
        tail -n 50 "$HOLOSTREAM_LAUNCHER_LOG" >&2 || true
        return 1
      fi
      if ! kill -0 "$HOLOSTREAM_TELEMETRY_PID" 2>/dev/null; then
        echo "ERROR: Kafka input telemetry exited before becoming visible to the scaler" >&2
        tail -n 50 "$HOLOSTREAM_TELEMETRY_LOG" >&2 || true
        return 1
      fi
      sleep 1
    done
    echo "ERROR: scaler did not observe positive HoloStream input within ${HOLOSTREAM_TELEMETRY_START_TIMEOUT}s" >&2
    tail -n 50 "$SOURCE_LOG" >&2 || true
    tail -n 50 "$SUB_LOG" >&2 || true
    return 1
  fi

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

wait_for_holostream_production() {
  local producer_status=0
  local telemetry_status=0

  while kill -0 "$PRODUCER_PID" 2>/dev/null; do
    if [[ -n "$METRICS_PID" ]] && ! kill -0 "$METRICS_PID" 2>/dev/null; then
      echo "ERROR: metrics collector failed while producers were running" >&2
      tail -n 50 "$WORK_DIR/metrics_collector.log" >&2 || true
      return 1
    fi
    if [[ -n "$HOLOSTREAM_TELEMETRY_PID" ]] &&
       ! kill -0 "$HOLOSTREAM_TELEMETRY_PID" 2>/dev/null; then
      if wait "$HOLOSTREAM_TELEMETRY_PID"; then
        HOLOSTREAM_TELEMETRY_PID=""
      else
        telemetry_status=$?
        HOLOSTREAM_TELEMETRY_PID=""
        echo "ERROR: Kafka input telemetry failed while producers were running (status $telemetry_status)" >&2
        tail -n 50 "$HOLOSTREAM_TELEMETRY_LOG" >&2 || true
        return "$telemetry_status"
      fi
    fi
    sleep 1
  done

  if wait "$PRODUCER_PID"; then
    producer_status=0
  else
    producer_status=$?
  fi
  PRODUCER_PID=""
  if [[ "$producer_status" -ne 0 ]]; then
    echo "ERROR: HoloStream producer launcher failed with status $producer_status" >&2
    tail -n 50 "$HOLOSTREAM_LAUNCHER_LOG" >&2 || true
    return "$producer_status"
  fi

  if [[ -n "$HOLOSTREAM_TELEMETRY_PID" ]]; then
    if wait "$HOLOSTREAM_TELEMETRY_PID"; then
      telemetry_status=0
    else
      telemetry_status=$?
    fi
    HOLOSTREAM_TELEMETRY_PID=""
  fi
  if [[ "$telemetry_status" -ne 0 ]]; then
    echo "ERROR: Kafka input telemetry failed with status $telemetry_status" >&2
    tail -n 50 "$HOLOSTREAM_TELEMETRY_LOG" >&2 || true
    return "$telemetry_status"
  fi

  local final_total
  # source.log intentionally contains per-poll deltas because the original Sponge scaler adds
  # every INPUT sample to its aggregate.  It is therefore not a cumulative completion record.
  # The telemetry CSV has an explicit cumulative totalEvents column and is the authoritative
  # bounded-production completion check.
  final_total=$(awk -F, 'NR > 1 && $2 ~ /^[0-9]+$/ {value=$2} END {print value}' \
    "$HOLOSTREAM_TELEMETRY_CSV")
  if [[ "$final_total" != "$TOTAL_EVENTS" ]]; then
    echo "ERROR: Kafka input telemetry ended at ${final_total:-missing}; expected $TOTAL_EVENTS" >&2
    return 1
  fi
  if ! grep -q "KAFKA_INPUT_TELEMETRY_DONE total=$TOTAL_EVENTS " "$HOLOSTREAM_TELEMETRY_LOG"; then
    echo "ERROR: Kafka input telemetry has no successful terminal marker" >&2
    tail -n 50 "$HOLOSTREAM_TELEMETRY_LOG" >&2 || true
    return 1
  fi
}

wait_for_terminal_metrics() {
  local stable=0
  local waited
  local row
  local input_offset
  local source_count
  local consumer_lag
  local sample_timestamp
  local last_sample_timestamp=""

  echo "Waiting for terminal metrics: input=$TOTAL_EVENTS source=$TOTAL_EVENTS consumerLag=0"
  for waited in $(seq 0 "$METRICS_TERMINAL_TIMEOUT_SEC"); do
    if [[ -z "$METRICS_PID" ]] || ! kill -0 "$METRICS_PID" 2>/dev/null; then
      echo "ERROR: metrics collector exited before terminal validation" >&2
      tail -n 50 "$WORK_DIR/metrics_collector.log" >&2 || true
      return 1
    fi
    row=$(awk -F, 'NR > 1 {line=$0} END {print line}' "$WORK_DIR/combined_metrics.csv" 2>/dev/null || true)
    if [[ -n "$row" ]]; then
      input_offset=$(printf '%s\n' "$row" | awk -F, '{print $2}')
      source_count=$(printf '%s\n' "$row" | awk -F, '{print $4}')
      consumer_lag=$(printf '%s\n' "$row" | awk -F, '{print $25}')
      sample_timestamp=$(printf '%s\n' "$row" | awk -F, '{print $1}')
      if [[ "$input_offset" == "$TOTAL_EVENTS" &&
            "$source_count" == "$TOTAL_EVENTS" &&
            "$consumer_lag" == "0" ]]; then
        if [[ "$sample_timestamp" != "$last_sample_timestamp" ]]; then
          stable=$((stable + 1))
          last_sample_timestamp=$sample_timestamp
        fi
        if [[ "$stable" -ge "$METRICS_TERMINAL_STABLE_SAMPLES" ]]; then
          echo "Terminal metrics stable for $stable samples after ${waited}s"
          return 0
        fi
      else
        stable=0
        last_sample_timestamp=$sample_timestamp
      fi
    fi
    sleep 1
  done
  echo "ERROR: terminal metrics did not stabilize within ${METRICS_TERMINAL_TIMEOUT_SEC}s" >&2
  tail -n 10 "$WORK_DIR/combined_metrics.csv" >&2 || true
  return 1
}

preserve_run_metrics() {
  local metrics_dir="$WORK_DIR/remote_tmp_metrics"
  local node
  local file
  local task_files=0
  local source_task_files=0

  echo "Preserving raw AM and executor metrics"
  mkdir -p "$metrics_dir"
  for node in $BASELINE_NODES; do
    mkdir -p "$metrics_dir/$node"
    for file in task_metrics.csv source_task_metrics.csv; do
      if ssh "$node" "test -f /tmp/$file"; then
        scp "$node:/tmp/$file" "$metrics_dir/$node/$file" >/dev/null
        if [[ "$file" == "task_metrics.csv" ]]; then
          task_files=$((task_files + 1))
        else
          source_task_files=$((source_task_files + 1))
        fi
      fi
    done
  done
  if [[ "$source_task_files" -lt 1 ]]; then
    echo "ERROR: source telemetry preservation incomplete: source_task_files=$source_task_files" >&2
    return 1
  fi
  if [[ "$task_files" -ne "$EXPECTED_NM_COUNT" ]]; then
    echo "WARNING: original Sponge emits task metrics in executor logs, not task_metrics.csv "
    echo "  preserved CSV files: $task_files/$EXPECTED_NM_COUNT; preserving YARN logs instead"
  fi
  if ! ssh "$AM_HOST" "test -f /tmp/source_aggregate_metrics.csv"; then
    echo "ERROR: required source aggregate metric is missing: $AM_HOST:/tmp/source_aggregate_metrics.csv" >&2
    return 1
  fi
  scp "$AM_HOST:/tmp/source_aggregate_metrics.csv" "$WORK_DIR/source_aggregate_metrics.csv" >/dev/null
  if ssh "$AM_HOST" "test -f /tmp/scaler_metrics.csv"; then
    scp "$AM_HOST:/tmp/scaler_metrics.csv" "$WORK_DIR/scaler_metrics.csv" >/dev/null
  else
    python3 - "$WORK_DIR/combined_metrics.csv" "$WORK_DIR/scaler_metrics.csv" "$JOB_ID" <<'PY'
import csv
import sys

source, output, job_id = sys.argv[1:]
columns = [
    "timestamp", "jobId", "avgCpu", "avgInput", "avgProcess", "queue",
    "numExecutors", "numLambdaExecutors", "lastActionWasScaleOut",
    "prevFutureCompleted", "registeredVmWorkers", "activeVmWorkers",
    "vmWorkerRunningTasks",
]
with open(source, newline="") as input_file, open(output, "w", newline="") as output_file:
    reader = csv.DictReader(input_file)
    writer = csv.DictWriter(output_file, fieldnames=columns)
    writer.writeheader()
    for row in reader:
        writer.writerow({
            "timestamp": row["timestamp"],
            "jobId": job_id,
            "avgCpu": row["avgCpu"],
            "avgInput": row["avgInput"],
            "avgProcess": row["avgProcess"],
            "queue": row["queueSize"],
            "numExecutors": row["numExecutors"],
            "numLambdaExecutors": row["numLambdaExecutors"],
            "lastActionWasScaleOut": "unknown",
            "prevFutureCompleted": "unknown",
            "registeredVmWorkers": row["registeredVmWorkers"],
            "activeVmWorkers": row["activeVmWorkers"],
            "vmWorkerRunningTasks": row["vmWorkerRunningTasks"],
        })
PY
  fi
  for file in scaling_decisions.csv; do
    if ssh "$AM_HOST" "test -f /tmp/$file"; then
      scp "$AM_HOST:/tmp/$file" "$WORK_DIR/$file" >/dev/null
    fi
  done

  mkdir -p "$metrics_dir/yarn_logs"
  scp "$AM_HOST:/data/hadoop/yarn/logs/$APP_ID/container_*/driver.stderr" \
    "$metrics_dir/yarn_logs/" >/dev/null 2>&1 || true
  if [[ -f "$WORK_DIR/executor_placement.csv" ]]; then
    while IFS=, read -r _ _ executor_id executor_type container_id _ physical_host _; do
      if [[ "$executor_type" != "Source" && "$executor_type" != "Compute" ]]; then
        continue
      fi
      # Placement reports contain YARN's link-qualified hostname (for example
      # node9-link-1), while inter-node SSH is configured under node9.
      local ssh_host="${physical_host%%-link-*}"
      scp "$ssh_host:/data/hadoop/yarn/logs/$APP_ID/$container_id/evaluator.stderr" \
        "$metrics_dir/yarn_logs/${ssh_host}-${executor_id}-evaluator.stderr" \
        >/dev/null 2>&1 || true
      scp "$ssh_host:/data/hadoop/yarn/logs/$APP_ID/$container_id/evaluator.stdout" \
        "$metrics_dir/yarn_logs/${ssh_host}-${executor_id}-evaluator.stdout" \
        >/dev/null 2>&1 || true
    done < "$WORK_DIR/executor_placement.csv"
  fi

  if [[ "$OFFLOADING" == "1" ]]; then
    local -a lifecycle_offload_nodes
    mkdir -p "$metrics_dir/vmworker_logs"
    IFS=',' read -ra lifecycle_offload_nodes <<< "$OFFLOAD_NODES"
    for node in "${lifecycle_offload_nodes[@]}"; do
      mkdir -p "$metrics_dir/vmworker_logs/$node"
      scp "$node:/tmp/vmworker-*.log" "$metrics_dir/vmworker_logs/$node/" \
        >/dev/null 2>&1 || true
      if ssh "$node" "test -f /tmp/start-warm-pool-$node.log"; then
        scp "$node:/tmp/start-warm-pool-$node.log" \
          "$metrics_dir/vmworker_logs/$node/" >/dev/null 2>&1 || true
      fi
    done
  fi

  "$KAFKA_HOME/bin/kafka-consumer-groups.sh" \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --describe \
    --group "$KAFKA_CONSUMER_GROUP" \
    > "$WORK_DIR/final_consumer_group.txt"
  : > "$WORK_DIR/final_kafka_offsets.txt"
  IFS=',' read -ra metrics_topics <<< "$METRICS_INPUT_TOPICS"
  for topic in "${metrics_topics[@]}"; do
    "$KAFKA_HOME/bin/kafka-get-offsets.sh" \
      --bootstrap-server "$KAFKA_BOOTSTRAP" \
      --topic "$topic" \
      --time -1 >> "$WORK_DIR/final_kafka_offsets.txt"
  done
}

validate_cloudlab_worker_lifecycle() {
  local metrics_dir="$WORK_DIR/remote_tmp_metrics"
  local output="$WORK_DIR/worker_lifecycle_validation.json"

  if [[ "$REQUIRE_CLOUDLAB_LIFECYCLE_VALIDATION" != "true" ]]; then
    return 0
  fi

  echo "Validating repeated CloudLab VM worker invocation lifecycles"
  python3 "$SCRIPT_DIR/validate_worker_lifecycle.py" \
    --master-log-dir "$metrics_dir/yarn_logs" \
    --worker-log-dir "$metrics_dir/vmworker_logs" \
    --minimum-reactivations-per-worker "$MIN_CLOUDLAB_REACTIVATIONS_PER_WORKER" \
    --output-json "$output"
}

stop_metrics_collector() {
  local status=0
  if [[ -z "$METRICS_PID" ]]; then
    return 0
  fi
  if kill -0 "$METRICS_PID" 2>/dev/null; then
    kill -TERM "$METRICS_PID" 2>/dev/null || true
  fi
  wait "$METRICS_PID" || status=$?
  METRICS_PID=""
  if [[ "$status" -ne 0 && "$status" -ne 143 ]]; then
    echo "ERROR: metrics collector exited with status $status" >&2
    return "$status"
  fi
}

write_final_validation() {
  local row
  local input_offset
  local source_count
  local consumer_lag
  local producer_status="not_applicable"
  local nemo_sha256
  local nexmark_sha256
  local placement_passed=false
  local evaluator_error_count=0
  local placement_timestamp
  local placement_job
  local executor_id
  local executor_type
  local container_id
  local requested_host
  local physical_host
  local row_passed
  local application_state="UNKNOWN"
  local final_application_state="UNKNOWN"
  local yarn_status_ok=false
  local driver_fatal_error_count=0
  local cloudlab_lifecycle_required="$REQUIRE_CLOUDLAB_LIFECYCLE_VALIDATION"
  local cloudlab_lifecycle_passed=true

  row=$(awk -F, 'NR > 1 {line=$0} END {print line}' "$WORK_DIR/combined_metrics.csv")
  input_offset=$(printf '%s\n' "$row" | awk -F, '{print $2}')
  source_count=$(printf '%s\n' "$row" | awk -F, '{print $4}')
  consumer_lag=$(printf '%s\n' "$row" | awk -F, '{print $25}')
  if [[ "$PRODUCER_IMPL" == "holostream" ]]; then
    producer_status=$(python3 -c \
      'import json,sys; print(json.load(open(sys.argv[1]))["status"])' \
      "$WORK_DIR/holostream_producer_completion.json")
  fi
  nemo_sha256=$(sha256sum "$REBUILT_NEMO" | awk '{print $1}')
  nexmark_sha256=$(sha256sum "$REBUILT_NEXMARK" | awk '{print $1}')
  if [[ -f "$WORK_DIR/executor_placement_verification.json" ]] &&
     grep -q '"passed":[[:space:]]*true' "$WORK_DIR/executor_placement_verification.json"; then
    placement_passed=true
  elif [[ "$STRICT_EXECUTOR_PLACEMENT" != "true" ]]; then
    placement_passed=true
  fi
  : > "$WORK_DIR/evaluator_errors.txt"
  if [[ -f "$WORK_DIR/executor_placement.csv" ]]; then
    while IFS=, read -r placement_timestamp placement_job executor_id executor_type \
      container_id requested_host physical_host row_passed; do
      if [[ "$executor_type" != "Source" && "$executor_type" != "Compute" ]]; then
        continue
      fi
      ssh \
        -o BatchMode=yes \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/tmp/casp-internal-known-hosts \
        "$physical_host" \
        "grep -E 'InvalidClassException|OutOfMemoryError:|Exception in thread|Failed to deserialize|Failed to decode' '/data/hadoop/yarn/logs/$APP_ID/$container_id/evaluator.stderr' 2>/dev/null || true" \
        >> "$WORK_DIR/evaluator_errors.txt"
    done < "$WORK_DIR/executor_placement.csv"
  fi
  evaluator_error_count=$(wc -l < "$WORK_DIR/evaluator_errors.txt" | tr -d ' ')

  if "$HADOOP_HOME/bin/yarn" application -status "$APP_ID" \
      > "$WORK_DIR/yarn_application_status.txt" 2>&1; then
    yarn_status_ok=true
    application_state=$(awk -F: '
      $1 ~ /^[[:space:]]*State[[:space:]]*$/ {
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2); print $2; exit
      }' "$WORK_DIR/yarn_application_status.txt")
    final_application_state=$(awk -F: '
      $1 ~ /^[[:space:]]*Final-State[[:space:]]*$/ {
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2); print $2; exit
      }' "$WORK_DIR/yarn_application_status.txt")
    application_state=${application_state:-UNKNOWN}
    final_application_state=${final_application_state:-UNKNOWN}
  fi

  grep -E \
    'REEFUncaughtExceptionHandler uncaughtException|Thread TaskDispatcher thread threw an uncaught exception|Received a resource manager error|SEVERE: Uncaught exception|OutOfMemoryError:|InvalidClassException|Failed to deserialize|Failed to decode' \
    "$SUB_LOG" > "$WORK_DIR/driver_fatal_errors.txt" || true
  driver_fatal_error_count=$(wc -l < "$WORK_DIR/driver_fatal_errors.txt" | tr -d ' ')

  if [[ "$cloudlab_lifecycle_required" == "true" ]]; then
    cloudlab_lifecycle_passed=false
    if [[ -f "$WORK_DIR/worker_lifecycle_validation.json" ]] &&
       grep -q '"passed":[[:space:]]*true' "$WORK_DIR/worker_lifecycle_validation.json"; then
      cloudlab_lifecycle_passed=true
    fi
  fi

  python3 -c '
import json
import sys
from pathlib import Path

(
    output, job_id, app_id, expected, produced, source, lag, producer_status,
    nemo_sha256, nexmark_sha256, placement_passed, evaluator_error_count,
    safe_worker_reactivation, yarn_status_ok, application_state,
    final_application_state, driver_fatal_error_count,
    cloudlab_lifecycle_required, cloudlab_lifecycle_passed,
) = sys.argv[1:]
payload = {
    "applicationId": app_id,
    "applicationState": application_state,
    "artifactSha256": {
        "nemoClient": nemo_sha256,
        "nexmark": nexmark_sha256,
    },
    "consumerLag": int(lag),
    "cloudlabWorkerLifecyclePassed": cloudlab_lifecycle_passed == "true",
    "cloudlabWorkerLifecycleRequired": cloudlab_lifecycle_required == "true",
    "driverFatalErrorCount": int(driver_fatal_error_count),
    "evaluatorErrorCount": int(evaluator_error_count),
    "expectedInputEvents": int(expected),
    "finalApplicationState": final_application_state,
    "jobId": job_id,
    "metricsCollectorStatus": "terminal_validated",
    "placementPassed": placement_passed == "true",
    "producerStatus": producer_status,
    "rawMetricsPreserved": True,
    "safeWorkerReactivation": safe_worker_reactivation == "true",
    "sourceProcessedEvents": int(source),
    "terminalInputOffset": int(produced),
    "yarnStatusCommandSucceeded": yarn_status_ok == "true",
    "validated": (
        producer_status in ("success", "not_applicable")
        and int(produced) == int(expected)
        and int(source) == int(expected)
        and int(lag) == 0
        and placement_passed == "true"
        and int(evaluator_error_count) == 0
        and int(driver_fatal_error_count) == 0
        and yarn_status_ok == "true"
        and application_state == "RUNNING"
        and (
            cloudlab_lifecycle_required != "true"
            or cloudlab_lifecycle_passed == "true"
        )
    ),
}
Path(output).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
' "$WORK_DIR/final_validation.json" "$JOB_ID" "$APP_ID" "$TOTAL_EVENTS" \
    "$input_offset" "$source_count" "$consumer_lag" "$producer_status" \
    "$nemo_sha256" "$nexmark_sha256" "$placement_passed" \
    "$evaluator_error_count" "$SAFE_WORKER_REACTIVATION" "$yarn_status_ok" \
    "$application_state" "$final_application_state" "$driver_fatal_error_count" \
    "$cloudlab_lifecycle_required" "$cloudlab_lifecycle_passed"
  grep -q '"validated": true' "$WORK_DIR/final_validation.json"
}

run_producer_with_source_log() {
  local events=$1
  local live=$2
  local parallelism=${3:-$PRODUCER_PARALLELISM}
  local status

  start_producer_with_source_log "$events" "$live" "$parallelism"
  if [[ "$PRODUCER_IMPL" == "holostream" ]]; then
    wait_for_holostream_production
    return $?
  fi
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
    if [[ "$PRODUCER_IMPL" == "holostream" &&
          -n "$HOLOSTREAM_TELEMETRY_PID" ]] &&
       ! kill -0 "$HOLOSTREAM_TELEMETRY_PID" 2>/dev/null; then
      echo "ERROR: Kafka input telemetry exited before phase $phase started" >&2
      tail -n 50 "$HOLOSTREAM_TELEMETRY_LOG" >&2 || true
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

# 1. Generate vm_addresses.txt only for runs that actually exercise offloading.
if [[ "$OFFLOADING" == "1" ]]; then
  echo "Generating vm_addresses.txt for offloading nodes: $OFFLOAD_NODES"
  IFS=',' read -ra NODE_LIST <<< "$OFFLOAD_NODES"
  python3 "$SCRIPT_DIR/generate_vm_addresses.py" \
    --nodes "${NODE_LIST[@]}" \
    --first-port "$FIRST_PORT" \
    --workers-per-node "$WORKERS_PER_NODE" \
    --output "$NEMO_REPO_ROOT/vm_addresses.txt"
else
  echo "Offloading disabled; skipping VM address generation and warm-pool provisioning"
fi

# 2. Create Kafka input topic(s) and prefill
if [[ "$PRODUCER_IMPL" == "holostream" ]]; then
  echo "Resetting fixed HoloStream Kafka topics"
  python3 "$HOLOSTREAM_TOPIC_RESETTER" \
    --kafka-node "$KAFKA_NODE" \
    --kafka-home "$KAFKA_HOME" \
    --bootstrap-servers "$KAFKA_BOOTSTRAP" \
    --zookeeper "$KAFKA_ZOOKEEPER" \
    --consumer-group "$KAFKA_CONSUMER_GROUP" \
    --partitions "$KAFKA_PARTITIONS" \
    --replication-factor "$KAFKA_REPLICATION_FACTOR" \
    --timeout-seconds "$HOLOSTREAM_TOPIC_RESET_TIMEOUT" \
    --output-dir "$WORK_DIR"
else
  configure_input_topic "$TOPIC" "$TOPIC_CONFIG_DESCRIBE"
fi

if [[ "$PREFILL_EVENTS" -gt 0 ]]; then
  echo "Prefilling $PREFILL_EVENTS records"
  run_producer_with_source_log "$PREFILL_EVENTS" false
fi

# 3. Start warm VMWorker pools on all offload nodes for scale-out runs only.
if [[ "$OFFLOADING" == "1" ]]; then
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
fi

# 4. Preflight check: YARN NMs must be up before we submit
check_vm_workers() {
  local vm_file=$1
  local max_fail=${2:-5}
  local fails=0
  local line host endpoint port attempt reachable
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    host=${line%%:*}
    endpoint=${line#*:}
    port=${endpoint%%,*}
    if [[ -z "$host" || ! "$port" =~ ^[0-9]+$ ]]; then
      echo "  WARNING: Invalid VM worker address: $line"
      fails=$((fails + 1))
      continue
    fi
    reachable=false
    for attempt in $(seq 1 10); do
      if timeout 2 bash -c "echo > /dev/tcp/$host/$port" 2>/dev/null; then
        reachable=true
        break
      fi
      sleep 1
    done
    if [[ "$reachable" != "true" ]]; then
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
if [[ "$OFFLOADING" == "1" ]]; then
  echo "Checking VM worker connectivity..."
  check_vm_workers "$NEMO_REPO_ROOT/vm_addresses.txt" "${VM_CONNECTIVITY_FAIL_LIMIT:-5}" || exit 1
fi

ensure_yarn_nodes

echo "Cleaning stale baseline worker metrics"
for node in $BASELINE_NODES; do
  ssh "$node" "rm -f /tmp/source_task_metrics.csv /tmp/task_metrics.csv" || true
done

if [[ "$PIN_AM_TO_SOURCE" == "true" ]]; then
  withhold_compute_nodemanagers
fi

# 5. Launch subscriber with the selected offloading mode.
echo "Launching subscriber"
export TOPIC QUERY EXECUTOR_JSON NUM_EVENTS=$TOTAL_EVENTS STREAM_TIMEOUT
export LOG_FILE="$SUB_LOG"
export OFFLOADING
export NUM_MAX_LAMBDA
export AUTOSCALING
export SAFE_WORKER_REACTIVATION
export JOB_ID
export NEMO_WORK_DIR="$WORK_DIR"
export NEMO_JOB_ID="$JOB_ID"
export SOURCE_HOSTS COMPUTE_HOSTS STRICT_EXECUTOR_PLACEMENT EXECUTOR_PLACEMENT_REPORT
export KAFKA_INPUT_FORMAT HOLOSTREAM_AUCTION_TOPIC HOLOSTREAM_BID_TOPIC
export HOLOSTREAM_AUCTION_EVENTS HOLOSTREAM_BID_EVENTS
export KAFKA_CONSUMER_GROUP KAFKA_PARTITIONS

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
printf 'appId,amHost\n%s,%s\n' "$APP_ID" "$AM_HOST" > "$WORK_DIR/application_master.csv"

if [[ "$PIN_AM_TO_SOURCE" == "true" ]]; then
  restore_compute_nodemanagers
fi

if [[ -n "$AM_HOST" && "$AM_HOST" != "N/A" ]]; then
  echo "Cleaning stale AM-side fallback metrics on $AM_HOST"
  ssh "$AM_HOST" "rm -f /tmp/scaling_decisions.csv /tmp/scaler_metrics.csv /tmp/source_metrics.csv /tmp/source_aggregate_metrics.csv /tmp/source_task_metrics.csv /tmp/task_metrics.csv" || true
else
  echo "WARNING: AM host unavailable; skipping AM-side metrics cleanup" >&2
fi

if ! verify_executor_placement; then
  echo "ERROR: executor placement verification failed before live production. Cleaning up $APP_ID." >&2
  cleanup_after_placement_failure
  exit 1
fi

if ! verify_optimization_passes; then
  echo "ERROR: optimization-policy verification failed before live production. Cleaning up $APP_ID." >&2
  cleanup_after_placement_failure
  exit 1
fi

if ! verify_source_roots_running; then
  echo "ERROR: source-root verification failed before live production. Cleaning up $APP_ID." >&2
  cleanup_after_placement_failure
  exit 1
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
if [[ "$OFFLOADING" == "1" ]]; then
  write_lambda_executor_command
fi
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
if [[ "$PRODUCER_IMPL" == "holostream" ]]; then
  METRICS_INPUT_TOPICS="$HOLOSTREAM_AUCTION_TOPIC,$HOLOSTREAM_BID_TOPIC"
else
  METRICS_INPUT_TOPICS="$TOPIC"
fi
echo "  Kafka input topics: $METRICS_INPUT_TOPICS"
KAFKA_COMMAND_MODE="$METRICS_KAFKA_COMMAND_MODE" \
METRICS_JOB_ID="$JOB_ID" \
METRICS_RESUME=false \
METRICS_MAX_CONSECUTIVE_FAILURES="$METRICS_MAX_CONSECUTIVE_FAILURES" \
python3 "$SCRIPT_DIR/metrics_collector.py" \
  "$METRICS_INPUT_TOPICS" \
  "$WORK_DIR" \
  "$SUB_LOG" \
  "$AM_HOST" \
  "$KAFKA_RESULTS_TOPIC" \
  "$BASELINE_NODES" > "$WORK_DIR/metrics_collector.log" 2>&1 &
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
    for _ in $(seq 1 "$SCALER_START_PHASE_DELAY_SEC"); do
      if ! kill -0 "$PRODUCER_PID" 2>/dev/null; then
        echo "ERROR: producer exited during the delayed scaler gate" >&2
        exit 1
      fi
      if [[ "$PRODUCER_IMPL" == "holostream" &&
            -n "$HOLOSTREAM_TELEMETRY_PID" ]] &&
         ! kill -0 "$HOLOSTREAM_TELEMETRY_PID" 2>/dev/null; then
        echo "ERROR: Kafka input telemetry exited during the delayed scaler gate" >&2
        exit 1
      fi
      sleep 1
    done
    echo "Starting scaler/backpressure after delayed phase gate"
    start_autoscaler_commands "after_phase_delay" "$SCALER_START_PHASE" "$phase_target_rate" "$SCALER_START_PHASE_DELAY_SEC"
    if [[ "$PRODUCER_IMPL" == "holostream" ]]; then
      wait_for_holostream_production
    else
      producer_status=0
      wait "$PRODUCER_PID" || producer_status=$?
      if [[ "$producer_status" -ne 0 ]]; then
        exit "$producer_status"
      fi
    fi
  else
    run_producer_with_source_log "$LIVE_EVENTS" true
  fi
fi

if [[ "$PRODUCER_IMPL" == "holostream" ]]; then
  wait_for_terminal_metrics
  preserve_run_metrics
  if ! validate_cloudlab_worker_lifecycle; then
    echo "ERROR: CloudLab VM worker lifecycle validation failed" >&2
  fi
  write_final_validation
  stop_metrics_collector
fi

# 11. Report status
echo ""
echo "Autoscaler harness running."
echo "  Subscriber PID: $SUB_PID"
echo "  Subscriber log: $SUB_LOG"
echo "  Work dir: $WORK_DIR"
echo "  Scaling commands: $WORK_DIR/scaling.txt"
echo "  Scaler enable log: $SCALER_ENABLE_LOG"
if [[ -n "$METRICS_PID" ]]; then
  echo "  Metrics collector PID: $METRICS_PID"
else
  echo "  Metrics collector: stopped after terminal validation"
fi
echo "  Metrics collector log: $WORK_DIR/metrics_collector.log"
echo "  Kafka input topics: $METRICS_INPUT_TOPICS"
echo ""
echo "To monitor:"
echo "  tail -f $SUB_LOG"
echo "  tail -f $WORK_DIR/metrics_collector.log"
echo "  yarn application -list"
echo ""
echo "To stop:"
echo "  yarn application -kill <APP_ID>"
echo "  kill $SUB_PID"
if [[ -n "$METRICS_PID" ]]; then
  echo "  kill $METRICS_PID"
fi
echo ""
echo "To check Kafka offsets:"
if [[ "$PRODUCER_IMPL" == "holostream" ]]; then
  echo "  ssh $KAFKA_NODE \"$KAFKA_HOME/bin/kafka-get-offsets.sh --bootstrap-server '$KAFKA_BOOTSTRAP' --topic '$HOLOSTREAM_AUCTION_TOPIC' --time -1\""
  echo "  ssh $KAFKA_NODE \"$KAFKA_HOME/bin/kafka-get-offsets.sh --bootstrap-server '$KAFKA_BOOTSTRAP' --topic '$HOLOSTREAM_BID_TOPIC' --time -1\""
else
  echo "  ssh $KAFKA_NODE \"$KAFKA_HOME/bin/kafka-get-offsets.sh --bootstrap-server '$KAFKA_BOOTSTRAP' --topic '$TOPIC' --time -1\""
fi
echo ""
echo "To plot metrics after the test:"
echo "  python3 $SCRIPT_DIR/plot_metrics.py $WORK_DIR"
