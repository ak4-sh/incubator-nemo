#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Run Nemo Sponge Nexmark Query 8 with Kafka input and collect Kafka lag metrics.

Usage:
  scripts/run_nexmark_q8_kafka_with_metrics.sh [result_dir]

Common environment overrides:
  BOOTSTRAP_SERVERS       Kafka bootstrap servers. Default: node1:9092,node2:9092,node3:9092
  CONSUMER_GROUP          Kafka consumer group to describe. Default: __ALL__
  KAFKA_TOPICS            CSV topic list for end-offset sampling. Default: __ALL__
  KAFKA_LAG_INTERVAL      Lag sampling interval seconds. Default: 1
  DEPLOY_MODE             Nemo deploy mode. Default: yarn
  EXECUTOR_JSON           Nemo executor JSON. Default: examples/resources/1.json
  TIMEOUT                 Nexmark stream timeout seconds. Default: 450
  SOURCE_PARALLELISM      Nemo source parallelism. Default: 10
  WINDOW                  Nexmark window size seconds. Default: 10
  INTERVAL                Nexmark window period seconds. Default: 1
  CPU_DELAY               Nexmark cpuDelayMs. Default: 1000
  SAMPLING                Nexmark samplingRate. Default: 0.1
  ENABLE_OFFLOADING       Sponge offloading toggle. Default: false
  ENABLE_OFFLOADING_DEBUG Sponge offloading debug toggle. Default: false
  POOL_SIZE               lambda_warmup_pool. Default: 0
  FLUSH_BYTES             Sponge flush_bytes. Default: 10485760
  FLUSH_COUNT             Sponge flush_count. Default: 10000
  FLUSH_PERIOD            Sponge flush_period ms. Default: 1000
  MIN_VM_TASK             Sponge min_vm_task. Default: 1
  EC2                     Nemo ec2 flag. Default: true
  EXCLUDE_JARS            Nemo exclude_jars value.
  KAFKA_HOME              Optional Kafka installation for collector tools.
  BUILD_IF_MISSING        Run Maven when required jars are missing. Default: false

Outputs:
  <result_dir>/nemo_q8_kafka.log
  <result_dir>/kafka_lag.csv
  <result_dir>/metric.log
  <result_dir>/run.env
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ ! -f pom.xml || ! -x bin/run_nexmark.sh ]]; then
  echo "Run this script from the incubator-nemo repository root." >&2
  exit 1
fi

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
if [[ "$CURRENT_BRANCH" != "sponge" ]]; then
  echo "Expected incubator-nemo to be on branch 'sponge', found '${CURRENT_BRANCH:-unknown}'." >&2
  exit 1
fi

RESULT_DIR="${1:-results/nexmark-q8-kafka-$(date +%Y%m%d-%H%M%S)}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-node1:9092,node2:9092,node3:9092}"
CONSUMER_GROUP="${CONSUMER_GROUP:-__ALL__}"
KAFKA_TOPICS="${KAFKA_TOPICS:-__ALL__}"
KAFKA_LAG_INTERVAL="${KAFKA_LAG_INTERVAL:-1}"

DEPLOY_MODE="${DEPLOY_MODE:-yarn}"
EXECUTOR_JSON="${EXECUTOR_JSON:-$(pwd)/examples/resources/1.json}"
TIMEOUT="${TIMEOUT:-450}"
SOURCE_PARALLELISM="${SOURCE_PARALLELISM:-10}"
WINDOW="${WINDOW:-10}"
INTERVAL="${INTERVAL:-1}"
CPU_DELAY="${CPU_DELAY:-1000}"
SAMPLING="${SAMPLING:-0.1}"

ENABLE_OFFLOADING="${ENABLE_OFFLOADING:-false}"
ENABLE_OFFLOADING_DEBUG="${ENABLE_OFFLOADING_DEBUG:-false}"
POOL_SIZE="${POOL_SIZE:-0}"
FLUSH_BYTES="${FLUSH_BYTES:-10485760}"
FLUSH_COUNT="${FLUSH_COUNT:-10000}"
FLUSH_PERIOD="${FLUSH_PERIOD:-1000}"
MIN_VM_TASK="${MIN_VM_TASK:-1}"
EC2="${EC2:-true}"
EXCLUDE_JARS="${EXCLUDE_JARS:-httpclient-4.2.5:httpcore-4.2.5:netty-:avro-}"
BUILD_IF_MISSING="${BUILD_IF_MISSING:-false}"

find_required_artifact() {
  local dir="$1"
  local pattern="$2"
  if [[ ! -d "$dir" ]]; then
    return 0
  fi
  find "$dir" -maxdepth 1 -type f -name "$pattern" 2>/dev/null | head -n 1 || true
}

NEXMARK_JAR="$(find_required_artifact examples/nexmark/target 'nexmark-*-shaded.jar')"
CLIENT_JAR="$(find_required_artifact client/target 'nemo-client-*-shaded.jar')"
LAMBDA_JAR="$(find_required_artifact offloading/workers/lambda/target 'offloading-lambda-*.jar')"

if [[ -z "$NEXMARK_JAR" || -z "$CLIENT_JAR" || -z "$LAMBDA_JAR" ]]; then
  if [[ "$BUILD_IF_MISSING" == "true" ]]; then
    echo "Missing Nemo artifacts; running Maven build."
    mvn clean install -DskipTests -T 2C
  else
    echo "Missing built Nemo artifacts. Build first, or rerun with BUILD_IF_MISSING=true." >&2
    echo "Expected:" >&2
    echo "  examples/nexmark/target/nexmark-*-shaded.jar" >&2
    echo "  client/target/nemo-client-*-shaded.jar" >&2
    echo "  offloading/workers/lambda/target/offloading-lambda-*.jar" >&2
    exit 1
  fi
fi

mkdir -p "$RESULT_DIR"

cat > "$RESULT_DIR/run.env" <<EOF
branch=$CURRENT_BRANCH
bootstrap_servers=$BOOTSTRAP_SERVERS
consumer_group=$CONSUMER_GROUP
kafka_topics=$KAFKA_TOPICS
kafka_lag_interval=$KAFKA_LAG_INTERVAL
deploy_mode=$DEPLOY_MODE
executor_json=$EXECUTOR_JSON
timeout=$TIMEOUT
source_parallelism=$SOURCE_PARALLELISM
window=$WINDOW
interval=$INTERVAL
cpu_delay=$CPU_DELAY
sampling=$SAMPLING
enable_offloading=$ENABLE_OFFLOADING
enable_offloading_debug=$ENABLE_OFFLOADING_DEBUG
pool_size=$POOL_SIZE
flush_bytes=$FLUSH_BYTES
flush_count=$FLUSH_COUNT
flush_period=$FLUSH_PERIOD
min_vm_task=$MIN_VM_TASK
ec2=$EC2
exclude_jars=$EXCLUDE_JARS
build_if_missing=$BUILD_IF_MISSING
EOF

LAG_CSV="$RESULT_DIR/kafka_lag.csv"
NEMO_LOG="$RESULT_DIR/nemo_q8_kafka.log"

echo "Starting Kafka lag collector -> $LAG_CSV"
scripts/collect_kafka_lag.sh \
  "$BOOTSTRAP_SERVERS" \
  "$LAG_CSV" \
  "$KAFKA_LAG_INTERVAL" \
  "$CONSUMER_GROUP" \
  "$KAFKA_TOPICS" &
LAG_PID="$!"

cleanup() {
  if kill -0 "$LAG_PID" >/dev/null 2>&1; then
    kill "$LAG_PID" >/dev/null 2>&1 || true
    wait "$LAG_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

rm -f metric.log

echo "Running Nemo Nexmark Query 8 from Kafka on Sponge branch"
set +e
./bin/run_nexmark.sh \
  -ec2 "$EC2" \
  -job_id nexmark-Q8-kafka-sponge \
  -deploy_mode "$DEPLOY_MODE" \
  -executor_json "$EXECUTOR_JSON" \
  -user_main org.apache.beam.sdk.nexmark.Main \
  -optimization_policy org.apache.nemo.compiler.optimizer.policy.StreamingPolicy \
  -scheduler_impl_class_name org.apache.nemo.runtime.master.scheduler.StreamingScheduler \
  -enable_offloading "$ENABLE_OFFLOADING" \
  -enable_offloading_debug "$ENABLE_OFFLOADING_DEBUG" \
  -lambda_warmup_pool "$POOL_SIZE" \
  -flush_bytes "$FLUSH_BYTES" \
  -flush_count "$FLUSH_COUNT" \
  -flush_period "$FLUSH_PERIOD" \
  -exclude_jars "$EXCLUDE_JARS" \
  -source_parallelism "$SOURCE_PARALLELISM" \
  -is_local_source false \
  -min_vm_task "$MIN_VM_TASK" \
  -user_args "--runner=org.apache.nemo.client.beam.NemoRunner --streaming=true --query=8 --manageResources=false --monitorJobs=true --streamTimeout=${TIMEOUT} --isRateLimited=true --windowSizeSec=${WINDOW} --windowPeriodSec=${INTERVAL} --fanout=1 --cpuDelayMs=${CPU_DELAY} --samplingRate=${SAMPLING} --sourceType=KAFKA --pubSubMode=SUBSCRIBE_ONLY --bootstrapServers=${BOOTSTRAP_SERVERS}" \
  2>&1 | tee "$NEMO_LOG"
NEMO_EXIT="${PIPESTATUS[0]}"
set -e

cleanup
trap - EXIT

if [[ -f metric.log ]]; then
  cp metric.log "$RESULT_DIR/metric.log"
fi

echo "Nemo exit code: $NEMO_EXIT"
echo "Results: $RESULT_DIR"
exit "$NEMO_EXIT"
