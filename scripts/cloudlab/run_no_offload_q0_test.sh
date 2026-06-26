#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/cloudlab_env.sh"

QUERY=${QUERY:-0}
RUN_ID=${RUN_ID:-no-offload-q${QUERY}-$(date +%Y%m%d-%H%M%S)}
TOPIC=${TOPIC:-nexmark-${RUN_ID}}
KAFKA_RESULTS_TOPIC=${KAFKA_RESULTS_TOPIC:-${TOPIC}-results}
KAFKA_ZOOKEEPER=${KAFKA_ZOOKEEPER:-node1:2181,node2:2181,node3:2181}
KAFKA_PARTITIONS=${KAFKA_PARTITIONS:-8}
NUM_EVENTS=${NUM_EVENTS:-1000}
PRODUCER_RATE=${PRODUCER_RATE:-1000}
PRODUCER_PARALLELISM=${PRODUCER_PARALLELISM:-1}
RATE_PERIOD_SEC=${RATE_PERIOD_SEC:-60}
STREAM_TIMEOUT=${STREAM_TIMEOUT:-180}
WAIT_TIMEOUT_SEC=${WAIT_TIMEOUT_SEC:-240}
SINK_TYPE=${SINK_TYPE:-KAFKA}
KAFKA_CONSUMER_GROUP=${KAFKA_CONSUMER_GROUP:-${RUN_ID}-consumer}
EXECUTOR_JSON=${EXECUTOR_JSON:-$NEMO_REPO_ROOT/configs/cloudlab/nemo-yarn-kafka-1source-8slot-8compute.json}
WORK_DIR=${WORK_DIR:-/tmp/nx-${RUN_ID}}
LOG_FILE=${LOG_FILE:-/tmp/nx-sub-${RUN_ID}.log}
JOB_ID=${JOB_ID:-nx-q${QUERY}-${RUN_ID}}

sum_topic_offsets() {
  local topic=$1
  ssh "$KAFKA_NODE" \
    "$KAFKA_HOME/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list '$KAFKA_BOOTSTRAP' --topic '$topic' --time -1" \
    2>/dev/null | awk -F: '{sum += $3} END {print sum + 0}'
}

wait_for_app_id() {
  local deadline=$((SECONDS + 120))
  while [[ $SECONDS -lt $deadline ]]; do
    if [[ -f "$LOG_FILE" ]]; then
      local app_id
      app_id=$(grep -o 'application_[0-9]\+_[0-9]\+' "$LOG_FILE" | head -1 || true)
      if [[ -n "$app_id" ]]; then
        printf '%s\n' "$app_id"
        return 0
      fi
    fi
    sleep 2
  done
  return 1
}

wait_for_app_running() {
  local app_id=$1
  local deadline=$((SECONDS + 120))
  while [[ $SECONDS -lt $deadline ]]; do
    local state
    state=$($HADOOP_HOME/bin/yarn application -status "$app_id" 2>/dev/null | awk -F: '/State/ {gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2; exit}' || true)
    if [[ "$state" == "RUNNING" ]]; then
      return 0
    fi
    if [[ "$state" == "FAILED" || "$state" == "KILLED" || "$state" == "FINISHED" ]]; then
      echo "YARN app reached terminal state before test input: $state" >&2
      return 1
    fi
    sleep 2
  done
  return 1
}

cleanup_app() {
  if [[ -n "${APP_ID:-}" ]]; then
    $HADOOP_HOME/bin/yarn application -kill "$APP_ID" >/dev/null 2>&1 || true
  fi
}
trap cleanup_app EXIT

echo "== No-offload Q${QUERY} functional test =="
echo "  run id:        $RUN_ID"
echo "  topic:         $TOPIC"
echo "  result topic:  $KAFKA_RESULTS_TOPIC"
echo "  events:        $NUM_EVENTS"
echo "  executor json: $EXECUTOR_JSON"
echo "  log:           $LOG_FILE"

if [[ ! -f "$STANDALONE_PRODUCER_OUT/StandaloneNexmarkKafkaProducer.class" ]]; then
  echo "Compiling standalone producer"
  JAVA_HOME="$JAVA_HOME" bash "$SCRIPT_DIR/compile_standalone_producer.sh"
fi

mkdir -p "$WORK_DIR"
rm -f "$LOG_FILE" "$WORK_DIR/source.log" "$WORK_DIR/producer_metrics.csv"

echo "Creating Kafka topics"
ssh "$KAFKA_NODE" \
  "$KAFKA_HOME/bin/kafka-topics.sh --zookeeper '$KAFKA_ZOOKEEPER' --create --topic '$TOPIC' --partitions $KAFKA_PARTITIONS --replication-factor 1 || true"
ssh "$KAFKA_NODE" \
  "$KAFKA_HOME/bin/kafka-topics.sh --zookeeper '$KAFKA_ZOOKEEPER' --create --topic '$KAFKA_RESULTS_TOPIC' --partitions $KAFKA_PARTITIONS --replication-factor 1 || true"

echo "Launching Nemo subscriber with OFFLOADING=0"
QUERY=$QUERY \
TOPIC=$TOPIC \
SINK_TYPE=$SINK_TYPE \
KAFKA_RESULTS_TOPIC=$KAFKA_RESULTS_TOPIC \
KAFKA_CONSUMER_GROUP=$KAFKA_CONSUMER_GROUP \
EXECUTOR_JSON=$EXECUTOR_JSON \
STREAM_TIMEOUT=$STREAM_TIMEOUT \
NUM_EVENTS=$NUM_EVENTS \
CPU_DELAY_MS=0 \
OFFLOADING=0 \
NUM_MAX_LAMBDA=0 \
NEMO_WORK_DIR=$WORK_DIR \
JOB_ID=$JOB_ID \
LOG_FILE=$LOG_FILE \
bash "$SCRIPT_DIR/run_q8_subscriber_yarn.sh"

APP_ID=$(wait_for_app_id)
echo "YARN app: $APP_ID"
wait_for_app_running "$APP_ID"

echo "Producing $NUM_EVENTS events"
java -Dnemo.work.dir="$WORK_DIR" -cp "$STANDALONE_PRODUCER_CP" StandaloneNexmarkKafkaProducer \
  "$KAFKA_BOOTSTRAP" "$TOPIC" "$NUM_EVENTS" "$PRODUCER_RATE" "$PRODUCER_RATE" "$RATE_PERIOD_SEC" true "$PRODUCER_PARALLELISM"

echo "Waiting for result topic to reach $NUM_EVENTS records"
deadline=$((SECONDS + WAIT_TIMEOUT_SEC))
while [[ $SECONDS -lt $deadline ]]; do
  input_total=$(sum_topic_offsets "$TOPIC")
  result_total=$(sum_topic_offsets "$KAFKA_RESULTS_TOPIC")
  echo "  input=$input_total result=$result_total"
  if [[ "$result_total" -ge "$NUM_EVENTS" ]]; then
    echo "SUCCESS: result topic reached $result_total records"
    exit 0
  fi
  sleep 5
done

echo "FAILURE: timed out waiting for result topic" >&2
echo "  final input:  $(sum_topic_offsets "$TOPIC")" >&2
echo "  final result: $(sum_topic_offsets "$KAFKA_RESULTS_TOPIC")" >&2
echo "  subscriber log: $LOG_FILE" >&2
exit 1
