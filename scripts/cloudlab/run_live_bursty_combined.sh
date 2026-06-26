#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/cloudlab_env.sh"

TOPIC=${TOPIC:-nexmark-live-$(date +%H%M%S)}

# Burst mode configuration (default: custom sustained bursts)
BURST_MODE=${BURST_MODE:-custom}
FIRST_RATE=${FIRST_RATE:-50000}
NEXT_RATE=${NEXT_RATE:-200000}
STEADY_DURATION_SEC=${STEADY_DURATION_SEC:-60}
BURST_DURATION_SEC=${BURST_DURATION_SEC:-45}
NUM_BURSTS=${NUM_BURSTS:-3}
RAMP_UP_SEC=${RAMP_UP_SEC:-60}

# Legacy mode support
TOTAL_EVENTS=${TOTAL_EVENTS:-10000}
PREFILL_EVENTS=${PREFILL_EVENTS:-100}
LIVE_EVENTS=$((TOTAL_EVENTS - PREFILL_EVENTS))
RATE_PERIOD_SEC=${RATE_PERIOD_SEC:-50}
SUBSCRIBER_WAIT=${SUBSCRIBER_WAIT:-10}
EXECUTOR_JSON=${EXECUTOR_JSON:-$NEMO_REPO_ROOT/configs/cloudlab/nemo-yarn-kafka-1source-8slot-8compute.json}
KAFKA_ZOOKEEPER=${KAFKA_ZOOKEEPER:-node1:2181,node2:2181,node3:2181}
KAFKA_PARTITIONS=${KAFKA_PARTITIONS:-8}
PRODUCER_PARALLELISM=${PRODUCER_PARALLELISM:-8}

if [[ "$BURST_MODE" == "legacy" && "$LIVE_EVENTS" -lt 1 ]]; then
  echo "TOTAL_EVENTS must be greater than PREFILL_EVENTS" >&2
  exit 1
fi

echo "Creating Kafka topic $TOPIC with $KAFKA_PARTITIONS partitions"
ssh "$KAFKA_NODE" "$KAFKA_HOME/bin/kafka-topics.sh --zookeeper '$KAFKA_ZOOKEEPER' --create --topic '$TOPIC' --partitions $KAFKA_PARTITIONS --replication-factor 1 || true"
ssh "$KAFKA_NODE" "$KAFKA_HOME/bin/kafka-configs.sh --zookeeper '$KAFKA_ZOOKEEPER' --entity-type topics --entity-name '$TOPIC' --alter --add-config min.insync.replicas=1"

echo "Prefilling $PREFILL_EVENTS records (parallelism=1)"
if [[ "$BURST_MODE" == "custom" ]]; then
  java -cp "$STANDALONE_PRODUCER_CP" StandaloneNexmarkKafkaProducer \
    "$KAFKA_BOOTSTRAP" "$TOPIC" "$FIRST_RATE" "$NEXT_RATE" "$STEADY_DURATION_SEC" "$BURST_DURATION_SEC" "$NUM_BURSTS" false "1" "$RAMP_UP_SEC"
else
  java -cp "$STANDALONE_PRODUCER_CP" StandaloneNexmarkKafkaProducer \
    "$KAFKA_BOOTSTRAP" "$TOPIC" "$PREFILL_EVENTS" "$FIRST_RATE" "$NEXT_RATE" "$RATE_PERIOD_SEC" false "1"
fi

export TOPIC EXECUTOR_JSON NUM_EVENTS=$TOTAL_EVENTS STREAM_TIMEOUT=${STREAM_TIMEOUT:-900}
SUB_LOG=${SUB_LOG:-/tmp/nx-sub-${TOPIC}.log}
export LOG_FILE="$SUB_LOG"

echo "Starting subscriber in background"
"$SCRIPT_DIR/run_q8_subscriber_yarn.sh" &
SUB_PID=$!

sleep "$SUBSCRIBER_WAIT"

echo "Producing live bursty records (parallelism=$PRODUCER_PARALLELISM)"
if [[ "$BURST_MODE" == "custom" ]]; then
  java -cp "$STANDALONE_PRODUCER_CP" StandaloneNexmarkKafkaProducer \
    "$KAFKA_BOOTSTRAP" "$TOPIC" "$FIRST_RATE" "$NEXT_RATE" "$STEADY_DURATION_SEC" "$BURST_DURATION_SEC" "$NUM_BURSTS" true "$PRODUCER_PARALLELISM" "$RAMP_UP_SEC"
else
  java -cp "$STANDALONE_PRODUCER_CP" StandaloneNexmarkKafkaProducer \
    "$KAFKA_BOOTSTRAP" "$TOPIC" "$LIVE_EVENTS" "$FIRST_RATE" "$NEXT_RATE" "$RATE_PERIOD_SEC" true "$PRODUCER_PARALLELISM"
fi

echo "Final Kafka offset"
ssh "$KAFKA_NODE" "$KAFKA_HOME/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list '$KAFKA_BOOTSTRAP' --topic '$TOPIC' --time -1"

echo "Subscriber is still running as PID $SUB_PID. Kill the YARN app with yarn application -kill <APP_ID> when done."
echo "Subscriber log: $SUB_LOG"
