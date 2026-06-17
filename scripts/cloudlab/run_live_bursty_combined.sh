#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/cloudlab_env.sh"

TOPIC=${TOPIC:-nexmark-live-$(date +%H%M%S)}
TOTAL_EVENTS=${TOTAL_EVENTS:-10000}
PREFILL_EVENTS=${PREFILL_EVENTS:-100}
LIVE_EVENTS=$((TOTAL_EVENTS - PREFILL_EVENTS))
FIRST_RATE=${FIRST_RATE:-1000}
NEXT_RATE=${NEXT_RATE:-50000}
RATE_PERIOD_SEC=${RATE_PERIOD_SEC:-5}
SUBSCRIBER_WAIT=${SUBSCRIBER_WAIT:-10}
EXECUTOR_JSON=${EXECUTOR_JSON:-$NEMO_REPO_ROOT/configs/cloudlab/nemo-yarn-kafka-1source-8compute.json}

if [[ "$LIVE_EVENTS" -lt 1 ]]; then
  echo "TOTAL_EVENTS must be greater than PREFILL_EVENTS" >&2
  exit 1
fi

echo "Creating Kafka topic $TOPIC"
ssh -A "$KAFKA_NODE" "$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server '$KAFKA_BOOTSTRAP' --create --topic '$TOPIC' --partitions 1 --replication-factor 1 || true"
ssh -A "$KAFKA_NODE" "$KAFKA_HOME/bin/kafka-configs.sh --bootstrap-server '$KAFKA_BOOTSTRAP' --entity-type topics --entity-name '$TOPIC' --alter --add-config min.insync.replicas=1"

echo "Prefilling $PREFILL_EVENTS records to avoid Beam empty-topic timestamp crash"
java -cp "$STANDALONE_PRODUCER_CP" StandaloneNexmarkKafkaProducer \
  "$KAFKA_BOOTSTRAP" "$TOPIC" "$PREFILL_EVENTS" "$FIRST_RATE" "$NEXT_RATE" "$RATE_PERIOD_SEC" false

export TOPIC EXECUTOR_JSON NUM_EVENTS=$TOTAL_EVENTS STREAM_TIMEOUT=${STREAM_TIMEOUT:-300}
SUB_LOG=${SUB_LOG:-/tmp/nx-sub-${TOPIC}.log}
export LOG_FILE="$SUB_LOG"

echo "Starting subscriber in background"
"$SCRIPT_DIR/run_q8_subscriber_yarn.sh" &
SUB_PID=$!

sleep "$SUBSCRIBER_WAIT"

echo "Producing $LIVE_EVENTS live bursty records"
java -cp "$STANDALONE_PRODUCER_CP" StandaloneNexmarkKafkaProducer \
  "$KAFKA_BOOTSTRAP" "$TOPIC" "$LIVE_EVENTS" "$FIRST_RATE" "$NEXT_RATE" "$RATE_PERIOD_SEC" true

echo "Final Kafka offset"
ssh -A "$KAFKA_NODE" "$KAFKA_HOME/bin/kafka-get-offsets.sh --bootstrap-server '$KAFKA_BOOTSTRAP' --topic '$TOPIC'"

echo "Subscriber is still running as PID $SUB_PID. Kill the YARN app with yarn application -kill <APP_ID> when done."
echo "Subscriber log: $SUB_LOG"
