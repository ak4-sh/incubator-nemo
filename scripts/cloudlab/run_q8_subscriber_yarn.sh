#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/cloudlab_env.sh"

TOPIC=${TOPIC:?Set TOPIC to a Kafka topic}
JOB_ID=${JOB_ID:-nx-q8-${TOPIC}-$(date +%H%M%S)}
EXECUTOR_JSON=${EXECUTOR_JSON:-$NEMO_REPO_ROOT/configs/cloudlab/nemo-yarn-kafka-1source-8compute.json}
STREAM_TIMEOUT=${STREAM_TIMEOUT:-240}
NUM_EVENTS=${NUM_EVENTS:-100000}
CPU_DELAY_MS=${CPU_DELAY_MS:-0}
OFFLOADING=${OFFLOADING:-0}
NUM_MAX_LAMBDA=${NUM_MAX_LAMBDA:-4}
LOG_FILE=${LOG_FILE:-/tmp/${JOB_ID}.log}

EXTRA_ARGS=()
if [[ "$OFFLOADING" == "1" ]]; then
  EXTRA_ARGS+=(
    -enable_offloading true
    -offloading_type cloudlab-vm
    -num_max_lambda "$NUM_MAX_LAMBDA"
  )
fi

echo "Launching Q8 subscriber"
echo "  topic=$TOPIC"
echo "  job_id=$JOB_ID"
echo "  executor_json=$EXECUTOR_JSON"
echo "  offloading=$OFFLOADING"
echo "  log=$LOG_FILE"

java -cp "$NEMO_CLIENT_CP" org.apache.nemo.client.JobLauncher \
  -job_id "$JOB_ID" \
  -user_main org.apache.beam.sdk.nexmark.Main \
  -deploy_mode yarn \
  -executor_json "$EXECUTOR_JSON" \
  -optimization_policy org.apache.nemo.compiler.optimizer.policy.DefaultPolicy \
  "${EXTRA_ARGS[@]}" \
  -user_args "--runner=org.apache.nemo.client.beam.NemoRunner --streaming=true --query=8 --manageResources=false --monitorJobs=true --streamTimeout=${STREAM_TIMEOUT} --isRateLimited=false --windowSizeSec=10 --windowPeriodSec=1 --fanout=1 --cpuDelayMs=${CPU_DELAY_MS} --samplingRate=1.0 --numEvents=${NUM_EVENTS} --sourceType=KAFKA --pubSubMode=SUBSCRIBE_ONLY --bootstrapServers=${KAFKA_BOOTSTRAP} --kafkaTopic=${TOPIC} --sinkType=COUNT_ONLY --jobName=${JOB_ID}" \
  2>&1 | tee "$LOG_FILE"
