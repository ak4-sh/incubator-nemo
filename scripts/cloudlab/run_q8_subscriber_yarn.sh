#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/cloudlab_env.sh"

QUERY=${QUERY:-8}
TOPIC=${TOPIC:?Set TOPIC to a Kafka topic}
SINK_TYPE=${SINK_TYPE:-COUNT_ONLY}
KAFKA_RESULTS_TOPIC=${KAFKA_RESULTS_TOPIC:-}
JOB_ID=${JOB_ID:-nx-q${QUERY}-${TOPIC}-$(date +%H%M%S)}
KAFKA_CONSUMER_GROUP=${KAFKA_CONSUMER_GROUP:-disaggregated-streaming}
EXECUTOR_JSON=${EXECUTOR_JSON:-$NEMO_REPO_ROOT/configs/cloudlab/nemo-yarn-kafka-1source-8slot-12compute.json}
STREAM_TIMEOUT=${STREAM_TIMEOUT:-240}
NUM_EVENTS=${NUM_EVENTS:-100000}
KAFKA_INPUT_FORMAT=${KAFKA_INPUT_FORMAT:-BEAM_EVENT}
HOLOSTREAM_AUCTION_TOPIC=${HOLOSTREAM_AUCTION_TOPIC:-nexmark-auction}
HOLOSTREAM_BID_TOPIC=${HOLOSTREAM_BID_TOPIC:-nexmark-bid}
HOLOSTREAM_AUCTION_EVENTS=${HOLOSTREAM_AUCTION_EVENTS:-0}
HOLOSTREAM_BID_EVENTS=${HOLOSTREAM_BID_EVENTS:-0}
CPU_DELAY_MS=${CPU_DELAY_MS:-0}
EC2=${EC2:-false}
OFFLOADING=${OFFLOADING:-0}
AUTOSCALING=${AUTOSCALING:-false}
OPTIMIZATION_POLICY=${OPTIMIZATION_POLICY:-org.apache.nemo.compiler.optimizer.policy.StreamingPolicy}
LATENCY_LIMIT=${LATENCY_LIMIT:-300000}
NUM_MAX_LAMBDA=${NUM_MAX_LAMBDA:-4}
LOG_FILE=${LOG_FILE:-/tmp/${JOB_ID}.log}
SOURCE_HOSTS=${SOURCE_HOSTS:-}
COMPUTE_HOSTS=${COMPUTE_HOSTS:-}
STRICT_EXECUTOR_PLACEMENT=${STRICT_EXECUTOR_PLACEMENT:-false}
EXECUTOR_PLACEMENT_REPORT=${EXECUTOR_PLACEMENT_REPORT:-${NEMO_WORK_DIR:-/tmp}/executor_placement.csv}

EXTRA_ARGS=()
if [[ "$OFFLOADING" == "1" ]]; then
  EXTRA_ARGS+=(
    -enable_offloading true
    -offloading_type cloudlab-vm
    -num_max_lambda "$NUM_MAX_LAMBDA"
    -autoscaling "$AUTOSCALING"
    -latency_limit "$LATENCY_LIMIT"
  )
fi

echo "Launching Q${QUERY} subscriber"
echo "  topic=$TOPIC"
echo "  sink=$SINK_TYPE"
echo "  job_id=$JOB_ID"
echo "  kafka_consumer_group=$KAFKA_CONSUMER_GROUP"
echo "  kafka_input_format=$KAFKA_INPUT_FORMAT"
echo "  executor_json=$EXECUTOR_JSON"
echo "  ec2_cpu_normalization=$EC2"
echo "  offloading=$OFFLOADING"
echo "  autoscaling=$AUTOSCALING"
echo "  optimization_policy=$OPTIMIZATION_POLICY"
echo "  source_hosts=${SOURCE_HOSTS:-<unset>}"
echo "  compute_hosts=${COMPUTE_HOSTS:-<unset>}"
echo "  strict_executor_placement=$STRICT_EXECUTOR_PLACEMENT"
echo "  executor_placement_report=$EXECUTOR_PLACEMENT_REPORT"
echo "  log=$LOG_FILE"

SINK_ARGS="--sinkType=${SINK_TYPE}"
if [[ "$SINK_TYPE" == "KAFKA" && -n "$KAFKA_RESULTS_TOPIC" ]]; then
  SINK_ARGS="${SINK_ARGS} --kafkaResultsTopic=${KAFKA_RESULTS_TOPIC}"
fi

nohup java -Dnemo.work.dir="${NEMO_WORK_DIR:-/tmp}" -Dnemo.job.id="$JOB_ID" -cp "$NEMO_CLIENT_CP" org.apache.nemo.client.JobLauncher \
  -job_id "$JOB_ID" \
  -user_main org.apache.beam.sdk.nexmark.Main \
  -deploy_mode yarn \
  -executor_json "$EXECUTOR_JSON" \
  -source_hosts "$SOURCE_HOSTS" \
  -compute_hosts "$COMPUTE_HOSTS" \
  -strict_executor_placement "$STRICT_EXECUTOR_PLACEMENT" \
  -executor_placement_report "$EXECUTOR_PLACEMENT_REPORT" \
  -ec2 "$EC2" \
  -optimization_policy "$OPTIMIZATION_POLICY" \
  -scheduler_impl_class_name org.apache.nemo.runtime.master.scheduler.StreamingScheduler \
  "${EXTRA_ARGS[@]}" \
  -user_args "--runner=org.apache.nemo.client.beam.NemoRunner --streaming=true --query=${QUERY} --manageResources=false --monitorJobs=true --streamTimeout=${STREAM_TIMEOUT} --isRateLimited=false --windowSizeSec=10 --windowPeriodSec=1 --fanout=1 --cpuDelayMs=${CPU_DELAY_MS} --samplingRate=1.0 --numEvents=${NUM_EVENTS} --sourceType=KAFKA --pubSubMode=SUBSCRIBE_ONLY --bootstrapServers=${KAFKA_BOOTSTRAP} --kafkaTopic=${TOPIC} --kafkaConsumerGroup=${KAFKA_CONSUMER_GROUP} --kafkaInputFormat=${KAFKA_INPUT_FORMAT} --holoStreamAuctionTopic=${HOLOSTREAM_AUCTION_TOPIC} --holoStreamBidTopic=${HOLOSTREAM_BID_TOPIC} ${SINK_ARGS} --jobName=${JOB_ID}" \
  > "$LOG_FILE" 2>&1 &

SUB_PID=$!
echo "$SUB_PID" > "/tmp/nemo-subscriber-${TOPIC}.pid"
echo "Subscriber launched in background (PID: $SUB_PID, log: $LOG_FILE)"
