#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/../.."

# Q6 step-rate sweep for finding the first autoscaler scale-out threshold.
# Defaults: 100 prefill events, then 5k, 10k, 15k, ... every 20 seconds.

STEP_START_RATE=${STEP_START_RATE:-5000}
STEP_RATE=${STEP_RATE:-5000}
STEP_DURATION_SEC=${STEP_DURATION_SEC:-20}
STEP_NUM_STEPS=${STEP_NUM_STEPS:-20}
PREFILL_EVENTS=${PREFILL_EVENTS:-100}

last_rate=$((STEP_START_RATE + STEP_RATE * (STEP_NUM_STEPS - 1)))
planned_live_events=$((STEP_DURATION_SEC * STEP_NUM_STEPS * (STEP_START_RATE + last_rate) / 2))
total_events=$((PREFILL_EVENTS + planned_live_events))

echo "Q6 step-rate sweep"
echo "  prefill events: $PREFILL_EVENTS"
echo "  start rate:     $STEP_START_RATE ev/s"
echo "  step rate:      +$STEP_RATE ev/s"
echo "  step duration:  $STEP_DURATION_SEC s"
echo "  steps:          $STEP_NUM_STEPS"
echo "  final rate:     $last_rate ev/s"
echo "  live events:    $planned_live_events"
echo "  total events:   $total_events"

QUERY=6 \
EXECUTOR_JSON=${EXECUTOR_JSON:-"$PWD/configs/cloudlab/nemo-yarn-kafka-1source-8slot-4compute-8g-cap3.json"} \
AUTOSCALING=${AUTOSCALING:-true} \
TOTAL_EVENTS=${TOTAL_EVENTS:-$total_events} \
PREFILL_EVENTS=$PREFILL_EVENTS \
BURST_MODE=step \
STEP_START_RATE=$STEP_START_RATE \
STEP_RATE=$STEP_RATE \
STEP_DURATION_SEC=$STEP_DURATION_SEC \
STEP_NUM_STEPS=$STEP_NUM_STEPS \
PRODUCER_RATE_LIMITED=${PRODUCER_RATE_LIMITED:-true} \
RATE_PERIOD_SEC=$STEP_DURATION_SEC \
SUBSCRIBER_WAIT=${SUBSCRIBER_WAIT:-10} \
PARTITION_ASSIGN_BUFFER=${PARTITION_ASSIGN_BUFFER:-90} \
STREAM_TIMEOUT=${STREAM_TIMEOUT:-1800} \
"$SCRIPT_DIR/run_autoscaler_smoke.sh"
