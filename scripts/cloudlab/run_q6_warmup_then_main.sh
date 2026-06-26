#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/cloudlab_env.sh"

RUN_PREFIX=${RUN_PREFIX:-nexmark-q6}
TIMESTAMP=${TIMESTAMP:-$(date +%Y%m%d-%H%M%S)}

# Holostream query6ModWarmup10GB analogue: 2.5M closed-auction events,
# unrate-limited producer, no Kafka prefill before deployment.
WARMUP_EVENTS=${WARMUP_EVENTS:-2500000}
WARMUP_FIRST_RATE=${WARMUP_FIRST_RATE:-20000}
WARMUP_NEXT_RATE=${WARMUP_NEXT_RATE:-20000}
WARMUP_RATE_PERIOD_SEC=${WARMUP_RATE_PERIOD_SEC:-1500}
WARMUP_BURST_MODE=${WARMUP_BURST_MODE:-legacy}
WARMUP_TIMEOUT_SEC=${WARMUP_TIMEOUT_SEC:-7200}
WARMUP_RUN_ID=${WARMUP_RUN_ID:-${RUN_PREFIX}-warmup-${TIMESTAMP}}

MAIN_EVENTS=${MAIN_EVENTS:-23850000}
MAIN_FIRST_RATE=${MAIN_FIRST_RATE:-50000}
MAIN_NEXT_RATE=${MAIN_NEXT_RATE:-200000}
MAIN_RATE_PERIOD_SEC=${MAIN_RATE_PERIOD_SEC:-450}
MAIN_BURST_MODE=${MAIN_BURST_MODE:-custom}
MAIN_STEADY_DURATION_SEC=${MAIN_STEADY_DURATION_SEC:-60}
MAIN_BURST_DURATION_SEC=${MAIN_BURST_DURATION_SEC:-45}
MAIN_NUM_BURSTS=${MAIN_NUM_BURSTS:-3}
MAIN_RAMP_UP_SEC=${MAIN_RAMP_UP_SEC:-60}
MAIN_TIMEOUT_SEC=${MAIN_TIMEOUT_SEC:-1800}
MAIN_RUN_ID=${MAIN_RUN_ID:-${RUN_PREFIX}-main-${TIMESTAMP}}

EXECUTOR_JSON=${EXECUTOR_JSON:-configs/cloudlab/nemo-yarn-kafka-1source-8slot-12compute.json}
COMPLETION_MODE=${COMPLETION_MODE:-source_plus_some_output}
AUTOSCALING=${AUTOSCALING:-false}
PREFILL_EVENTS=${PREFILL_EVENTS:-0}
CPU_DELAY_MS=${CPU_DELAY_MS:-2}
KEEP_WARM_POOL=${KEEP_WARM_POOL:-1}

run_phase() {
  local run_id=$1
  local benchmark_name=$2
  local total_events=$3
  local first_rate=$4
  local next_rate=$5
  local rate_period_sec=$6
  local timeout_sec=$7
  local producer_rate_limited=$8
  local burst_mode=$9
  local plot=${10}

  echo "== Running $benchmark_name =="
  echo "  run_id=$run_id"
  echo "  events=$total_events"
  echo "  producer_rate_limited=$producer_rate_limited"

  RUN_ID="$run_id" \
  QUERY=6 \
  BENCHMARK_NAME="$benchmark_name" \
  TOTAL_EVENTS="$total_events" \
  NUM_EVENTS="$total_events" \
  PREFILL_EVENTS="$PREFILL_EVENTS" \
  BURST_MODE=legacy \
  FIRST_RATE="$first_rate" \
  NEXT_RATE="$next_rate" \
  RATE_PERIOD_SEC="$rate_period_sec" \
  STEADY_DURATION_SEC="$MAIN_STEADY_DURATION_SEC" \
  BURST_DURATION_SEC="$MAIN_BURST_DURATION_SEC" \
  NUM_BURSTS="$MAIN_NUM_BURSTS" \
  RAMP_UP_SEC="$MAIN_RAMP_UP_SEC" \
  PRODUCER_RATE_LIMITED="$producer_rate_limited" \
  BURST_MODE="$burst_mode" \
  COMPLETION_MODE="$COMPLETION_MODE" \
  EXECUTOR_JSON="$EXECUTOR_JSON" \
  AUTOSCALING="$AUTOSCALING" \
  CPU_DELAY_MS="$CPU_DELAY_MS" \
  BENCHMARK_TIMEOUT_SEC="$timeout_sec" \
  STREAM_TIMEOUT="$timeout_sec" \
  KEEP_WARM_POOL="$KEEP_WARM_POOL" \
  PLOT="$plot" \
  "$SCRIPT_DIR/run_sponge_q0_benchmark.sh"
}

run_phase "$WARMUP_RUN_ID" "nexmark-q6-warmup" "$WARMUP_EVENTS" \
  "$WARMUP_FIRST_RATE" "$WARMUP_NEXT_RATE" "$WARMUP_RATE_PERIOD_SEC" \
  "$WARMUP_TIMEOUT_SEC" false "$WARMUP_BURST_MODE" 0

run_phase "$MAIN_RUN_ID" "nexmark-q6-main" "$MAIN_EVENTS" \
  "$MAIN_FIRST_RATE" "$MAIN_NEXT_RATE" "$MAIN_RATE_PERIOD_SEC" \
  "$MAIN_TIMEOUT_SEC" true "$MAIN_BURST_MODE" 1

echo "Q6 warmup and main run completed."
echo "  Warmup artifacts: $NEMO_REPO_ROOT/results/cloudlab/$WARMUP_RUN_ID"
echo "  Main artifacts:   $NEMO_REPO_ROOT/results/cloudlab/$MAIN_RUN_ID"
