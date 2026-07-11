#!/usr/bin/env bash
set -euo pipefail
cd ~/incubator-nemo

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RUN_ID="nexmark-q6-step-${TIMESTAMP}" \
QUERY=6 \
BENCHMARK_NAME="nexmark-q6-step" \
TOTAL_EVENTS=23850000 \
NUM_EVENTS=23850000 \
PREFILL_EVENTS=0 \
FIRST_RATE=50000 \
NEXT_RATE=100000 \
RATE_PERIOD_SEC=450 \
RAMP_UP_SEC=120 \
STEADY_DURATION_SEC=0 \
BURST_DURATION_SEC=400 \
NUM_BURSTS=1 \
PRODUCER_RATE_LIMITED=true \
BURST_MODE=custom \
COMPLETION_MODE=queue_drained \
EXECUTOR_JSON=configs/cloudlab/nemo-yarn-kafka-1source-8slot-12compute.json \
AUTOSCALING=true \
CPU_DELAY_MS=2 \
BENCHMARK_TIMEOUT_SEC=3600 \
STREAM_TIMEOUT=3600 \
KEEP_WARM_POOL=1 \
PLOT=1 \
scripts/cloudlab/run_sponge_q0_benchmark.sh
