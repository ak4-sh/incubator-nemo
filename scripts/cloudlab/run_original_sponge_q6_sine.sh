#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

export HOLOSTREAM_CONFIG=${HOLOSTREAM_CONFIG:-$SCRIPT_DIR/holostream/config/sponge_hardware_matched_q6_sine_60k450k.json}
export JOB_ID=${JOB_ID:-original-sponge-q6-sine-60k450k-$(date -u +%Y%m%dT%H%M%SZ)}
export WORK_DIR=${WORK_DIR:-/tmp/$JOB_ID}
export STREAM_TIMEOUT=${STREAM_TIMEOUT:-1800}
export PARTITION_ASSIGN_BUFFER=${PARTITION_ASSIGN_BUFFER:-20}
# The extended trace runs for 930 seconds. Leave enough headroom for remote
# producer staging and orderly completion validation.
export HOLOSTREAM_PRODUCER_TIMEOUT=${HOLOSTREAM_PRODUCER_TIMEOUT:-1200}

# Arm the unmodified Sponge scaler before live production starts. This enables native policy
# decisions; it does not externally force scale-out or scale-in at a workload phase boundary.
export AUTOSCALING=true
export OFFLOADING=1
export SAFE_WORKER_REACTIVATION=${SAFE_WORKER_REACTIVATION:-false}
export SCALER_START_MODE=immediate

# Four physical scale-out hosts, each capped at 27 one-core VM workers.
export OFFLOAD_NODES=${OFFLOAD_NODES:-node4,node6,node7,node8}
export WORKERS_PER_NODE=${WORKERS_PER_NODE:-27}
export NUM_MAX_LAMBDA=${NUM_MAX_LAMBDA:-108}
export LAMBDA_CAPACITY=1
export LAMBDA_SLOT=1
export LAMBDA_MEMORY=1769

exec "$SCRIPT_DIR/run_original_sponge_q6_smoke.sh"
