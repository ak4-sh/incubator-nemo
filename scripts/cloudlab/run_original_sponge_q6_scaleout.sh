#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

export HOLOSTREAM_CONFIG=${HOLOSTREAM_CONFIG:-$SCRIPT_DIR/holostream/config/sponge_hardware_matched_q6_scaleout_225k450k.json}
export JOB_ID=${JOB_ID:-original-sponge-q6-scaleout-225k450k-$(date -u +%Y%m%dT%H%M%SZ)}
export WORK_DIR=${WORK_DIR:-/tmp/$JOB_ID}
export STREAM_TIMEOUT=${STREAM_TIMEOUT:-1800}
export PARTITION_ASSIGN_BUFFER=${PARTITION_ASSIGN_BUFFER:-20}

# Sponge's scaler is armed before live input begins. The start-scaler control message is required
# by the unmodified implementation; it does not represent an externally forced scale-out decision.
export AUTOSCALING=true
export OFFLOADING=1
export SCALER_START_MODE=immediate

export OFFLOAD_NODES=${OFFLOAD_NODES:-node4,node6,node7,node8}
export WORKERS_PER_NODE=${WORKERS_PER_NODE:-27}
export NUM_MAX_LAMBDA=${NUM_MAX_LAMBDA:-108}
export LAMBDA_CAPACITY=1
export LAMBDA_SLOT=1
export LAMBDA_MEMORY=1769

exec "$SCRIPT_DIR/run_original_sponge_q6_smoke.sh"
