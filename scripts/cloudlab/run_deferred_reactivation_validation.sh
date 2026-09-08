#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# Reproduce the two-scale-out workload that exposed the DEACTIVATING/activate race.
export HOLOSTREAM_CONFIG=${HOLOSTREAM_CONFIG:-$SCRIPT_DIR/holostream/config/sponge_hardware_matched_q6_figure9b_scaled_164k900k.json}
export JOB_ID=${JOB_ID:-sponge-q6-deferred-reactivation-164k900k-$(date -u +%Y%m%dT%H%M%SZ)}
export WORK_DIR=${WORK_DIR:-/tmp/$JOB_ID}
export SAFE_WORKER_REACTIVATION=true
export REQUIRE_CLOUDLAB_LIFECYCLE_VALIDATION=true
# Initial warm-up is invocation 1. Require at least two subsequent invocations of the same
# logical worker so this run proves reuse beyond the formerly successful first scale-out.
export MIN_CLOUDLAB_REACTIVATIONS_PER_WORKER=${MIN_CLOUDLAB_REACTIVATIONS_PER_WORKER:-2}

exec "$SCRIPT_DIR/run_original_sponge_q6_sine.sh"
