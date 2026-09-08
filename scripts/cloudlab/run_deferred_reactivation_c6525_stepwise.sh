#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
NEMO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

export HOLOSTREAM_CONFIG=${HOLOSTREAM_CONFIG:-$SCRIPT_DIR/holostream/config/sponge_q6_c6525_stepwise_100k1200k.json}
export JOB_ID=${JOB_ID:-sponge-q6-c6525-deferred-reactivation-stepwise-$(date -u +%Y%m%dT%H%M%SZ)}
export WORK_DIR=${WORK_DIR:-/tmp/$JOB_ID}

# One node-sized source executor and four node-sized Q6 compute executors.
# YARN exposes 30 vcores per 32-thread host; each executor receives 29 and
# leaves one YARN vcore plus two OS-reserved threads as headroom.
export EXECUTOR_JSON=${EXECUTOR_JSON:-$NEMO_ROOT/configs/cloudlab/nemo-yarn-kafka-c6525-1source-4compute-node29.json}
export EXPECTED_EXECUTOR_MEMORY_MB=114688
export EXPECTED_EXECUTOR_CAPACITY=29
export EXPECTED_NODE_MEMORY_MB=122880
export EXPECTED_NODE_VCORES=30
export EXPECTED_SOURCE_SLOTS=16
export EXPECTED_COMPUTE_SLOTS=12

# Lifecycle validation uses reusable, single-core VM workers.  Each 32-thread
# offload host contributes 31 workers, leaving one logical CPU for the OS and
# worker-pool supervisor; 31 * 1769 MiB also stays well below 128 GiB/host.
export OFFLOAD_NODES=${OFFLOAD_NODES:-node4,node6,node7,node8}
export WORKERS_PER_NODE=${WORKERS_PER_NODE:-31}
export NUM_MAX_LAMBDA=${NUM_MAX_LAMBDA:-124}
export SAFE_WORKER_REACTIVATION=true
export REQUIRE_CLOUDLAB_LIFECYCLE_VALIDATION=true
export MIN_CLOUDLAB_REACTIVATIONS_PER_WORKER=${MIN_CLOUDLAB_REACTIVATIONS_PER_WORKER:-2}

# The reference HoloStream generator encodes event time relative to a fixed
# synthetic Nexmark epoch.  Sponge's latency-limit guard compares that event
# time with wall-clock time, so a later replay can otherwise be aborted even
# when Kafka queue residence is healthy.  Keep the guard out of this lifecycle
# experiment; native scaling still uses its normal queue-delay/CPU policy.
export LATENCY_LIMIT=${LATENCY_LIMIT:-2147483647}

export STREAM_TIMEOUT=${STREAM_TIMEOUT:-1800}
export HOLOSTREAM_PRODUCER_TIMEOUT=${HOLOSTREAM_PRODUCER_TIMEOUT:-1500}

exec "$SCRIPT_DIR/run_original_sponge_q6_sine.sh"
