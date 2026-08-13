#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
NEMO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
HADOOP_HOME=${HADOOP_HOME:-/users/akash01/hadoop}
export HADOOP_HOME
export PATH="$HADOOP_HOME/bin:$PATH"

bash "$SCRIPT_DIR/verify_original_sponge_behavior.sh"

if [[ ! "${HOLOSTREAM_EXPECTED_PRODUCER_SHA256:-}" =~ ^[0-9a-fA-F]{64}$ ]]; then
  echo "Set HOLOSTREAM_EXPECTED_PRODUCER_SHA256 to the shared producer binary SHA-256." >&2
  exit 2
fi

export PRODUCER_IMPL=holostream
export QUERY=6
export KAFKA_INPUT_FORMAT=HOLOSTREAM_MUS
export HOLOSTREAM_CONFIG=${HOLOSTREAM_CONFIG:-$SCRIPT_DIR/holostream/config/sponge_hardware_matched_q6_smoke_20k.json}
export HOLOSTREAM_RESET_TOPICS=${HOLOSTREAM_RESET_TOPICS:-true}
export HOLOSTREAM_AUCTION_TOPIC=${HOLOSTREAM_AUCTION_TOPIC:-nexmark-auction}
export HOLOSTREAM_BID_TOPIC=${HOLOSTREAM_BID_TOPIC:-nexmark-bid}
export KAFKA_PARTITIONS=${KAFKA_PARTITIONS:-4}
export PRODUCER_PARALLELISM=${PRODUCER_PARALLELISM:-4}
export PREFILL_EVENTS=0
export STREAM_TIMEOUT=${STREAM_TIMEOUT:-300}
export PARTITION_ASSIGN_BUFFER=${PARTITION_ASSIGN_BUFFER:-20}

export EXECUTOR_JSON=${EXECUTOR_JSON:-$NEMO_ROOT/configs/cloudlab/nemo-yarn-kafka-holostream-matched-r1r2r3-node55.json}
export BASELINE_NODES=${BASELINE_NODES:-"node5 node9 node10 node11 node12"}
export EXPECTED_NM_COUNT=5
export SOURCE_HOSTS=${SOURCE_HOSTS:-node5-link-1}
export COMPUTE_HOSTS=${COMPUTE_HOSTS:-node9-link-1,node10-link-1,node11-link-1,node12-link-1}
export STRICT_EXECUTOR_PLACEMENT=true
export EXPECTED_EXECUTOR_PLACEMENT_ROWS=5
export EXPECTED_SOURCE_SLOTS=${EXPECTED_SOURCE_SLOTS:-16}
export EXPECTED_COMPUTE_SLOTS=${EXPECTED_COMPUTE_SLOTS:-12}
export OFFLOAD_NODES=${OFFLOAD_NODES:-node4,node6,node7,node8}

# Smoke-test defaults keep the scaler inactive while validating the original rewritten graph.
export WORKERS_PER_NODE=${WORKERS_PER_NODE:-1}
export NUM_MAX_LAMBDA=${NUM_MAX_LAMBDA:-4}
export LAMBDA_MEMORY=${LAMBDA_MEMORY:-1769}
export AUTOSCALING=${AUTOSCALING:-false}
export OFFLOADING=${OFFLOADING:-0}
export EC2=true

export OPTIMIZATION_POLICY=${OPTIMIZATION_POLICY:-org.apache.nemo.compiler.optimizer.policy.StreamingR1R2R3Policy}
export REQUIRED_OPTIMIZATION_PASSES=${REQUIRED_OPTIMIZATION_PASSES:-R1ReshapingPass,R2ReshapingPass,R3ReshapingPass}

# The Beam adapter exposes one Kafka root. That root covers all four Auction and all four Bid
# partitions, so Nemo should split it into eight source tasks without an artificial Flatten root.
export REQUIRED_SOURCE_ROOTS=${REQUIRED_SOURCE_ROOTS:-1}
export REQUIRED_SOURCE_TASKS=${REQUIRED_SOURCE_TASKS:-8}

export JOB_ID=${JOB_ID:-original-sponge-q6-smoke-$(date -u +%Y%m%dT%H%M%SZ)}
export WORK_DIR=${WORK_DIR:-/tmp/$JOB_ID}
mkdir -p "$WORK_DIR"

python3 "$SCRIPT_DIR/validate_hardware_matched_sponge.py" \
  --executor-config "$EXECUTOR_JSON" \
  --source-host "$SOURCE_HOSTS" \
  --compute-hosts "$COMPUTE_HOSTS" \
  --offload-hosts "$OFFLOAD_NODES" \
  --source-slots "$EXPECTED_SOURCE_SLOTS" \
  --compute-slots "$EXPECTED_COMPUTE_SLOTS" \
  --output-json "$WORK_DIR/hardware_preflight.json"

exec "$SCRIPT_DIR/run_autoscaler_smoke.sh"
