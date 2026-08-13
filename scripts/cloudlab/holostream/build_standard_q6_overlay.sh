#!/usr/bin/env bash
set -euo pipefail

HOLOSTREAM_ROOT=${HOLOSTREAM_ROOT:-/users/akash01/holostream}
SPONGE_ROOT=${SPONGE_ROOT:-/users/akash01/nemo-original-sponge-holostream}
GO_BIN=${GO_BIN:-/users/akash01/deps/go1.25.6/bin/go}
PRODUCER_BINARY=${PRODUCER_BINARY:-/users/akash01/holostream-producer-ref-2217278/bin/nexmarkKafkaProducer}
EXPECTED_PRODUCER_SHA256=${EXPECTED_PRODUCER_SHA256:-67dd182d4367829ae227dfab8edb61fa64c26fecbe9a30b91c1c94a5f5ea1bf6}
OVERLAY_JSON=$SPONGE_ROOT/scripts/cloudlab/holostream/overlays/q6_zero_dummy_overlay.cloudlab.json

cd "$HOLOSTREAM_ROOT"

"$GO_BIN" build -overlay="$OVERLAY_JSON" -o ./bin/worker ./cmd/worker/worker.go
"$GO_BIN" build -overlay="$OVERLAY_JSON" -o ./bin/coordinator ./cmd/coordinator/coordinator.go

cp "$PRODUCER_BINARY" ./bin/nexmarkKafkaProducer
actual_producer_sha256=$(sha256sum ./bin/nexmarkKafkaProducer | awk '{print $1}')
if [[ "$actual_producer_sha256" != "$EXPECTED_PRODUCER_SHA256" ]]; then
  echo "ERROR: producer SHA-256 mismatch: expected=$EXPECTED_PRODUCER_SHA256 actual=$actual_producer_sha256" >&2
  exit 1
fi

# runExperiment.py invokes make twice. These target stamps keep those invocations
# from replacing the overlay-built worker/coordinator or the pinned producer.
touch build_worker build_coordinator build_nexmark_kafka_producer

echo "Built standard-Q6 HoloStream binaries with the zero-length dummy-field overlay."
echo "Pinned producer SHA-256: $actual_producer_sha256"
