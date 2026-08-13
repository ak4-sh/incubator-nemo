#!/usr/bin/env bash
set -euo pipefail

BASE_COMMIT=${ORIGINAL_SPONGE_BASE:-4c976182e4a4ae18d740143212f6881347d162b6e}
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
PROTECTED_MANIFEST=$SCRIPT_DIR/original_sponge_protected.sha256

# The compiled CloudLab checkout is transferred without Git metadata. In that case, verify the
# same protected boundary against content hashes generated directly from BASE_COMMIT.
if ! git -C "$REPO_ROOT" cat-file -e "$BASE_COMMIT^{commit}" 2>/dev/null; then
  if [[ ! -f "$PROTECTED_MANIFEST" ]]; then
    echo "Missing original Sponge protected-file manifest: $PROTECTED_MANIFEST" >&2
    exit 1
  fi
  cd "$REPO_ROOT"
  sha256sum --check --quiet "$PROTECTED_MANIFEST"
  echo "Original Sponge protected paths match $BASE_COMMIT (SHA-256 manifest)"
  exit 0
fi

protected_paths=(
  compiler/frontend/beam/src/main/java/org/apache/nemo/compiler/frontend/beam/PipelineTranslationContext.java
  compiler/optimizer/src/main/java/org/apache/nemo/compiler/optimizer/pass/compiletime/composite/StreamingR1R3ReshapingPass.java
  compiler/optimizer/src/main/java/org/apache/nemo/compiler/optimizer/pass/compiletime/composite/StreamingR1ReshapingPass.java
  compiler/optimizer/src/main/java/org/apache/nemo/compiler/optimizer/pass/compiletime/composite/StreamingR2ReshapingPass.java
  compiler/optimizer/src/main/java/org/apache/nemo/compiler/optimizer/pass/compiletime/composite/StreamingR3ReshapingPass.java
  compiler/optimizer/src/main/java/org/apache/nemo/compiler/optimizer/pass/compiletime/reshaping/R1ReshapingPass.java
  compiler/optimizer/src/main/java/org/apache/nemo/compiler/optimizer/pass/compiletime/reshaping/R2ReshapingPass.java
  compiler/optimizer/src/main/java/org/apache/nemo/compiler/optimizer/pass/compiletime/reshaping/R3NoR2ReshapingPass.java
  compiler/optimizer/src/main/java/org/apache/nemo/compiler/optimizer/pass/compiletime/reshaping/R3ReshapingPass.java
  runtime/master/src/main/java/org/apache/nemo/runtime/master/MasterUtils.java
  runtime/master/src/main/java/org/apache/nemo/runtime/master/ScaleInOutManager.java
  runtime/master/src/main/java/org/apache/nemo/runtime/master/backpressure/InputAndQueueSizeBasedBackpressure.java
  runtime/master/src/main/java/org/apache/nemo/runtime/master/scaler/InputAndCpuBasedScaler.java
)

# CloudLab requires only these two executor-common networking adaptations. Everything else in the
# original executor/task/watermark implementation remains protected.
allowed_executor_common_changes=(
  runtime/executor-common/src/main/java/org/apache/nemo/runtime/executor/common/ExecutorChannelManagerMap.java
  runtime/executor-common/src/main/java/org/apache/nemo/runtime/executor/common/datatransfer/DefaultByteTransportImpl.java
)

cd "$REPO_ROOT"
changed=0
for path in "${protected_paths[@]}"; do
  if ! git diff --quiet "$BASE_COMMIT" -- "$path"; then
    echo "PROTECTED_PATH_CHANGED $path" >&2
    git diff --stat "$BASE_COMMIT" -- "$path" >&2
    changed=1
  fi
done

while IFS= read -r path; do
  allowed=0
  for allowed_path in "${allowed_executor_common_changes[@]}"; do
    if [[ "$path" == "$allowed_path" ]]; then
      allowed=1
      break
    fi
  done
  if [[ "$allowed" -eq 0 ]]; then
    echo "PROTECTED_PATH_CHANGED $path" >&2
    changed=1
  fi
done < <(git diff --name-only "$BASE_COMMIT" -- \
  runtime/executor-common/src/main/java/org/apache/nemo/runtime/executor/common)

if [[ "$changed" -ne 0 ]]; then
  echo "Original Sponge behavior verification failed." >&2
  exit 1
fi

echo "Original Sponge protected paths match $BASE_COMMIT"
