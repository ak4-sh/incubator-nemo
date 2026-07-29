#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

NEMO_CLIENT_JAR=${1:-$REPO_ROOT/client/target/nemo-client-0.2-SNAPSHOT-shaded.jar}
NEXMARK_JAR=${2:-$REPO_ROOT/examples/nexmark/target/nexmark-0.2-SNAPSHOT-shaded.jar}

for artifact in "$NEMO_CLIENT_JAR" "$NEXMARK_JAR"; do
  if [[ ! -f "$artifact" ]]; then
    echo "ERROR: required CloudLab artifact is missing: $artifact" >&2
    exit 1
  fi
done

VERIFY_DIR=$(mktemp -d)
trap 'rm -rf "$VERIFY_DIR"' EXIT

jar tf "$NEMO_CLIENT_JAR" |
  grep -E '^org/apache/beam/.*[.]class$' |
  sort -u > "$VERIFY_DIR/nemo-beam-classes.txt"
jar tf "$NEXMARK_JAR" |
  grep -E '^org/apache/beam/.*[.]class$' |
  sort -u > "$VERIFY_DIR/nexmark-beam-classes.txt"

comm -12 \
  "$VERIFY_DIR/nemo-beam-classes.txt" \
  "$VERIFY_DIR/nexmark-beam-classes.txt" \
  > "$VERIFY_DIR/duplicate-beam-classes.txt"

if [[ -s "$VERIFY_DIR/duplicate-beam-classes.txt" ]]; then
  echo "ERROR: Nemo client and Nexmark artifacts contain duplicate Beam classes." >&2
  echo "REEF loads reef/global/* in nondeterministic order across evaluators." >&2
  head -n 25 "$VERIFY_DIR/duplicate-beam-classes.txt" >&2
  exit 1
fi

if ! grep -qx 'org/apache/beam/sdk/transforms/display/DisplayData.class' \
  "$VERIFY_DIR/nemo-beam-classes.txt"; then
  echo "ERROR: Nemo client does not provide Beam DisplayData" >&2
  exit 1
fi
if ! grep -qx 'org/apache/beam/sdk/nexmark/Main.class' \
  "$VERIFY_DIR/nexmark-beam-classes.txt"; then
  echo "ERROR: Nexmark artifact does not provide the Nexmark entry point" >&2
  exit 1
fi

echo "Verified CloudLab artifacts: no duplicate Beam classes"
echo "  Nemo client: $NEMO_CLIENT_JAR"
echo "  Nexmark:     $NEXMARK_JAR"
