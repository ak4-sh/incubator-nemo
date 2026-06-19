#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  collect_kafka_lag.sh <bootstrap_servers> <output_csv> [interval_seconds] [consumer_group] [topics_csv]

Examples:
  collect_kafka_lag.sh node1:9092,node2:9092,node3:9092 results/kafka_lag.csv 1
  collect_kafka_lag.sh node1:9092,node2:9092,node3:9092 results/kafka_lag.csv 1 nexmark-Q8-consumer person,auction

Environment:
  KAFKA_HOME  Optional Kafka installation directory. If unset, Kafka tools must be on PATH.

Notes:
  - If consumer_group is omitted or "__ALL__", the script samples all consumer groups.
  - If topics_csv is omitted or "__ALL__", the script samples end offsets for all non-internal topics.
  - Consumer group lag is written as kind=consumer_group_lag.
  - Topic end offsets are written as kind=topic_end_offset.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || "$#" -lt 2 ]]; then
  usage
  exit 0
fi

BOOTSTRAP_SERVERS="$1"
OUTPUT_CSV="$2"
INTERVAL_SECONDS="${3:-1}"
CONSUMER_GROUP="${4:-__ALL__}"
TOPICS_CSV="${5:-__ALL__}"

if [[ -n "${KAFKA_HOME:-}" ]]; then
  KAFKA_BIN="${KAFKA_HOME%/}/bin"
else
  KAFKA_BIN=""
fi

kafka_tool() {
  local tool="$1"
  if [[ -n "$KAFKA_BIN" && -x "$KAFKA_BIN/$tool" ]]; then
    printf '%s\n' "$KAFKA_BIN/$tool"
  elif command -v "$tool" >/dev/null 2>&1; then
    command -v "$tool"
  else
    return 1
  fi
}

CONSUMER_GROUPS_TOOL="$(kafka_tool kafka-consumer-groups.sh || true)"
TOPICS_TOOL="$(kafka_tool kafka-topics.sh || true)"
RUN_CLASS_TOOL="$(kafka_tool kafka-run-class.sh || true)"

if [[ -z "$CONSUMER_GROUPS_TOOL" ]]; then
  echo "Missing kafka-consumer-groups.sh. Set KAFKA_HOME or add Kafka tools to PATH." >&2
  exit 1
fi

mkdir -p "$(dirname "$OUTPUT_CSV")"
if [[ ! -f "$OUTPUT_CSV" ]]; then
  echo "timestamp_unix,timestamp_iso,kind,consumer_group,topic,partition,current_offset,log_end_offset,lag,consumer_id,host,client_id" \
    > "$OUTPUT_CSV"
fi

list_groups() {
  "$CONSUMER_GROUPS_TOOL" --bootstrap-server "$BOOTSTRAP_SERVERS" --list 2>/dev/null \
    | sed '/^[[:space:]]*$/d' || true
}

list_topics() {
  if [[ "$TOPICS_CSV" != "__ALL__" ]]; then
    tr ',' '\n' <<< "$TOPICS_CSV" | sed '/^[[:space:]]*$/d'
  elif [[ -n "$TOPICS_TOOL" ]]; then
    "$TOPICS_TOOL" --bootstrap-server "$BOOTSTRAP_SERVERS" --list 2>/dev/null \
      | sed '/^[[:space:]]*$/d' \
      | grep -v '^__' || true
  fi
}

sample_group() {
  local group="$1"
  local ts_unix="$2"
  local ts_iso="$3"
  local group_snapshot

  group_snapshot="$("$CONSUMER_GROUPS_TOOL" \
    --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --describe \
    --group "$group" 2>/dev/null || true)"

  awk -v ts_unix="$ts_unix" -v ts_iso="$ts_iso" -v group="$group" '
      BEGIN { OFS="," }
      /^GROUP[[:space:]]+/ { next }
      /^[[:space:]]*$/ { next }
      /Consumer group .* has no active members/ { next }
      /Error:/ { next }
      NF >= 6 {
        topic=$2
        partition=$3
        current=$4
        end=$5
        lag=$6
        consumer=(NF >= 7 ? $7 : "")
        host=(NF >= 8 ? $8 : "")
        client=(NF >= 9 ? $9 : "")
        g=$1
        if (topic == "" || topic == "TOPIC") next
        print ts_unix, ts_iso, "consumer_group_lag", g, topic, partition, current, end, lag, consumer, host, client
      }
    ' <<< "$group_snapshot" >> "$OUTPUT_CSV"
}

sample_topic_end_offsets() {
  local ts_unix="$1"
  local ts_iso="$2"

  [[ -n "$RUN_CLASS_TOOL" ]] || return 0

  while IFS= read -r topic; do
    [[ -n "$topic" ]] || continue
    "$RUN_CLASS_TOOL" kafka.tools.GetOffsetShell \
      --broker-list "$BOOTSTRAP_SERVERS" \
      --topic "$topic" \
      --time -1 2>/dev/null \
      | awk -F ':' -v ts_unix="$ts_unix" -v ts_iso="$ts_iso" '
        BEGIN { OFS="," }
        NF >= 3 {
          print ts_unix, ts_iso, "topic_end_offset", "", $1, $2, "", $3, "", "", "", ""
        }
      ' >> "$OUTPUT_CSV" || true
  done < <(list_topics)
}

trap 'exit 0' INT TERM

while true; do
  ts_unix="$(date +%s)"
  ts_iso="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

  if [[ "$CONSUMER_GROUP" == "__ALL__" ]]; then
    while IFS= read -r group; do
      [[ -n "$group" ]] || continue
      sample_group "$group" "$ts_unix" "$ts_iso"
    done < <(list_groups)
  else
    sample_group "$CONSUMER_GROUP" "$ts_unix" "$ts_iso"
  fi

  sample_topic_end_offsets "$ts_unix" "$ts_iso"
  sleep "$INTERVAL_SECONDS"
done
