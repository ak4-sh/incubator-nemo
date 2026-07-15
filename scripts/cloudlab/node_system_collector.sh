#!/usr/bin/env bash
set -euo pipefail

OUT=${1:?output csv path}
ROLE=${2:-unknown}
INTERVAL=${INTERVAL:-1}
STOP_FILE=${STOP_FILE:-}

mkdir -p "$(dirname "$OUT")"

read_cpu() {
  awk '/^cpu / {
    idle=$5+$6
    total=0
    for (i=2; i<=NF; i++) total += $i
    print idle, total
  }' /proc/stat
}

sum_net() {
  awk -F'[: ]+' '
    $2 != "lo" {
      rx += $3
      tx += $11
    }
    END {print rx+0, tx+0}
  ' /proc/net/dev
}

sum_disk() {
  awk '
    $3 ~ /^(sd[a-z]|vd[a-z]|xvd[a-z]|nvme[0-9]+n[0-9]+)$/ {
      read_bytes += $6 * 512
      write_bytes += $10 * 512
    }
    END {print read_bytes+0, write_bytes+0}
  ' /proc/diskstats
}

sum_rss() {
  local pattern=$1
  ps -eo rss=,args= | awk -v pat="$pattern" '
    $0 ~ pat {rss += $1}
    END {print rss+0}
  '
}

printf 'timestamp_ms,hostname,role,cpu_util,rm_rss_kb,nm_rss_kb,dn_rss_kb,nn_rss_kb,kafka_rss_kb,producer_rss_kb,nemo_rss_kb,reef_rss_kb,vmworker_rss_kb,rx_bytes,tx_bytes,disk_read_bytes,disk_write_bytes\n' > "$OUT"

read prev_idle prev_total < <(read_cpu)
while true; do
  sleep "$INTERVAL"
  if [[ -n "$STOP_FILE" && -e "$STOP_FILE" ]]; then
    break
  fi

  read idle total < <(read_cpu)
  read rx tx < <(sum_net)
  read disk_read disk_write < <(sum_disk)

  diff_idle=$((idle - prev_idle))
  diff_total=$((total - prev_total))
  cpu_util=$(awk -v idle="$diff_idle" -v total="$diff_total" 'BEGIN {
    if (total <= 0) printf "0.0000"; else printf "%.4f", 1.0 - (idle / total)
  }')
  prev_idle=$idle
  prev_total=$total

  ts=$(date +%s%3N)
  host=$(hostname -s)
  rm_rss=$(sum_rss '[R]esourceManager')
  nm_rss=$(sum_rss '[N]odeManager')
  dn_rss=$(sum_rss '[D]ataNode')
  nn_rss=$(sum_rss '[N]ameNode')
  kafka_rss=$(sum_rss '[k]afka.Kafka|[Q]uorumPeerMain')
  producer_rss=$(sum_rss '[S]tandaloneNexmarkKafkaProducer')
  nemo_rss=$(sum_rss '[o]rg.apache.nemo.client.JobLauncher')
  reef_rss=$(sum_rss '[R]EEFLauncher')
  vmworker_rss=$(sum_rss '[V]MWorker')

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$ts" "$host" "$ROLE" "$cpu_util" "$rm_rss" "$nm_rss" "$dn_rss" "$nn_rss" \
    "$kafka_rss" "$producer_rss" "$nemo_rss" "$reef_rss" "$vmworker_rss" \
    "$rx" "$tx" "$disk_read" "$disk_write" >> "$OUT"
done
