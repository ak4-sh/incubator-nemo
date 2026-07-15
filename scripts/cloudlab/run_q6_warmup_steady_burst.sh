#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/../.."

# Q6 autoscaling run with a low-rate live warmup before measured phases.
# This preserves the default Sponge scaler behavior while avoiding the
# prefill/readiness path arming the scaler immediately before high-rate input.

PREFILL_EVENTS=${PREFILL_EVENTS:-100}

WARMUP_RATE=${WARMUP_RATE:-20000}
WARMUP_DURATION_SEC=${WARMUP_DURATION_SEC:-100}

STEADY_RATE=${STEADY_RATE:-100000}
STEADY_DURATION_SEC=${STEADY_DURATION_SEC:-150}

BURST_RATE=${BURST_RATE:-200000}
BURST_DURATION_SEC=${BURST_DURATION_SEC:-150}

SCALER_START_PHASE=${SCALER_START_PHASE:-2}
SCALER_START_PHASE_DELAY_SEC=${SCALER_START_PHASE_DELAY_SEC:-60}
if [[ "$SCALER_START_PHASE_DELAY_SEC" -ge "$STEADY_DURATION_SEC" ]]; then
  echo "ERROR: SCALER_START_PHASE_DELAY_SEC must be less than STEADY_DURATION_SEC" >&2
  exit 1
fi

warmup_events=$((WARMUP_RATE * WARMUP_DURATION_SEC))
steady_events=$((STEADY_RATE * STEADY_DURATION_SEC))
steady_settling_events=$((STEADY_RATE * SCALER_START_PHASE_DELAY_SEC))
steady_measured_duration_sec=$((STEADY_DURATION_SEC - SCALER_START_PHASE_DELAY_SEC))
steady_measured_events=$((STEADY_RATE * steady_measured_duration_sec))
burst_events=$((BURST_RATE * BURST_DURATION_SEC))
measured_events=$((steady_measured_events + burst_events))
live_events=$((warmup_events + steady_events + burst_events))
total_events=$((PREFILL_EVENTS + live_events))

phase_rates="${WARMUP_RATE},${STEADY_RATE},${BURST_RATE}"
phase_durations="${WARMUP_DURATION_SEC},${STEADY_DURATION_SEC},${BURST_DURATION_SEC}"

echo "Q6 warmup/steady/burst autoscaling run"
echo "  prefill events:   $PREFILL_EVENTS"
echo "  warmup:           ${WARMUP_RATE} ev/s for ${WARMUP_DURATION_SEC}s = ${warmup_events} events (excluded)"
echo "  steady settling:  ${STEADY_RATE} ev/s for ${SCALER_START_PHASE_DELAY_SEC}s = ${steady_settling_events} events (excluded)"
echo "  measured steady:  ${STEADY_RATE} ev/s for ${steady_measured_duration_sec}s = ${steady_measured_events} events"
echo "  measured burst:   ${BURST_RATE} ev/s for ${BURST_DURATION_SEC}s = ${burst_events} events"
echo "  measured events:  $measured_events"
echo "  live events:      $live_events"
echo "  total events:     $total_events"
echo "  phase rates:      $phase_rates"
echo "  phase durations:  $phase_durations"
echo "  scaler start:     phase ${SCALER_START_PHASE} + ${SCALER_START_PHASE_DELAY_SEC}s"

QUERY=6 \
EXECUTOR_JSON=${EXECUTOR_JSON:-"$PWD/configs/cloudlab/nemo-yarn-kafka-1source-8slot-4compute-8g-cap3.json"} \
AUTOSCALING=${AUTOSCALING:-true} \
TOTAL_EVENTS=${TOTAL_EVENTS:-$total_events} \
PREFILL_EVENTS=$PREFILL_EVENTS \
BURST_MODE=phases \
PHASE_RATES=$phase_rates \
PHASE_DURATIONS_SEC=$phase_durations \
SCALER_START_MODE=after_phase_delay \
SCALER_START_PHASE=$SCALER_START_PHASE \
SCALER_START_PHASE_DELAY_SEC=$SCALER_START_PHASE_DELAY_SEC \
PRODUCER_RATE_LIMITED=${PRODUCER_RATE_LIMITED:-true} \
SUBSCRIBER_WAIT=${SUBSCRIBER_WAIT:-10} \
PARTITION_ASSIGN_BUFFER=${PARTITION_ASSIGN_BUFFER:-90} \
STREAM_TIMEOUT=${STREAM_TIMEOUT:-1800} \
"$SCRIPT_DIR/run_autoscaler_smoke.sh"
