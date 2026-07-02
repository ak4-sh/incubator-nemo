# Sponge Q6 Rate Comparison Run — sponge-q0-20260701-185204

**Date**: 2026-07-01  
**Query**: NexMark Q6 (running auction average, stateful)  
**Rate profile**: 50k ev/s for 120s → step to 200k ev/s, 102M total events  
**Benchmark**: Holostream vs Sponge (Nemo) apples-to-apples comparison

## Result: FAILED (source stalled at 200k/s phase)

## What worked
- **50k/s phase (t=0–120s)**: Nemo consumed cleanly at 50k ev/s, Kafka lag reached 0
  within ~28s and stayed there. kafkaQueueTimeP50 settled at ~355ms.
  Lambda offload workers processed in lockstep with the source.

## What failed
- **200k/s phase (t=120s+)**: Source consumed at 200k/s for ~10s, then stalled.
  Only ~1.6M of the 96M burst-phase events were consumed before the source froze.

## Root cause: COLLECT_LATENCY death spiral

Sponge offloads stateful computation to lambda/VM workers. After each batch the
Nemo driver must **collect** results back from all workers — a sequential round-trip.

When the event rate stepped 4× to 200k/s, each collect batch was 4× larger.
COLLECT_LATENCY grew unboundedly:

| t after step change | COLLECT_LATENCY (median) |
|---------------------|--------------------------|
| +2s  | 2,201 ms |
| +6s  | 3,744 ms |
| +14s | 8,674 ms |
| +20s | 10,738 ms |
| +27s | 27,459 ms |
| +40s | 41,974 ms |

As collect latency grew,  (offload throughput) collapsed from 200k → 0
ev/s within ~20s. Backpressure queued ~9M events and throttled the Kafka source to
zero. The autoscaler attempted SCALE_OUT (already at 173 lambda workers) but adding
more workers makes the collect round-trip worse, not better — the driver must gather
from even more workers per cycle.

## Conclusion
The collect bottleneck is architectural: Sponge's offloading model is stable at
moderate load (50k ev/s, collect RTT ~355ms) but enters an unrecoverable spiral at
4× higher input rate because the driver-side collect operation scales worse than
linearly with event rate. Holostream avoids this by keeping state in-process with
no collect round-trip overhead.

## Run notes
- Coordination fix applied: producer waits for  heartbeat in subscriber
  log before starting (ensures Kafka partitions are assigned, prevents producer
  race-ahead of Nemo startup).
- Producer mode: BURST_MODE=custom with STEADY_DURATION_SEC=120, NUM_BURSTS=1,
  RAMP_UP_SEC=0 — gives a true one-time step change (not the cyclic legacy mode).
