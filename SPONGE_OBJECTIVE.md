# Sponge-Style Autoscaling for Nemo on CloudLab

## Objective

Build, verify, and benchmark an autoscaling Kafka streaming pipeline on CloudLab using Apache Nemo with Sponge-style elastic VM offloading (a-la disaggregated-streaming).

**Target topology:**
- **node0**: control (YARN RM, HDFS NN, job submission, metrics collection)
- **node1,node2,node3**: Kafka brokers (`node1:9092,node2:9092,node3:9092`)
- **node5,node9,node10,node11,node12**: YARN NodeManagers (baseline compute; node9 currently unreachable)
- **node4,node6,node7,node8,node13**: VM/offload workers (no YARN NMs, pure function pool; 40 JVMs each → 200 total)

**Pipeline:** Kafka Source → Nemo streaming job → Kafka Sink (Q0). Autoscaler dynamically migrates tasks to/from VM workers based on queue depth and processing rate.

---

## Current Progress

### Done
- **Core infrastructure**: Java 11 for all components; fixed Hadoop/YARN configs (classpath, NM hostname, env); fixed address plane (short hostnames, internal IPs).
- **VM offloading pipeline**: Warm pool startup, `add-lambda-executor` path, VM worker activation (bidirectional ACTIVATE handshake), no more "No address VM-1" errors.
- **Scale-out (migrate tasks to VM workers)**: Fixed via three-patch approach — `hasEligibleTasksToMigrate()` guard, `clearPrevSelectedTasksToMoveLambda()` cache clear, and `isAllTasksScheduledAtStartTime()` handling taskToBeStopped. Verified working (500k events, tasks migrated to VM-16/17/18/19).
- **Scale-in (migrate tasks back to YARN)**: Fixed by removing `avgProcess == 0` guard and fixing `removeTask`/`executingTask` task map management. Verified working.
- **Scale-in loop fix**: `lastActionWasScaleOut` + `hasLambdaTasksToScaleIn()` guards prevent repeat phantom scale-in. Verified (exactly 1 SCALE_OUT + 1 SCALE_IN row in 500k test).
- **Parallel Kafka producer**: `StandaloneNexmarkKafkaProducer` rewritten with `numGenerators` (default 8), batch-based pacing (500 ev/batch + accumulated delay sleep), optimized producer config (`acks=1`, `linger.ms=5`, `compression=lz4`). Achieves ~66k ev/s aggregate with 8 generators (exceeds 50k target).
- **Per-event `Thread.sleep` removed**: `Thread.sleep(20µs)` had ~1ms OS overhead, limiting throughput to 878 ev/s. Batch pacing solves this.
- **Source.log fix**: Producer now writes `source.log` directly with its actual measured throughput every second (replaces shell-based `write_source_log_for_producer` which wrote target rates independently of actual throughput). This prevents phantom queue from rate mismatch.
- **Metrics pipeline**: `producer_metrics.csv`, `source_metrics.csv`, `task_metrics.csv`, `scaling_decisions.csv`, `combined_metrics.csv` (via `metrics_collector.py`), `plot_metrics.py`.

### In Progress
- End-to-end verification with the source.log fix: rebuild, deploy, run 8M+ events to overlap scaler check window (80s delay).

### Blocked / Known Issues
- **node9 unreachable**: Only 4 of 5 baseline workers available; YARN tests must run with `EXPECTED_NM_COUNT=4`.
- **YARN `yarn application -kill` crashes NMs**: Hadoop 2.7.2 bug. Workaround: restart NMs after each app kill.
- **Scaler "Prev future not finished"**: The running test (`application_1781834805217_0001`) has a stuck migration future. Root cause may be stale task-to-stop state.
- **BURSTY vs batch pacing interaction**: The BURSTY shape controls event-type distribution, not rate. Batch pacing uses `firstEventRate`/`nextEventRate` for timing. During normal phase (target 50k ev/s), pacing sleeps to match. During burst (target 200k ev/s), can't exceed ~66k ev/s anyway.
- **Scaler needs running source for avgInput > 0**: Without active source input, avgInput=0 and avgProcess=0, and neither scale-out nor scale-in triggers. Need overlapping producer active window (≥80s) for a real queue to accumulate.

---

## Task List

### High Priority
- [x] Fix `source.log` to reflect actual produce rate (producer writes directly)
- [x] Build project (compiled standalone producer)
- [ ] Kill stuck YARN app, rebuild Nemo shaded jars, restart test
- [ ] Run scaled-down Sponge pattern (≥8M events to overlap 80s scaler delay)
- [ ] Verify scale-out triggers at ~80s and tasks migrate to VM workers
- [ ] Verify scale-in triggers when queue drains

### Medium Priority
- [ ] Run full 23.85M / 450s Sponge pattern
- [ ] Run Q8 with bursty pattern and autoscaling

### Low Priority
- [ ] Verify end-to-end plots with `plot_metrics.py`
- [ ] Automate YARN log collection and METRICLOG counter aggregation
- [ ] Fix node9 or document as permanently degraded

---

## Key Context

### Scaler Behavior (`InputAndCpuBasedScaler`)
- 80-second initial delay after YARN app reaches RUNNING
- Checks every 1 second
- Scale-out path 1 (queue): `avgInput > 0 && avgProcess > 0 && queue / processRate > 2.0`
- `queue = expectedInput - actualSourceCount`, where `expectedInput` comes from `source.log` (actual produce rate since the fix)
- Scale-out path 2 (CPU): `avgCpu > 0.8` (never fires, typical CPU ~2%)
- Scale-in: `avgInput <= 0 && avgProcess <= 0 && hasLambdaTasksToScaleIn() && avgCpu < 0.8 && lastActionWasScaleOut`

### Producer Performance
- ~66k ev/s aggregate with 8 generators (8k per generator)
- `Generator.next()` + serialization: ~15µs/event
- Per-event `Thread.sleep` overhead: ~1ms (OS scheduler tick) → 878 ev/s max
- Batch pacing: 500 ev/batch, sleep for accumulated delay → accurate high-throughput

### Config Defaults
- `FIRST_RATE=50000`, `NEXT_RATE=200000`, `RATE_PERIOD_SEC=50`
- `KAFKA_PARTITIONS=8`, `PRODUCER_PARALLELISM=8`
- `NUM_MAX_LAMBDA=170`, `STREAM_TIMEOUT=900`
- `SCALING_INTERVAL_SEC=80` (initial delay in scaler)
