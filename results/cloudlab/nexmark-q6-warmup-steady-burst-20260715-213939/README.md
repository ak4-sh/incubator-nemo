# Nexmark Q6 Warmup/Steady/Burst Autoscaling Run

Successful Sponge/Q6 CloudLab run with delayed scaler start and 4 GB Source executor.

## Run IDs

- Run ID: `nexmark-q6-warmup-steady-burst-20260715-213939`
- Topic: `nexmark-auto-213939`
- Application: `application_1784073595628_0007`
- Query: `6`
- AM host: `node11`
- Date: 2026-07-15 UTC
- Executor config: `configs/cloudlab/nemo-yarn-kafka-1source-8slot-4compute-8g-cap3.json`
- Work dir: `/tmp/nx-auto-nexmark-auto-213939`

## Topology And Memory

- Baseline YARN workers: `node5,node9,node10,node11,node12`
- Offload JVM worker nodes: `node4,node6,node7,node8,node13`
- Source executor: 4096 MB, 8 slots; launch log confirmed `-XX:MaxHeapSize=3996m`
- Compute executors: 4 x 8192 MB
- Compute logical slots: 12 total (`capacity=3`, `slot=3` on each compute executor)
- YARN containers: 6 total including AM

## Producer Schedule

```text
prefill:       100 events
phase 1:       20k ev/s for 100s
phase 2:      100k ev/s for 150s
scaler start: phase 2 + 60s
phase 3:      200k ev/s for 150s
```

Producer completed the full live workload:

```text
KAFKA_PRODUCER_DONE topic=nexmark-auto-213939 totalSent=47000000 elapsedMs=400726 avgRate=117287.12
```

## Scale-Out Result

No false scale-out occurred during the 100k phase after scaler enable. Scale-out fired during the 200k phase via the queue-delay trigger:

```text
1784087280308,SCALE_OUT,0.5565,200000.0000,108331.8000,243277.0000,243277,0.5583,4,QUEUE,0.8000,0.8392,1.2000,0.6000,2.2457,2.0000,4,160
```

Interpretation:

- Trigger: `QUEUE`
- Avg CPU: `0.5565`
- Avg input: `200000`
- Avg process: `108331.8`
- Queue: `243277`
- Queue delay: `2.2457s`
- Queue threshold: `2.0s`
- Scale-out ratio: `0.5583`
- Offload workers available: `160`

Post-scale processing recovered to roughly `198k-228k ev/s`, and the queue drained instead of collapsing to zero processing.

## Outcome

- Full 47M live-event producer schedule completed.
- No `OutOfMemoryError` or `FailedRuntime` was found in the subscriber log.
- This run fixed the previous 2 GB Source heap OOM by using 4 GB Source memory.
- App was killed after producer completion and post-scale observation.

## Files

- `combined_metrics.csv`: collector timeline.
- `scaling_decisions.csv`: filtered scale decisions for this run window.
- `scaler_metrics.csv`: filtered scaler state timeline for `nx-q6-nexmark-auto-213939`.
- `source_aggregate_metrics.csv`: AM-side source progress timeline for this job.
- `source_task_metrics.csv`: source-task queue time, idle time, and input rate.
- `task_metrics.csv`: plot-compatible task metrics filtered to this run, including AM task rows and `VM-*` offload rows.
- `am-node11/`: raw AM-side metric files copied from `/tmp` on the AM host.
- `node11-source_task_metrics.csv`: raw baseline-node source task metrics available for this run.
- `offload-task-metrics/`: raw per-offload-node task metrics. Some rows are stale warm-pool leftovers from previous jobs; use root `task_metrics.csv` for filtered analysis.
- `producer_metrics.csv`: producer throughput timeline.
- `producer_phases.csv`: producer phase start/end timestamps.
- `scaler_enable.csv`: delayed scaler start timestamp.
- `scaling.txt`: control commands used by the harness.
- `source.log`: source progress updates forwarded by the harness.
- `metrics_collector.log`: metrics collector log.
- `logs/subscriber.log`: submit-side JobLauncher/harness log.
- `logs/yarn-application_1784073595628_0007.log.gz`: compressed aggregated YARN app log.
- `SHA256SUMS`: checksums for preserved run artifacts.

Plots are not included in this artifact because the local Python environment did not have `pandas` available when the run was archived.
