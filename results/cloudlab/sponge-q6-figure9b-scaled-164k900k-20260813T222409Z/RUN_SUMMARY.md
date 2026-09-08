# Sponge Figure 9b scaled sine run: 164K–900K events/s

- Run ID: `sponge-q6-figure9b-scaled-164k900k-20260813T222409Z`
- Date: 2026-08-13
- Classification: **failed scaling run; retain for diagnosis, not comparison results**

## Workload

- Shape: Figure 9b-style sine ascent and descent.
- Aggregate target rates: `163636, 163636, 212728, 348544, 531820, 716728, 850908, 900000, 850908, 716728, 531820, 348544, 212728, 163636` events/s.
- Phase durations: `120, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 60` seconds.
- Expected and produced events: `61,379,940`.
- Producer status: success.
- Final Kafka consumer lag: `0`.
- Config SHA-256: `a0cccd59f6fd2c344232f99050ec5cf4a05830cffa1a6776dfd5a9d122c0cb33`.

## Sponge setup

- Native autoscaler enabled from job start.
- EC2 CPU normalization enabled.
- Four baseline Q6 compute executors under strict host placement.
- One source executor with eight Kafka source tasks.
- Warm pool: 108 single-core VM workers, 27 on each of four offload hosts.
- No manual scale trigger.

## Scaling timeline

- First decision: `22:31:18`.
  - Native estimated queue delay: `2.5277 s`.
  - Selected migration ratio: `30.8771%`.
  - Migration wait completed after `45.842 s`, at `22:32:04`.
- Second decision: `22:32:14`.
  - Native estimated queue delay: `13.2688 s`.
  - Selected migration ratio: `66.7170%`.
  - This decision did not complete successfully.
- Across the run, tasks were assigned to 32 distinct VM-worker IDs (33 assignments total). This is not a concurrent-worker count.

## Failure

- At `22:32:46`, the task-dispatcher thread attempted to activate `VM-6` while it remained in `DEACTIVATING` state.
- Exception: `RuntimeException: Worker 6/DEACTIVATING is not deactive but try to activate`.
- REEF propagated the uncaught exception as a resource-manager error and the driver connection terminated.
- The harness's terminal offset check passed before the crash because all source records had already been consumed. That check does **not** make this a successful query/scaling run.

## Metric peaks

- Maximum source lag: `986,311` events.
- Maximum Kafka consumer lag: `2,812,704` events.
- Maximum source Kafka residence time:
  - Unweighted task mean: `2,363.930 ms`.
  - Sample-weighted mean: `4,404.698 ms`.
  - P50: `4,220.924 ms`.
  - P95/P99: `4,712.172 ms`.
- Maximum collected average input rate: `850,908 events/s`.
- Maximum collected average processing rate: `868,090.2 events/s`.

## Cluster cleanup

- Active YARN applications: `0`.
- Offload VM-worker processes: `0` on all four offload hosts.
- ResourceManagers: `1`.
- Running NodeManagers: `5`.

