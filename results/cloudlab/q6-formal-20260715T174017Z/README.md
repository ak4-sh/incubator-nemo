# CloudLab Sponge/Nemo Q6 Formal Run

Run ID: `q6-formal-20260715T174017Z`
Topic: `q6-formal-20260715T174017Z-topic`
YARN application: `application_1784096431599_0002`
ApplicationMaster host: `node5`
Archive created: 2026-07-15 UTC

## Outcome

The formal workload completed without retry after live production began. Strict placement passed before live production. The producer completed all 47,000,000 live events, and the topic contains 47,000,100 total records including the 100-event prefill. Final Kafka consumer-group lag was 0 on all 8 partitions, and the subscriber reported `Curr input: 0` / `Avg process input: 0` during the post-producer observation window.

Harness exit status: `0`.

## Configuration

Source: 1 executor, 4096 MB, 8 slots, placed on `node5-link-1`.
Compute: 4 executors, 8192 MB, capacity 3, slot 3, placed one each on `node9-link-1`, `node10-link-1`, `node11-link-1`, `node12-link-1`.
AM: unrestricted across the five NodeManager nodes, actual host `node5`.
Offload: 160 prestarted VMWorkers, 32 each on `node4,node6,node7,node8,node13`.
Kafka brokers: `node1,node2,node3`.
HDFS: NameNode and one DataNode on `node0`; final safe mode `OFF`.
Autoscaler: enabled from the beginning with `SCALER_START_MODE=immediate`.

## Placement

See `workdir/executor_placement.csv` and `workdir/executor_placement_verification.json`.

Observed placement:

- Source `Executor0`: `node5-link-1`
- Compute `Executor1`: `node9-link-1`
- Compute `Executor2`: `node10-link-1`
- Compute `Executor3`: `node11-link-1`
- Compute `Executor4`: `node12-link-1`

Verification result: `passed=true`.

## Workload

Prefill: 100 events.
Warmup: 20,000 events/s for 100s.
Steady: 100,000 events/s for 150s.
Burst: 200,000 events/s for 150s.
Live events produced: 47,000,000.
Total topic records including prefill: 47,000,100.
Measured events excluding warmup and prefill: 45,000,000.

Producer phase log: `workdir/producer_phases.csv`.
Final producer summary in `workdir/formal_harness.log`: totalSent=47,000,000, elapsedMs=400701, avgRate=117294.44.

## Scaling And Drain

Formal scaler decisions are curated in `formal_metrics/scaling_decisions_am_node5.csv` and also visible in `workdir/subscriber.log`.

Observed formal decisions:

- Scale out at timestamp `1784138137235`, trigger `QUEUE`, 160 lambda executors.
- Scale in at timestamp `1784138535240`, trigger `IDLE`, after drain.

Final consumer-group rows are in `workdir/consumer_group_final_formal_rows.txt`; all 8 partitions had lag 0.

## Cleanup Verification

Final cleanup files:

- `workdir/yarn_apps_final_after_reef_kill.txt`: zero active applications.
- `workdir/yarn_nodes_final_after_reef_kill.txt`: five RUNNING NodeManagers, zero containers.
- `workdir/process_scan_final_after_reef_kill.txt`: only scan commands remained; no stale REEFLauncher, evaluator, producer, collector, or VMWorker processes.
- `workdir/hdfs_safemode_after_cleanup.txt`: safe mode OFF.
- `workdir/hdfs_report_after_cleanup.txt`: one live DataNode on `node0-link-1`.

No ResourceManager or NodeManager restart was required after cleanup.

## Archive Layout

- `workdir/`: complete local run directory, including command/env, preflight, placement, producer logs, timestamps, Kafka snapshots, and cleanup verification.
- `node_metrics/`: one-second system metrics for all 14 nodes. All node CSVs have headers and hundreds of rows.
- `userlogs/`: direct NodeManager-local logs for AM, Source, and Compute containers. Aggregated `yarn logs` is also present but nearly empty because log aggregation was disabled.
- `formal_metrics/`: curated formal AM/source metrics from node5.
- `remote_tmp_metrics/`: raw `/tmp` capture from selected nodes, including VMWorker logs and GC logs. This directory may include leftover files from earlier runs because `/tmp` was not run-isolated; use `formal_metrics/`, `workdir/`, `node_metrics/`, and `userlogs/` as the authoritative formal-run artifacts.

## Deviations / Notes

- The formal topic was intentionally created during preflight before harness launch, so the harness emitted a topic-exists warning. This did not reuse an aborted formal-run topic.
- The built-in metrics collector attempted `/users/akash01/kafka/bin/kafka-get-offsets.sh`, which does not exist in this Kafka install. Manual `GetOffsetShell` snapshots were captured in `workdir/kafka_offsets_*.txt`, and final consumer-group lag was recorded separately.
- Query output uses the harness COUNT_ONLY mode rather than a separate result topic. Completion was assessed by producer completion, final Kafka lag 0, and source/input drain to zero.
- `remote_tmp_metrics/` is a raw collection from host `/tmp`; stale files from earlier smoke/test runs can appear there.

## Provenance

Git commit: `7c843a3c506ad169ecf1e000393a089c323b65b5`.
Dirty tree diff: `workdir/dirty.diff`.
Executor JSON: `workdir/executor.json`.
JAR SHA-256 checksums: `workdir/jar_sha256.txt`.
Exact environment/command: `workdir/formal_command.env` and `workdir/formal_harness.log`.
