# Nemo Q6 Warmup and Main Run Summary

This document records the experimental conditions and outcomes for the Nemo Nexmark Q6 two-phase run on CloudLab. It is intended to capture the run context needed for research-paper reporting and later reproducibility.

## Overview

- System: Apache Nemo on YARN
- Workload: Nexmark Query 6
- Source: Kafka, `SUBSCRIBE_ONLY`
- Sink: Kafka
- Run structure: warmup run followed by main benchmark run
- Kafka prefill before query deployment: `0` events
- Completion criterion: `source_plus_some_output`
- Autoscaling: `false`
- Offloading infrastructure: enabled and VM workers started, but autoscaling/migration disabled
- Executor config: `configs/cloudlab/nemo-yarn-kafka-1source-8slot-12compute.json`
- Q6 output interpretation: output count is not expected to equal input count; success requires full source consumption plus observed non-zero Q6 output.

## Cluster Layout

- Control node: `node0`
- Kafka brokers: `node1,node2,node3`
- YARN/Nemo worker nodes: `node5,node9,node10,node11,node12`
- Offload VM worker pool nodes: `node4,node6,node7,node8,node13`
- Kafka bootstrap: `node1:9092,node2:9092,node3:9092`
- Kafka partitions: `8`
- Kafka home: `/users/akash01/kafka`
- Hadoop home: `/users/akash01/hadoop`
- Java: Java 11

## Executor and Runtime Configuration

- Executor JSON: `configs/cloudlab/nemo-yarn-kafka-1source-8slot-12compute.json`
- Source executors: `1`
- Source executor capacity/slots: `8`
- Compute executors: `12`
- Compute executor slots: `1` each
- Stream scheduler: `org.apache.nemo.runtime.master.scheduler.StreamingScheduler`
- Optimization policy: `org.apache.nemo.compiler.optimizer.policy.StreamingPolicy`
- Source tasks/Kafka partitions: `8`
- CPU delay: `2 ms`
- Kafka consumer groups:
  - Warmup: `nexmark-q6-warmup-20260626-165650-consumer`
  - Main: `nexmark-q6-main-20260626-165650-consumer`
- VM worker pool:
  - Offload nodes: `5`
  - Workers per offload node: `32`
  - Total VM workers: `160`
  - Max VM executors configured: `160`
- Scaling decisions observed: `0`
- Scale-out decisions observed: `0`
- Scale-in decisions observed: `0`
- VM task metric rows: `0`

## Warmup Run

- Run ID: `nexmark-q6-warmup-20260626-165650`
- YARN application: `application_1782444527113_0006`
- AM host: `node5`
- Input topic: `nexmark-nexmark-q6-warmup-20260626-165650`
- Result topic: `nexmark-nexmark-q6-warmup-20260626-165650-results`
- Artifact directory: `/users/akash01/incubator-nemo/results/cloudlab/nexmark-q6-warmup-20260626-165650`

### Warmup Input Shape

- Total events: `2,500,000`
- Producer parallelism: `8`
- Producer mode: legacy bursty mode, configured as effectively steady
- Producer rate limited: `false`
- First rate target: `20,000 events/s`
- Next rate target: `20,000 events/s`
- Rate period: `1500 s`
- Producer elapsed time: `128.253 s`
- Producer average rate: `19,492.72 events/s`

This warmup was configured to be similar to Holostream's `query6ModWarmup10GB.json` in event count and unrate-limited warmup behavior. Nemo does not currently reproduce Holostream's Pebble state materialization/loading semantics.

### Warmup Results

- Input Kafka offset total: `2,500,000`
- Final source count: `2,500,000`
- Result Kafka offset total: `76,440`
- Max input/source lag: `0`
- Max input/result lag: `0`
- Max scaler queue: `6,120`
- Max Kafka queue-time p95: `228.654 ms`
- Completion status: success

## Main Benchmark Run

- Run ID: `nexmark-q6-main-20260626-165650`
- YARN application: `application_1782444527113_0007`
- AM host: `node5`
- Input topic: `nexmark-nexmark-q6-main-20260626-165650`
- Result topic: `nexmark-nexmark-q6-main-20260626-165650-results`
- Artifact directory: `/users/akash01/incubator-nemo/results/cloudlab/nexmark-q6-main-20260626-165650`

### Main Input Shape

- Total events: `23,850,000`
- Producer parallelism: `8`
- Producer mode: custom sustained burst mode
- Producer rate limited: `true`
- Ramp-up: `60 s @ 50,000 events/s`
- Number of burst cycles: `3`
- Per-cycle steady phase: `60 s @ 50,000 events/s`
- Per-cycle burst phase: `45 s @ 200,000 events/s`
- Producer elapsed time: `255.536 s`
- Producer average rate: `93,333.23 events/s`

### Main Results

- Input Kafka offset total: `23,850,000`
- Final source count: `23,850,000`
- Result Kafka offset total: `611,787`
- Max input/source lag: `0`
- Max input/result lag: `0`
- Max scaler queue: `1,592,855`
- Max Kafka queue-time p95: `15,697.887 ms`
- Completion status: success

## Generated Main-Run Plots

Plots were generated in:

`/users/akash01/incubator-nemo/results/cloudlab/nexmark-q6-main-20260626-165650/plots`

Generated plot files:

- `input_rate.png`
- `kafka_source_lag.png`
- `kafka_result_lag.png`
- `source_kafka_queue_time.png`
- `latency.png`
- `cpu.png`

## Artifact Files

Each run directory contains the following main artifacts:

- `README.md`: automatically generated run manifest
- `combined_metrics.csv`: collector timeline
- `producer_metrics.csv`: producer throughput timeline
- `scaler_metrics.csv`: periodic scaler state timeline
- `source_aggregate_metrics.csv`: AM-side source progress timeline
- `source_task_metrics.csv`: source-task queue time, idle time, and input rate
- `task_metrics.csv`: task-rate metrics filtered to the run window
- `harness.log`: benchmark wrapper output
- `subscriber.log`: submit-side JobLauncher output
- `metrics_collector.log`: background metrics collector output
- `am-node5/`: raw AM-side metrics
- `offload-task-metrics/`: raw per-offload-node task metrics

## Operational Notes and Caveats

- Q6 output cardinality is workload-dependent and is not expected to match input cardinality.
- `source_plus_some_output` was used because Q6 does not emit one output per input event.
- The VM worker pool was started and checked for reachability, but `AUTOSCALING=false`; no scale-out/scale-in decisions occurred and no tasks migrated to VM workers.
- The warmup matched the Holostream warmup input scale and unrate-limited behavior, but not Holostream's Pebble warmup state persistence/loading.
- During the two-phase run setup, stale `REEFLauncher` processes from previous attempts interfered with YARN allocation. These were cleaned up, and the final successful warmup/main run completed after recovery.
- After completion, stale REEF processes were removed and YARN was reset to a clean state with exactly five RUNNING worker NodeManagers and zero active containers.

## Final Cluster State After Cleanup

- Active YARN applications: `0`
- YARN NodeManagers: `5` RUNNING
- Running containers: `0`
- Worker NodeManagers:
  - `node5`
  - `node9`
  - `node10`
  - `node11`
  - `node12`
