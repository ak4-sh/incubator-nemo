# Original Sponge Q6 scale-out run — 225k to 450k events/s

## Outcome

This run completed successfully and passed terminal validation.

- Job ID: `original-sponge-q6-scaleout-225k450k-20260813T061000Z`
- YARN application: `application_1786601258904_0001`
- Total records: `101,250,000`
- Terminal Kafka consumer lag: `0`
- Source-processed records: `101,250,000`
- Producer status: `success`
- Decode/evaluator errors detected by the harness: `0`
- Strict placement validation: passed
- Original-Sponge protected-code verification: passed against commit `4c976182e4a4ae18d740143212f6881347d162b6e`

## Workload

The same supervised HoloStream producer binary populated the Auction and Bid Kafka topics.

| Phase | Configured rate | Configured duration | Producer-observed start |
|---|---:|---:|---:|
| Baseline | 225,000 events/s | 150 s | 0.000 s |
| Overload | 450,000 events/s | 150 s | 163.120 s |

The producer emitted `6,199,200` Auction records and `95,050,800` Bid records across four replicas and four partitions per topic.

## Topology and resources

- Query: standard Beam Nexmark Query 6 through the HoloStream tuple adapter.
- Kafka source: one logical unbounded root, split into 8 source tasks (4 Auction partitions and 4 Bid partitions).
- Q6 compute/target parallelism: 4.
- Baseline placement:
  - source executor on `node5`;
  - compute executors on `node9`, `node10`, `node11`, and `node12`.
- Source/compute executors each received a full-node resource allocation under the hardware-matched configuration.
- Offload pool: 108 one-core VM workers, 27 on each of `node4`, `node6`, `node7`, and `node8`.
- ResourceManager: Java 8; NodeManagers and Nemo executors: Java 11.
- CPU normalization: `ec2=true`.
- Optimization policy: original `StreamingR1R2R3Policy`; R1, R2, and R3 application was verified from the driver log.

## Native scaling result

The 225k baseline did not cause an adaptive routing decision. During the 450k phase, the original scaler crossed its delay threshold and logged:

```text
26/08/13 06:17:28 ... Move 0.29999999999999993 percent of tasks in all vm executors
```

Despite the original log text saying `percent`, the value is a routing fraction: approximately 30%. The decision occurred about 98.1 seconds after the observed start of phase 2. R1/R2/R3 had predeployed 20 transient task instances at startup; productive activation subsequently rose to 32 VM workers in the preserved log.

`workdir/scaling_decisions.csv` is a derived plot marker reconstructed from that preserved driver-log line. The authoritative raw evidence is `workdir/native_scaleout.log` and `workdir/remote_tmp_metrics/yarn_logs/driver.stderr`.

## Telemetry

- Source Kafka residence time came from passive per-source-task sampling of Kafka record append timestamps.
- The collector retained unweighted and weighted task aggregates plus raw one-second source-task rows.
- Valid collected unweighted queue-residence means ranged from approximately 14.38 ms to 88.91 ms.
- Kafka offsets and consumer-group lag were independently collected for both Auction and Bid topics.
- The final offset, source count, and consumer lag remained terminally stable for three collector samples.

## Artifact locations

- Machine-readable verdict: `workdir/final_validation.json`
- Combined metrics: `workdir/combined_metrics.csv`
- Per-topic Kafka metrics: `workdir/kafka_topic_metrics.csv`
- Raw source telemetry: `workdir/remote_tmp_metrics/node5/source_task_metrics.csv`
- Producer plan/completion: `workdir/holostream_producer_plan.json` and `workdir/holostream_producer_completion.json`
- Placement proof: `workdir/executor_placement_verification.json`
- Rewrite proof: `workdir/optimization_pass_verification.txt`
- Driver/executor logs: `workdir/remote_tmp_metrics/yarn_logs/`
- Generated plots: `workdir/plots/`

