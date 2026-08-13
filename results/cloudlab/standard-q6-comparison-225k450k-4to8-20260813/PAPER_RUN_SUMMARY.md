# Standard Nexmark Q6: Sponge and HoloStream Run Summary

## Experimental controls

- Query: standard Nexmark Query 6 over raw Auction and Bid events.
- Producer: identical pinned HoloStream producer binary for all runs.
- Producer SHA-256: `67dd182d4367829ae227dfab8edb61fa64c26fecbe9a30b91c1c94a5f5ea1bf6`.
- Kafka input: four Auction partitions and four Bid partitions.
- Event mix: `6,199,200` Auctions and `95,050,800` Bids.
- Total input: `101,250,000` records per run.
- Baseline workload: `225,000` events/s for `150` s.
- Overload workload: `450,000` events/s for `150` s.
- Baseline target-operator parallelism: `4`.
- Terminal Kafka lag: `0` for all three runs.
- Decode/runtime failures: none in the measured runs.
- Sink: count/discard-only; results measure execution, not record-by-record output validation.

## Sponge

### Query and topology

- Runtime: Apache Nemo/Sponge with original behavior protected against commit `4c976182e4a4ae18d740143212f6881347d162b6e`.
- Query: standard Beam Nexmark Q6 through the HoloStream tuple adapter.
- Source: one logical unbounded Kafka root split into `8` tasks.
- Source split: `4` Auction partitions and `4` Bid partitions.
- Compute/target parallelism: `4`.
- Logical path:
  - Kafka source → tuple adapter → event filter/windowing.
  - Winning-bid `CoGroupByKey` → key by seller.
  - Accumulating window → `MovingMeanSellingPrice(10)` → count sink.

### Baseline placement

- Source executor: `node5`.
- Compute executors: `node9`, `node10`, `node11`, and `node12`.
- Source allocation: `55` vcores and `112` GiB memory.
- Compute allocation: `55` vcores and `112` GiB memory per executor.
- Placement: strict host placement; validation passed.
- ResourceManager JVM: Java 8.
- NodeManager and Nemo executor JVMs: Java 11.
- CPU normalization: `ec2=true`.

### Sponge policy

- Compile-time policy: original `StreamingR1R2R3Policy`.
- Rewrite validation: R1, R2, and R3 were all observed in the driver log.
- R1/R2/R3 predeployment: `20` transient task instances.
- Active scale trigger: estimated backlog delay greater than `2` s.
- Backlog-delay estimate: `queued records / source processing rate`.
- Decision interval: approximately `1` s after the initial observation guard.
- Routing formula: `min(0.95, 1 - processing rate / input rate + 0.1)`.
- CPU statistics were collected, but the active scale-out path used backlog delay.
- Baseline phase: no adaptive routing decision.

### Offload resources and decision

- Offload hosts: `node4`, `node6`, `node7`, and `node8`.
- Available offload pool: `108` one-core workers.
- Per-host offload workers: `27`.
- Trigger time: `261.3` s after live production began.
- Trigger delay: `98.1` s after the observed overload-phase start.
- Trigger observation: `900,000` queued records and `360,000` records/s processing rate.
- Estimated backlog delay at trigger: `2.5` s.
- Selected routing fraction: `0.30` (`30%`).
- Productively active offload workers after scaling: `32`.
- Log wording: “percent” denotes a routing fraction, not `0.3%`.

### Sponge results

- Observed overload-phase start: `163.1` s.
- Mean baseline source throughput: `224.8k` events/s.
- Mean overload source throughput: `448.8k` events/s.
- Mean baseline Kafka residence time: `57.8` ms.
- Mean pre-scale Kafka residence time: `78.5` ms.
- Mean post-scale Kafka residence time: `15.2` ms.
- Source-processed records: `101,250,000`.
- Final consumer lag: `0`.
- Outcome: sustained both offered rates with bounded Kafka residence time.

## HoloStream

### Common query and topology

- Query: standard HoloStream Nexmark Q6 over raw Auction and Bid events.
- Auction source parallelism: `4`.
- Bid source parallelism: `4`.
- Winning-bid/closed-auction operator parallelism: `4`.
- Target operator: `statefulMapper`.
- Target state: moving mean of the last `10` final prices per seller.
- Target parallelism: `4 → 8`.
- Sink parallelism: `1`.
- Logical path:
  - Auction source + Bid source → `closedAuction`/winning-bid join.
  - Key by seller ID → `statefulMapper` → count/discard sink.

### Placement

- Kafka brokers: `node1`, `node2`, and `node3`.
- Producers: four replicas on `node0`.
- Sources and sink: `node5`.
- Initial `closedAuction` tasks: one each on `node9`–`node12`.
- Initial `statefulMapper` tasks: one each on `node9`–`node12`.
- Added mapper tasks: one each on `node4`, `node6`, `node7`, and `node8`.
- Worker processes had unrestricted access to their assigned physical nodes.

### HoloStream scaling policy

- Scale-out type: configured time-triggered reconfiguration.
- Configured trigger: `258` s after application workload timing began.
- Target change: `statefulMapper`, parallelism `4 → 8`.
- Reconfiguration protocol: `lazy`.
- Protocol variant: `no-migration`.
- Partition policy: `consistent-even`.
- State partitions: `128` buckets.
- Post-scale allocation: `16` buckets per mapper.
- Scaling was scheduled rather than selected by an adaptive load policy.

## HoloStream with in-memory state

### Configuration

- State backend: in-memory.
- Observed scale-out time: `258.0` s.
- Added mapper nodes: `node4`, `node6`, `node7`, and `node8`.
- Reconfiguration control-path duration: approximately `134.1` ms.

### Results

- Mean baseline source throughput: `224.9k` events/s.
- Mean overload source throughput: `449.9k` events/s.
- Mean baseline Kafka residence time: `29.4` ms.
- Mean pre-scale Kafka residence time: `52.8` ms.
- Mean post-scale Kafka residence time: `55.0` ms.
- Source-consumed records: `101,250,000`.
- Final consumer lag: `0`.
- Outcome: sustained both offered rates with bounded Kafka residence time.

## HoloStream with Pebble state

### Configuration

- State backend: local Pebble.
- Controlled difference from the memory run: `StateBackendType` only.
- Pebble concurrent `GetMany`: disabled.
- State warmup: disabled.
- Observed scale-out time: `257.0` s.
- Reconfiguration control-path duration: approximately `131.0` ms.

### Results

- Mean baseline source throughput: `227.9k` events/s.
- Mean overload source throughput: `345.7k` events/s.
- Mean pre-scale overload throughput: `351.9k` events/s.
- Mean post-scale overload throughput: `324.7k` events/s.
- Mean baseline Kafka residence time: `42.1` ms.
- Mean pre-scale Kafka residence time: `5.04` s.
- Mean post-scale Kafka residence time: `14.84` s.
- Peak Kafka residence time: approximately `47` s.
- Backlogged input drained by approximately `345` s.
- Source-consumed records: `101,250,000`.
- Final consumer lag: `0`.
- Outcome: stable at `225k` events/s; unable to sustain `450k` events/s live.

## Comparison

- Sponge and memory-backed HoloStream sustained approximately `225k` and `450k` events/s.
- Their Kafka residence times remained bounded in the tens-of-milliseconds range.
- Sponge residence time decreased after native offloading.
- HoloStream memory residence time remained approximately flat after scale-out.
- Pebble accumulated a large Kafka backlog during the `450k` phase.
- Pebble completed all records only after draining accumulated work.
- Memory-backed HoloStream and Sponge are comparable in offered-load capacity for this workload.
- Pebble is not comparable at `450k` events/s under the tested cold-state configuration.

## Interpretation limits

- Inputs, rates, event mix, query semantics, and baseline parallelism were matched.
- The producer was rerun with the same pinned implementation and configuration; this was not a single immutable Kafka replay.
- Beam Q6 and HoloStream Q6 are semantically matched but have different physical execution graphs.
- Post-scale mechanisms are intentionally system-native, not resource-identical.
- HoloStream added four node-resident mapper tasks.
- Sponge routed `30%` of work to `32` active one-core workers.
- Results support a workload-capacity comparison, not a per-core efficiency claim.
- Source throughput measures Kafka ingestion, not final sink throughput.
- Count/discard sinks do not establish record-by-record output equivalence.

## Metrics and plots

- Aggregation interval: `5` s.
- Throughput: mean within each source task and interval, then sum across `8` tasks.
- Kafka residence: unweighted mean of valid source-task interval means.
- Kafka residence source: Kafka append timestamp sampled passively at the source.
- Sponge queue sampling: one sample per `1,000` records.
- Percentile and weighted-mean lines: excluded from the comparison plots.
- Scale-out markers: observed runtime decision/event timestamps.
- [Source throughput plot](comparison_input_throughput.png)
- [Kafka residence plot—linear scale](comparison_kafka_queue_residence_time_linear.png)
- [Kafka residence plot—log scale](comparison_kafka_queue_residence_time_log.png)
- [Numerical summary](comparison_summary.csv)
- [Machine-readable validation](comparison_validation.json)

## Run artifacts

- [Sponge run summary](../original-sponge-q6-scaleout-225k450k-20260813T061000Z/RUN_SUMMARY.md)
- [HoloStream memory run summary](../holostream-standard-q6-memory-225k450k-4to8-20260813T143500Z/RUN_SUMMARY.md)
- [HoloStream Pebble run summary](../holostream-standard-q6-pebble-225k450k-4to8-20260813T150500Z/RUN_SUMMARY.md)
