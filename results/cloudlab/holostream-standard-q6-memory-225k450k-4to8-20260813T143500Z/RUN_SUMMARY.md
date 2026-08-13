# HoloStream standard Q6 memory run — 225k to 450k events/s, 4 to 8 mappers

## Outcome

The matched HoloStream memory run completed successfully and passed its terminal checks.

- Result ID: `holostream-standard-q6-memory-225k450k-4to8-20260813T143500Z`
- HoloStream harness result: `Experiment Status: SUCCEEDED`
- Query: standard HoloStream Nexmark Query 6 over raw Auction and Bid records
- Total produced and source-consumed records: `101,250,000`
- Terminal Kafka consumer lag: `0`
- Decode/runtime panics during the measured run: `0`
- Metric database integrity: `ok`
- Cluster audit after cleanup: no HoloStream, producer, Kafka, HDFS, YARN, Nemo, or REEF process remained

This is the HoloStream counterpart to `../original-sponge-q6-scaleout-225k450k-20260813T061000Z`.

## Workload

The run used the exact HoloStream producer binary used by the Sponge run. Its SHA-256 was:

```text
67dd182d4367829ae227dfab8edb61fa64c26fecbe9a30b91c1c94a5f5ea1bf6
```

Four producer replicas produced one Auction partition and one Bid partition each.

| Phase | Aggregate rate | Configured duration | Per-replica Auction rate | Per-replica Bid rate |
|---|---:|---:|---:|---:|
| Baseline | 225,000 events/s | 150 s | 3,444 events/s | 52,806 events/s |
| Overload | 450,000 events/s | 150 s | 6,888 events/s | 105,612 events/s |

The first logged overload rates appeared at `08:41:24`. The producer emitted `6,199,200` Auction records and `95,050,800` Bid records. Each Auction partition ended at offset `1,549,800`; each Bid partition ended at offset `23,762,700`. The `disaggregated-streaming` consumer group reached every terminal offset.

## Logical pipeline and parallelism

```text
Auction Kafka source (4) ─┐
                          ├─> closedAuction / winning bid join (4)
Bid Kafka source (4) ─────┘
    -> keyBy sellerId
    -> statefulMapper / moving mean of last 10 final prices (4 -> 8)
    -> discard/count sink (1)
```

The target operator is `statefulMapper`. It began at parallelism 4 and scaled to 8. This is the standard raw-event Q6 path, not the earlier modified query whose Kafka source contained precomputed closed auctions.

## Placement and scaling

- Kafka brokers: `node1`, `node2`, `node3` (`10.10.1.2` through `10.10.1.4`).
- Four producers: `node0` (`10.10.1.1`).
- Eight source tasks and the sink: `node5` (`10.10.1.6`).
- Initial four `closedAuction` tasks and four `statefulMapper` tasks: one of each on `node9`, `node10`, `node11`, and `node12`.
- Added mapper tasks: one each on `node4`, `node6`, `node7`, and `node8`.

The configured rescale trigger was 258 seconds. The log records the transition at `08:43:12`:

```text
Worker num change: 4 -> 8
Worker list change details: [10 12 14 16] -> [10 12 14 16 17 18 19 20]
```

The 128 state buckets were evenly repartitioned to 16 buckets per mapper. All six lazy/no-migration reconfiguration steps completed in approximately 134 ms in total.

## Compatibility overlay

Standard Q6 requests a zero-byte dummy field. The unmodified HoloStream dummy-field generator dereferences element zero of an empty byte slice and panicked during the first smoke test. The run therefore used a build-time Go overlay which returns an empty string when the requested length is zero.

The overlay and build script live in the Sponge repository under `scripts/cloudlab/holostream/`. No HoloStream source file was changed. The build also pins and verifies the exact producer binary above.

## Telemetry validation

The portable SQLite snapshot contains:

- `204,048` metric rows;
- `21` operator instances;
- `27` metric types;
- `2,355` valid Kafka queue-residence samples;
- `1,332` valid `statefulMapper` processing-time samples;
- timestamps from `2026-08-13 08:38:49.770232518-06:00` through `2026-08-13 08:45:53.047685947-06:00`.

Kafka queue-residence values in this database are nanoseconds and must be divided by `1e6` for milliseconds. The original database is preserved with its WAL files; `metricCollector.snapshot.db` is the portable, transactionally consistent copy recommended for analysis.

## Artifact checksums

| Artifact | SHA-256 |
|---|---|
| `config.json` | `744ea4a5e69dfe4f8a7e52767359e8b6e69932844ff799b69dc10f21cc45d059` |
| `run.log` | `540471593dbba2e0669805343356d6e6a5193e0e7bf7d537d7ad3a4e3a4609d5` |
| `metricDB/metricCollector.db` | `e49db66228ec0838696e3ac183c187a7621eb8d63feaae2eb74df1dcfcdea755` |
| `metricDB/metricCollector.snapshot.db` | `5650d5051ad857f481ef8d8e043ff6188a34b78eae0b96643c55b2a708fd4f8e` |

## Comparison boundary

The two runs now use the same producer implementation, event mix, aggregate rates, durations, raw Auction/Bid input, standard Q6 semantics, four-way baseline target parallelism, and the same physical baseline hosts. Their scaling mechanisms intentionally remain system-native: HoloStream explicitly changes `statefulMapper` from 4 to 8 physical tasks, whereas Sponge uses its original R1/R2/R3 offloading path and native routing fraction across one-core VM workers. Consequently, the input and logical query are matched, while the post-scale physical execution graphs reflect the systems being compared.
