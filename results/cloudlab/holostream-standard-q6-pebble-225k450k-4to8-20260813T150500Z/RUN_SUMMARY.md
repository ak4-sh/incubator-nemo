# HoloStream standard Q6 Pebble run — 225k to 450k events/s, 4 to 8 mappers

## Outcome

The Pebble counterpart to the matched HoloStream memory run completed successfully.

- Result ID: `holostream-standard-q6-pebble-225k450k-4to8-20260813T150500Z`
- HoloStream harness result: `Experiment Status: SUCCEEDED`
- Query: standard HoloStream Nexmark Query 6 over raw Auction and Bid records
- State backend: local Pebble
- Total produced and source-consumed records: `101,250,000`
- Terminal Kafka consumer lag: `0`
- Decode/runtime panics during the measured run: `0`
- Metric database integrity: `ok`
- Post-run cluster audit: clean

This run is paired with the memory result in `../holostream-standard-q6-memory-225k450k-4to8-20260813T143500Z` and the Sponge result in `../original-sponge-q6-scaleout-225k450k-20260813T061000Z`.

## Controlled difference from memory

The archived Pebble and memory configurations differ in exactly one field:

```json
"StateBackendType": "pebble"
```

The producer binary, input records, workload phases, topology, placement, reconfiguration time, partition policy, bucket count, and all other configuration fields were held fixed. Pebble concurrent GetMany and warmup were disabled in both configurations, so this is a backend-only cold-state comparison.

## Workload and topology

| Phase | Aggregate rate | Duration |
|---|---:|---:|
| Baseline | 225,000 events/s | 150 s |
| Overload | 450,000 events/s | 150 s |

The pinned producer SHA-256 was `67dd182d4367829ae227dfab8edb61fa64c26fecbe9a30b91c1c94a5f5ea1bf6`.

```text
Auction Kafka source (4) ─┐
                          ├─> closedAuction / winning bid join (4)
Bid Kafka source (4) ─────┘
    -> keyBy sellerId
    -> Pebble-backed statefulMapper / last-10 moving mean (4 -> 8)
    -> discard/count sink (1)
```

Four producer replicas emitted `6,199,200` Auction records and `95,050,800` Bid records. Each Auction partition ended at offset `1,549,800`; each Bid partition ended at `23,762,700`.

## Scaling result

The configured 258-second scale-out occurred at `09:12:38-06:00`:

```text
Worker num change: 4 -> 8
Worker list change details: [10 12 14 16] -> [10 12 14 16 17 18 19 20]
```

The new mapper tasks ran on `node4`, `node6`, `node7`, and `node8`. The 128 buckets were redistributed to 16 buckets per mapper. All six lazy/no-migration steps completed in approximately `131.0 ms`.

During the overload phase, Pebble sink throughput was variable and frequently below the approximately 27.5k/s closed-auction arrival rate. The accumulated work drained by approximately 345 seconds, well before the 420-second shutdown. Kafka offsets and consumer-group lag independently confirmed complete source consumption.

## Telemetry

The portable SQLite snapshot contains:

- `204,048` metric rows;
- `21` operator instances;
- `27` metric types;
- `2,536` valid Kafka queue-residence samples;
- `1,704` valid `statefulMapper` processing-time samples;
- timestamps from `2026-08-13 09:08:16.352924904-06:00` through `2026-08-13 09:15:19.631505833-06:00`.

Kafka queue-residence values are stored in nanoseconds and require division by `1e6` for milliseconds. Use `metricCollector.snapshot.db` for portable analysis; the original database and WAL are also preserved.

## Checksums

| Artifact | SHA-256 |
|---|---|
| `config.json` | `5c7c6ba0adb9c132a680061a87b949483f449793898e290da2d7389ed4468166` |
| `run.log` | `f6614459f054d4709f802890e5a44801f6258a34c8333b6ec5e39179c9c463d5` |
| `metricDB/metricCollector.db` | `7cf8803de50cc739fcac6f49909c9a337ea178fdc7ea0152e309828aa50e1510` |
| `metricDB/metricCollector.snapshot.db` | `daef867adcb17ad80d9e86a89e69b38955aad7714e3d5ecc796bb34e021dd390` |

## Operational note

An initial launch attempt stopped before Kafka or the workload started because the non-interactive shell lacked Go on `PATH`. The harness cleaned it up. The successful run documented here was relaunched with the explicit Go path, rebuilt and checksum-verified the overlay binaries, and is the only Pebble result archived locally.
