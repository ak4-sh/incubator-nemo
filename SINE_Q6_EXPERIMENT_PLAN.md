# Matched Nexmark Q6 stepped-sine experiment

> **Historical plan; scope clarified 2026-09-08.** The 375-second, 15-second-step
> schedule below describes the original proposal and the archived short run, not
> the current default configuration. The committed matched producer/memory/Pebble
> JSONs now use a 930-second trace (209,700,000 events), 45-second intermediate
> phases, a 150-second peak, and HoloStream transitions at 300/630 seconds. The
> validator checks those current JSONs. The separate peak45 and Figure 9b configs
> preserve other workload variants; consult each run's producer plan for its input.
>
> Rewriting the graph with R1/R2/R3 does not by itself establish that the automatic
> controller invoked R3 redirection. See the [migration investigation](SPONGE_Q6_MIGRATION_REINVESTIGATION_20260907.md)
> and [archive outcome index](results/cloudlab/SINE_REACTIVATION_ARCHIVE_20260908.md)
> before interpreting the historical scaling-methodology claims below. The paired
> HoloStream sine configurations are plans, not evidence that those runs completed.

## Purpose

- Reproduce the shape of Sponge Figure 9b with a capacity-adjusted stepped sine trace.
- Compare Sponge, HoloStream memory, and HoloStream Pebble on the same raw Nexmark input.
- Preserve the standard Query 6 logical pipeline and the hardware placement from the successful paired runs.
- Keep HoloStream source unchanged. All new configuration and orchestration lives in this repository.

## Fixed workload

- Producer implementation: the unchanged HoloStream Nexmark producer used by all three runs.
- Producer replicas: `4`.
- Kafka inputs: `4` Auction partitions and `4` Bid partitions.
- Nexmark mix: Auction/Bid proportions derived from the existing `3:46` generator configuration.
- Workload duration: `375` seconds.
- Total records: `60,150,000`.
- Shape: long low-rate baseline, seven rising plateaus, one peak, six falling plateaus, and a final low-rate observation period.
- Fifteen-second intermediate plateaus are three times longer than the approximately five-second steps visible in Figure 9b, allowing this cluster's telemetry and control paths to observe each level.

| Phase | Elapsed interval | Aggregate rate (events/s) | Per-replica Auction rate | Per-replica Bid rate |
|---:|---:|---:|---:|---:|
| 1 | 0–120 s | 60,000 | 918 | 14,082 |
| 2 | 120–135 s | 90,000 | 1,378 | 21,122 |
| 3 | 135–150 s | 140,000 | 2,143 | 32,857 |
| 4 | 150–165 s | 200,000 | 3,061 | 46,939 |
| 5 | 165–180 s | 260,000 | 3,980 | 61,020 |
| 6 | 180–195 s | 330,000 | 5,051 | 77,449 |
| 7 | 195–210 s | 400,000 | 6,122 | 93,878 |
| 8 | 210–225 s | 450,000 | 6,888 | 105,612 |
| 9 | 225–240 s | 400,000 | 6,122 | 93,878 |
| 10 | 240–255 s | 330,000 | 5,051 | 77,449 |
| 11 | 255–270 s | 260,000 | 3,980 | 61,020 |
| 12 | 270–285 s | 200,000 | 3,061 | 46,939 |
| 13 | 285–300 s | 140,000 | 2,143 | 32,857 |
| 14 | 300–315 s | 90,000 | 1,378 | 21,122 |
| 15 | 315–375 s | 60,000 | 918 | 14,082 |

The total rate is exact in every phase: four times the sum of the per-replica Auction and Bid rates equals the stated aggregate rate.

## Logical query and baseline placement

```text
Auction Kafka source (4) ─┐
                          ├─> closedAuction / winning-bid join (4)
Bid Kafka source (4) ─────┘
    -> keyBy sellerId
    -> statefulMapper / moving mean of last 10 final prices (baseline 4)
    -> discard/count sink (1)
```

- Source and sink host: `node5`.
- Four baseline compute hosts: `node9`, `node10`, `node11`, and `node12`.
- Four scale-out hosts: `node4`, `node6`, `node7`, and `node8`.
- Sponge scale-out capacity: at most `27` one-core VM workers per scale-out host, `108` total.
- HoloStream scale-out placement: one additional `statefulMapper` on each scale-out host.

## Scaling methodology

### Sponge

- Use the original Sponge R1/R2/R3 graph-rewriting and scaling behavior.
- Arm the native scaler before live production begins with `SCALER_START_MODE=immediate`.
- Do not force a scale-out or scale-in at a workload boundary.
- Record native decisions, routing fractions, requested workers, connected workers, and active VM workers.
- Keep the target compute operator at baseline parallelism `4`; offloaded one-core VM workers are additional resources selected by Sponge.

### HoloStream memory and Pebble

- Use identical deterministic time-triggered schedules for both backends.
- Scale `statefulMapper` from `4` to `8` at `t=180 s`, when the rising trace first exceeds the `300k events/s` policy boundary.
- Scale `statefulMapper` from `8` to `4` at `t=255 s`, when the falling trace first drops below that boundary.
- Use lazy/no-migration reconfiguration, 128 buckets, and consistent-even partitioning.
- Do not retime either HoloStream transition after inspecting Sponge results.

### Interpretation boundary

- Input, query, baseline topology, and physical host allocation are controlled.
- Scaling policies are intentionally system-specific: Sponge decides natively, while HoloStream receives declared time-triggered reconfigurations.
- Therefore, results characterize each system's end-to-end response under its documented policy. They must not be presented as an isolated comparison of automatic policy quality.
- Memory versus Pebble is a backend-only comparison: their configurations differ only in `StateBackendType`.

## Configuration files

- Sponge launcher: `scripts/cloudlab/run_original_sponge_q6_sine.sh`
- Shared Sponge producer input: `scripts/cloudlab/holostream/config/sponge_hardware_matched_q6_sine_60k450k.json`
- HoloStream memory: `scripts/cloudlab/holostream/configs/standard_q6_memory_sine_60k450k_4to8to4.json`
- HoloStream Pebble: `scripts/cloudlab/holostream/configs/standard_q6_pebble_sine_60k450k_4to8to4.json`

Validate all three configurations before deployment:

```bash
python3 scripts/cloudlab/validate_sine_q6_configs.py
bash -n scripts/cloudlab/run_original_sponge_q6_sine.sh
```
