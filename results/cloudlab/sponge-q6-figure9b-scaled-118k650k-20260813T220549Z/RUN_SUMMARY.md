# Sponge Q6 — Figure 9b-shaped 118K–650K run

- Job ID: `sponge-q6-figure9b-scaled-118k650k-20260813T220549Z`
- YARN application: `application_1786658675876_0001`
- Workload config: [`sponge_hardware_matched_q6_figure9b_scaled_118k650k.json`](../../../scripts/cloudlab/holostream/config/sponge_hardware_matched_q6_figure9b_scaled_118k650k.json)
- Config SHA-256: `8f5a9b89a18398b388d7c4db342dee9a191872b9536306d137ee4e41e6ad097d`
- Shape scaling: every rate from the preceding 100K–550K trace was multiplied by `650/550`; timings were unchanged
- Producer: unchanged shared HoloStream Go producer, four replicas on `node0`
- Query: Beam Nexmark Q6 through Nemo/Sponge, unbounded Kafka source, count-only sink
- Baseline placement: one source executor on `node5`; four compute executors on `node9`–`node12`
- Source tasks: 8
- Q6 target parallelism: 4
- Native Sponge autoscaling: enabled from the start; no manual phase-triggered command
- Offload capacity: 108 one-core VM workers, 27 each on `node4`, `node6`, `node7`, and `node8`
- Original R1/R2/R3 rewrites: verified
- EC2 CPU normalization: enabled

## Workload

- Baseline: 118,180 events/s for 120 seconds
- Figure 9b-shaped excursion: `118,180; 153,636; 251,728; 384,092; 517,636; 614,544; 650,000; 614,544; 517,636; 384,092; 251,728; 153,636` events/s, 5 seconds per step
- Cooldown: 118,180 events/s for 60 seconds
- Scheduled duration: 240 seconds
- Total events: 44,329,660
- Producer wall duration including launcher and rate-limiter overhead: 258.191 seconds

## Validation

- Producer status: success
- Input offsets: 44,329,660 / 44,329,660
- Source-processed events: 44,329,660 / 44,329,660
- Terminal Kafka consumer lag: 0
- Evaluator error count: 0
- Strict placement: passed
- Raw AM and executor logs: preserved
- Final validation: passed

## Observed response

- Maximum 16-second collector sample of source input rate: 614,544 events/s
- Maximum 16-second collector sample of source processing rate: 625,492 events/s
- Maximum Kafka source lag (`inputOffset - sourceCount`): 484,339 events
- Maximum source Kafka residence, unweighted task mean: 111.400 ms
- Maximum source Kafka residence, sample-weighted mean: 150.604 ms
- Maximum source Kafka residence P95: 157.643 ms
- Maximum native Sponge queue-delay estimate: 1.140 seconds
- Native scale-out decisions: 0
- The queue and lag recovered during the descending and cooldown portions of the trace.
- The native queue-delay estimate remained below Sponge's unchanged 2-second scale-out trigger.

## Cleanup

- The unbounded YARN application was stopped after terminal validation.
- All warm VMWorker and REEF processes were stopped.
- The ResourceManager and five NodeManagers were restarted and verified healthy.
- The cluster was left with zero running applications.

