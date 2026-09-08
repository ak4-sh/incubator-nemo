# Sponge Q6 — Figure 9b-shaped 100K–550K run

- Job ID: `sponge-q6-figure9b-scaled-100k550k-20260813T214837Z`
- YARN application: `application_1786657206356_0001`
- Workload config: [`sponge_hardware_matched_q6_figure9b_scaled_100k550k.json`](../../../scripts/cloudlab/holostream/config/sponge_hardware_matched_q6_figure9b_scaled_100k550k.json)
- Config SHA-256: `709af4324fd5971c654ed3968575dd726c7af8d46c69d9f561b9605a36fbcd37`
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

- Baseline: 100K events/s for 120 seconds
- Figure 9b-shaped excursion: `100K, 130K, 213K, 325K, 438K, 520K, 550K, 520K, 438K, 325K, 213K, 130K`, 5 seconds per step
- Cooldown: 100K events/s for 60 seconds
- Scheduled duration: 240 seconds
- Total events: 37,510,000
- Producer wall duration including launcher and rate-limiter overhead: 260.914 seconds

## Validation

- Producer status: success
- Input offsets: 37,510,000 / 37,510,000
- Source-processed events: 37,510,000 / 37,510,000
- Terminal Kafka consumer lag: 0
- Evaluator error count: 0
- Strict placement: passed
- Raw AM and executor logs: preserved
- Final validation: passed

## Observed response

- Maximum measured source input rate: 519,910 events/s
- Maximum Kafka source lag (`inputOffset - sourceCount`): 317,332 events
- Maximum source Kafka residence, unweighted task mean: 93.114 ms
- Maximum source Kafka residence, sample-weighted mean: 131.337 ms
- Maximum source Kafka residence P95: 143.965 ms
- Maximum native Sponge queue-delay estimate: 0.715 seconds
- Native scale-out decisions: 0
- The queue and lag recovered during the descending/cooldown portion of the trace.
- The absence of scale-out is expected under the native policy: the internal queue-delay estimate never reached the 2-second trigger.

## Cleanup

- The unbounded YARN application was stopped after terminal validation.
- All 108 warm VMWorker processes were stopped.
- YARN was left with five healthy NodeManagers and zero running applications.

