# Sponge Q6 sine run — 45-second peak

- Status: successful; terminal validation passed.
- Application: `application_1786654396454_0001`.
- Workload: 60K/s baseline, 15-second surrounding sine phases, 450K/s for 45 seconds, then a 60K/s cooldown.
- Expected, produced, and source-processed events: 73,650,000.
- Terminal Kafka consumer lag: 0.
- Evaluator errors: 0.
- Native scaler: enabled immediately.
- Native scale-out decisions: 0.
- Peak source input and processing rates: 450,000 events/s.
- Maximum source Kafka residence-time unweighted mean: 96.64 ms.
- Maximum source Kafka residence-time P95/P99: 147.74 ms.
- Maximum instantaneous Kafka consumer lag: 2,247,865 events; drained to zero.
- Placement: one source executor on node5 and four compute executors on nodes9–12.
- Source tasks: 8; Q6 target compute parallelism: 4.
- Warm offload pool: 108 one-core workers, 27 each on nodes4, node6, node7, and node8.
- Nemo client SHA-256: `50bf7da1afb25eda4aa948489950c23a83a2f7e46be29a9858967ef62820b077`.
- Nexmark SHA-256: `4f4912ff4da1e77a6a350bc4fd2859deb19bda2777b395bb70f45d3ace9b16b6`.
- HoloStream producer SHA-256: `67dd182d4367829ae227dfab8edb61fa64c26fecbe9a30b91c1c94a5f5ea1bf6`.
- Workload config SHA-256: `a05014edd57a9adadbfd0f5f03654568f2e5c4dba34708451739d0f5fc6735db`.

The configured peak duration is 45 seconds. The consolidated phase-marker span between the first recorded 450K and descending 400K markers is 42.147 seconds because phase markers are emitted asynchronously by the four producer replicas; event quotas and terminal offsets are exact.
