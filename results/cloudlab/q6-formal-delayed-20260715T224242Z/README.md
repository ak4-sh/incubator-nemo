# CloudLab Sponge/Nemo Q6 Delayed-Gate Run

Run ID: `q6-formal-delayed-20260715T224242Z`
Topic: `q6-formal-delayed-20260715T224242Z-topic`
YARN application: `application_1784154419515_0001`
ApplicationMaster host: `node11`

## Outcome

Harness status: `0`.
Producer completed `47,000,000` live events in `400657 ms` at `117307.32 events/s`.
Final Kafka consumer-group lag was `0` on all 8 partitions.

The delayed-gate interpretation passed:

- Strict executor placement passed before live production.
- Scaler enabled during phase 2 after the configured 60-second delay.
- No `SCALE_OUT` occurred during the active 100k/s phase-2 observation window.
- First `SCALE_OUT` occurred after the 200k/s burst began.

## Timeline

- Phase 1 warmup start: `1784155628238`
- Phase 2 100k/s start: `1784155728260`
- Scaler enable: `1784155789272`, `61.012 s` after phase 2 start
- Phase 3 200k/s start: `1784155878284`
- First scale-out: `1784155879289`, `1.005 s` after phase 3 start
- Producer completion: `1784156028306`
- First observed committed consumer lag zero in `combined_metrics.csv`: `1784156042216`, `13.910 s` after producer completion
- Scale-in: `1784156035296`

## Active 100k Window

Window: scaler enable `1784155789272` through phase 3 start `1784155878284`.

- Max scaler queue: `187900`
- Max source Kafka queue avg: `361.84 ms`
- Max source Kafka queue max: `836.0 ms`
- Avg processing range: `90455.0` to `102822.6 events/s`
- No scaler decisions were recorded in this window.

## Scaling

First scale-out decision:

`1784155879289,SCALE_OUT,0.4935,200000.0000,99565.8000,284900.0000,284900,0.6022,4,QUEUE,0.8000,0.5965,1.2000,0.6000,2.8614,2.0000,4,160`

The decision log's `queueDelay` field was `2.8614` against threshold `2.0000`.
Scale-out added the prestarted pool of `160` lambda executors.
Scale-in occurred at `1784156035296` after drain/idle.

## Placement

Strict placement passed:

- Source `Executor0`: `node5-link-1`
- Compute `Executor1`: `node9-link-1`
- Compute `Executor2`: `node10-link-1`
- Compute `Executor3`: `node11-link-1`
- Compute `Executor4`: `node12-link-1`

Offload task metrics showed active VM execution on:

- `node7`: `VM-73` through `VM-79`, 7 tasks
- `node8`: `VM-110`, `VM-111`, `VM-112`, `VM-115`, `VM-116`, 5 tasks

The full offload pool was still prestarted as configured: 160 workers across `node4,node6,node7,node8,node13`.

## Metrics Notes

`scripts/cloudlab/metrics_collector.py` was fixed before this run to use `kafka-run-class.sh kafka.tools.GetOffsetShell` instead of the missing `kafka-get-offsets.sh`, and to append committed consumer lag fields to `combined_metrics.csv`.

The wrapper was manually interrupted after producer completion and after consumer lag had reached zero because its post-run drain loop summed the consumer-group `LOG-END-OFFSET` column instead of `LAG`. Cleanup and archive snapshots were refreshed manually afterward. Final YARN state shows zero active applications and five running NodeManagers with zero containers.
