# HoloStream-fed Sponge/Nemo Q6 Run

Run ID: `q6-holostream-metricsfix-20260728T233300Z`

YARN application: `application_1785281863435_0001`

Sponge implementation commit: `d89746299`

Beam adapter commit: `0d55405978` in the sibling `beam-fork` repository

## Outcome

This is the first full archived Sponge Query 6 run fed by the copied
HoloStream Nexmark producer and the HoloStream MUS-to-Beam adapter.

- Final validation: passed (`workdir/final_validation.json`)
- Produced and consumed events: `47,000,000`
- Auctions: `2,877,600`
- Bids: `44,122,400`
- Final committed Kafka consumer lag: `0`
- Producer status: `success`
- Producer lifecycle: `414,285 ms`
- Strict executor placement: passed
- Evaluator errors found by the archive scan: `0`
- Consolidated collector samples: `35`
- Raw per-node metrics preserved: yes

The source is intentionally unbounded. After production completed, offsets
drained, and terminal validation passed, the YARN application was deliberately
killed. Cleanup left no active YARN applications, five running baseline
NodeManagers with zero containers, and HDFS safe mode off.

## Workload

The eight producer replicas used the same copied HoloStream producer binary,
whose SHA-256 was:

`4da322aa859cbaf86f2f7d622ca8820447d5f7eca77dcfc856be23400d2d9038`

Each replica owned one Kafka partition for both topics. The three configured
phases were:

1. `20,000 events/s` for `100 s`
2. `100,000 events/s` for `150 s`
3. `200,000 events/s` for `150 s`

The exact launch environment is in `workdir/formal_command.env`; the generated
producer plan, configuration hash, per-replica results, and final per-partition
offsets are preserved in `workdir/holostream_producer_plan.json`,
`workdir/holostream_producer_completion.json`, and
`workdir/final_kafka_offsets.txt`.

## Placement and scaling

Strict placement put the Source executor on `node5-link-1` and one Compute
executor on each of `node9-link-1`, `node10-link-1`, `node11-link-1`, and
`node12-link-1`.

One queue-triggered scale-out decision was recorded:

`1785282335336,SCALE_OUT,0.1045,100000.0000,20263.6000,108682.0000,108682,0.8974,4,QUEUE,0.8000,0.2012,1.2000,0.6000,5.3634,2.0000,4,160`

The complete placement report, scaler series, task metrics, and container logs
are included under `workdir/`.

## Telemetry

Kafka input telemetry, committed consumer lag, source progress, CPU/scaler
state, task rates, and Kafka queue time are all present.

Across the consolidated collector samples:

- Maximum consumer lag: `998,547` events
- Maximum source queue-time P50: `278.489 ms`
- Maximum source queue-time P95: `349.128 ms`
- Maximum source queue-time P99: `1,104.523 ms`
- Final source queue-time P99 sample: `343.167 ms`

Kafka queue time is calculated from Kafka append timestamps. Beam event time
continues to come from the decoded HoloStream Nexmark tuple, so broker residence
time does not alter Query 6 event-time semantics.

This run uses a discarding sink. Result-topic offsets and end-to-end result
latency are therefore not applicable; no `kafka_result_lag.png` is included.

## Archive layout

- `workdir/final_validation.json`: machine-readable terminal checks
- `workdir/combined_metrics.csv`: consolidated monitoring samples
- `workdir/kafka_input_telemetry.csv`: independent Kafka input-offset series
- `workdir/kafka_topic_metrics.csv`: per-topic offset/rate/lag series
- `workdir/source_task_metrics.csv`: consolidated source metrics
- `workdir/task_metrics.csv`: consolidated baseline and offload task metrics
- `workdir/remote_tmp_metrics/`: untouched per-node raw metric files
- `workdir/container_logs/`: captured YARN container logs
- `workdir/plots/`: generated plots
- `workdir/*before_cleanup*` and `workdir/*after_cleanup*`: cluster state

`SHA256SUMS` covers every archived file except the manifest itself.
