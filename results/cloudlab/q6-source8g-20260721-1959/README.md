# Sponge Q6 source-8G run

- Run ID/topic: `q6-source8g-20260721-1959`
- YARN application: `application_1784663909674_0001`
- ApplicationMaster host: `node10`
- Git commit: `8708fd400e0f3a8b9132a77e1d80a3828b47c010`
- Result: producer completed and all eight Kafka partitions reached zero committed lag.
- Cleanup: the unbounded streaming application was explicitly killed after drain.

## Configuration

- Source: one executor on `node5-link-1`, 8192 MB, capacity/slots 8.
- Compute: four executors on `node9-link-1` through `node12-link-1`, 8192 MB each, capacity/slots 3.
- Offload pool: 160 workers across `node4,node6,node7,node8,node13`.
- Traffic phases: 100 seconds at 20k events/s, 150 seconds at 100k events/s, and 150 seconds at 200k events/s.
- Autoscaler gate: phase 2 plus 60 seconds.
- Kafka topic timestamp: `message.timestamp.type=LogAppendTime`.

The producer sent 47,000,000 live events in 400.686 seconds (117,298.8 events/s average). The source executor did not OOM with its 8192 MB allocation. Strict executor placement passed. The AM was not pinned and landed on node10.

## Layout

- `workdir/`: combined metrics, producer phases/rates, placement verification, topic configuration, collector logs, and generated plots.
- `remote_metrics/node10/`: ApplicationMaster scaler, scaling-decision, aggregate-source, and task metrics.
- `remote_metrics/node5/`: source-executor metrics.
- `remote_metrics/node9/` through `node12/`: baseline executor task metrics.
- `offload_metrics/`: per-node VM-worker task metrics.
- `subscriber.log`: Nemo client/subscriber output.
- `executor.json`: exact source-8G executor configuration.

The scaling log records one `SCALE_OUT` shortly after phase 3 began and one post-production `SCALE_IN` decision.
