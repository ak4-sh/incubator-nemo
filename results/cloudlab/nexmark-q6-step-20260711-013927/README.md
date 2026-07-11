# nexmark-q6-step Benchmark Run

Kafka source benchmark with VM offloading.

## Run IDs

- Run ID: `nexmark-q6-step-20260711-013927`
- Application: `application_1783707667887_0012`
- Query: `6`
- Completion mode: `queue_drained`
- Input topic: `nexmark-nexmark-q6-step-20260711-013927`
- Result topic: `nexmark-nexmark-q6-step-20260711-013927-results`
- Kafka consumer group: `nexmark-q6-step-20260711-013927-consumer`
- Executor config: `configs/cloudlab/nemo-yarn-kafka-1source-8slot-12compute.json`
- Work dir: `/tmp/nx-nexmark-q6-step-20260711-013927`
- AM host: `node9`
- Date: 2026-07-11T01:52:26-06:00

## Settings

- Total events: `23850000`
- Prefill events: `0`
- First rate target: `50000` events/s
- Next rate target: `100000` events/s
- Rate period: `450` seconds
- CPU delay: `2` ms
- Completion mode: `queue_drained`
- Kafka partitions: `8`
- Producer parallelism: `8`
- Offload nodes: `node4,node6,node7,node8,node13`
- Workers per offload node: `32`
- Max VM executors: `160`
- Autoscaling: `true`
- Producer rate limited: `true`

## Results

- Input Kafka offset total: `23850000`
- Result Kafka offset total: `38788`
- Final source count: `23850000`
- Max input/source lag: `254470`
- Max input/result lag: `23811212`
- Max scaler queue: `354470`
- Max Kafka queue-time p95: `7854.577` ms
- Scaling decisions: `1`
- Scale-out decisions: `1`
- Scale-in decisions: `0`
- VM task metric rows: `3088`

## Files

- `combined_metrics.csv`: collector timeline.
- `source_aggregate_metrics.csv`: AM-side source progress timeline.
- `source_task_metrics.csv`: source-task queue time, idle time, and input rate.
- `scaler_metrics.csv`: periodic scaler state timeline.
- `producer_metrics.csv`: producer throughput timeline.
- `scaling_decisions.csv`: plot-compatible scaling decisions.
- `source_metrics.csv`: plot-compatible source metrics.
- `task_metrics.csv`: plot-compatible task metrics filtered to this run window.
- `am-node9/`: raw AM-side metrics.
- `offload-task-metrics/`: raw per-offload-node task metrics.
- `subscriber.log`: submit-side JobLauncher log.
- `harness.log`: benchmark wrapper/harness output.

## Plotting

From the repository root:

```bash
python3 scripts/cloudlab/plot_metrics.py /users/akash01/incubator-nemo/results/cloudlab/nexmark-q6-step-20260711-013927
```
