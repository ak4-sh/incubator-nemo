# nexmark-q6-warmup Benchmark Run

Kafka source benchmark with VM offloading.

## Run IDs

- Run ID: `nexmark-q6-warmup-20260626-165650`
- Application: `application_1782444527113_0006`
- Query: `6`
- Completion mode: `source_plus_some_output`
- Input topic: `nexmark-nexmark-q6-warmup-20260626-165650`
- Result topic: `nexmark-nexmark-q6-warmup-20260626-165650-results`
- Kafka consumer group: `nexmark-q6-warmup-20260626-165650-consumer`
- Executor config: `configs/cloudlab/nemo-yarn-kafka-1source-8slot-12compute.json`
- Work dir: `/tmp/nx-nexmark-q6-warmup-20260626-165650`
- AM host: `node5`
- Date: 2026-06-26T17:00:36-05:00

## Settings

- Total events: `2500000`
- Prefill events: `0`
- First rate target: `20000` events/s
- Next rate target: `20000` events/s
- Rate period: `1500` seconds
- CPU delay: `2` ms
- Completion mode: `source_plus_some_output`
- Kafka partitions: `8`
- Producer parallelism: `8`
- Offload nodes: `node4,node6,node7,node8,node13`
- Workers per offload node: `32`
- Max VM executors: `160`
- Autoscaling: `false`
- Producer rate limited: `false`

## Results

- Input Kafka offset total: `2500000`
- Result Kafka offset total: `76440`
- Final source count: `2500000`
- Max input/source lag: `0`
- Max input/result lag: `0`
- Max scaler queue: `6120`
- Max Kafka queue-time p95: `228.654` ms
- Scaling decisions: `0`
- Scale-out decisions: `0`
- Scale-in decisions: `0`
- VM task metric rows: `0`

## Files

- `combined_metrics.csv`: collector timeline.
- `source_aggregate_metrics.csv`: AM-side source progress timeline.
- `source_task_metrics.csv`: source-task queue time, idle time, and input rate.
- `scaler_metrics.csv`: periodic scaler state timeline.
- `producer_metrics.csv`: producer throughput timeline.
- `scaling_decisions.csv`: plot-compatible scaling decisions.
- `source_metrics.csv`: plot-compatible source metrics.
- `task_metrics.csv`: plot-compatible task metrics filtered to this run window.
- `am-node5/`: raw AM-side metrics.
- `offload-task-metrics/`: raw per-offload-node task metrics.
- `subscriber.log`: submit-side JobLauncher log.
- `harness.log`: benchmark wrapper/harness output.

## Plotting

From the repository root:

```bash
python3 scripts/cloudlab/plot_metrics.py /users/akash01/incubator-nemo/results/cloudlab/nexmark-q6-warmup-20260626-165650
```
