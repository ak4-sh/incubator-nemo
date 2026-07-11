# nexmark-q6-main Benchmark Run

Kafka source benchmark with VM offloading.

## Run IDs

- Run ID: `nemo-q6-holostream-50k100k-20260710-011554`
- Application: `application_1783667641736_0001`
- Query: `6`
- Completion mode: `source_plus_some_output`
- Input topic: `nexmark-nemo-q6-holostream-50k100k-20260710-011554`
- Result topic: `nexmark-nemo-q6-holostream-50k100k-20260710-011554-results`
- Kafka consumer group: `nemo-q6-holostream-50k100k-20260710-011554-consumer`
- Executor config: `configs/cloudlab/nemo-yarn-kafka-1source-8slot-4compute.json`
- Work dir: `/tmp/nx-nemo-q6-holostream-50k100k-20260710-011554`
- AM host: `node5`
- Date: 2026-07-10T01:29:35-06:00

## Settings

- Total events: `23850000`
- Prefill events: `0`
- First rate target: `50000` events/s
- Next rate target: `100000` events/s
- Rate period: `450` seconds
- CPU delay: `2` ms
- Completion mode: `source_plus_some_output`
- Kafka partitions: `8`
- Producer parallelism: `8`
- Offload nodes: `node4,node6,node7,node8,node12,node13`
- Workers per offload node: `32`
- Max VM executors: `192`
- Autoscaling: `true`
- Producer rate limited: `true`

## Results

- Input Kafka offset total: `23850000`
- Result Kafka offset total: `0`
- Final source count: `609786`
- Max input/source lag: `23240214`
- Max input/result lag: `23850000`
- Max scaler queue: `23240214`
- Max Kafka queue-time p95: `118141.000` ms
- Scaling decisions: `1`
- Scale-out decisions: `1`
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
python3 scripts/cloudlab/plot_metrics.py /users/akash01/incubator-nemo/results/cloudlab/nemo-q6-holostream-50k100k-20260710-011554
```
