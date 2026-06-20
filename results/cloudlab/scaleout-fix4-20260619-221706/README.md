# Sponge Q0 Benchmark Run

Full Sponge-style Q0 Kafka source to Kafka sink benchmark with VM offloading.

## Run IDs

- Run ID: `scaleout-fix4-20260619-221706`
- Application: `application_1781911776702_0009`
- Query: `0`
- Input topic: `nexmark-scaleout-fix4-20260619-221706`
- Result topic: `nexmark-scaleout-fix4-20260619-221706-results`
- Executor config: `configs/cloudlab/nemo-yarn-kafka-1source-cap4-8compute.json`
- Work dir: `/tmp/nx-scaleout-fix4-20260619-221706`
- AM host: `node11`
- Date: 2026-06-19T22:22:40-05:00

## Settings

- Total events: `10000000`
- Prefill events: `100`
- First rate target: `50000` events/s
- Next rate target: `200000` events/s
- Rate period: `450` seconds
- CPU delay: `2` ms
- Kafka partitions: `8`
- Producer parallelism: `8`
- Offload nodes: `node4,node6,node7,node8,node13`
- Workers per offload node: `40`
- Max VM executors: `170`

## Results

- Input Kafka offset total: `10000000`
- Result Kafka offset total: `10000000`
- Final source count: `10000000`
- Max input/source lag: `2728847`
- Max input/result lag: `3823609`
- Max scaler queue: `16218104`
- Max Kafka queue-time p95: `23924.211` ms
- Scaling decisions: `2`
- Scale-out decisions: `1`
- Scale-in decisions: `1`
- VM task metric rows: `242`

## Files

- `combined_metrics.csv`: collector timeline.
- `source_aggregate_metrics.csv`: AM-side source progress timeline.
- `source_task_metrics.csv`: source-task queue time, idle time, and input rate.
- `scaler_metrics.csv`: periodic scaler state timeline.
- `producer_metrics.csv`: producer throughput timeline.
- `scaling_decisions.csv`: plot-compatible scaling decisions.
- `source_metrics.csv`: plot-compatible source metrics.
- `task_metrics.csv`: plot-compatible task metrics filtered to this run window.
- `am-node11/`: raw AM-side metrics.
- `offload-task-metrics/`: raw per-offload-node task metrics.
- `subscriber.log`: submit-side JobLauncher log.
- `harness.log`: benchmark wrapper/harness output.

## Plotting

From the repository root:

```bash
python3 scripts/cloudlab/plot_metrics.py /users/akash01/incubator-nemo/results/cloudlab/scaleout-fix4-20260619-221706
```
