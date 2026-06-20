# Sponge Q0 Benchmark Run

Full Sponge-style Q0 Kafka source to Kafka sink benchmark with VM offloading.

## Run IDs

- Run ID: `sponge-q0-20260619-213819`
- Application: `application_1781911776702_0005`
- Query: `0`
- Input topic: `nexmark-sponge-q0-20260619-213819`
- Result topic: `nexmark-sponge-q0-20260619-213819-results`
- Executor config: `configs/cloudlab/nemo-yarn-kafka-1source-cap4-8compute.json`
- Work dir: `/tmp/nx-sponge-q0-20260619-213819`
- AM host: `node11`
- Date: 2026-06-19T21:48:18-05:00

## Settings

- Total events: `23850000`
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

- Input Kafka offset total: `23850000`
- Result Kafka offset total: `6092124`
- Final source count: `7631796`
- Max input/source lag: `16218204`
- Max input/result lag: `17757876`
- Max scaler queue: `16218104`
- Max Kafka queue-time p95: `689.333` ms
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
- `am-node11/`: raw AM-side metrics.
- `offload-task-metrics/`: raw per-offload-node task metrics.
- `subscriber.log`: submit-side JobLauncher log.
- `harness.log`: benchmark wrapper/harness output.

## Plotting

From the repository root:

```bash
python3 scripts/cloudlab/plot_metrics.py /users/akash01/incubator-nemo/results/cloudlab/sponge-q0-20260619-213819
```
