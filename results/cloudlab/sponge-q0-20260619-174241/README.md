# Sponge Q0 Benchmark Run

Full Sponge-style Q0 Kafka source to Kafka sink benchmark with VM offloading.

## Run IDs

- Run ID: `sponge-q0-20260619-174241`
- Application: `application_1781901266080_0007`
- Query: `0`
- Input topic: `nexmark-sponge-q0-20260619-174241`
- Result topic: `nexmark-sponge-q0-20260619-174241-results`
- Executor config: `configs/cloudlab/nemo-yarn-kafka-1source-cap4-8compute.json`
- Work dir: `/tmp/nx-sponge-q0-20260619-174241`
- AM host: `node11`
- Date: 2026-06-19T17:57:04-05:00

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

- Input Kafka offset total: `78000000`
- Result Kafka offset total: `40029146`
- Final source count: `44109955`
- Scaling decisions: `1`
- Scale-out decisions: `1`
- Scale-in decisions: `0`
- VM task metric rows: `888`

## Files

- `combined_metrics.csv`: collector timeline.
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
python3 scripts/cloudlab/plot_metrics.py /users/akash01/incubator-nemo/results/cloudlab/sponge-q0-20260619-174241
```
