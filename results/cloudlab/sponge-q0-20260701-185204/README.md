# nexmark-q6-sponge-rate Benchmark Run

Kafka source benchmark with VM offloading.

## Run IDs

- Run ID: `sponge-q0-20260701-185204`
- Application: `application_1782940923440_0003`
- Query: `6`
- Completion mode: `source_plus_some_output`
- Input topic: `nexmark-sponge-q0-20260701-185204`
- Result topic: `nexmark-sponge-q0-20260701-185204-results`
- Kafka consumer group: `sponge-q0-20260701-185204-consumer`
- Executor config: `configs/cloudlab/nemo-yarn-kafka-1source-8slot-12compute.json`
- Work dir: `/tmp/nx-sponge-q0-20260701-185204`
- AM host: `node9`
- Date: 2026-07-01T19:09:33-05:00

## Settings

- Total events: `102000000`
- Prefill events: `0`
- First rate target: `50000` events/s
- Next rate target: `200000` events/s
- Rate period: `450` seconds
- CPU delay: `2` ms
- Completion mode: `source_plus_some_output`
- Kafka partitions: `8`
- Producer parallelism: `8`
- Offload nodes: `node4,node6,node7,node8,node13`
- Workers per offload node: `32`
- Max VM executors: `160`
- Autoscaling: `true`
- Producer rate limited: `true`

## Results

- Input Kafka offset total: `102000000`
- Result Kafka offset total: `179786`
- Final source count: `7606564`
- Max input/source lag: `94393436`
- Max input/result lag: `101820214`
- Max scaler queue: `59393436`
- Max Kafka queue-time p95: `4779.769` ms
- Scaling decisions: `1`
- Scale-out decisions: `1`
- Scale-in decisions: `0`
- VM task metric rows: `8072`

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
python3 scripts/cloudlab/plot_metrics.py /users/akash01/incubator-nemo/results/cloudlab/sponge-q0-20260701-185204
```
