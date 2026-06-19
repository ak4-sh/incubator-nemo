# Nexmark Q0 Cap4 Autoscaler Run

Successful end-to-end Q0 Kafka source to Kafka sink run with Sponge-style VM offloading.

## Run IDs

- Application: `application_1781901266080_0001`
- Query: `0`
- Input topic: `nexmark-q0-cap4-153506`
- Result topic: `nexmark-q0-cap4-153506-results`
- Executor config: `configs/cloudlab/nemo-yarn-kafka-1source-cap4-8compute.json`
- Submit/work dir during run: `/tmp/nx-auto-nexmark-q0-cap4-153506`
- AM host during run: `node10`
- Date: 2026-06-19

## Benchmark Settings

- Total events: `8,000,000`
- Prefill events: `100`
- Live events: `7,999,900`
- Kafka partitions: `8`
- Producer parallelism: `8`
- First rate target: `50,000` events/s
- Next rate target: `200,000` events/s
- Rate period: `50` seconds
- CPU delay: `2` ms
- Source executor: 1 executor with `capacity: 4`, `slot: 4`
- Compute executors: `8`
- Warm VM workers: `200`

## Results

- Input Kafka offset total: `8,000,000`
- Result Kafka offset total: `8,000,000`
- Final source count: `8,000,000`
- Final Kafka lag: `0`
- Scale-out decisions: `1`
- Scale-in decisions: `1`
- Scale-out: `queue=64450`, `ratio=0.7421`, `numExecutors=179`
- Scale-in: `queue=-100`
- VM task metrics for this run: `node6=1687` rows, `node7=1720` rows

## Files

- `combined_metrics.csv`: collector timeline with Kafka offset, source count, lag, CPU, rates, queue, executor count.
- `producer_metrics.csv`: producer throughput timeline.
- `source.log`: producer cumulative source-log input updates.
- `scaling.txt`: scaling commands sent to the job.
- `metrics_collector.log`: collector log.
- `subscriber.log`: submit-side JobLauncher/subscriber log.
- `am-node10/scaling_decisions.csv`: AM-side scaling decisions.
- `am-node10/source_metrics.csv`: AM-side source count metrics.
- `am-node10/task_metrics.csv`: AM-side task metrics.
- `offload-task-metrics/*-task_metrics.csv`: per-offload-node VM task metrics.
- `scaling_decisions.csv`, `source_metrics.csv`: plot-compatible copies of AM-side metrics.
- `task_metrics.csv`: plot-compatible task metrics filtered to this run (`178190...` timestamps) from AM/offload node files.

## Plotting

The existing plotting script can be run from the repository root:

```bash
python3 scripts/cloudlab/plot_metrics.py results/cloudlab/nexmark-q0-cap4-153506
```

The script expects `scaling_decisions.csv` and `task_metrics.csv` directly under the work directory for some plots. Those plot-compatible files are present at the artifact root, while raw host-specific copies are preserved under `am-node10/` and `offload-task-metrics/`.
