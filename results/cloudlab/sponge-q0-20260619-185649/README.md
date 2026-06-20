# Sponge Q0 Benchmark Run

Full Sponge-style Q0 Kafka source to Kafka sink benchmark with VM offloading.

## Run IDs

- Run ID: `sponge-q0-20260619-185649`
- Application: `application_1781911776702_0001`
- Query: `0`
- Input topic: `nexmark-sponge-q0-20260619-185649`
- Result topic: `nexmark-sponge-q0-20260619-185649-results`
- Executor config: `configs/cloudlab/nemo-yarn-kafka-1source-cap4-8compute.json`
- Work dir: `/tmp/nx-sponge-q0-20260619-185649`
- AM host: `node11`
- Date: 2026-06-19T19:05:36-05:00

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
- Result Kafka offset total: `23850000`
- Final source count: `23850000` after filtering stale AM fallback metric rows
- Scaling decisions: `2`
- Scale-out decisions: `1`
- Scale-in decisions: `1`
- VM task metric rows: `663`

## Notes

- Raw AM `/tmp/source_metrics.csv` contained stale fallback rows from earlier runs (`8215025` and `78000000`). The Kafka result topic is the authoritative end-to-end completion signal for Q0 and reached `23850000`.
- Scale-out decision: `1781913628070,SCALE_OUT,0.1408,200000.0000,49939.2000,179554.0000,179554,0.8527,179`.
- Scale-in decision: `1781913768080,SCALE_IN,0.0130,0.0000,0.0000,-100.0000,-100,1.0000,179`.
- VM task metrics were observed for `VM-78`, `VM-79`, `VM-111`, and `VM-112`.

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
- `plots/`: generated PNG plots from `plot_metrics.py`.

## Plotting

From the repository root:

```bash
python3 scripts/cloudlab/plot_metrics.py /users/akash01/incubator-nemo/results/cloudlab/sponge-q0-20260619-185649
```
