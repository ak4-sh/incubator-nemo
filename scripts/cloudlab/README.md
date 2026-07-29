# CloudLab NEXMark Q8 Reproduction Scripts

This directory preserves the CloudLab/Nemo/Kafka scripts used for the Sponge-style NEXMark Q8 reproduction.

## Build

From the repo root:

```bash
scripts/cloudlab/build_cloudlab_java8.sh
```

This builds Nemo with Java 8 bytecode and compiles the standalone Kafka producer.

Important outputs:

- `client/target/nemo-client-0.2-SNAPSHOT-shaded.jar`
- `examples/nexmark/target/nexmark-0.2-SNAPSHOT-shaded.jar`
- `offloading/workers/vm/target/offloading-vm-0.2-SNAPSHOT-shaded.jar`
- `build/cloudlab-producer/StandaloneNexmarkKafkaProducer.class`

## Baseline Live Q8

```bash
scripts/cloudlab/run_live_bursty_combined.sh
```

Useful overrides:

```bash
TOTAL_EVENTS=10000 TOPIC=nexmark-live-test scripts/cloudlab/run_live_bursty_combined.sh
```

The script creates a topic, pre-fills 100 records to avoid the Beam empty-topic timestamp crash, starts Q8 in `SUBSCRIBE_ONLY`, then writes live bursty records with the standalone producer.

## Subscriber Only

```bash
TOPIC=nexmark-q8-bursty-100k scripts/cloudlab/run_q8_subscriber_yarn.sh
```

For the known working 1-source/8-compute layout, use:

```bash
EXECUTOR_JSON=configs/cloudlab/nemo-yarn-kafka-1source-8slot-12compute.json \
TOPIC=nexmark-q8-bursty-100k \
scripts/cloudlab/run_q8_subscriber_yarn.sh
```

## Standalone Producer

Compile:

```bash
scripts/cloudlab/compile_standalone_producer.sh
```

Legacy BURSTY mode:

```bash
source scripts/cloudlab/cloudlab_env.sh
java -cp "$STANDALONE_PRODUCER_CP" StandaloneNexmarkKafkaProducer \
  node7:9092,node8:9092,node9:9092 topic-name 10000 1000 50000 5 true
```

Two-phase mode:

```bash
source scripts/cloudlab/cloudlab_env.sh
java -cp "$STANDALONE_PRODUCER_CP" StandaloneNexmarkKafkaProducer \
  node7:9092,node8:9092,node9:9092 topic-name 9000 1000 1000 50000 true
```

## Warm VM Worker Pool

On a worker node:

```bash
scripts/cloudlab/start_warm_pool.sh 25321 4 \
  /users/akash01/incubator-nemo/offloading/workers/vm/target/offloading-vm-0.2-SNAPSHOT-shaded.jar \
  10000000
```

Generate `vm_addresses.txt` in the repo root:

```bash
scripts/cloudlab/generate_vm_addresses.py --nodes node10 --first-port 25321 --workers-per-node 4 --output vm_addresses.txt
```

`JobLauncher` reads literal `vm_addresses.txt` from the current working directory.

## CloudLab VM Offload Smoke Test

```bash
scripts/cloudlab/force_cloudlab_vm_offload_smoke.sh
```

This creates a topic, starts warm VMWorkers, launches a Q8 subscriber with `-offloading_type cloudlab-vm`, then appends `add-lambda-executor 1 1 1 1024` to `scaling.txt`.

Validated behavior so far:

- Driver connects to a warm worker such as `node10:25321`.
- VMWorker receives `SEND_ADDRESS`.
- VMWorker opens a control channel back to the driver.

Current blocker:

- `LambdaContainerManager` times out waiting for `WORKER_INIT_DONE`.
- Error: `Cannot init lambda container for 1/ VM-1`.
- Next files to inspect: `runtime/lambda-executor/src/main/java/org/apache/nemo/runtime/lambdaexecutor/OffloadingHandler.java` and `runtime/master/src/main/java/org/apache/nemo/runtime/master/lambda/LambdaContainerManager.java`.

## Safety Notes

- Do not run `$HADOOP_HOME/sbin/stop-yarn.sh` during experiments.
- Kill apps with `yarn application -kill <APP_ID>`.
- Kafka CLI tools are on broker nodes such as `node7`; scripts use `ssh -A node7`.
- Older Q8 scripts assumed Java 8 everywhere. For the current Sponge/Q6 branch, use the Java split documented below.

## Sponge/Q6 CloudLab Runbook

Last validated: 2026-07-15 UTC, on the 14-node CloudLab cluster.

### Intended Cluster Roles

The current Sponge/Q6 setup uses five baseline YARN worker nodes:

```text
node5
node9
node10
node11
node12
```

The offload VM worker pool nodes are:

```text
node4
node6
node7
node8
node13
```

`node0` is the control node and ResourceManager host. Kafka brokers are separate from the baseline YARN workers. The live Hadoop worker file should match the five baseline nodes:

```bash
cat /users/akash01/hadoop/etc/hadoop/slaves
```

Expected:

```text
node5
node9
node10
node11
node12
```

Do not let `start-yarn.sh` start NodeManagers on `node4,node6,node7,node8,node13`; those nodes are reserved for offload JVM workers.

### Java Runtime Split

Use Java 8 for the ResourceManager and Java 11 for NodeManagers/executor containers.

The current `scripts/cloudlab/cloudlab_env.sh` defaults to:

```bash
JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

When starting the ResourceManager manually, force Java 8:

```bash
ssh -o BatchMode=yes node0 \
  "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64; \
   export PATH=\$JAVA_HOME/bin:\$PATH; \
   /users/akash01/hadoop/sbin/yarn-daemon.sh start resourcemanager"
```

### Clean YARN Restart

Before restarting, make sure `/users/akash01/hadoop/etc/hadoop/slaves` contains only the five baseline nodes listed above.

Stop YARN:

```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
/users/akash01/hadoop/sbin/stop-yarn.sh
```

If `stop-yarn.sh` reports `no resourcemanager to stop`, check and stop the RM explicitly:

```bash
ssh -o BatchMode=yes node0 "jps -lv | grep ResourceManager || true"
ssh -o BatchMode=yes node0 "kill <RESOURCE_MANAGER_PID>"
```

Start YARN:

```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
/users/akash01/hadoop/sbin/start-yarn.sh
```

If the NodeManagers start but the ResourceManager is not listening on `8032`, start only the RM again:

```bash
ssh -o BatchMode=yes node0 \
  "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64; \
   export PATH=\$JAVA_HOME/bin:\$PATH; \
   /users/akash01/hadoop/sbin/yarn-daemon.sh start resourcemanager"
```

Verify:

```bash
yarn node -list
yarn application -list
ssh -o BatchMode=yes node0 \
  "curl -s -m 3 http://10.10.1.1:8088/ws/v1/cluster/info"
```

Expected healthy state:

```text
Total Nodes: 5
RUNNING: node5, node9, node10, node11, node12
Running applications: 0 before a new test
ResourceManager: ACTIVE
```

Confirm no NodeManagers are running on offload nodes:

```bash
for n in node4 node6 node7 node8 node13; do
  echo "===$n==="
  ssh -o BatchMode=yes "$n" "jps -lv | grep NodeManager || true"
done
```

Expected: no `NodeManager` lines.

### Executor Config for Q6

Use:

```text
configs/cloudlab/nemo-yarn-kafka-1source-8slot-4compute-8g-cap3.json
```

Current contents:

```json
[
  { "type": "Transient", "memory_mb": 768,  "capacity": 1, "slot": 1, "num": 0 },
  { "type": "Reserved",  "memory_mb": 768,  "capacity": 1, "slot": 1, "num": 0 },
  { "type": "Source",    "memory_mb": 4096, "capacity": 8, "slot": 8, "num": 1 },
  { "type": "Compute",   "memory_mb": 8192, "capacity": 3, "slot": 3, "num": 4 }
]
```

This creates one source executor and four compute executors. Each compute executor is one YARN container with `capacity=3` and `slot=3`, so each compute container can host three logical executor slots. Total compute logical slots: `4 * 3 = 12`.

The `capacity=3,slot=3` setting matters for Q6 because the streaming stages are long-lived. With `capacity=1,slot=1`, upstream stages can occupy all compute slots and starve Stage2/Stage3 even when the Pado scheduler fix is present.

### Known Scheduler Fix

The branch contains the Pado scheduler deadlock fix:

```text
3f663b918 Fix Pado streaming scheduler deadlock: upstream stages first
```

That commit makes streaming scheduling upstream-first by topological order and dispatches stage tasks by ascending stage ID. The cap3 executor config is still needed because the scheduler cannot place later long-running stages if the cluster exposes too few logical slots.

### 5k/100k Q6 Smoke Run

Known working command:

```bash
QUERY=6 \
EXECUTOR_JSON=/users/akash01/incubator-nemo/configs/cloudlab/nemo-yarn-kafka-1source-8slot-4compute-8g-cap3.json \
AUTOSCALING=true \
TOTAL_EVENTS=100000 \
PREFILL_EVENTS=0 \
FIRST_RATE=5000 \
NEXT_RATE=5000 \
RATE_PERIOD_SEC=20 \
PRODUCER_RATE_LIMITED=true \
scripts/cloudlab/run_autoscaler_smoke.sh
```

The streaming app does not naturally exit just because the producer reaches `TOTAL_EVENTS`; kill the YARN app after confirming metrics:

To consume the unchanged HoloStream producer's MUS tuples for raw-event Query
6, select the HoloStream producer implementation. The harness validates the
Sponge-owned copy of the producer configuration, derives the Auction and Bid
counts and phase schedule, and invokes the supervised launcher directly:

```bash
PRODUCER_IMPL=holostream \
QUERY=6 \
KAFKA_PARTITIONS=8 \
PRODUCER_PARALLELISM=8 \
HOLOSTREAM_CONFIG="$PWD/scripts/cloudlab/holostream/config/sponge_q6_formal.json" \
HOLOSTREAM_RESET_TOPICS=true \
HOLOSTREAM_EXPECTED_PRODUCER_SHA256=<sha256-from-holostream-run> \
scripts/cloudlab/run_autoscaler_smoke.sh
```

The configuration must use the unchanged HoloStream schema with exactly one
Auction and one Bid logical source. Do not add `Topic` or `ExitOnCompletion`.
The producer writes the fixed `nexmark-auction` and `nexmark-bid` topics. The
launcher waits for exact per-partition offsets and then terminates the upstream
producer processes, whose `Run()` method intentionally remains alive after
bounded generation.

In HoloStream mode, the harness creates and verifies both typed topics with
`KAFKA_PARTITIONS` partitions and
`message.timestamp.type=LogAppendTime`. `PRODUCER_PARALLELISM` must equal
`KAFKA_PARTITIONS` and the configuration's `ProducerIPs` length. HoloStream
mode defaults to no prefill and requires
`HOLOSTREAM_EXPECTED_PRODUCER_SHA256`, taken from the producer binary used for
the corresponding HoloStream run. Because the upstream producer uses fixed
topic names, the harness also requires the explicit destructive authorization
`HOLOSTREAM_RESET_TOPICS=true`. It holds a fixed-topic lock for the entire run,
records the old topic metadata, deletes and recreates only the two allowed
topics, and verifies that every recreated partition starts at offset zero.

Each run receives a unique Kafka consumer group derived from `JOB_ID`, so
committed offsets from a previous incarnation of the fixed topics cannot skip
new records. The machine-readable plan is written to
`workdir/holostream_producer_plan.json`, and the reset evidence is written to
`workdir/holostream_topic_reset.json`; completion details and replica logs are
written alongside them.

For autoscaler input telemetry, the harness does not parse producer output.
Instead, `KafkaInputOffsetTelemetry` samples the end offsets of both typed
topics once per second and writes the cumulative total in the legacy
`N events` format to `workdir/source.log`. The producer supervisor's
human-readable output is kept separately in
`workdir/holostream_launcher.log`, and every Kafka sample is recorded in
`workdir/kafka_input_telemetry.csv`. The run fails before benchmarking if the
topics do not begin at offset zero, the sampler exits early, or the scaler does
not report a positive input rate after production starts. At completion, the
sampler and producer supervisor must both succeed and the final sampled total
must exactly match the plan.

The checked-in eight-replica formal comparison configuration is:

```text
scripts/cloudlab/holostream/config/sponge_q6_formal.json
```

It reproduces the archived Sponge Q6 arrival schedule: 100 seconds at 20k,
150 seconds at 100k, and 150 seconds at 200k events/second. Auction and Bid
rates use the 3:46 split and produce exactly 47,000,000 records: 2,877,600
Auctions and 44,122,400 Bids. If deletion succeeds but topic recreation or
zero-offset verification fails, the harness aborts before starting Sponge's
producer launcher and preserves the partial-reset error in the reset manifest.

```bash
yarn application -list
yarn application -kill <APP_ID>
```

For the validated 5k/100k run, the producer wrote 100000 events and the source reported `Source event 100000`. Aggregated stage metrics showed:

```text
Stage0 inputP=100000 output=100000
Stage1 inputP=100000 output=98000
Stage2 inputP=98000  output=2532
Stage3 inputP=2532   output=0
```

Stage3 producing zero output is not by itself a runtime failure for this smoke test; the important signal is that Stage2 and Stage3 receive and process input instead of being starved.

### Failure Signatures

Netty mismatch from an old/stale shaded artifact:

```text
java.lang.NoSuchMethodError: io.netty.util.Recycler$Handle.recycle(Ljava/lang/Object;)V
```

AWS/Apache HTTP dependency mismatch from an old/stale shaded artifact:

```text
java.lang.NoSuchFieldError: INSTANCE
at org.apache.http.conn.ssl.SSLConnectionSocketFactory
```

If these reappear, first verify the submitted shaded JAR, not the memory config:

```bash
JAR=client/target/nemo-client-0.2-SNAPSHOT-shaded.jar
jar tf "$JAR" | grep '^org/apache/nemo/shaded/apache/http/' | head
jar tf "$JAR" | grep '^org/apache/http/conn/ssl/SSLConnectionSocketFactory.class'
javap -classpath "$JAR" -verbose \
  com.amazonaws.http.apache.client.impl.ApacheConnectionManagerFactory \
  | grep -E 'org/apache/.*/http|org/apache/http'
```

Expected:

- relocated HTTP classes exist under `org/apache/nemo/shaded/apache/http/`
- no unrelocated `org/apache/http/conn/ssl/SSLConnectionSocketFactory.class`
- AWS bytecode references the relocated package

For Q6 heap pressure, do not use the old 1 GB compute config. Use 8 GB compute executors as in the cap3 JSON above.
The Source executor is 4 GB because the 2 GB Source configuration reached heap OOM after scale-out in the 200k ev/s phase.

### Formal Run Plots

Generate reproducible plots from a formal run archive with:

```bash
python3 scripts/cloudlab/plot_formal_run.py \
  results/cloudlab/q6-formal-delayed-20260715T224242Z
```

The script writes PNGs under the archive's `plots/` directory. It resolves the formal archive layout directly:

- `workdir/combined_metrics.csv`
- `workdir/kafka_topic_metrics.csv` (new runs; one row per Kafka topic/sample)
- `workdir/kafka_input_topics.json`
- `workdir/producer_metrics.csv`
- `workdir/producer_phases.csv`
- `workdir/scaler_enable.csv`
- `formal_metrics/scaler_metrics_am_*.csv`
- `formal_metrics/scaling_decisions_am_*.csv`
- `formal_metrics/task_metrics_*.csv`
- `node_metrics/node*.csv`

Kafka lag and offset plots use the real aggregate fields in
`workdir/combined_metrics.csv`: `inputLag`, `consumerLag`, `inputOffset`,
`consumerCommittedOffset`, and `consumerLogEndOffset`. For HoloStream runs,
these fields are sums across Auction and Bid only when both topics returned
valid data; otherwise they are `-1`. `workdir/kafka_topic_metrics.csv` retains
the individual topic values and drives the Auction, Bid, and aggregate
rate/lag plots. Beam-mode runs continue to use the same schema with one input
topic.

Kafka offset and consumer-group commands run locally on `node0`, so Kafka
telemetry does not depend on a forwarded SSH agent. For managed HoloStream
runs, the harness remains alive until `inputOffset`, `sourceCount`, and
committed consumer lag reach their expected terminal values for multiple
collector samples. It then copies raw AM and executor metrics into
`workdir/remote_tmp_metrics/`, writes `workdir/final_validation.json`, and
stops the collector before returning. Collector CSVs are never truncated on
restart; intentional continuation requires `METRICS_RESUME=true`, matching
run metadata, and matching CSV headers.

Kafka queue-residence metrics and `sourceCount` remain aggregate across source
tasks. Nemo's current source-task metric rows do not contain Kafka topic
identity, so the collector does not present those measurements as per-topic
values. The script intentionally does not use
`workdir/consumer_group_lag_timeseries.csv`, because older wrapper logic wrote
the consumer-group `LOG-END-OFFSET` value there instead of `LAG`.

The node RSS and network plots are collector diagnostics, not authoritative cluster-wide resource accounting. `node_nemo_rss.png` shows only processes matched by the node collector's Nemo pattern, and the script warns if that match appears incomplete. `node_network_throughput.png` assumes `rx_bytes` and `tx_bytes` are cumulative byte counters and ignores negative deltas from counter resets.
