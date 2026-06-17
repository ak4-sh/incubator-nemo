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
EXECUTOR_JSON=configs/cloudlab/nemo-yarn-kafka-1source-8compute.json \
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
- Use Java 8 consistently on CloudLab.
