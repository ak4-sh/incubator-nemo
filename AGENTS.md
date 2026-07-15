Current Cluster Context
=======================

Latest Sponge/Q6 State (2026-07-15 UTC)
---------------------------------------

- Current Git status:
  - Branch: `cloudlab-build-fixes-wip`.
  - Local branch is in sync with `origin/cloudlab-build-fixes-wip`.
  - Worktree is clean after the latest commits.
  - Latest commits pushed to origin:

```text
01351c80a Add CloudLab executor placement controls
67ebf6934 Add CloudLab Q6 formal run artifacts
7c843a3c5 Document committed Q6 run results artifact
```

- Formal CloudLab Sponge/Nemo Q6 run status:
  - Run ID: `q6-formal-20260715T174017Z`.
  - Topic: `q6-formal-20260715T174017Z-topic`.
  - YARN application: `application_1784096431599_0002`.
  - AM host: `node5`.
  - Archive committed under:
    `results/cloudlab/q6-formal-20260715T174017Z`.
  - Archive includes README, exact command/env, git diff, executor JSON, JAR checksums, placement CSV/JSON, producer phase logs, Kafka offset snapshots, all 14 node-system metrics, direct NodeManager userlogs, curated formal scaler/source metrics, cleanup verification, and SHA-256 manifest.
  - Outcome: strict placement passed before production; Source ran on `node5-link-1`; Computes ran one each on `node9-link-1`, `node10-link-1`, `node11-link-1`, and `node12-link-1`.
  - Producer completed `47,000,000` live events; topic total including prefill is `47,000,100`.
  - Final Kafka consumer-group lag was `0` on all 8 partitions.
  - Subscriber drained after producer completion (`Curr input: 0`, `Avg process input: 0`).
  - Cleanup verification showed zero active YARN apps, five RUNNING NodeManagers with zero containers, HDFS safe mode OFF, and one DataNode on `node0-link-1`.

- Placement implementation status:
  - Implemented and pushed in commit `01351c80a`.
  - Uses Nemo/REEF executor host placement through `SOURCE_HOSTS`, `COMPUTE_HOSTS`, `STRICT_EXECUTOR_PLACEMENT`, and `EXECUTOR_PLACEMENT_REPORT`; it does not require YARN node labels.
  - Strict harness verification waits for AM-side `executor_placement.csv` and aborts before live producer phases on placement failure.
  - Focused validation passed before commit:

```bash
python3 -m unittest scripts/cloudlab/test_verify_executor_placement.py
mvn -pl runtime/master -Dtest=ExecutorPlacementPolicyTest test
mvn -pl conf,client,runtime/master -am -DskipTests compile
```

- Plotting note:
  - Plot script is `scripts/cloudlab/plot_metrics.py`.
  - It expects `pandas` and `matplotlib`; default `/usr/bin/python3` on node0 did not have `pandas` installed during the formal-run handoff.
  - For the formal archive, the natural work directory is:
    `results/cloudlab/q6-formal-20260715T174017Z/workdir`.
  - Curated formal scaler/source/task metrics are also available under:
    `results/cloudlab/q6-formal-20260715T174017Z/formal_metrics`.

- Current branch context is Sponge/Q6 on the 14-node CloudLab cluster.
- Use **node0** as the control/submission node and YARN ResourceManager host.
- Baseline YARN NodeManagers should run only on **node5, node9, node10, node11, node12**.
- Offload JVM worker nodes are **node4, node6, node7, node8, node13**. Do not start YARN NodeManagers there.
- Live Hadoop worker file must contain only the five baseline nodes:
  `/users/akash01/hadoop/etc/hadoop/slaves`.
- Runtime split for the current branch:
  - ResourceManager: Java 8 (`/usr/lib/jvm/java-8-openjdk-amd64`)
  - NodeManagers / YARN executor containers: Java 11 (`/usr/lib/jvm/java-11-openjdk-amd64`)
- `scripts/cloudlab/cloudlab_env.sh` currently defaults to Java 11 for Nemo runs.
- If `start-yarn.sh` starts NMs but RM is not reachable on `node0:8032`, explicitly start RM on node0 with Java 8:

```bash
ssh -o BatchMode=yes node0 \
  "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64; \
   export PATH=\$JAVA_HOME/bin:\$PATH; \
   /users/akash01/hadoop/sbin/yarn-daemon.sh start resourcemanager"
```

- Healthy YARN state before starting a test:

```text
yarn node -list
Total Nodes: 5
RUNNING: node5, node9, node10, node11, node12

yarn application -list
Running applications: 0
```

- Verify offload nodes are clean before baseline tests:

```bash
for n in node4 node6 node7 node8 node13; do
  echo "===$n==="
  ssh -o BatchMode=yes "$n" "jps -lv | grep NodeManager || true"
done
```

- Current Q6 executor config to use:
  `configs/cloudlab/nemo-yarn-kafka-1source-8slot-4compute-8g-cap3.json`.
- That config means:
  - 1 Source executor, 4096 MB, 8 slots
  - 4 Compute executors, each 8192 MB, `capacity=3`, `slot=3`
  - 12 total compute logical slots
- Formal CloudLab Q6 placement run must use Nemo/REEF executor host placement, not YARN labels:

```bash
QUERY=6 \
EXECUTOR_JSON=/users/akash01/incubator-nemo/configs/cloudlab/nemo-yarn-kafka-1source-8slot-4compute-8g-cap3.json \
BASELINE_NODES="node5 node9 node10 node11 node12" \
EXPECTED_NM_COUNT=5 \
SOURCE_HOSTS=node5-link-1 \
COMPUTE_HOSTS=node9-link-1,node10-link-1,node11-link-1,node12-link-1 \
STRICT_EXECUTOR_PLACEMENT=true \
SCALER_START_MODE=immediate \
AUTOSCALING=true \
BURST_MODE=phases \
PREFILL_EVENTS=100 \
TOTAL_EVENTS=47000100 \
PHASE_RATES=20000,100000,200000 \
PHASE_DURATIONS_SEC=100,150,150 \
PRODUCER_PARALLELISM=8 \
OFFLOAD_NODES=node4,node6,node7,node8,node13 \
WORKERS_PER_NODE=32 \
scripts/cloudlab/run_q6_warmup_steady_burst.sh
```

  - The AM may run on any of the five NodeManager hosts.
  - The harness waits for the AM-side `executor_placement.csv` report and fails before live producer phases if Source is not on node5, any Compute is on node5, two Computes share a node, any of node9-node12 lacks a Compute, or a Source/Compute lands outside the allowed baseline nodes.
- Exact current executor/memory JSON:

```json
[
  { "type": "Transient", "memory_mb": 768,  "capacity": 1, "slot": 1, "num": 0 },
  { "type": "Reserved",  "memory_mb": 768,  "capacity": 1, "slot": 1, "num": 0 },
  { "type": "Source",    "memory_mb": 4096, "capacity": 8, "slot": 8, "num": 1 },
  { "type": "Compute",   "memory_mb": 8192, "capacity": 3, "slot": 3, "num": 4 }
]
```

- Reproducible build tarball for the current known-good deployment:
  `/users/akash01/sponge-q6-build-5a7bd3fb-20260715.tar.gz`.
  - Size: `501M`.
  - Tarball SHA256:

```text
4a2ce7864d19cc056442e6346fc348d390803b4090a06ff6ac80b7f25f3a13ff
```

  - Built from clean commit `5a7bd3fb8020310d0eb7acae022775af75b4411c` on branch `cloudlab-build-fixes-wip`.
  - Rebuild command used:

```bash
JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 \
PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin:$PATH \
mvn clean install -Dmaven.test.skip=true -DskipITs -T 2C
```

  - Build result: Maven `BUILD SUCCESS`, finished `2026-07-15T04:30:11Z`.
  - Bundle contains:
    - `jars/nemo-client-0.2-SNAPSHOT-shaded.jar`
    - `jars/nexmark-0.2-SNAPSHOT-shaded.jar`
    - `jars/offloading-vm-0.2-SNAPSHOT-shaded.jar`
    - `source/repository.bundle`
    - `source/source-5a7bd3fb.tar.gz`
    - Q6 config, CloudLab run scripts, docs, `BUILD_MANIFEST.txt`, `SHA256SUMS`, and verification outputs.
  - Payload validation was performed by extracting to `/tmp/verify-sponge-q6-build-5a7bd3fb` and running `sha256sum -c SHA256SUMS`; all files passed.
  - Key classfile checks report Java 8 bytecode (`major version: 52`).
  - Client shaded jar checks passed: Apache HTTP and Netty are relocated under `org/apache/nemo/shaded/...`; AWS bytecode references relocated Apache HTTP.
  - Jar SHA256 values:

```text
6e721372a82c7f06507ff443e24ac348f8d9ae632e698769fd27c19d98f30bbe  nemo-client-0.2-SNAPSHOT-shaded.jar
b764af214a5d3c2c2dae798a5bc13cb32ccba6e572719d5a1370d6e3a0664b8a  nexmark-0.2-SNAPSHOT-shaded.jar
a6c5644d1a67512fc3accef9f96f7519e808aa52ebf8fcd33b4337296a0047df  offloading-vm-0.2-SNAPSHOT-shaded.jar
```

  - Download from a local machine with:

```bash
scp akash01@node0:/users/akash01/sponge-q6-build-5a7bd3fb-20260715.tar.gz .
```

- Keep `capacity=3,slot=3` for Q6. The older `capacity=1,slot=1` compute config can starve Stage2/Stage3 because streaming tasks are long-lived.
- The relevant scheduler history is commit `3f663b918 Fix Pado streaming scheduler deadlock: upstream stages first`; the fix is present, but enough logical slots are still required.
- Latest Q6 scale-out threshold run:
  - Date/time: 2026-07-15 UTC.
  - Script: `scripts/cloudlab/run_q6_step_rate_sweep.sh`.
  - Overrides: `STEP_START_RATE=80000`, `STEP_RATE=5000`, `STEP_DURATION_SEC=20`, `STEP_NUM_STEPS=45`, `PREFILL_EVENTS=100`, `AUTOSCALING=true`.
  - Topic: `nexmark-auto-190851`.
  - App: `application_1784073595628_0002`.
  - Topology/config: node0 RM/submission, node1-node3 Kafka, node5/node9/node10/node11/node12 baseline YARN workers, node4/node6/node7/node8/node13 offload JVM workers, executor JSON `nemo-yarn-kafka-1source-8slot-4compute-8g-cap3.json`.
  - Memory: Source executor 2048 MB (`-Xmx` about 1948 MB), four Compute executors 8192 MB each (`-Xmx` about 8092 MB). Total Nemo executor containers: 5 (1 Source + 4 Compute); YARN containers including driver: 6.
  - Scale-out occurred at about **135k ev/s**. AM-side decision row from `node5:/tmp/scaling_decisions.csv`:

```text
1784078208373,SCALE_OUT,0.6250,135000.0000,123177.8000,153024.0000,153024,0.1613,165
```

  - Interpretation: avg CPU `0.625`, avg input `135000`, avg process `123177.8`, queue `153024`, scale-out ratio `0.1613`.
  - Confirmed migration future completed in `node5:/tmp/scaler_metrics.csv` with `lastActionWasScaleOut=true` and `prevFutureCompleted=true`.
  - The app was killed immediately after confirming successful scale-out. Post-kill state: `yarn application -list` showed 0 running apps; `yarn node -list` showed 5 RUNNING baseline nodes and 0 containers.
- Latest delayed-scaler Q6 validation run:
  - Date/time: 2026-07-15 UTC.
  - Script: `scripts/cloudlab/run_q6_warmup_steady_burst.sh`.
  - Topic: `nexmark-auto-202953`.
  - App: `application_1784073595628_0005`.
  - AM host: `node12`.
  - Executor config: `configs/cloudlab/nemo-yarn-kafka-1source-8slot-4compute-8g-cap3.json`.
  - Memory/topology: 1 Source executor at 2048 MB, 4 Compute executors at 8192 MB each, `capacity=3`, `slot=3`, 12 compute scheduling slots, 5 Nemo executor containers, 6 YARN containers including AM.
  - Harness timeline:

```text
prefill:       100 events
phase 1:       20k ev/s for 100s
phase 2:      100k ev/s for 150s
scaler start: phase 2 + 60s
phase 3:      200k ev/s for 150s
```

  - Phase markers from `/tmp/nx-auto-nexmark-auto-202953/producer_phases.csv`:

```text
1784082839401,start,1,20000,100,0,673
1784082939414,end,1,20000,100,2001319,100686
1784082939414,start,2,100000,150,2001389,100686
1784083089432,end,2,100000,150,17003405,250704
1784083089432,start,3,200000,150,17003480,250704
```

  - Scaler enable marker from `/tmp/nx-auto-nexmark-auto-202953/scaler_enable.csv`:

```text
1784083000414,after_phase_delay,2,100000,60
```

  - Result: no false scale-out during warmup or the stabilized 100k phase. At 100k after scaler enable, AM-side metrics were stable around CPU `0.52-0.54`, avg process `99k-101k`, queue `30k-40k`, 4 executors, and no decision rows.
  - Scale-out fired shortly after the 200k phase began. AM-side decision row from `node12:/tmp/scaling_decisions.csv`:

```text
1784083091956,SCALE_OUT,0.4980,200000.0000,101195.2000,229582.0000,229582,0.5940,4,QUEUE,0.8000,0.7399,1.2000,0.6000,2.2687,2.0000,4,160
```

  - Interpretation: trigger `QUEUE`; avg CPU `0.498`, avg input `200000`, avg process `101195.2`, queue `229582`, queue delay `2.2687s`, queue threshold `2.0s`, 4 baseline executors, 160 offload workers available.
  - The app was killed after confirming scale-out. Cleanup state: `yarn application -list` showed 0 running apps, no local producer/subscriber/collector processes remained for `nexmark-auto-202953`, and offload VMWorker pools on node4/node6/node7/node8/node13 were stopped. The run files were preserved under `/tmp/nx-auto-nexmark-auto-202953/` and `/tmp/nx-auto-sub-nexmark-auto-202953.log`.
  - Methodology note: the delayed scaler start is harness-only. It does not change Sponge's default scaler logic; it avoids letting prefill/readiness and low-rate warmup samples arm or bias the scaler before the measured 100k phase.
- Latest successful Q6 full warmup/steady/burst run with 4 GB Source:
  - Date/time: 2026-07-15 UTC.
  - Script: `scripts/cloudlab/run_q6_warmup_steady_burst.sh`.
  - Topic: `nexmark-auto-213939`.
  - App: `application_1784073595628_0007`.
  - AM host: `node11`.
  - Executor config: `configs/cloudlab/nemo-yarn-kafka-1source-8slot-4compute-8g-cap3.json`.
  - Memory/topology:
    - 1 Source executor at 4096 MB; launch log confirmed `-XX:MaxHeapSize=3996m`.
    - 4 Compute executors at 8192 MB each.
    - Compute `capacity=3`, `slot=3`, for 12 resident compute scheduling slots.
    - 5 Nemo executor containers, 6 YARN containers including the AM.
  - Harness timeline:

```text
prefill:       100 events
phase 1:       20k ev/s for 100s
phase 2:      100k ev/s for 150s
scaler start: phase 2 + 60s
phase 3:      200k ev/s for 150s
```

  - Producer completed the full live workload:

```text
KAFKA_PRODUCER_DONE topic=nexmark-auto-213939 totalSent=47000000 elapsedMs=400726 avgRate=117287.12
```

  - Phase markers from `/tmp/nx-auto-nexmark-auto-213939/producer_phases.csv`:

```text
1784087026149,start,1,20000,100,0,659
1784087126170,end,1,20000,100,2012308,100680
1784087126170,start,2,100000,150,2012683,100680
1784087276187,end,2,100000,150,17014701,250697
1784087276187,start,3,200000,150,17014836,250697
1784087426208,end,3,200000,150,47000000,400718
```

  - Scaler enable marker from `/tmp/nx-auto-nexmark-auto-213939/scaler_enable.csv`:

```text
1784087187173,after_phase_delay,2,100000,60
```

  - Result: no false scale-out during the 100k phase after scaler enable. Metrics stayed stable around CPU `0.53-0.55`, avg process `99k-101k`, and 4 baseline executors.
  - Scale-out fired correctly during the 200k phase via queue delay:

```text
1784087280308,SCALE_OUT,0.5565,200000.0000,108331.8000,243277.0000,243277,0.5583,4,QUEUE,0.8000,0.8392,1.2000,0.6000,2.2457,2.0000,4,160
```

  - Interpretation: trigger `QUEUE`; avg CPU `0.5565`; avg input `200000`; avg process `108331.8`; queue `243277`; queue delay `2.2457s`; threshold `2.0s`; ratio `0.5583`; 160 offload workers available.
  - Post-scale behavior was healthy: processing recovered to roughly `198k-228k ev/s` and the queue drained instead of collapsing to zero processing.
  - No `OutOfMemoryError` or `FailedRuntime` was found in the subscriber log for this run. The previous 2048 MB Source run failed after scale-out with Source heap OOM; this 4096 MB Source run avoided that failure.
  - App was killed after producer completion and post-scale observation.
  - Permanent committed result artifact:
    `results/cloudlab/nexmark-q6-warmup-steady-burst-20260715-213939/`.
  - Results commit:
    `87ebf42ec Add Q6 warmup steady burst successful run results`.
  - The committed result directory contains:
    - `combined_metrics.csv`
    - `producer_metrics.csv`
    - `producer_phases.csv`
    - `scaler_enable.csv`
    - `scaling_decisions.csv`
    - `scaler_metrics.csv`
    - `source_aggregate_metrics.csv`
    - `source_task_metrics.csv`
    - `task_metrics.csv`
    - raw AM-side metrics under `am-node11/`
    - raw offload-node task metrics under `offload-task-metrics/`
    - `logs/subscriber.log`
    - compressed YARN log `logs/yarn-application_1784073595628_0007.log.gz`
    - `README.md` and `SHA256SUMS`
  - Root-level metrics in that result directory are filtered to the run window/job where appropriate. Raw offload-node files may contain stale warm-pool rows from previous jobs; use root `task_metrics.csv` for filtered analysis.
  - Plot PNGs are not included because the local Python environment did not have `pandas` when the artifact was archived.
  - Original scratch artifacts were:
    - `/tmp/app_1784073595628_0007.log`
    - `/tmp/nx-auto-sub-nexmark-auto-213939.log`
    - `/tmp/nx-auto-nexmark-auto-213939/`
- Known working Q6 smoke command:

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

- Streaming jobs do not naturally exit when the producer reaches `TOTAL_EVENTS`; kill the YARN app after confirming metrics.
- More detailed runbook: `scripts/cloudlab/README.md`, section `Sponge/Q6 CloudLab Runbook`.

Topology
--------

- **node0**: control node, YARN ResourceManager, HDFS NameNode, build/scripts/log collection, job submission point.
- **node1, node2, node3**: Kafka brokers. Bootstrap servers: `node1:9092,node2:9092,node3:9092`.
- **node5, node9, node10, node11, node12**: baseline Sponge/Nemo workers, regular JVM executors, YARN NodeManagers should run only here.
- **node4, node6, node7, node8, node13**: JVM function pool / offloading workers for Lambda-like/serverless execution path. Do not run YARN NodeManagers here for baseline runs.

Filesystem Note
---------------
- `/users/akash01/hadoop` and `/users/akash01/incubator-nemo` are on **local filesystem** (`/dev/sda3`) on each node — NOT NFS.
- Config files edited on node0 are invisible to worker nodes until explicitly copied via `scp`.
- JARs built on node0 must be distributed (e.g., via HDFS or YARN distributed cache; the client uploads them automatically).

Current YARN State
------------------
- 5 NodeManager registrations on baseline workers: node5, node9, node10, node11, node12.
- NMs now register with **internal hostnames** (`node5-link-1` → `10.10.1.6`) instead of external FQDNs (`c220g2-011125.wisc.cloudlab.us` → `128.105.145.128`).
- Current 2026-07-15 restart: RM active on node0, five baseline NMs RUNNING, no active YARN applications, offload nodes have no NodeManager JVMs.
- NMs restarted with Java 11; RM restarted separately with Java 8 after `start-yarn.sh` did not leave RM reachable on `8032`.
- Older notes below may mention prior application IDs or all-Java-11 YARN configuration; prefer the `Latest Sponge/Q6 State` section above for current operations.
- Latest clean restart after `application_1781926211327_0002` (sponge-q0-test): app killed, local subscriber/producer/collector stopped, warm VMWorker pools killed, orphan REEFLauncher processes cleaned up, RM and all five baseline NMs restarted; YARN shows all five baseline NMs `RUNNING` with `0` containers and no active applications; HDFS safemode OFF, 10 live datanodes.
- `/users/akash01/hadoop/etc/hadoop/slaves` on all nodes contains only: `node5`, `node9`, `node10`, `node11`, `node12`.

Configuration Changes Applied
-----------------------------

### Hadoop/YARN Config (distributed to all baseline nodes via scp)

- `/users/akash01/hadoop/etc/hadoop/hadoop-env.sh`: Java 11 for NodeManagers/executor containers, explicit Hadoop component homes. Start ResourceManager with Java 8 manually when needed:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/users/akash01/hadoop
export HADOOP_COMMON_HOME=/users/akash01/hadoop
export HADOOP_HDFS_HOME=/users/akash01/hadoop
export HADOOP_YARN_HOME=/users/akash01/hadoop
export HADOOP_MAPRED_HOME=/users/akash01/hadoop
```

- `/users/akash01/hadoop/etc/hadoop/yarn-env.sh`: sets `NM_HOST` env var and `YARN_NODEMANAGER_OPTS`:
```bash
export NM_HOST=$(hostname -s)
export YARN_NODEMANAGER_OPTS="-Dyarn.nodemanager.hostname=${NM_HOST} -DNM_HOST=${NM_HOST} $YARN_NODEMANAGER_OPTS"
```

- `/users/akash01/hadoop/etc/hadoop/yarn-site.xml`:
  - Explicit `yarn.application.classpath` with absolute paths (fixes missing YARN classes in AM).
  - `yarn.nodemanager.hostname` → `${NM_HOST}` (resolved from env var at startup; avoids circular reference that occurred with `${yarn.nodemanager.hostname}`).
  - Removed `yarn.nodemanager.admin-env` override (caused invalid `JAVA_HOME` expansion).
  - RM hostname set to `node0`.
  - `yarn.nodemanager.vmem-check-enabled` → `false`: Q6 executors exceed the default 2.1× vmem ratio (using >2.9 GB vmem against a 1 GB physical limit), triggering NM container kills that cascade back to kill the NM itself (see NodeManager Kill Cascade below).
  - `yarn.nodemanager.pmem-check-enabled` → `false`: Q6 executors also exceed physical memory limits (950 MB–1.2 GB against 1 GB container limit), triggering the same cascade. Distributed to all 5 baseline nodes via scp.

- `/users/akash01/hadoop/etc/hadoop/slaves`: 5 baseline nodes only.

### Nemo Config

- `bin/run_nexmark.sh`: includes Beam vendor gRPC jar in client classpath.
- `/users/akash01/deps/beam-vendor-grpc-1_21_0-0.1.jar` → symlink to Maven local copy.
- Executor JSON configs in `configs/cloudlab/`:
  - `nemo-yarn-1source-5compute-small.json`: 1 Source + 5 Compute, `capacity: 1`.
  - `nemo-yarn-kafka-1source-8compute.json`: 1 Source + 8 Compute with Source `slot: 1` (insufficient for Q8 with 4 Kafka source tasks).
  - `nemo-yarn-kafka-1source-cap4-8compute.json`: 1 Source + 8 Compute with Source `slot: 4` (required for Q8 autoscaler runs).
  - `nemo-yarn-kafka-1source-8slot-12compute.json`: 1 Source (8 slots) + 12 Compute (2048 MB each, up from 1024 MB); required for Q6 with JVM offloading.
  - `nemo-yarn-kafka-1source-5compute.json`, `nemo-yarn-kafka-1source-4compute.json`.

Issues Found And Resolved
-------------------------

### Java Mismatch
- YARN containers used Java 8 while Nemo jars compiled for Java 11. Current operational fix is a split runtime: NodeManagers/executor containers use Java 11, while the ResourceManager is started with Java 8 for Hadoop 2.7 compatibility.

### Missing Beam gRPC Class
- `org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ProtocolMessageEnum` missing from classpath. Fixed by adding Beam vendor gRPC jar to launcher classpath.

### Missing Hadoop YARN Class in AM
- `AMRMClientAsync$CallbackHandler` not found. Fixed by setting explicit `yarn.application.classpath` in `yarn-site.xml`.

### Stale/Stuck Processes
- Stale NodeManagers on offload nodes, duplicate NM registrations. Fixed by terminating NMs everywhere, restarting RM, and starting NMs only on baseline workers.
- Stale `JobLauncher`/`REEFLauncher`/`run_q8_subscriber_yarn.sh` processes can survive killed/timed-out runs and cause confusing YARN state. Clean them up before restarting RM/NMs.
- Latest cleanup also removed stale `/tmp/source.log` from node0 before the next benchmark run.

### "No address VM-1" Error (New)
- **Root cause:** `CloudLabVMLambdaResourceRequester` creates executor IDs with `VM-` prefix (e.g., `VM-1`), but codebase checked only `executorId.contains("Lambda")`. When `DefaultByteTransportImpl.connectTo("VM-1")` is called, it invokes `nameResolver.lookup("VM-1")`, which fails because `LambdaByteTransport` never registers VM executor addresses with `NemoNameServer`.
- **Fix:** Added `RuntimeIdManager.isLambdaExecutorId(String)` helper that checks for both `"Lambda"` and `"VM-"` prefixes. Updated all 6 files:
  - `ExecutorChannelManagerMap.java` (2 checks)
  - `DefaultByteTransportImpl.java` (1 check)
  - `LambdaByteTransport.java` (1 check)
  - `DefaultExecutorRepresenterImpl.java` (3 checks)
  - `ContainerManager.java` (1 check)
  - `RuntimeMaster.java` (3 checks)

### Address Plane External IP (New)
- `NemoNameServer` and `LambdaContainerManager` use `localAddressProvider.getLocalAddress()` which returns external IP (e.g., `128.105.145.65`). Created `ShortHostnameLocalAddressProvider` that returns short hostname (e.g., `node0`) by stripping domain from `InetAddress.getLocalHost().getHostName()`.
- Bound in `JobLauncher.java` via `Configurations.merge()`.

### Hadoop YARN External Hostname (New)
- NodeManagers registered with external FQDN (`c220g2-011125.wisc.cloudlab.us`), causing RM to connect via external IP (`128.105.145.128`). Fixed by:
  - Adding `yarn.nodemanager.hostname` property to `yarn-site.xml` with value `${NM_HOST}`.
  - Setting `NM_HOST=$(hostname -s)` in `yarn-env.sh`.
  - Distributing both files to all 5 baseline nodes via `scp`.
  - After restart, NMs register as `node5-link-1:...` (internal hostname → `10.10.1.6`).

### Hadoop Config Distribution (New)
- `/users/akash01/hadoop` is local filesystem — config changes on node0 invisible to workers. All config file edits (`yarn-env.sh`, `yarn-site.xml`, `slaves`) must be explicitly `scp`'d to each baseline node.

### Q8 Migration Schedulability (New)
- **Root cause:** `ContainerTypeAwareSchedulingConstraint.testSchedulability()` required strict equality between task placement and executor container type. Scale-out via `sendMigrationAllStages()` tags stopped tasks with `ResourcePriorityProperty.LAMBDA`, but VM executors have `containerType = "VM"`, so `"VM".equals("Lambda")` was false. The scheduler rejected all 179 VM executors and left migrated Q8 tasks in the `couldNotSchedule` retry loop forever.
- **Fix:** `ContainerTypeAwareSchedulingConstraint.java` now allows VM executors to schedule Lambda-tagged tasks: `task placement == LAMBDA && executor container type == VM`.
- **Result:** Q8 with `configs/cloudlab/nemo-yarn-kafka-1source-cap4-8compute.json` successfully migrated Stage1 and Stage2 tasks to VM executors. AM logs showed `Stage1-*` and `Stage2-*` scheduled to `VM-76`, `VM-77`, `VM-78`, `VM-79`, `VM-110`, `VM-111`, `VM-112`, and `VM-116`.
- **Important:** Q8 also requires Source `slot: 4`; the older `nemo-yarn-kafka-1source-8compute.json` has Source `slot: 1` and leaves `Stage0-1/2/3` unscheduled.

Current Nexmark Status
----------------------

### Q0 Direct Runs
- `application_1781740448420_0004` (10 events): processed all 10, killed manually (streaming).
- `application_1781742418407_0001` (50k events): processed all 50000, killed manually.
- Final app status: `State=KILLED`, `Final-State=KILLED`, `Progress=100%`.

### Kafka Q8 Streaming
- Current autoscaler Q8 runs use 12 tasks: 4 Source tasks + 4 Stage1 (transform) + 4 Stage2 (GBK final). The Source executor must expose 4 slots.
- Consumer: `auto.offset.reset=earliest`, `enable.auto.commit=false`, `group.id=` (empty).
- `run_q8_subscriber_yarn.sh` uses `StreamingPolicy` + `StreamingScheduler`.

#### Pado Streaming Scheduler Deadlock
- `StreamingScheduler` dispatches Stage2 tasks before Stage1 tasks. With fewer slots than total tasks, Stage2 fills all slots and Stage1 never dispatches.
- 3-compute (4 slots): 4/9 tasks dispatched, 5 stuck, zero input.
- 5-compute (6 slots): 6/9 tasks dispatched, 3 stuck, zero input.
- **8-compute with Source `slot: 4` (12 total task slots): current minimum viable Q8 autoscaler config** — all 4 Source, 4 Stage1, and 4 Stage2 tasks can fit.

#### Q8 Finite Window Output
- Stage2 consistently shows `output=0` even though Stage1 outputs are shuffled to Stage2. Root cause: unbounded Kafka source watermark does not advance, so streaming windows never close/flush.

### Kafka Q0 with Offloading
- Full bursty run (100 prefill + 100000 live = 100100 total) succeeded with 200 VMWorkers.
- Source and KAFKA results topic offsets both confirmed at 100100.

### Phase 1 Metrics Instrumentation
- **Problem:** Source-task queue time was not tracked; per-executor aggregate source metrics and per-task metrics shared the same CSV file (`source_metrics.csv`) with incompatible schemas; scaler only wrote decision rows without periodic state; task metrics lacked `jobId` and `executorId` columns.
- **Solution (commit `32c280b1b`):**
  - `UnboundedSourceReadable.java`: tracks per-source-task Kafka queue time (avg/max/samples every 5s), writes `source_task_metrics.csv` with 10-column schema (`timestamp,jobId,taskId,idleTimeNs,kafkaQueueTimeNs,kafkaQueueTimeAvgNs,kafkaQueueTimeMaxNs,kafkaQueueSamples,inputRate,recordsRead`).
  - `SourceEventAggregator.java`: writes `source_aggregate_metrics.csv` with 5-column schema (`timestamp,jobId,totalSourceCount,executorId,executorSourceCount`), separate from task-level metrics to avoid schema conflicts.
  - `InputAndCpuBasedScaler.java`: writes periodic `scaler_metrics.csv` every 1s with 10 columns (`timestamp,jobId,avgCpu,avgInput,avgProcess,queue,numExecutors,numLambdaExecutors,lastActionWasScaleOut,prevFutureCompleted`) with NaN/Infinity guard.
  - `TaskEventRateCalculator.java`: 12-column schema with `jobId`, `executorId`; `mkdirs` guard.
  - `JobConf.java`: default `JobId` to `"unknown"` instead of null.
  - `OffloadingExecutor.java`: throw `RuntimeException` on `InjectionException` (fail fast instead of silent `WORKER_INIT_DONE`).
  - `RuntimeMaster.java`: remove `taskDispatcher.setWaiting(true/false)` and `resourceRequestCounter` increment during lambda container provisioning (fixes scale-out stall where container request paused `TaskDispatcher` and blocked all task scheduling).
- **Script changes (same commit):**
  - `run_autoscaler_smoke.sh`: propagate `NEMO_JOB_ID`, clean stale baseline worker metrics (`/tmp/source_task_metrics.csv`, `/tmp/task_metrics.csv`), pass `result_topic` and source hosts to collector.
  - `run_q8_subscriber_yarn.sh`: pass `-Dnemo.job.id=$JOB_ID`.
  - `run_sponge_q0_benchmark.sh`: filter `scaler_metrics.csv` by run window/jobId, exclude `NaN`/`Infinity`; filter `task_metrics.csv` by `NF==12`; generate README with lag/queue metrics; collect `source_aggregate_metrics.csv` from AM host; collect `source_task_metrics.csv` from each baseline node.
  - `metrics_collector.py`: track `resultOffset`/`resultLag`; read source queue-time from baseline worker nodes; use `scaler_metrics.csv`; poll source queue-time p50/p95/p99.
  - `plot_metrics.py`: plot `kafka_result_lag`, `source_kafka_queue_time`; handle 12-column `task_metrics`; backward-compatible with older CSVs.
- **Build:** `mvn -pl runtime/master,client,offloading/workers/vm -am -DskipTests package` passed after all changes. Shaded client jar rebuilt with latest timestamp 2026-06-19 23:57 -0600.

### Full Sponge Q0 Benchmark (Latest)
- New wrapper `scripts/cloudlab/run_sponge_q0_benchmark.sh` created with full Sponge defaults: `TOTAL_EVENTS=23850000`, `RATE_PERIOD_SEC=450`, `FIRST_RATE=50000`, `NEXT_RATE=200000`.
- Run `sponge-q0-20260619-164645`, app `application_1781901266080_0002`, AM host `node9`.
  - Producer sent `23,849,900` live records plus 100 prefill = `23,850,000` total at ~58.3K ev/s average.
  - Kafka offsets confirmed `23,850,000` across 8 partitions.
  - Source metrics confirmed `23,850,000` consumed.
  - Result topic `nexmark-sponge-q0-20260619-164645-results` confirmed `23,850,000`.
  - Scale-out triggered once: `1781906080547,SCALE_OUT,0.0965,46426.0000,21618.4000,27808.0000,27808,0.6658,179`.
  - VM task migration succeeded: 132 VM task metric rows in `task_metrics.csv`.
  - Scale-in triggered once after backlog drained: `1781906116551,SCALE_IN,0.0200,0.0000,0.0000,-100.0000,-100,1.0000,179`.
  - No phantom scale-in loop observed (exactly 1 scale-in row).
  - Artifacts saved to `results/cloudlab/sponge-q0-20260619-164645/`.

### Custom Burst Mode (Latest)
- **Problem:** Legacy `BURSTY` mode produced only 1-second micro-bursts at 200K ev/s, which was barely distinguishable from the 50K ev/s steady rate due to the producer's ~66K ev/s max. The scaler only triggered at ~380s of a 408s run.
- **Solution:** Implemented `CUSTOM_BURST` mode in `StandaloneNexmarkKafkaProducer.java` with sustained multi-second bursts.
- **Pattern:** Ramp-up (60s at 50K) + N cycles of (steady 60s at 50K + burst 45s at 200K).
- **Implementation:** Time-based per-second batch loops with `RateShape.SQUARE` and `isRateLimited=false`. Events generated per second = `ratePerGen`, then sleep remainder of second. Respects `maxEvents` cap for prefill/controlled runs.
- **Default config:** `STEADY_DURATION_SEC=60`, `BURST_DURATION_SEC=45`, `NUM_BURSTS=3`, `RAMP_UP_SEC=60`, `FIRST_RATE=50000`, `NEXT_RATE=200000`.
- **Run `sponge-q0-20260619-174241`, app `application_1781901266080_0007`, AM host `node11`:**
  - Producer correctly generated full burst pattern: 60s ramp-up + 3 cycles of (60s steady + 45s burst) = 375s total, 39M events per phase (78M total).
  - Scale-out triggered early: `1781909586802,SCALE_OUT,0.3355,200000.0000,142812.8000,228542.0000,228542,0.3033,179`.
  - Queue built up to 228K during first burst (not 27K at 380s like legacy mode).
  - **Issue:** Prefill generated 39M instead of 100 because custom burst ignored `events` parameter.
  - **Fix:** Added optional `maxEvents` parameter (11th arg). When `maxEvents > 0`, caps total events. `run_autoscaler_smoke.sh` passes `events` as `maxEvents`.
  - **Recompiled:** `StandaloneNexmarkKafkaProducer.class` in `build/cloudlab-producer/` (was stale from old compilation).
- **Run `sponge-q0-20260619-180648`, app `application_1781901266080_0008`, AM host initially `node11`:**
  - Producer fix worked: prefill generated exactly 100 records, live producer generated `23,849,900`, input topic reached `23,850,000`.
  - Scale-out fired once: `1781910629888,SCALE_OUT,0.1438,200000.0000,51583.0000,127442.0000,127442,0.8528,179`.
  - Run failed after scale-out: source/result plateaued around `8,215,025` / `7,640,472`, REEF logged `FailedRuntime`, app fell back to `ACCEPTED` with `AM Host: N/A`, and YARN showed only one RUNNING NM.
  - `/tmp/source_metrics.csv` on AM alternated between current `8215025` and stale `78000000`, confirming stale fallback metric contamination; Kafka result offset was the authoritative incomplete result.
  - Aggregated YARN logs also showed `HDFSUtils` warning `java.net.UnknownHostException: hdfs-master` during setup; this was not confirmed as the root cause because the job processed ~8.2M records before the runtime failure.
  - Cleanup performed: killed `application_1781901266080_0008`, killed stale submit-side `JobLauncher`, restarted RM and all five baseline NMs; YARN returned to 5 RUNNING NMs with 0 containers and no active apps.
- `run_sponge_q0_benchmark.sh` now fails fast during monitoring if the YARN app leaves `RUNNING`, AM host becomes `N/A`, RUNNING NM count drops below `EXPECTED_NM_COUNT`, or source/result progress stalls after producer completion. It saves artifacts and runs cleanup on monitor failure.
- **Run `sponge-q0-20260619-185649`, app `application_1781911776702_0001`, AM host `node11`:**
  - Producer generated exactly `23,849,900` live records plus 100 prefill = `23,850,000` total at ~93.3K ev/s average.
  - Kafka input offsets reached `23,850,000`; Kafka result topic reached `23,850,000`, so Q0 end-to-end completed successfully.
  - Scale-out fired once: `1781913628070,SCALE_OUT,0.1408,200000.0000,49939.2000,179554.0000,179554,0.8527,179`.
  - Scale-in fired once after drain: `1781913768080,SCALE_IN,0.0130,0.0000,0.0000,-100.0000,-100,1.0000,179`.
  - VM task metrics: 663 rows across `VM-78`, `VM-79`, `VM-111`, and `VM-112`.
  - Raw AM source metrics still contained stale fallback rows (`8215025`/`78000000`), but sanitized combined metrics and Kafka result offsets confirmed completion.
  - Plots generated with `scripts/cloudlab/plot_metrics.py`; plot script now handles 11-column `task_metrics.csv` and filters stale source counts where `sourceCount > topicOffset`.
  - Artifacts saved to `results/cloudlab/sponge-q0-20260619-185649/`.
  - After cleanup, YARN NMs dropped to 0; all five baseline NMs were restarted and verified RUNNING with 0 containers.

### Scale-In Loop Fix
- **Root cause:** `scaleInIfIdle()` used `executorRegistry.getLambdaExecutors().size() > 0` as its only guard. After scale-in moved all tasks back, lambda executors remained registered forever, so every 1s idle tick re-triggered scale-in. `writeScalingDecision("SCALE_IN")` was called before checking whether lambda executors actually had eligible tasks, so no-op attempts were recorded as real scale-in rows.
- **Fix (3 changes in `InputAndCpuBasedScaler.java`):**
  1. `lastActionWasScaleOut` flag: set `true` after scale-out completes; `scaleInIfIdle()` returns immediately unless a scale-out happened first. Cleared after any scale-in outcome (empty lambda, no stages, or successful migration).
  2. `hasLambdaTasksToScaleIn()` helper: checks at least one lambda executor has non-CR running tasks. Replaces bare `getLambdaExecutors().size() > 0` check.
  3. `writeScalingDecision("SCALE_IN")` moved inside the future, after confirming lambda executors and stages are non-empty.
- **Result:** Before fix: 62+ SCALE_IN rows (one per second). After fix: exactly 1 SCALE_IN row.

### Scale-In Idle Input Fix
- **Root cause:** In sustained Q0 run `nexmark-auto-115819`, scale-out succeeded but scale-in did not trigger because `avgInputRate` stayed at the final positive delta (`13450`) after producer input stopped. `InputAndCpuBasedScaler` uses `DescriptiveStatistics(1)`, so if no later zero-delta input update reaches `addCurrentInput()`, the last positive input average remains forever and blocks `avgInput <= 0`.
- **Fix in `InputAndCpuBasedScaler.java`:**
  1. Added `lastInputUpdateTime` and a 30s `INPUT_IDLE_TIMEOUT_MS` stale-input guard.
  2. `scaleInIfIdle()` now treats input as idle when `avgInput <= 0 || isInputStale()` while still requiring `avgProcess <= 0` and lambda tasks to scale in.
  3. `addCurrentInput()` now records zero/non-positive updates as `avgInputRate=0` instead of returning silently.
  4. Negative cumulative deltas are guarded: they log a warning, reset current input rate to zero, and do not subtract from `aggInput`.
- Rebuilt `client/target/nemo-client-0.2-SNAPSHOT-shaded.jar` after this fix; latest timestamp observed: 2026-06-19 12:12:22 -0500.

### Scale-In Backlog Guard (Latest)
- **Root cause:** In Q8 run `nexmark-auto-134204`, scale-in fired while `queue = aggInput - currSourceEvent` was still `315884`. Input and source-processing rates were both zero because the producer finished and source metrics plateaued, but the backlog was not drained.
- **Fix in `InputAndCpuBasedScaler.java`:** `scaleInIfIdle()` now computes `queue = aggInput.get() - currSourceEvent` and requires `queue <= 0` before calling `scaleIn()`.
- **Behavior:** If input/process rates are idle but `queue > 0`, scale-in is skipped and logs `Input and processing are idle, but queue remains ...; skipping scale-in`.
- **Verification:** `mvn -pl runtime/master -am -DskipTests compile` passed after this change. The shaded client jar has not yet been rebuilt after the queue-guard fix.

### NodeManager Kill Cascade (Q6 NM Failures Root Cause)
- **Root cause:** Hadoop 2.7's `DefaultContainerExecutor` launches YARN containers as bash scripts **without** `setsid`. Containers therefore inherit the NodeManager's Linux process group (PGID). When the NM kills a container (for vmem/pmem violation, or CONTAINER_STOP from RM after `yarn application -kill`), bash propagates SIGTERM to its entire process group — which includes the NM itself. With 7–13 containers running per NM, 7–13 SIGTERMs arrive in rapid succession and the NM shuts down gracefully, taking all 5 baseline NMs down within milliseconds.
- **Two cascade triggers:**
  1. **Memory limit violations** — NM exceeds vmem (>2.1× ratio) or pmem (physical) limit → NM kills container → SIGTERM cascade kills NM.
  2. **Explicit app kill** — `yarn application -kill` → RM sends CONTAINER_STOP to each NM → NM kills containers → SIGTERM cascade kills NM.
- **Fixes applied:**
  1. `yarn.nodemanager.vmem-check-enabled=false` and `yarn.nodemanager.pmem-check-enabled=false` in `yarn-site.xml` (distributed to all baseline nodes) — eliminates memory-violation triggers.
  2. Compute executor memory 2048 MB in `nemo-yarn-kafka-1source-8slot-12compute.json` — prevents actual JVM OOM independent of YARN limits.
  3. `cleanup()` in `run_sponge_q0_benchmark.sh` restarts NMs after every YARN app kill (both triggers kill NMs). NM liveness verified via `pgrep -f proc_nodemanager` over SSH, **not** `yarn node -list` (ghost NMs show RUNNING for ~10 min after actual NM death).
  4. `wait || true` added to SSH NM-restart loops in `cleanup()` — without it, `set -e` exits the harness script if any SSH call returns non-zero (e.g., `yarn-daemon.sh start` on a node where the NM is already running or the SSH connection blinks).
- **Permanent root fix (not yet applied):** Wrap container launch command with `setsid` in `DefaultContainerExecutor` (Hadoop source change) to put each container in its own process group, preventing SIGTERM propagation to the NM.

### Metrics Pipeline (Latest)
- `StandaloneNexmarkKafkaProducer.java`: writes `producer_metrics.csv` every 5s with `timestamp,outputRate,totalSent,elapsedMs`.
- `StandaloneNexmarkKafkaProducer.java`: source-log writes are cumulative and append-mode; the final write uses the final total.
- `UnboundedSourceReadable.java`: tracks `idleTimeNs` (waiting for advance/pollRecord), samples `queueTimeNs` every 1000 msgs (time since message timestamp), reports input rate.
- `TaskEventRateCalculator.java`: writes `task_metrics.csv` with per-task inputRate, outputRate, processingTime, deserTime, inbytes, etc.
- `SourceEventAggregator.java`: writes `source_aggregate_metrics.csv` with per-executor source counts every 1s (separate from task-level metrics to avoid schema conflicts).
- `InputAndCpuBasedScaler.java`: writes `scaling_decisions.csv` (on decision) with timestamp, action, avgCpu, avgInput, avgProcess, queue, ratio, numExecutors; writes `scaler_metrics.csv` (periodic every 1s) with avgCpu, avgInput, avgProcess, queue, numExecutors, numLambdaExecutors, flags.
- `metrics_collector.py` (new): background collector running on submit node (node0). Polls every 5s: Kafka latest offset via SSH to node1, source counts/CPU/scaling data via SSH to AM host. Writes `combined_metrics.csv`.
- `plot_metrics.py` (new): post-run plotting script that reads CSV files.
- All CSV writers fall back to `/tmp` when `nemo.work.dir` system property is unset (since YARN containers don't inherit client env vars).
- `metrics_collector.py` accepts optional 4th arg `am_host` for SSH-based reads; `run_autoscaler_smoke.sh` captures AM host from `yarn application -status`.

Active Scaler Implementation
---------------------------

`InputAndCpuBasedScaler.java`:
- 80-second initial delay before first decision.
- Skips first 10 observations and first 30 seconds of source handling.
- Checks every 1 second after initial delay.
- Scale-out triggers when: `queue / processingRate > scalerTriggerQueueDelay` AND `ratio > 0.1`.
- Scale-out path: `scalingWithRatio()` → `sendMigrationAllStages(ratio, vmComputeExecutors, LAMBDA)`.
- Scale-in triggers when: `(avgInput <= 0 || isInputStale()) && avgProcess <= 0 && queue <= 0 && hasLambdaTasksToScaleIn() && avgCpu < scalerScaleoutTriggerCPU`.
- `queue` is `aggInput - currSourceEvent`; non-empty backlog blocks scale-in even when input and processing rates are idle.
- Scale-in path: `scaleInIfIdle()` → `scaleIn()` → `sendMigration(ratios, lambdaExecutors, slist, COMPUTE)`.
- `queueSizeBasedScalingRatio` guards `avgInput <= 0`, clamps ratio to `[0.0, 0.95]`, and now handles `processingRate <= 0 && queue > 0` by triggering conservative queue-based scale-out instead of returning empty.
- `cpuBasedScalingRatio` clamps ratio.
- `hasLambdaTasksToScaleIn()`: checks lambda executors have non-CR running tasks (prevents phantom scale-in).
- `lastActionWasScaleOut` guard: `scaleInIfIdle()` returns immediately unless a scale-out happened first (prevents repeat scale-in loop).
- `writeScalingDecision("SCALE_IN")` moved inside future after empty checks (no phantom CSV rows).
- All metrics CSV writers (source_metrics, task_metrics, scaling_decisions) fall back to `/tmp` when `nemo.work.dir` is unset.

`InputAndQueueSizeBasedBackpressure.java`:
- `sendBackpressureWrapper()` is active and sends the in-memory `backpressureRate` to source executors after the 5s scaling-hint guard.
- `backpressureRate` starts from `policyConf.bpMinEvent` (`5000`) because the constructor overwrites the field initializer.
- `addCurrentInput` ignores non-positive heartbeat values and derives deltas from cumulative source-log totals.
- `queueBasedBackpressure()` exists but the active scheduling loop uses `cpuBasedBackpressure()`.

`ScaleInOutManager.java`:
- `sendMigrationAllStages` passes actual ratio instead of `1.0`.
- `prevSelectedTasksToMoveLambda` cleared on scale-in (`resourceType == COMPUTE`).

`MasterUtils.java`:
- `getMaxMigrationCntPerStage` uses `Math.max(1, ceil(count * ratio))` for positive ratios.

`JobScaler.java` (commented loop):
- Divide-by-zero guards: `workers.isEmpty()`, `tasksPerWorker == 0`, `ratio <= 0`, `input_rate == 0`, `numWorkers == 0`, `divide == 0`, `executorCpuUseMap.isEmpty()`.

Relevant Files
--------------

### Source (Nemo)
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/scaler/InputAndCpuBasedScaler.java`: active scaler
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/ScaleInOutManager.java`: migration manager
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/MasterUtils.java`: task count math
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/JobScaler.java`: old commented loop
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/TaskScheduledMapMaster.java`: task scheduling/migration state
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/scheduler/ContainerTypeAwareSchedulingConstraint.java`: VM-compatible schedulability; allows `VM` executors for `Lambda`-tagged migrated tasks
- `common/src/main/java/org/apache/nemo/common/RuntimeIdManager.java`: `isLambdaExecutorId()` helper
- `runtime/executor-common/src/main/java/org/apache/nemo/runtime/executor/common/ExecutorChannelManagerMap.java`: VM- prefix fix
- `runtime/executor-common/src/main/java/org/apache/nemo/runtime/executor/common/datatransfer/DefaultByteTransportImpl.java`: VM- prefix fix
- `runtime/lambda-executor/src/main/java/org/apache/nemo/runtime/lambdaexecutor/datatransfer/LambdaByteTransport.java`: VM- prefix fix
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/DefaultExecutorRepresenterImpl.java`: VM- prefix fix
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/resource/ContainerManager.java`: VM- prefix fix
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/RuntimeMaster.java`: VM- prefix fix
- `runtime/driver/src/main/java/org/apache/nemo/driver/ShortHostnameLocalAddressProvider.java`: returns short hostname
- `client/src/main/java/org/apache/nemo/client/JobLauncher.java`: binds `ShortHostnameLocalAddressProvider`, scaling service, source.log reader
- `common/src/main/java/org/apache/nemo/common/NetworkUtils.java`: internal `10.10.*` address selection for Netty server binding
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/SourceEventAggregator.java`: writes `source_aggregate_metrics.csv`
- `runtime/master/src/main/java/org/apache/nemo/runtime/master/backpressure/InputAndQueueSizeBasedBackpressure.java`: backpressure with aggInput tracking
- `runtime/executor-common/src/main/java/org/apache/nemo/runtime/executor/common/monitoring/TaskEventRateCalculator.java`: writes `task_metrics.csv`
- `compiler/frontend/beam/src/main/java/org/apache/nemo/compiler/frontend/beam/source/UnboundedSourceReadable.java`: tracks idle time, queue time (sampled 1000 msgs), input rate

### Scripts
- `scripts/cloudlab/run_autoscaler_smoke.sh`: autoscaler smoke test harness; exports `NEMO_WORK_DIR`, captures AM host for metrics, no longer syncs `source.log` to AM host
- `scripts/cloudlab/run_q6_warmup_then_main.sh`: Q6 orchestration wrapper — runs warmup (2.5M events, unrate-limited) then main (23.85M events, rate-limited bursty) phases back-to-back with NM restart between phases
- `scripts/cloudlab/run_q8_subscriber_yarn.sh`: subscriber launcher; passes `-Dnemo.work.dir`
- `scripts/cloudlab/run_live_bursty_combined.sh`: live bursty producer pattern
- `scripts/cloudlab/start_warm_pool.sh`: VMWorker pool starter (defaults to Java 11)
- `scripts/cloudlab/generate_vm_addresses.py`: VM address generator (handles comma-separated nodes)
- `scripts/cloudlab/force_cloudlab_vm_offload_smoke.sh`: manual scaling.txt smoke test
- `scripts/cloudlab/cloudlab_env.sh`: env defaults
- `scripts/cloudlab/metrics_collector.py` (new): background collector, polls Kafka offsets via SSH to node1, reads driver CSVs via SSH to AM host, writes `combined_metrics.csv`
- `scripts/cloudlab/plot_metrics.py` (new): post-run plotting from CSV data
- `scripts/cloudlab/StandaloneNexmarkKafkaProducer.java`: producer with `producer_metrics.csv` output every 5s

### Config
- `/users/akash01/hadoop/etc/hadoop/yarn-env.sh` (distributed to all baseline nodes)
- `/users/akash01/hadoop/etc/hadoop/yarn-site.xml` (distributed)
- `/users/akash01/hadoop/etc/hadoop/hadoop-env.sh` (distributed)
- `/users/akash01/hadoop/etc/hadoop/slaves` (distributed)
- `configs/cloudlab/nemo-yarn-kafka-1source-8compute.json`
- `configs/cloudlab/nemo-yarn-kafka-1source-cap4-8compute.json`: Q8 autoscaler benchmark config with Source `slot: 4`
- `configs/cloudlab/nemo-yarn-kafka-1source-8slot-12compute.json`: Q6 benchmark config (Compute 2048 MB)
- `configs/cloudlab/nemo-yarn-kafka-1source-4compute.json`
- `configs/cloudlab/nemo-yarn-1source-5compute-small.json`

### Built JARs
- `client/target/nemo-client-0.2-SNAPSHOT-shaded.jar` (rebuilt after Phase 1 metrics + runtime fixes commit `32c280b1b`; latest timestamp observed: 2026-06-19 23:57 -0600)
- `examples/nexmark/target/nexmark-0.2-SNAPSHOT-shaded.jar`
- `offloading/workers/vm/target/offloading-vm-0.2-SNAPSHOT-shaded.jar`

Key Decisions
-------------
- Use only 5 baseline workers as YARN NodeManagers.
- Do not run YARN NodeManagers on offload nodes (node4, node6, node7, node8, node13) or Kafka nodes (node1, node2, node3).
- Use Java 11 for Hadoop/YARN and Nemo throughout.
- Q0 with KAFKA sink for complete end-to-end verification via result topic offsets.
- 8-compute plus one Source executor with `slot: 4` is the current minimum viable Q8 autoscaler config for 4 Source + 4 Stage1 + 4 Stage2 tasks.
- Active scaler (`InputAndCpuBasedScaler`) preferred over old commented `JobScaler` loop.
- Config files must be manually distributed (`scp`) to all nodes; no shared filesystem.
- Default executor JSON for autoscaler smoke tests: `nemo-yarn-kafka-1source-8compute.json`.
- Q8 autoscaler benchmarks should use `configs/cloudlab/nemo-yarn-kafka-1source-cap4-8compute.json`; the older `1source-8compute` config has insufficient Source slots for Q8.
- `yarn.nodemanager.hostname` resolved from environment variable `${NM_HOST}` (not system property) to avoid circular reference in Hadoop Configuration.
- VM executor IDs use `VM-` prefix; treat same as `Lambda` prefix everywhere via `isLambdaExecutorId()`.
- VM executors must be schedulable for `Lambda`-tagged migrated tasks via `ContainerTypeAwareSchedulingConstraint`.
- Scale-in requires backlog drained (`aggInput - currSourceEvent <= 0`) before moving tasks back from VM executors.
- Address plane uses short hostnames via `ShortHostnameLocalAddressProvider`.

Next Steps
----------
1. **Q6 scale-in investigation:** autoscale8 showed scale-out fires correctly but scale-in never triggered (backlog drained before idle conditions held). Investigate whether scale-in should be more aggressive or whether the run duration is simply too short for the idle-input timeout to expire after drain.
2. **Permanent NM cascade fix:** Wrap container launch in `DefaultContainerExecutor` with `setsid` to put each container in its own process group, preventing SIGTERM propagation to the NM on any container kill.
3. Rerun Q8 sustained benchmark with the cap4 config to verify E2E latency also appears for multi-stage query.
4. Investigate why Q8 source metrics plateaued at `7,684,016` of `8,000,000` in `nexmark-auto-134204` after VM migration.
5. For the Pado scheduler deadlock, investigate `StreamingScheduler`/`TaskDispatcher` to prefer upstream tasks over downstream tasks.
6. If future scale-out does not trigger, debug `avgInputRate`, `avgSrcProcessingRate`, `ExecutorMetricMap` delivery, and `sendMigrationAllStages` call path.

Critical Context Reminders
-------------------------
- `run_q8_subscriber_yarn.sh` is now non-blocking via `nohup ... > "$LOG_FILE" 2>&1 &`; do not reintroduce `| tee` for the long-running Java command because it keeps the harness attached to subscriber output.
- `NEMO_WORK_DIR` env var must be set so `JobLauncher` finds `scaling.txt` in the correct directory.
- The autoscaler harness must not reintroduce a background `source.log` scp/sync loop. `JobLauncher` reads `source.log` locally on node0 and forwards input updates to the AM via RPC.
- Scaling commands in `scaling.txt` must be in `nemo.work.dir` (from system property `nemo.work.dir`, env var `NEMO_WORK_DIR`, or `user.dir`).
- VM- prefix executor IDs will trigger "No address VM-1" error without the `isLambdaExecutorId()` fix.
- Q8 scale-out migration requires `ContainerTypeAwareSchedulingConstraint` to allow `VM` executors for `Lambda`-tagged tasks; otherwise migrated Stage1/Stage2 tasks will loop in `couldNotSchedule`.
- Q8 requires Source `slot: 4`; use `configs/cloudlab/nemo-yarn-kafka-1source-cap4-8compute.json`. The older `nemo-yarn-kafka-1source-8compute.json` has Source `slot: 1` and leaves `Stage0-1/2/3` unscheduled.
- `yarn.nodemanager.hostname` in `yarn-site.xml` must NOT reference itself (`${yarn.nodemanager.hostname}` creates circular ref and resolves to empty). Use `${NM_HOST}` referencing an env var.
- Hadoop config files are per-node (local filesystem). Always `scp` after editing.
- Old `run_q8_subscriber_yarn.sh` processes can linger and interfere with new runs.
- If `yarn node -list` shows duplicate or stale NMs, verify actual processes with `ps` on each node. `yarn-daemon.sh stop` can miss processes if pidfiles are stale; use careful process-level cleanup and then restart one RM plus one NM per baseline node.
- `InputAndCpuBasedScaler` scale-in loop is fixed: `lastActionWasScaleOut` guards re-entry, `hasLambdaTasksToScaleIn()` prevents phantom scale-in.
- `InputAndCpuBasedScaler` scale-in now requires `queue <= 0`; a non-empty backlog blocks scale-in even when input and processing rates are idle.
- All Java metrics CSV writers (`SourceEventAggregator`, `TaskEventRateCalculator`, `InputAndCpuBasedScaler`) fall back to `/tmp` when `nemo.work.dir` is null; YARN containers do not inherit client env vars.
- `metrics_collector.py` needs `am_host` as 4th argument to read driver-side CSVs via SSH; `run_autoscaler_smoke.sh` now captures `AM_HOST` from `yarn application -status` after the app reaches RUNNING.
- VM executor task metrics are written to offload nodes' local `/tmp/task_metrics.csv`, not the AM host. Check `node4,node6,node7,node8,node13` for current-run `VM-*` rows.
- Before the next benchmark, the workspace is clean for YARN runtime state: latest app was killed, local subscriber/producer/collector are stopped, warm VMWorker pools were killed, and YARN showed exactly five RUNNING baseline NMs with zero containers. The harness now removes AM-side fallback metric files automatically after it discovers the AM host.
- Phase 1 metrics instrumentation complete. CSV schema contract: `source_task_metrics.csv` is 10-column (source-task level), `source_aggregate_metrics.csv` is 5-column (AM-side per-executor aggregates), `scaler_metrics.csv` is 10-column (periodic scaler state at 1s), `task_metrics.csv` is 12-column (per-task rates), `scaling_decisions.csv` is 8-column (decision events only).
- Final Phase 1 full Q0 run `sponge-q0-test-20260619-230438` succeeded: 23.85M events in/out, 1 SCALE_OUT, 1 SCALE_IN, 589 VM rows, max queue-time p95=6141ms, no NaN/Infinity in any CSV.
- **NM kill cascade:** `DefaultContainerExecutor` (Hadoop 2.7) places containers in the NM's process group. Any container kill (memory violation OR `yarn application -kill`) propagates SIGTERM to the NM. **Always disable vmem and pmem checks** (`yarn-site.xml`) and restart NMs after every YARN app termination. Use `pgrep -f proc_nodemanager` for liveness, not `yarn node -list` (ghost entries persist ~10 min).
- **cleanup() robustness:** SSH NM restart loops in `cleanup()` use `wait || true` and `ssh ... || true` so that `set -e` does not abort the harness if any SSH call returns non-zero. Without this, the harness silently dies between warmup and main phases.
- **Q6 executor memory:** Use `nemo-yarn-kafka-1source-8slot-12compute.json` (Compute 2048 MB). Q6's stateful operators (WinningBids CoGroupByKey, GBK windowing) use 950 MB–1.2 GB physical memory; 1024 MB containers trigger pmem cascade.
- Full Sponge Q0 benchmark completed successfully: `sponge-q0-20260619-164645`, app `application_1781901266080_0002`, AM host `node9`. Input/source/result all reached `23,850,000`. Scale-out and scale-in fired exactly once each with no phantom loop. VM task metrics confirmed 132 rows. Artifacts saved to `results/cloudlab/sponge-q0-20260619-164645/`.

### Kafka Q6 with JVM Offloading (autoscale7, Latest)
- **Run:** `nexmark-q6-warmup-20260626-233401` (warmup) + `nexmark-q6-main-20260626-233401` (main).
- **Config:** `nemo-yarn-kafka-1source-8slot-12compute.json` — 1 Source (8 slots) + 12 Compute (2048 MB each), offloading to 160 VMWorkers across node4/node6/node7/node8/node13.
- **Warmup:** 2,500,000 events — `input=2500000 source=2500000 result=76568`. Success.
- **Main:** 23,850,000 events — `input=23850000 source=23850000 result=732019`. **Success — first complete Q6 end-to-end run.**
- **Fixes that enabled success (vs. repeated NM kill cascades in autoscale2–6):**
  1. `yarn.nodemanager.vmem-check-enabled=false` — stopped vmem-violation cascade.
  2. `yarn.nodemanager.pmem-check-enabled=false` + Compute 2048 MB — stopped pmem-violation cascade.
  3. `wait || true` in `cleanup()` — stopped harness dying at warmup→main NM restart step.
- **Artifacts:** `results/cloudlab/nexmark-q6-warmup-20260626-233401/` and `results/cloudlab/nexmark-q6-main-20260626-233401/` (plots: input_rate, kafka_source_lag, kafka_result_lag, source_kafka_queue_time, latency, cpu, task_rates).

### Kafka Q6 No-Warmup Baseline (autoscale9, Latest)
- **Run:** nexmark-q6-main-20260627-154638 (no warmup phase).
- **Purpose:** Holostream comparison baseline -- mirrors no-warmup approach used in Holostream configs (IsWarmUp: false, LoadWarmUpData: false).
- **Config:** nemo-yarn-kafka-1source-8slot-12compute.json, 160 VMWorkers, AUTOSCALING=true.
- **Main:** 23,850,000 events at custom burst (50k/200k ev/s) -- input=23850000 source=23850000 result=75040. Success.
- **Autoscaler:** SCALE_OUT fired once at t=173s (queue=62432, ratio=0.21, avgInput=50k ev/s). Same single scale-out as autoscale8.
- **Artifacts:** results/cloudlab/nexmark-q6-main-20260627-154638/

### Kafka Q6 with JVM Offloading + Autoscaling (autoscale8)
- **Run:** `nexmark-q6-warmup-20260627-091117` (warmup) + `nexmark-q6-main-20260627-091117` (main).
- **Config:** same as autoscale7 (`nemo-yarn-kafka-1source-8slot-12compute.json`, 160 VMWorkers) but with `AUTOSCALING=true`.
- **Warmup:** 2,500,000 events — `input=2500000 source=2500000 result=76364`. Success.
- **Main:** 23,850,000 events at 93,332 ev/s avg — `input=23850000 source=23850000 result=75875`. **Success.**
- **Autoscaler behavior:**
  - `SCALE_OUT` fired once: queue=78,678, avgInput=50,000 ev/s, ratio=0.33 (during first burst phase).
  - Tasks migrated to VMWorkers on node7 (1,386 metric rows) and node8 (1,005 rows); node4/6/13 saw minimal activity.
  - `SCALE_IN` did not fire — backlog drained to 0 and run completed before the idle-input + empty-queue + avgProcess≤0 conditions held long enough to trigger.
- **Artifacts:** `results/cloudlab/nexmark-q6-{warmup,main}-20260627-091117/` (plots, CSVs, scaling decisions).

### E2E Latency Fix (Commit `903ca46e5`)
- **Root cause of empty latency plot:** Two latency mechanisms — `OperatorVertexOutputCollector.java:158-201` was inside `/* ... */` comment (dead code); `OperatorMetricCollector.processDone()` was only called from `SinkEmtter` (zero-output vertices), not from `ExternalMainEmitter` (Kafka sink path).
- **Fix:** Added `processDone()` calls to `ExternalMainEmitter.emitData()` and `InternalExternalMainEmitter.emitData()`.
- **Preflight fix:** `run_sponge_q0_benchmark.sh` Kafka SSH check wrapped with `timeout 10` to prevent hanging on stale SSH agent.
- **Autoscaler smoke test (50K events):** Confirmed `COLLECT_LATENCY` lines appear every ~1s; steady-state median 5-108ms.
- **Full benchmark `sponge-q0-20260620-134020`:** 23.85M events in/out via KAFKA sink, 234 COLLECT_LATENCY rows, median 5-186ms, tail max 1,160ms, 1 SCALE_OUT + 1 SCALE_IN, all 8 plots (including latency.png at 57KB) generated. Artifacts at `results/cloudlab/sponge-q0-20260620-134020/`.
